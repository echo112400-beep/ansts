"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.OtelAndPrometheusExporter = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M12 TASK-42 & M50-4: W3C `traceparent` OpenTelemetry Span Waterfall Propagator,
 * Histogram Quantile (`p50`/`p95`/`p99`) Calculator, Multi-Window SLO Burn-Rate Evaluator & Prometheus `/metrics` Exporter.
 */



























class OtelAndPrometheusExporter {
  counters = new Map();
  gauges = new Map();
  histograms = new Map();
  spans = [];

  static createTraceparent(traceId, spanId) {
    const tid = traceId ?? crypto.randomBytes(16).toString("hex");
    const sid = spanId ?? crypto.randomBytes(8).toString("hex");
    return `00-${tid}-${sid}-01`;
  }

  static parseTraceparent(header) {
    const parts = header.trim().split("-");
    if (parts.length !== 4 || parts[1].length !== 32 || parts[2].length !== 16) {
      throw new Error(`Invalid W3C traceparent header: '${header}'`);
    }
    return {
      version: parts[0],
      traceId: parts[1],
      parentSpanId: parts[2],
      flags: parts[3]
    };
  }

  recordSpan(span) {
    this.spans.push(span);
    if (this.spans.length > 200) {
      this.spans.shift();
    }
  }

  recordTraceWaterfall(
  operationName,
  stages)
  {
    const traceId = crypto.randomBytes(16).toString("hex");
    const rootSpanId = crypto.randomBytes(8).toString("hex");
    const baseStart = Date.now();
    let cursor = baseStart;
    const created = [];

    for (const st of stages) {
      const spanId = crypto.randomBytes(8).toString("hex");
      const rec = {
        traceId,
        spanId,
        parentSpanId: rootSpanId,
        name: st.name,
        startTimeMs: cursor,
        endTimeMs: cursor + st.durationMs,
        attributes: st.attributes ?? {}
      };
      cursor += st.durationMs;
      this.recordSpan(rec);
      created.push(rec);
    }

    const rootRec = {
      traceId,
      spanId: rootSpanId,
      name: operationName,
      startTimeMs: baseStart,
      endTimeMs: cursor,
      attributes: { childSpans: stages.length, totalDurationMs: cursor - baseStart }
    };
    this.recordSpan(rootRec);

    return {
      traceparent: OtelAndPrometheusExporter.createTraceparent(traceId, rootSpanId),
      traceId,
      rootSpanId,
      spans: [rootRec, ...created]
    };
  }

  incCounter(name, delta = 1) {
    this.counters.set(name, (this.counters.get(name) ?? 0) + delta);
  }

  setGauge(name, val) {
    this.gauges.set(name, val);
  }

  observeHistogram(name, val) {
    let arr = this.histograms.get(name);
    if (!arr) {
      arr = [];
      this.histograms.set(name, arr);
    }
    arr.push(val);
    if (arr.length > 500) {
      arr.shift();
    }
  }

  computeQuantiles(name) {
    const raw = this.histograms.get(name) ?? [];
    if (raw.length === 0) {
      return { count: 0, sum: 0, p50: 0, p95: 0, p99: 0 };
    }
    const sorted = [...raw].sort((a, b) => a - b);
    const sum = Number(sorted.reduce((a, b) => a + b, 0).toFixed(2));
    const pick = (q) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
    return {
      count: sorted.length,
      sum,
      p50: Number(pick(0.5).toFixed(2)),
      p95: Number(pick(0.95).toFixed(2)),
      p99: Number(pick(0.99).toFixed(2))
    };
  }

  evaluateSloBurnRate(params)





  {
    const targetSloRatio = params.targetSloRatio ?? 0.999;
    const latencySloTargetMs = params.latencySloTargetMs ?? 250;
    const total = Math.max(1, params.totalRequests);
    const errRatio = params.errorRequests / total;
    const actualAvailabilityRatio = Number((1 - errRatio).toFixed(5));
    const allowedErrorRatio = Math.max(0.0001, 1 - targetSloRatio);
    const burnRate1h = Number((errRatio / allowedErrorRatio).toFixed(2));
    const errorBudgetRemainingPct = Number(
    Math.max(0, Math.min(100, (1 - errRatio / allowedErrorRatio) * 100)).toFixed(2));


    let alertSeverity = "HEALTHY";
    let recommendedRunbook = "NONE";
    if (burnRate1h >= 14.4 || params.p99LatencyMs > latencySloTargetMs * 1.5) {
      alertSeverity = "PAGE_P0";
      recommendedRunbook = "RB-SLO-BURN-AND-DLQ-HEAL";
    } else if (burnRate1h >= 3.0 || params.p99LatencyMs > latencySloTargetMs) {
      alertSeverity = "TICKET_P1";
      recommendedRunbook = "RB-CANARY-AND-BREAKER-RESET";
    }

    return {
      targetSloRatio,
      actualAvailabilityRatio,
      errorBudgetRemainingPct,
      burnRate1h,
      p99LatencyMs: params.p99LatencyMs,
      latencySloTargetMs,
      alertSeverity,
      recommendedRunbook
    };
  }

  renderPrometheusMetrics() {
    const lines = [];
    for (const [k, v] of this.counters.entries()) {
      lines.push(`# TYPE ${k} counter`);
      lines.push(`${k} ${v}`);
    }
    for (const [k, v] of this.gauges.entries()) {
      lines.push(`# TYPE ${k} gauge`);
      lines.push(`${k} ${v}`);
    }
    for (const [k] of this.histograms.entries()) {
      const q = this.computeQuantiles(k);
      lines.push(`# TYPE ${k} summary`);
      lines.push(`${k}{quantile="0.5"} ${q.p50}`);
      lines.push(`${k}{quantile="0.95"} ${q.p95}`);
      lines.push(`${k}{quantile="0.99"} ${q.p99}`);
      lines.push(`${k}_sum ${q.sum}`);
      lines.push(`${k}_count ${q.count}`);
    }
    return lines.join("\n") + "\n";
  }

  getRecordedSpans() {
    return [...this.spans];
  }
}exports.OtelAndPrometheusExporter = OtelAndPrometheusExporter;