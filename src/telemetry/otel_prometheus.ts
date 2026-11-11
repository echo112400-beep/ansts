/**
 * @fileoverview M12 TASK-42, M50-4 & M62-2: W3C `traceparent` OpenTelemetry Span Waterfall Propagator,
 * Histogram Quantile (`p50`/`p95`/`p99`) Calculator, Multi-Window SLO Burn-Rate Evaluator,
 * Metric Label Sanitization (Preventing Cardinality Explosion) & Prometheus `/metrics` Exporter.
 */
import * as crypto from "node:crypto";

export interface OtelSpanRecord {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  startTimeMs: number;
  endTimeMs: number;
  attributes: Record<string, string | number>;
}

export interface HistogramSummary {
  count: number;
  sum: number;
  p50: number;
  p95: number;
  p99: number;
}

export interface SloBurnRateEvaluation {
  targetSloRatio: number;
  actualAvailabilityRatio: number;
  errorBudgetRemainingPct: number;
  burnRate1h: number;
  p99LatencyMs: number;
  latencySloTargetMs: number;
  alertSeverity: "HEALTHY" | "TICKET_P1" | "PAGE_P0";
  recommendedRunbook: string;
}

export class OtelAndPrometheusExporter {
  private readonly counters: Map<string, number> = new Map();
  private readonly gauges: Map<string, number> = new Map();
  private readonly histograms: Map<string, number[]> = new Map();
  private readonly spans: OtelSpanRecord[] = [];
  public static readonly MAX_METRIC_ENTRIES = 500;

  public static createTraceparent(traceId?: string, spanId?: string): string {
    const tid = traceId ?? crypto.randomBytes(16).toString("hex");
    const sid = spanId ?? crypto.randomBytes(8).toString("hex");
    return `00-${tid}-${sid}-01`;
  }

  public static parseTraceparent(header: string): { version: string; traceId: string; parentSpanId: string; flags: string } {
    const parts = header.trim().split("-");
    if (parts.length !== 4 || parts[1].length !== 32 || parts[2].length !== 16) {
      throw new Error(`Invalid W3C traceparent header: '${header}'`);
    }
    return {
      version: parts[0],
      traceId: parts[1],
      parentSpanId: parts[2],
      flags: parts[3],
    };
  }

  public recordSpan(span: OtelSpanRecord): void {
    this.spans.push(span);
    if (this.spans.length > 200) {
      this.spans.shift();
    }
  }

  public recordTraceWaterfall(
    operationName: string,
    stages: Array<{ name: string; durationMs: number; attributes?: Record<string, string | number> }>
  ): { traceparent: string; traceId: string; rootSpanId: string; spans: OtelSpanRecord[] } {
    const traceId = crypto.randomBytes(16).toString("hex");
    const rootSpanId = crypto.randomBytes(8).toString("hex");
    const baseStart = Date.now();
    let cursor = baseStart;
    const created: OtelSpanRecord[] = [];

    for (const st of stages) {
      const spanId = crypto.randomBytes(8).toString("hex");
      const rec: OtelSpanRecord = {
        traceId,
        spanId,
        parentSpanId: rootSpanId,
        name: st.name,
        startTimeMs: cursor,
        endTimeMs: cursor + st.durationMs,
        attributes: st.attributes ?? {},
      };
      cursor += st.durationMs;
      this.recordSpan(rec);
      created.push(rec);
    }

    const rootRec: OtelSpanRecord = {
      traceId,
      spanId: rootSpanId,
      name: operationName,
      startTimeMs: baseStart,
      endTimeMs: cursor,
      attributes: { childSpans: stages.length, totalDurationMs: cursor - baseStart },
    };
    this.recordSpan(rootRec);

    return {
      traceparent: OtelAndPrometheusExporter.createTraceparent(traceId, rootSpanId),
      traceId,
      rootSpanId,
      spans: [rootRec, ...created],
    };
  }

  /**
   * [M62-2] DFX-Diagnosability & Bounded Memory: Sanitizes metric names and enforces an upper bound
   * on distinct metrics to prevent unbounded cardinality memory exhaustion in long-running nodes.
   */
  private sanitizeMetricName(name: string): string {
    return name.trim().replace(/[^a-zA-Z0-9_:]/g, "_");
  }

  public incCounter(name: string, delta = 1): void {
    const safeName = this.sanitizeMetricName(name);
    if (!this.counters.has(safeName) && this.counters.size >= OtelAndPrometheusExporter.MAX_METRIC_ENTRIES) {
      return;
    }
    const safeDelta = Number.isFinite(delta) && delta > 0 ? delta : 1;
    this.counters.set(safeName, (this.counters.get(safeName) ?? 0) + safeDelta);
  }

  public setGauge(name: string, val: number): void {
    const safeName = this.sanitizeMetricName(name);
    if (!this.gauges.has(safeName) && this.gauges.size >= OtelAndPrometheusExporter.MAX_METRIC_ENTRIES) {
      return;
    }
    const safeVal = Number.isFinite(val) ? val : 0;
    this.gauges.set(safeName, safeVal);
  }

  public observeHistogram(name: string, val: number): void {
    const safeName = this.sanitizeMetricName(name);
    if (!this.histograms.has(safeName) && this.histograms.size >= OtelAndPrometheusExporter.MAX_METRIC_ENTRIES) {
      return;
    }
    const safeVal = Number.isFinite(val) && val >= 0 ? val : 0;
    let arr = this.histograms.get(safeName);
    if (!arr) {
      arr = [];
      this.histograms.set(safeName, arr);
    }
    arr.push(safeVal);
    if (arr.length > 500) {
      arr.shift();
    }
  }

  public computeQuantiles(name: string): HistogramSummary {
    const safeName = this.sanitizeMetricName(name);
    const raw = this.histograms.get(safeName) ?? [];
    if (raw.length === 0) {
      return { count: 0, sum: 0, p50: 0, p95: 0, p99: 0 };
    }
    const sorted = [...raw].sort((a, b) => a - b);
    const sum = Number(sorted.reduce((a, b) => a + b, 0).toFixed(2));
    const pick = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
    return {
      count: sorted.length,
      sum,
      p50: Number(pick(0.5).toFixed(2)),
      p95: Number(pick(0.95).toFixed(2)),
      p99: Number(pick(0.99).toFixed(2)),
    };
  }

  public evaluateSloBurnRate(params: {
    targetSloRatio?: number;
    totalRequests: number;
    errorRequests: number;
    p99LatencyMs: number;
    latencySloTargetMs?: number;
  }): SloBurnRateEvaluation {
    const targetSloRatio = params.targetSloRatio ?? 0.999;
    const latencySloTargetMs = params.latencySloTargetMs ?? 250;
    const total = Math.max(1, params.totalRequests);
    const errRatio = Math.max(0, Math.min(1, params.errorRequests / total));
    const actualAvailabilityRatio = Number((1 - errRatio).toFixed(5));
    const allowedErrorRatio = Math.max(0.0001, 1 - targetSloRatio);
    const burnRate1h = Number((errRatio / allowedErrorRatio).toFixed(2));
    const errorBudgetRemainingPct = Number(
      Math.max(0, Math.min(100, (1 - errRatio / allowedErrorRatio) * 100)).toFixed(2)
    );

    let alertSeverity: "HEALTHY" | "TICKET_P1" | "PAGE_P0" = "HEALTHY";
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
      recommendedRunbook,
    };
  }

  public renderPrometheusMetrics(): string {
    const lines: string[] = [];
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

  public getRecordedSpans(): OtelSpanRecord[] {
    return [...this.spans];
  }
}
