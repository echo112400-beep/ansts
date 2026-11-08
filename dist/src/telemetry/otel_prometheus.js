"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.OtelAndPrometheusExporter = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M12 TASK-42: W3C `traceparent` OpenTelemetry Span Propagator & Prometheus `/metrics` Text Exporter.
 */












class OtelAndPrometheusExporter {
  counters = new Map();
  gauges = new Map();
  /** Every span exported by this isolate, in emission order. */
  exportedSpans = [];

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
    this.exportedSpans.push(span);
  }

  incCounter(name, delta = 1) {
    this.counters.set(name, (this.counters.get(name) ?? 0) + delta);
  }

  setGauge(name, val) {
    this.gauges.set(name, val);
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
    return lines.join("\n") + "\n";
  }

  getRecordedSpans() {
    return [...this.exportedSpans];
  }
}exports.OtelAndPrometheusExporter = OtelAndPrometheusExporter;