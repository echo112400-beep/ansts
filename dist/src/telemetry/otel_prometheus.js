"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.OtelAndPrometheusExporter = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M12 TASK-42: W3C `traceparent` OpenTelemetry Span Propagator & Prometheus `/metrics` Text Exporter.
 */









class OtelAndPrometheusExporter {
  counters = new Map();
  gauges = new Map();
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
    return [...this.spans];
  }
}exports.OtelAndPrometheusExporter = OtelAndPrometheusExporter;