/**
 * @fileoverview M12 TASK-42: W3C `traceparent` OpenTelemetry Span Propagator & Prometheus `/metrics` Text Exporter.
 */
import * as crypto from "node:crypto";

export interface OtelSpanRecord {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  startTimeMs: number;
  endTimeMs: number;
  /** OTLP nanosecond start/end timestamps, set by the W3C trace-continuity propagator. */
  startTimeUnixNano?: string;
  endTimeUnixNano?: string;
  attributes: Record<string, string | number>;
}

export class OtelAndPrometheusExporter {
  private readonly counters: Map<string, number> = new Map();
  private readonly gauges: Map<string, number> = new Map();
  /** Every span exported by this isolate, in emission order. */
  public readonly exportedSpans: OtelSpanRecord[] = [];

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
    this.exportedSpans.push(span);
  }

  public incCounter(name: string, delta = 1): void {
    this.counters.set(name, (this.counters.get(name) ?? 0) + delta);
  }

  public setGauge(name: string, val: number): void {
    this.gauges.set(name, val);
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
    return lines.join("\n") + "\n";
  }

  public getRecordedSpans(): OtelSpanRecord[] {
    return [...this.exportedSpans];
  }
}
