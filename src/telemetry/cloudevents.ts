/**
 * @fileoverview M6 TASK-22: CloudEvents v1.0 Telemetry Bus & FinOps Cost Attribution Ledger.
 */
import * as crypto from "node:crypto";

/**
 * CloudEvents v1.0 envelope enriched with the W3C `traceparent` of the span that emitted it.
 */
export interface TracedCloudEventV1 extends CloudEventV1 {
  traceparent: string;
}

export interface CloudEventV1 {
  specversion: "1.0";
  id: string;
  source: string;
  type: string;
  subject: string;
  time: string;
  datacontenttype: "application/json";
  data: Record<string, unknown>;
}

export interface TenantFinOpsSummary {
  tenantId: string;
  totalLlmCostUsd: number;
  totalSandboxCpuCoreSeconds: number;
  totalSandboxCostUsd: number;
  grandTotalUsd: number;
}

export class CloudEventsBus {
  private readonly events: CloudEventV1[] = [];
  private readonly tenantCosts: Map<string, TenantFinOpsSummary> = new Map();

  public emitEvent(params: {
    type: string;
    subject: string;
    tenantId: string;
    llmCostUsd?: number;
    sandboxCpuCoreSeconds?: number;
    data?: Record<string, unknown>;
  }): CloudEventV1 {
    const evt: CloudEventV1 = {
      specversion: "1.0",
      id: `ce-${Date.now()}-${this.events.length + 1}`,
      source: "urn:ansts:control-plane:v2",
      type: params.type,
      subject: params.subject,
      time: new Date().toISOString(),
      datacontenttype: "application/json",
      data: { tenantId: params.tenantId, ...(params.data ?? {}) },
    };
    this.events.push(evt);

    const existing = this.tenantCosts.get(params.tenantId) ?? {
      tenantId: params.tenantId,
      totalLlmCostUsd: 0,
      totalSandboxCpuCoreSeconds: 0,
      totalSandboxCostUsd: 0,
      grandTotalUsd: 0,
    };
    const llmDelta = params.llmCostUsd ?? 0;
    const cpuSecDelta = params.sandboxCpuCoreSeconds ?? 0;
    const sandboxDelta = cpuSecDelta * 0.00004;
    existing.totalLlmCostUsd = Number((existing.totalLlmCostUsd + llmDelta).toFixed(6));
    existing.totalSandboxCpuCoreSeconds = Number(
      (existing.totalSandboxCpuCoreSeconds + cpuSecDelta).toFixed(3)
    );
    existing.totalSandboxCostUsd = Number((existing.totalSandboxCostUsd + sandboxDelta).toFixed(6));
    existing.grandTotalUsd = Number(
      (existing.totalLlmCostUsd + existing.totalSandboxCostUsd).toFixed(6)
    );
    this.tenantCosts.set(params.tenantId, existing);
    return evt;
  }

  /**
   * Emits a CloudEvent stamped with a fresh (or caller-supplied) W3C `traceparent` so the event can be
   * correlated with the OTLP span that produced it.
   */
  public emit(
    type: string,
    taskId: string,
    tenantId: string,
    data: Record<string, unknown> = {},
    options?: { traceparent?: string },
  ): TracedCloudEventV1 {
    const evt = this.emitEvent({ type, subject: taskId, tenantId, data });
    const traceparent =
      options?.traceparent ??
      `00-${crypto.randomBytes(16).toString("hex")}-${crypto.randomBytes(8).toString("hex")}-01`;
    // `emitEvent` returns the stored envelope; widening it in place keeps `allEvents()` consistent.
    (evt as TracedCloudEventV1).traceparent = traceparent;
    return evt as TracedCloudEventV1;
  }

  public getTenantReport(tenantId: string): TenantFinOpsSummary | undefined {
    return this.tenantCosts.get(tenantId);
  }
  public allEvents(): CloudEventV1[] {
    return [...this.events];
  }
}
