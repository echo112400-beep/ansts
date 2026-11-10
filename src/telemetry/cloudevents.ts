/**
 * @fileoverview M6 TASK-22 & M58-5: CloudEvents v1.0 Telemetry Bus & FinOps Cost Attribution Ledger
 * with Bounded Event Ring Buffer & Finite Non-Negative Cost Sanitization.
 */
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
  public readonly maxRetainedEvents: number;
  private eventSeq = 0;

  constructor(maxRetainedEvents = 1000) {
    this.maxRetainedEvents = Math.max(10, maxRetainedEvents);
  }

  public emitEvent(params: {
    type: string;
    subject: string;
    tenantId: string;
    llmCostUsd?: number;
    sandboxCpuCoreSeconds?: number;
    data?: Record<string, unknown>;
  }): CloudEventV1 {
    this.eventSeq += 1;
    const evt: CloudEventV1 = {
      specversion: "1.0",
      id: `ce-${Date.now()}-${this.eventSeq}`,
      source: "urn:ansts:control-plane:v2",
      type: params.type,
      subject: params.subject,
      time: new Date().toISOString(),
      datacontenttype: "application/json",
      data: { tenantId: params.tenantId, ...(params.data ?? {}) },
    };
    this.events.push(evt);
    while (this.events.length > this.maxRetainedEvents) {
      this.events.shift();
    }

    const existing = this.tenantCosts.get(params.tenantId) ?? {
      tenantId: params.tenantId,
      totalLlmCostUsd: 0,
      totalSandboxCpuCoreSeconds: 0,
      totalSandboxCostUsd: 0,
      grandTotalUsd: 0,
    };
    const rawLlm = params.llmCostUsd ?? 0;
    const rawCpuSec = params.sandboxCpuCoreSeconds ?? 0;
    const llmDelta = Number.isFinite(rawLlm) && rawLlm > 0 ? rawLlm : 0;
    const cpuSecDelta = Number.isFinite(rawCpuSec) && rawCpuSec > 0 ? rawCpuSec : 0;
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

  public getTenantReport(tenantId: string): TenantFinOpsSummary | undefined {
    return this.tenantCosts.get(tenantId);
  }

  public allEvents(): CloudEventV1[] {
    return [...this.events];
  }
}
