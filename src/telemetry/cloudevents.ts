/**
 * @fileoverview M6 TASK-22: CloudEvents v1.0 Telemetry Bus & FinOps Cost Attribution Ledger.
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

  public getTenantReport(tenantId: string): TenantFinOpsSummary | undefined {
    return this.tenantCosts.get(tenantId);
  }

  public allEvents(): CloudEventV1[] {
    return [...this.events];
  }
}
