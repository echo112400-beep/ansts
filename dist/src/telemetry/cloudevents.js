"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CloudEventsBus = void 0; /**
 * @fileoverview M6 TASK-22: CloudEvents v1.0 Telemetry Bus & FinOps Cost Attribution Ledger.
 */



















class CloudEventsBus {
  events = [];
  tenantCosts = new Map();

  emitEvent(params)






  {
    const evt = {
      specversion: "1.0",
      id: `ce-${Date.now()}-${this.events.length + 1}`,
      source: "urn:ansts:control-plane:v2",
      type: params.type,
      subject: params.subject,
      time: new Date().toISOString(),
      datacontenttype: "application/json",
      data: { tenantId: params.tenantId, ...(params.data ?? {}) }
    };
    this.events.push(evt);

    const existing = this.tenantCosts.get(params.tenantId) ?? {
      tenantId: params.tenantId,
      totalLlmCostUsd: 0,
      totalSandboxCpuCoreSeconds: 0,
      totalSandboxCostUsd: 0,
      grandTotalUsd: 0
    };
    const llmDelta = params.llmCostUsd ?? 0;
    const cpuSecDelta = params.sandboxCpuCoreSeconds ?? 0;
    const sandboxDelta = cpuSecDelta * 0.00004;
    existing.totalLlmCostUsd = Number((existing.totalLlmCostUsd + llmDelta).toFixed(6));
    existing.totalSandboxCpuCoreSeconds = Number(
    (existing.totalSandboxCpuCoreSeconds + cpuSecDelta).toFixed(3));

    existing.totalSandboxCostUsd = Number((existing.totalSandboxCostUsd + sandboxDelta).toFixed(6));
    existing.grandTotalUsd = Number(
    (existing.totalLlmCostUsd + existing.totalSandboxCostUsd).toFixed(6));

    this.tenantCosts.set(params.tenantId, existing);
    return evt;
  }

  getTenantReport(tenantId) {
    return this.tenantCosts.get(tenantId);
  }

  allEvents() {
    return [...this.events];
  }
}exports.CloudEventsBus = CloudEventsBus;