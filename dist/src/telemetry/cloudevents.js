"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CloudEventsBus = void 0; /**
 * @fileoverview M6 TASK-22 & M58-5: CloudEvents v1.0 Telemetry Bus & FinOps Cost Attribution Ledger
 * with Bounded Event Ring Buffer & Finite Non-Negative Cost Sanitization.
 */



















class CloudEventsBus {
  events = [];
  tenantCosts = new Map();
  maxRetainedEvents;
  eventSeq = 0;

  constructor(maxRetainedEvents = 1000) {
    this.maxRetainedEvents = Math.max(10, maxRetainedEvents);
  }

  emitEvent(params)






  {
    this.eventSeq += 1;
    const evt = {
      specversion: "1.0",
      id: `ce-${Date.now()}-${this.eventSeq}`,
      source: "urn:ansts:control-plane:v2",
      type: params.type,
      subject: params.subject,
      time: new Date().toISOString(),
      datacontenttype: "application/json",
      data: { tenantId: params.tenantId, ...(params.data ?? {}) }
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
      grandTotalUsd: 0
    };
    const rawLlm = params.llmCostUsd ?? 0;
    const rawCpuSec = params.sandboxCpuCoreSeconds ?? 0;
    const llmDelta = Number.isFinite(rawLlm) && rawLlm > 0 ? rawLlm : 0;
    const cpuSecDelta = Number.isFinite(rawCpuSec) && rawCpuSec > 0 ? rawCpuSec : 0;
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