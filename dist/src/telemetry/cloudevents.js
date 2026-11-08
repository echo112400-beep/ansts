"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CloudEventsBus = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M6 TASK-22: CloudEvents v1.0 Telemetry Bus & FinOps Cost Attribution Ledger.
 */ /**
 * CloudEvents v1.0 envelope enriched with the W3C `traceparent` of the span that emitted it.
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
  emit(
  type,
  taskId,
  tenantId,
  data = {},
  options)
  {
    const evt = this.emitEvent({ type, subject: taskId, tenantId, data });
    const traceparent =
    options?.traceparent ??
    `00-${crypto.randomBytes(16).toString("hex")}-${crypto.randomBytes(8).toString("hex")}-01`;
    // `emitEvent` returns the stored envelope; widening it in place keeps `allEvents()` consistent.
    evt.traceparent = traceparent;
    return evt;
  }

  getTenantReport(tenantId) {
    return this.tenantCosts.get(tenantId);
  }
  allEvents() {
    return [...this.events];
  }
}exports.CloudEventsBus = CloudEventsBus;