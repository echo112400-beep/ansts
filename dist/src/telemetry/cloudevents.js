"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CloudEventsBus = void 0;
class CloudEventsBus {
    events = [];
    tenantCosts = new Map();
    seenIdempotencyKeys = new Map();
    maxRetainedEvents;
    maxIdempotencyEntries;
    eventSeq = 0;
    sql;
    constructor(maxRetainedEvents = 1000, sql) {
        const rawMax = Number(maxRetainedEvents);
        this.maxRetainedEvents =
            Number.isFinite(rawMax) && rawMax > 0 ? Math.max(10, Math.floor(rawMax)) : 1000;
        this.maxIdempotencyEntries = this.maxRetainedEvents * 4;
        if (sql) {
            this.bindSqlStorage(sql);
        }
    }
    /**
     * [M66-2] Binds SQLite storage so cumulative per-tenant FinOps telemetry costs survive process restarts.
     */
    bindSqlStorage(sql) {
        this.sql = sql;
        this.sql.exec("CREATE TABLE IF NOT EXISTS persisted_cloudevents_finops (tenant_id TEXT PRIMARY KEY, total_llm_cost_usd REAL, total_sandbox_cpu_sec REAL, total_sandbox_cost_usd REAL, grand_total_usd REAL)");
        const rows = this.sql
            .exec("SELECT * FROM persisted_cloudevents_finops")
            .toArray();
        let restored = 0;
        for (const r of rows) {
            const tenantId = String(r.tenant_id);
            this.tenantCosts.set(tenantId, {
                tenantId,
                totalLlmCostUsd: Number(r.total_llm_cost_usd ?? 0),
                totalSandboxCpuCoreSeconds: Number(r.total_sandbox_cpu_sec ?? 0),
                totalSandboxCostUsd: Number(r.total_sandbox_cost_usd ?? 0),
                grandTotalUsd: Number(r.grand_total_usd ?? 0),
            });
            restored += 1;
        }
        return restored;
    }
    persistTenantFinOps(summary) {
        if (!this.sql)
            return;
        this.sql.exec("INSERT OR REPLACE INTO persisted_cloudevents_finops (tenant_id, total_llm_cost_usd, total_sandbox_cpu_sec, total_sandbox_cost_usd, grand_total_usd) VALUES (?, ?, ?, ?, ?)", summary.tenantId, summary.totalLlmCostUsd, summary.totalSandboxCpuCoreSeconds, summary.totalSandboxCostUsd, summary.grandTotalUsd);
    }
    /**
     * [M60-3 & M66-2] Supports optional `idempotencyKey` or `eventId` with bounded FIFO eviction (`maxIdempotencyEntries`)
     * so retried emissions never double-charge `tenantCosts` and long-running buses never leak memory.
     */
    emitEvent(params) {
        const dedupToken = params.idempotencyKey ?? params.eventId;
        if (dedupToken) {
            const scopedDedup = `${params.tenantId}::${dedupToken}`;
            const existingEvt = this.seenIdempotencyKeys.get(scopedDedup);
            if (existingEvt) {
                return existingEvt;
            }
        }
        this.eventSeq += 1;
        const evt = {
            specversion: "1.0",
            id: params.eventId ?? `ce-${Date.now()}-${this.eventSeq}`,
            source: "urn:ansts:control-plane:v2",
            type: params.type,
            subject: params.subject,
            time: new Date().toISOString(),
            datacontenttype: "application/json",
            data: { tenantId: params.tenantId, ...(params.data ?? {}) },
        };
        if (dedupToken) {
            const scopedDedup = `${params.tenantId}::${dedupToken}`;
            this.seenIdempotencyKeys.set(scopedDedup, evt);
            while (this.seenIdempotencyKeys.size > this.maxIdempotencyEntries) {
                const oldestKey = this.seenIdempotencyKeys.keys().next().value;
                if (oldestKey === undefined)
                    break;
                this.seenIdempotencyKeys.delete(oldestKey);
            }
        }
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
        existing.totalSandboxCpuCoreSeconds = Number((existing.totalSandboxCpuCoreSeconds + cpuSecDelta).toFixed(3));
        existing.totalSandboxCostUsd = Number((existing.totalSandboxCostUsd + sandboxDelta).toFixed(6));
        existing.grandTotalUsd = Number((existing.totalLlmCostUsd + existing.totalSandboxCostUsd).toFixed(6));
        this.tenantCosts.set(params.tenantId, existing);
        this.persistTenantFinOps(existing);
        return evt;
    }
    getIdempotencyCacheSize() {
        return this.seenIdempotencyKeys.size;
    }
    getTenantReport(tenantId) {
        return this.tenantCosts.get(tenantId);
    }
    allEvents() {
        return [...this.events];
    }
}
exports.CloudEventsBus = CloudEventsBus;
