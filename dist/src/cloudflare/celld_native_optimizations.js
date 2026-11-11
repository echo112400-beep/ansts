"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.V8HeapAndSqlBudgetGuard = exports.CelldKvOptimizerAndGuard = exports.DynamicWorkerSandboxLoader = exports.DurableObjectFacetSupervisor = void 0;
/**
 * @fileoverview M15 TASK-67 ~ TASK-70, M56-4, M59-2/M59-6 & M67-3: denoland/celld Native Durable Object Facets (`ctx.facets`),
 * Dynamic Workers (`env.ANSTS_SANDBOX_LOADER` with real `node:vm` isolate execution, Async Fetch Timeout, Input Deep-Clone & Bounded Spec Cache),
 * KV Single-Writer Sharding with TTL Validation & V8 128MB Heap/SQL Real Wall-Clock Budget Guard.
 */
const crypto = __importStar(require("node:crypto"));
const vm = __importStar(require("node:vm"));
class DurableObjectFacetSupervisor {
    activeFacets = new Map();
    facetSqlStores = new Map();
    maxRetainedFacetRecords;
    constructor(maxRetainedFacetRecords = 512) {
        this.maxRetainedFacetRecords = Math.max(8, maxRetainedFacetRecords);
    }
    pruneTerminalFacetHistory() {
        if (this.activeFacets.size <= this.maxRetainedFacetRecords)
            return;
        for (const [name, rec] of this.activeFacets.entries()) {
            if (this.activeFacets.size <= this.maxRetainedFacetRecords)
                break;
            if (rec.status === "ABORTED" || rec.status === "DELETED" || rec.status === "COMPLETED") {
                this.activeFacets.delete(name);
                this.facetSqlStores.delete(name);
            }
        }
    }
    spawnToolFacet(taskId, turnIndex, toolName, sqlFactory) {
        const facetName = `facet-${taskId}-t${turnIndex}-${toolName}`;
        const isolatedSqliteDbName = `sqlite://facet/${facetName}.db`;
        const record = {
            facetName,
            taskId,
            turnIndex,
            status: "SPAWNED",
            isolatedSqliteDbName,
        };
        const sql = sqlFactory();
        this.activeFacets.set(facetName, record);
        this.facetSqlStores.set(facetName, sql);
        this.pruneTerminalFacetHistory();
        return { facetRecord: record, facetSql: sql };
    }
    /**
     * [M56-4 & M59-6] Aborts a faulty facet, immediately releases its `facetSqlStores` handle,
     * and bounds terminal facet metadata history.
     */
    abortFaultyFacet(facetName, error) {
        const rec = this.activeFacets.get(facetName);
        if (!rec) {
            throw new Error(`Facet '${facetName}' not found`);
        }
        rec.status = "ABORTED";
        rec.abortReason = error.message;
        this.facetSqlStores.delete(facetName);
        this.pruneTerminalFacetHistory();
        return rec;
    }
    async deleteCompletedFacet(facetName) {
        const rec = this.activeFacets.get(facetName);
        if (!rec)
            return false;
        rec.status = "DELETED";
        this.facetSqlStores.delete(facetName);
        this.pruneTerminalFacetHistory();
        return true;
    }
    getActiveSqlStoreCount() {
        return this.facetSqlStores.size;
    }
    listFacets() {
        return Array.from(this.activeFacets.values());
    }
}
exports.DurableObjectFacetSupervisor = DurableObjectFacetSupervisor;
class DynamicWorkerSandboxLoader {
    loader;
    loadedSpecs = new Map();
    maxCachedSpecs;
    constructor(loader, maxCachedSpecs = 256) {
        this.loader = loader;
        const rawMax = Number(maxCachedSpecs);
        this.maxCachedSpecs = Number.isFinite(rawMax) && rawMax > 0 ? Math.max(1, Math.floor(rawMax)) : 256;
    }
    async executeUntrustedCodeInIsolate(params) {
        const t0 = performance.now();
        const codeHash = crypto.createHash("sha256").update(params.jsSourceCode, "utf-8").digest("hex").slice(0, 12);
        const isolateId = `dyn-worker-${params.taskId}-t${params.turnIndex}-${codeHash}`;
        const globalOutboundBlocked = !(params.allowInternetEgress ?? false);
        const clonedInput = JSON.parse(JSON.stringify(params.inputPayload ?? {}));
        const asyncTimeoutMs = params.asyncTimeoutMs !== undefined && Number.isFinite(params.asyncTimeoutMs) && params.asyncTimeoutMs > 0
            ? Math.floor(params.asyncTimeoutMs)
            : 2000;
        const spec = {
            compatibilityDate: "2026-01-01",
            mainModule: "sandbox_entry.js",
            modules: {
                "sandbox_entry.js": { js: params.jsSourceCode },
            },
            env: { TASK_ID: params.taskId, TURN_INDEX: String(params.turnIndex) },
            globalOutbound: globalOutboundBlocked ? null : undefined,
        };
        if (this.loadedSpecs.has(isolateId)) {
            this.loadedSpecs.delete(isolateId);
        }
        this.loadedSpecs.set(isolateId, spec);
        while (this.loadedSpecs.size > this.maxCachedSpecs) {
            const oldestKey = this.loadedSpecs.keys().next().value;
            if (oldestKey === undefined)
                break;
            this.loadedSpecs.delete(oldestKey);
        }
        if (this.loader) {
            const stub = this.loader.get(isolateId, async () => spec);
            const ep = stub.getEntrypoint("default");
            const resp = await ep.fetch("http://sandbox.internal/run", {
                method: "POST",
                body: JSON.stringify(clonedInput),
            });
            const out = (await resp.json());
            const coldStartLatencyMs = Number((performance.now() - t0).toFixed(3));
            return {
                isolateId,
                globalOutboundBlocked,
                coldStartLatencyMs,
                output: out,
            };
        }
        const transformedCode = params.jsSourceCode.replace(/export\s+default\s+/, "globalThis.__workerExport = ");
        const sandboxGlobals = {
            Response,
            Request,
            Headers,
            URL,
            JSON,
            Math,
            Date,
            inputPayload: clonedInput,
            env: { ...spec.env },
            fetch: globalOutboundBlocked
                ? async () => {
                    throw new Error("DynamicWorker Egress Blocked: globalOutbound is null");
                }
                : globalThis.fetch,
        };
        const context = vm.createContext(sandboxGlobals);
        const script = new vm.Script(transformedCode, { filename: "sandbox_entry.js" });
        script.runInContext(context, { timeout: 1000 });
        let output = {};
        const workerExport = sandboxGlobals.__workerExport;
        if (workerExport && typeof workerExport.fetch === "function") {
            const req = new Request("http://sandbox.internal/run", {
                method: "POST",
                body: JSON.stringify(clonedInput),
            });
            let timer = null;
            try {
                const timeoutPromise = new Promise((_, reject) => {
                    timer = setTimeout(() => {
                        reject(new Error(`DynamicWorker async fetch timed out after ${asyncTimeoutMs}ms`));
                    }, asyncTimeoutMs);
                });
                const resp = await Promise.race([
                    Promise.resolve(workerExport.fetch(req, { ...spec.env })),
                    timeoutPromise,
                ]);
                const text = await resp.text();
                output = text ? JSON.parse(text) : {};
            }
            finally {
                if (timer)
                    clearTimeout(timer);
            }
        }
        else if (sandboxGlobals.__result !== undefined) {
            output = sandboxGlobals.__result;
        }
        const coldStartLatencyMs = Math.max(0.001, Number((performance.now() - t0).toFixed(3)));
        return {
            isolateId,
            globalOutboundBlocked,
            coldStartLatencyMs,
            output,
        };
    }
    getLoadedSpec(isolateId) {
        return this.loadedSpecs.get(isolateId);
    }
    getCachedSpecCount() {
        return this.loadedSpecs.size;
    }
}
exports.DynamicWorkerSandboxLoader = DynamicWorkerSandboxLoader;
class CelldKvOptimizerAndGuard {
    kv;
    shardCount;
    static MAX_KV_VALUE_BYTES = 25 * 1024 * 1024;
    constructor(kv, shardCount = 32) {
        this.kv = kv;
        this.shardCount = Number.isInteger(shardCount) && shardCount > 0 ? shardCount : 32;
    }
    computeShardedKey(logicalNamespace, key) {
        const hash = crypto.createHash("sha256").update(`${logicalNamespace}:${key}`, "utf-8").digest();
        const shardId = hash.readUInt16BE(0) % this.shardCount;
        return {
            shardId,
            physicalKey: `kv-cell-${String(shardId).padStart(2, "0")}:${logicalNamespace}:${key}`,
        };
    }
    async putSafe(logicalNamespace, key, value, expirationTtl) {
        const byteLength = Buffer.byteLength(value, "utf-8");
        if (byteLength > CelldKvOptimizerAndGuard.MAX_KV_VALUE_BYTES) {
            throw new Error(`KV value size (${byteLength} bytes) exceeds celld 25 MiB limit (${CelldKvOptimizerAndGuard.MAX_KV_VALUE_BYTES})`);
        }
        if (expirationTtl !== undefined && (!Number.isFinite(expirationTtl) || expirationTtl <= 0)) {
            throw new Error(`KV expirationTtl must be a positive number of seconds (got ${expirationTtl})`);
        }
        const { shardId, physicalKey } = this.computeShardedKey(logicalNamespace, key);
        await this.kv.put(physicalKey, value, expirationTtl ? { expirationTtl } : undefined);
        return { shardId, physicalKey, byteLength };
    }
    async getSafe(logicalNamespace, key) {
        const { physicalKey } = this.computeShardedKey(logicalNamespace, key);
        return this.kv.get(physicalKey);
    }
}
exports.CelldKvOptimizerAndGuard = CelldKvOptimizerAndGuard;
class V8HeapAndSqlBudgetGuard {
    softHeapLimitMb;
    hardHeapLimitMb;
    sqlTimeoutMs;
    constructor(softHeapLimitMb = 96, hardHeapLimitMb = 128, sqlTimeoutMs = 5000) {
        const safeSoft = Number.isFinite(softHeapLimitMb) && softHeapLimitMb > 0 ? softHeapLimitMb : 96;
        const safeHard = Number.isFinite(hardHeapLimitMb) && hardHeapLimitMb >= safeSoft
            ? hardHeapLimitMb
            : Math.max(128, safeSoft);
        const safeTimeout = Number.isFinite(sqlTimeoutMs) && sqlTimeoutMs > 0 ? sqlTimeoutMs : 5000;
        this.softHeapLimitMb = safeSoft;
        this.hardHeapLimitMb = safeHard;
        this.sqlTimeoutMs = safeTimeout;
    }
    evaluateHeapPressure(currentHeapUsedMb) {
        const measuredHeapMb = currentHeapUsedMb !== undefined && Number.isFinite(currentHeapUsedMb) && currentHeapUsedMb >= 0
            ? currentHeapUsedMb
            : Number((process.memoryUsage().heapUsed / (1024 * 1024)).toFixed(2));
        const pct = Number(((measuredHeapMb / this.hardHeapLimitMb) * 100).toFixed(1));
        if (measuredHeapMb >= this.hardHeapLimitMb * 0.92) {
            return { status: "REJECT_NEW_STREAMS", heapUtilizationPct: pct, measuredHeapMb };
        }
        if (measuredHeapMb >= this.softHeapLimitMb) {
            return { status: "EVICT_WARM_BUFFERS", heapUtilizationPct: pct, measuredHeapMb };
        }
        return { status: "HEALTHY", heapUtilizationPct: pct, measuredHeapMb };
    }
    execWithBudget(sql, query, ...bindings) {
        const t0 = performance.now();
        const cursor = sql.exec(query, ...bindings);
        const elapsedMs = Number((performance.now() - t0).toFixed(3));
        if (elapsedMs > this.sqlTimeoutMs) {
            throw new Error(`SQLite statement exceeded celld per-statement timeout (${elapsedMs}ms > ${this.sqlTimeoutMs}ms): ${query.slice(0, 60)}`);
        }
        return { cursor, elapsedMs };
    }
}
exports.V8HeapAndSqlBudgetGuard = V8HeapAndSqlBudgetGuard;
