"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.V8HeapAndSqlBudgetGuard = exports.DynamicWorkerSandboxLoader = exports.DurableObjectFacetSupervisor = exports.CelldKvOptimizerAndGuard = void 0;




var crypto = _interopRequireWildcard(require("node:crypto"));
var vm = _interopRequireWildcard(require("node:vm"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M15 TASK-67 ~ TASK-70 & M56-4: denoland/celld Native Durable Object Facets (`ctx.facets`),
 * Dynamic Workers (`env.ANSTS_SANDBOX_LOADER` with real `node:vm` isolate execution & Bounded Spec Cache),
 * KV Single-Writer Sharding & V8 128MB Heap/SQL Real Wall-Clock Budget Guard.
 */













class DurableObjectFacetSupervisor {
  activeFacets = new Map();
  facetSqlStores = new Map();

  spawnToolFacet(
  taskId,
  turnIndex,
  toolName,
  sqlFactory)
  {
    const facetName = `facet-${taskId}-t${turnIndex}-${toolName}`;
    const isolatedSqliteDbName = `sqlite://facet/${facetName}.db`;
    const record = {
      facetName,
      taskId,
      turnIndex,
      status: "SPAWNED",
      isolatedSqliteDbName
    };
    const sql = sqlFactory();
    this.activeFacets.set(facetName, record);
    this.facetSqlStores.set(facetName, sql);
    return { facetRecord: record, facetSql: sql };
  }

  /**
   * [M56-4] Aborts a faulty facet and immediately releases its `facetSqlStores` handle
   * so aborted facets never leak SQLite store instances in memory.
   */
  abortFaultyFacet(facetName, error) {
    const rec = this.activeFacets.get(facetName);
    if (!rec) {
      throw new Error(`Facet '${facetName}' not found`);
    }
    rec.status = "ABORTED";
    rec.abortReason = error.message;
    this.facetSqlStores.delete(facetName);
    return rec;
  }

  async deleteCompletedFacet(facetName) {
    const rec = this.activeFacets.get(facetName);
    if (!rec) return false;
    rec.status = "DELETED";
    this.facetSqlStores.delete(facetName);
    return true;
  }

  getActiveSqlStoreCount() {
    return this.facetSqlStores.size;
  }

  listFacets() {
    return Array.from(this.activeFacets.values());
  }
}exports.DurableObjectFacetSupervisor = DurableObjectFacetSupervisor;

class DynamicWorkerSandboxLoader {
  loader;
  loadedSpecs = new Map();
  maxCachedSpecs;

  constructor(loader, maxCachedSpecs = 256) {
    this.loader = loader;
    this.maxCachedSpecs = Math.max(1, maxCachedSpecs);
  }

  async executeUntrustedCodeInIsolate(params)










  {
    const t0 = performance.now();
    const codeHash = crypto.createHash("sha256").update(params.jsSourceCode, "utf-8").digest("hex").slice(0, 12);
    const isolateId = `dyn-worker-${params.taskId}-t${params.turnIndex}-${codeHash}`;
    const globalOutboundBlocked = !(params.allowInternetEgress ?? false);

    const spec = {
      compatibilityDate: "2026-01-01",
      mainModule: "sandbox_entry.js",
      modules: {
        "sandbox_entry.js": { js: params.jsSourceCode }
      },
      env: { TASK_ID: params.taskId, TURN_INDEX: String(params.turnIndex) },
      globalOutbound: globalOutboundBlocked ? null : undefined
    };
    if (this.loadedSpecs.has(isolateId)) {
      this.loadedSpecs.delete(isolateId);
    }
    this.loadedSpecs.set(isolateId, spec);
    while (this.loadedSpecs.size > this.maxCachedSpecs) {
      const oldestKey = this.loadedSpecs.keys().next().value;
      if (oldestKey === undefined) break;
      this.loadedSpecs.delete(oldestKey);
    }

    if (this.loader) {
      const stub = this.loader.get(isolateId, async () => spec);
      const ep = stub.getEntrypoint("default");
      const resp = await ep.fetch("http://sandbox.internal/run", {
        method: "POST",
        body: JSON.stringify(params.inputPayload ?? {})
      });
      const out = await resp.json();
      const coldStartLatencyMs = Number((performance.now() - t0).toFixed(3));
      return {
        isolateId,
        globalOutboundBlocked,
        coldStartLatencyMs,
        output: out
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
      inputPayload: params.inputPayload ?? {},
      env: spec.env,
      fetch: globalOutboundBlocked ?
      async () => {
        throw new Error("DynamicWorker Egress Blocked: globalOutbound is null");
      } :
      globalThis.fetch
    };
    const context = vm.createContext(sandboxGlobals);
    const script = new vm.Script(transformedCode, { filename: "sandbox_entry.js" });
    script.runInContext(context, { timeout: 1000 });

    let output = {};
    const workerExport = sandboxGlobals.__workerExport;


    if (workerExport && typeof workerExport.fetch === "function") {
      const req = new Request("http://sandbox.internal/run", {
        method: "POST",
        body: JSON.stringify(params.inputPayload ?? {})
      });
      const resp = await workerExport.fetch(req, spec.env);
      const text = await resp.text();
      output = text ? JSON.parse(text) : {};
    } else if (sandboxGlobals.__result !== undefined) {
      output = sandboxGlobals.__result;
    }

    const coldStartLatencyMs = Math.max(0.001, Number((performance.now() - t0).toFixed(3)));
    return {
      isolateId,
      globalOutboundBlocked,
      coldStartLatencyMs,
      output
    };
  }

  getLoadedSpec(isolateId) {
    return this.loadedSpecs.get(isolateId);
  }

  getCachedSpecCount() {
    return this.loadedSpecs.size;
  }
}exports.DynamicWorkerSandboxLoader = DynamicWorkerSandboxLoader;

class CelldKvOptimizerAndGuard {
  kv;
  shardCount;
  static MAX_KV_VALUE_BYTES = 25 * 1024 * 1024;

  constructor(kv, shardCount = 32) {
    this.kv = kv;
    this.shardCount = shardCount;
  }

  computeShardedKey(logicalNamespace, key) {
    const hash = crypto.createHash("sha256").update(`${logicalNamespace}:${key}`, "utf-8").digest();
    const shardId = hash.readUInt16BE(0) % this.shardCount;
    return {
      shardId,
      physicalKey: `kv-cell-${String(shardId).padStart(2, "0")}:${logicalNamespace}:${key}`
    };
  }

  async putSafe(
  logicalNamespace,
  key,
  value,
  expirationTtl)
  {
    const byteLength = Buffer.byteLength(value, "utf-8");
    if (byteLength > CelldKvOptimizerAndGuard.MAX_KV_VALUE_BYTES) {
      throw new Error(
      `KV value size (${byteLength} bytes) exceeds celld 25 MiB limit (${CelldKvOptimizerAndGuard.MAX_KV_VALUE_BYTES})`);

    }
    const { shardId, physicalKey } = this.computeShardedKey(logicalNamespace, key);
    await this.kv.put(physicalKey, value, expirationTtl ? { expirationTtl } : undefined);
    return { shardId, physicalKey, byteLength };
  }

  async getSafe(logicalNamespace, key) {
    const { physicalKey } = this.computeShardedKey(logicalNamespace, key);
    return this.kv.get(physicalKey);
  }
}exports.CelldKvOptimizerAndGuard = CelldKvOptimizerAndGuard;

class V8HeapAndSqlBudgetGuard {
  softHeapLimitMb;
  hardHeapLimitMb;
  sqlTimeoutMs;

  constructor(softHeapLimitMb = 96, hardHeapLimitMb = 128, sqlTimeoutMs = 5000) {
    this.softHeapLimitMb = softHeapLimitMb;
    this.hardHeapLimitMb = hardHeapLimitMb;
    this.sqlTimeoutMs = sqlTimeoutMs;
  }

  evaluateHeapPressure(currentHeapUsedMb)



  {
    const measuredHeapMb =
    currentHeapUsedMb ?? Number((process.memoryUsage().heapUsed / (1024 * 1024)).toFixed(2));
    const pct = Number((measuredHeapMb / this.hardHeapLimitMb * 100).toFixed(1));
    if (measuredHeapMb >= this.hardHeapLimitMb * 0.92) {
      return { status: "REJECT_NEW_STREAMS", heapUtilizationPct: pct, measuredHeapMb };
    }
    if (measuredHeapMb >= this.softHeapLimitMb) {
      return { status: "EVICT_WARM_BUFFERS", heapUtilizationPct: pct, measuredHeapMb };
    }
    return { status: "HEALTHY", heapUtilizationPct: pct, measuredHeapMb };
  }

  execWithBudget(
  sql,
  query,
  ...bindings)
  {
    const t0 = performance.now();
    const cursor = sql.exec(query, ...bindings);
    const elapsedMs = Number((performance.now() - t0).toFixed(3));
    if (elapsedMs > this.sqlTimeoutMs) {
      throw new Error(
      `SQLite statement exceeded celld per-statement timeout (${elapsedMs}ms > ${this.sqlTimeoutMs}ms): ${query.slice(0, 60)}`);

    }
    return { cursor, elapsedMs };
  }
}exports.V8HeapAndSqlBudgetGuard = V8HeapAndSqlBudgetGuard;