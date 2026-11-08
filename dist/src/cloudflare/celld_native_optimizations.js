"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.V8HeapAndSqlBudgetGuard = exports.DynamicWorkerSandboxLoader = exports.DurableObjectFacetSupervisor = exports.CelldKvOptimizerAndGuard = void 0;




var crypto = _interopRequireWildcard(require("node:crypto"));
var vm = _interopRequireWildcard(require("node:vm"));var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M15 TASK-67 ~ TASK-70: denoland/celld Native Durable Object Facets (`ctx.facets`),
 * Dynamic Workers (`env.ANSTS_SANDBOX_LOADER` with real `node:vm` isolate execution),
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

  abortFaultyFacet(facetName, error) {
    const rec = this.activeFacets.get(facetName);
    if (!rec) {
      throw new Error(`Facet '${facetName}' not found`);
    }
    rec.status = "ABORTED";
    rec.abortReason = error.message;
    return rec;
  }

  async deleteCompletedFacet(facetName) {
    const rec = this.activeFacets.get(facetName);
    if (!rec) return false;
    rec.status = "DELETED";
    this.facetSqlStores.delete(facetName);
    return true;
  }

  listFacets() {
    return Array.from(this.activeFacets.values());
  }
}exports.DurableObjectFacetSupervisor = DurableObjectFacetSupervisor;

class DynamicWorkerSandboxLoader {
  loader;
  loadedSpecs = new Map();

  constructor(loader) {
    this.loader = loader;
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
    this.loadedSpecs.set(isolateId, spec);

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
        `KV value size (${byteLength} bytes) exceeds celld 25 MiB limit (${CelldKvOptimizerAndGuard.MAX_KV_VALUE_BYTES})`
      );
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
        `SQLite statement exceeded celld per-statement timeout (${elapsedMs}ms > ${this.sqlTimeoutMs}ms): ${query.slice(0, 60)}`
      );
    }
    return { cursor, elapsedMs };
  }

  /**
   * Runs a caller-supplied SQLite executor (any cursor shape) inside the celld per-statement budget and
   * reports the measured wall-clock duration of the execution.
   */
  executeBoundedSql(params)


  {
    const t0 = performance.now();
    const rows = params.executor();
    const elapsedMs = Number((performance.now() - t0).toFixed(3));
    if (elapsedMs > this.sqlTimeoutMs) {
      throw new Error(
        `SQLite statement exceeded celld per-statement timeout (${elapsedMs}ms > ${this.sqlTimeoutMs}ms): ${params.query.slice(0, 60)}`
      );
    }
    return { rows, elapsedMs };
  }
}exports.V8HeapAndSqlBudgetGuard = V8HeapAndSqlBudgetGuard;