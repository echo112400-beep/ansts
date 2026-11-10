/**
 * @fileoverview M15 TASK-67 ~ TASK-70, M56-4 & M59-2/M59-6: denoland/celld Native Durable Object Facets (`ctx.facets`),
 * Dynamic Workers (`env.ANSTS_SANDBOX_LOADER` with real `node:vm` isolate execution & Bounded Spec Cache),
 * KV Single-Writer Sharding with TTL Validation & V8 128MB Heap/SQL Real Wall-Clock Budget Guard.
 */
import * as crypto from "node:crypto";
import * as vm from "node:vm";
import {
  DynamicWorkerLoaderBindingLike,
  DynamicWorkerLoaderModuleSpec,
  KvNamespaceLike,
  SqlCursorLike,
  SqlStorageLike,
} from "./env.js";

export interface FacetLifecycleRecord {
  facetName: string;
  taskId: string;
  turnIndex: number;
  status: "SPAWNED" | "COMPLETED" | "ABORTED" | "DELETED";
  isolatedSqliteDbName: string;
  abortReason?: string;
}

export class DurableObjectFacetSupervisor {
  private readonly activeFacets: Map<string, FacetLifecycleRecord> = new Map();
  private readonly facetSqlStores: Map<string, SqlStorageLike> = new Map();
  public readonly maxRetainedFacetRecords: number;

  constructor(maxRetainedFacetRecords = 512) {
    this.maxRetainedFacetRecords = Math.max(8, maxRetainedFacetRecords);
  }

  private pruneTerminalFacetHistory(): void {
    if (this.activeFacets.size <= this.maxRetainedFacetRecords) return;
    for (const [name, rec] of this.activeFacets.entries()) {
      if (this.activeFacets.size <= this.maxRetainedFacetRecords) break;
      if (rec.status === "ABORTED" || rec.status === "DELETED" || rec.status === "COMPLETED") {
        this.activeFacets.delete(name);
        this.facetSqlStores.delete(name);
      }
    }
  }

  public spawnToolFacet(
    taskId: string,
    turnIndex: number,
    toolName: string,
    sqlFactory: () => SqlStorageLike
  ): { facetRecord: FacetLifecycleRecord; facetSql: SqlStorageLike } {
    const facetName = `facet-${taskId}-t${turnIndex}-${toolName}`;
    const isolatedSqliteDbName = `sqlite://facet/${facetName}.db`;
    const record: FacetLifecycleRecord = {
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
  public abortFaultyFacet(facetName: string, error: Error): FacetLifecycleRecord {
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

  public async deleteCompletedFacet(facetName: string): Promise<boolean> {
    const rec = this.activeFacets.get(facetName);
    if (!rec) return false;
    rec.status = "DELETED";
    this.facetSqlStores.delete(facetName);
    this.pruneTerminalFacetHistory();
    return true;
  }

  public getActiveSqlStoreCount(): number {
    return this.facetSqlStores.size;
  }

  public listFacets(): FacetLifecycleRecord[] {
    return Array.from(this.activeFacets.values());
  }
}

export class DynamicWorkerSandboxLoader {
  private readonly loader?: DynamicWorkerLoaderBindingLike;
  private readonly loadedSpecs: Map<string, DynamicWorkerLoaderModuleSpec> = new Map();
  private readonly maxCachedSpecs: number;

  constructor(loader?: DynamicWorkerLoaderBindingLike, maxCachedSpecs = 256) {
    this.loader = loader;
    this.maxCachedSpecs = Math.max(1, maxCachedSpecs);
  }

  public async executeUntrustedCodeInIsolate(params: {
    taskId: string;
    turnIndex: number;
    jsSourceCode: string;
    allowInternetEgress?: boolean;
    inputPayload?: Record<string, unknown>;
  }): Promise<{
    isolateId: string;
    globalOutboundBlocked: boolean;
    coldStartLatencyMs: number;
    output: Record<string, unknown>;
  }> {
    const t0 = performance.now();
    const codeHash = crypto.createHash("sha256").update(params.jsSourceCode, "utf-8").digest("hex").slice(0, 12);
    const isolateId = `dyn-worker-${params.taskId}-t${params.turnIndex}-${codeHash}`;
    const globalOutboundBlocked = !(params.allowInternetEgress ?? false);

    const spec: DynamicWorkerLoaderModuleSpec = {
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
      if (oldestKey === undefined) break;
      this.loadedSpecs.delete(oldestKey);
    }

    if (this.loader) {
      const stub = this.loader.get(isolateId, async () => spec);
      const ep = stub.getEntrypoint("default");
      const resp = await ep.fetch("http://sandbox.internal/run", {
        method: "POST",
        body: JSON.stringify(params.inputPayload ?? {}),
      });
      const out = (await resp.json()) as Record<string, unknown>;
      const coldStartLatencyMs = Number((performance.now() - t0).toFixed(3));
      return {
        isolateId,
        globalOutboundBlocked,
        coldStartLatencyMs,
        output: out,
      };
    }

    const transformedCode = params.jsSourceCode.replace(/export\s+default\s+/, "globalThis.__workerExport = ");
    const sandboxGlobals: Record<string, unknown> = {
      Response,
      Request,
      Headers,
      URL,
      JSON,
      Math,
      Date,
      inputPayload: params.inputPayload ?? {},
      env: spec.env,
      fetch: globalOutboundBlocked
        ? async () => {
            throw new Error("DynamicWorker Egress Blocked: globalOutbound is null");
          }
        : globalThis.fetch,
    };
    const context = vm.createContext(sandboxGlobals);
    const script = new vm.Script(transformedCode, { filename: "sandbox_entry.js" });
    script.runInContext(context, { timeout: 1000 });

    let output: Record<string, unknown> = {};
    const workerExport = sandboxGlobals.__workerExport as
      | { fetch?: (req: Request, env: unknown) => Promise<Response> | Response }
      | undefined;
    if (workerExport && typeof workerExport.fetch === "function") {
      const req = new Request("http://sandbox.internal/run", {
        method: "POST",
        body: JSON.stringify(params.inputPayload ?? {}),
      });
      const resp = await workerExport.fetch(req, spec.env);
      const text = await resp.text();
      output = text ? (JSON.parse(text) as Record<string, unknown>) : {};
    } else if (sandboxGlobals.__result !== undefined) {
      output = sandboxGlobals.__result as Record<string, unknown>;
    }

    const coldStartLatencyMs = Math.max(0.001, Number((performance.now() - t0).toFixed(3)));
    return {
      isolateId,
      globalOutboundBlocked,
      coldStartLatencyMs,
      output,
    };
  }

  public getLoadedSpec(isolateId: string): DynamicWorkerLoaderModuleSpec | undefined {
    return this.loadedSpecs.get(isolateId);
  }

  public getCachedSpecCount(): number {
    return this.loadedSpecs.size;
  }
}

export class CelldKvOptimizerAndGuard {
  private readonly kv: KvNamespaceLike;
  private readonly shardCount: number;
  public static readonly MAX_KV_VALUE_BYTES = 25 * 1024 * 1024;

  constructor(kv: KvNamespaceLike, shardCount = 32) {
    this.kv = kv;
    this.shardCount = Number.isInteger(shardCount) && shardCount > 0 ? shardCount : 32;
  }

  public computeShardedKey(logicalNamespace: string, key: string): { shardId: number; physicalKey: string } {
    const hash = crypto.createHash("sha256").update(`${logicalNamespace}:${key}`, "utf-8").digest();
    const shardId = hash.readUInt16BE(0) % this.shardCount;
    return {
      shardId,
      physicalKey: `kv-cell-${String(shardId).padStart(2, "0")}:${logicalNamespace}:${key}`,
    };
  }

  public async putSafe(
    logicalNamespace: string,
    key: string,
    value: string,
    expirationTtl?: number
  ): Promise<{ shardId: number; physicalKey: string; byteLength: number }> {
    const byteLength = Buffer.byteLength(value, "utf-8");
    if (byteLength > CelldKvOptimizerAndGuard.MAX_KV_VALUE_BYTES) {
      throw new Error(
        `KV value size (${byteLength} bytes) exceeds celld 25 MiB limit (${CelldKvOptimizerAndGuard.MAX_KV_VALUE_BYTES})`
      );
    }
    if (expirationTtl !== undefined && (!Number.isFinite(expirationTtl) || expirationTtl <= 0)) {
      throw new Error(`KV expirationTtl must be a positive number of seconds (got ${expirationTtl})`);
    }
    const { shardId, physicalKey } = this.computeShardedKey(logicalNamespace, key);
    await this.kv.put(physicalKey, value, expirationTtl ? { expirationTtl } : undefined);
    return { shardId, physicalKey, byteLength };
  }

  public async getSafe(logicalNamespace: string, key: string): Promise<string | null> {
    const { physicalKey } = this.computeShardedKey(logicalNamespace, key);
    return this.kv.get(physicalKey);
  }
}

export class V8HeapAndSqlBudgetGuard {
  private readonly softHeapLimitMb: number;
  private readonly hardHeapLimitMb: number;
  private readonly sqlTimeoutMs: number;

  constructor(softHeapLimitMb = 96, hardHeapLimitMb = 128, sqlTimeoutMs = 5000) {
    this.softHeapLimitMb = softHeapLimitMb;
    this.hardHeapLimitMb = hardHeapLimitMb;
    this.sqlTimeoutMs = sqlTimeoutMs;
  }

  public evaluateHeapPressure(currentHeapUsedMb?: number): {
    status: "HEALTHY" | "EVICT_WARM_BUFFERS" | "REJECT_NEW_STREAMS";
    heapUtilizationPct: number;
    measuredHeapMb: number;
  } {
    const measuredHeapMb =
      currentHeapUsedMb !== undefined && Number.isFinite(currentHeapUsedMb) && currentHeapUsedMb >= 0
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

  public execWithBudget<T = Record<string, unknown>>(
    sql: SqlStorageLike,
    query: string,
    ...bindings: unknown[]
  ): { cursor: SqlCursorLike<T>; elapsedMs: number } {
    const t0 = performance.now();
    const cursor = sql.exec<T>(query, ...bindings);
    const elapsedMs = Number((performance.now() - t0).toFixed(3));
    if (elapsedMs > this.sqlTimeoutMs) {
      throw new Error(
        `SQLite statement exceeded celld per-statement timeout (${elapsedMs}ms > ${this.sqlTimeoutMs}ms): ${query.slice(0, 60)}`
      );
    }
    return { cursor, elapsedMs };
  }
}
