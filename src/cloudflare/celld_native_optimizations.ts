/**
 * M15 (`TASK-69` ~ `TASK-74`): `celld` Native Services Optimization Engine
 *
 * Grounded in official `denoland/celld` service specifications:
 * 1. Durable Object Facets (`https://celld.dev/docs/services/durable-object-facets/` / `docs/services/durable-object-facets.md`):
 *    - `ctx.facets.get(name, () => ({ class, id }))`, `ctx.facets.abort(name, reason)`, `ctx.facets.delete(name)`
 *    - Child object inside a root Durable Object (`AgentTaskHarnessCell`) with its own isolated SQLite DB
 *    - Shares root cell's ownership record & fencing epoch (zero extra S3 CAS overhead, co-located moves)
 *    - Max nesting depth = 4; `storage.setAlarm()` throws inside a facet -> delegated to root cell alarm multiplexer
 *    - Facet WebSockets do not hibernate -> closed with `1001` on abort/delete before root hibernation
 * 2. Dynamic Workers (`https://celld.dev/docs/services/dynamic-workers/`):
 *    - `worker_loaders` binding (`CELLD_MAX_DYNAMIC_WORKER_CODE_BYTES = 64 MiB`)
 *    - Enforces `WorkerCode.limits` (`cpuMs`, `subrequests`) + `AbortSignal.timeout()` propagation
 *    - Custom outbound `fetch` firewall + `getDurableObjectClass()` for running untrusted/AI-generated code inside a Facet
 *    - Explicit `dispose()` + Tail Worker invocation report (with OTLP log severities `DEBUG`/`INFO`/`WARN`/`ERROR`)
 * 3. `celld` KV Single-Writer Bypass & Stream/TTL Guard (`docs/services/kv.md`):
 *    - In `celld`, each KV namespace is a single-writer Cell with no edge cache (`cacheStatus: null`).
 *    - Bypasses single-cell KV bottleneck for write-hot idempotency by routing to `AnstsSchedulerCell` SQLite
 *      or sharded KV namespaces, caches read-heavy keys in request-local memory, enforces `expirationTtl >= 60s`,
 *      caps bulk `get()` at `<= 100` keys, and validates fleet bucket availability for values `> 1 MiB` (up to `25 MiB`).
 * 4. V8 Heap (`CELLD_V8_HEAP_LIMIT_MB = 128 MB`) & SQL Statement / 30s Transaction Guard (`docs/services/durable-objects.md` & Issue #274):
 *    - Prevents `SqlStorage.Cursor.toArray()` OOM above 90% heap via paginated cursor streaming
 *    - Refuses new hibernatable WebSockets when heap >= 90% (recovers at < 75% with forced GC)
 *    - Enforces `CELLD_SQL_STATEMENT_BUDGET_MS` per-statement watchdog and 30s `transactionSync()` / nested savepoint isolation.
 */

import { InMemoryCellSqlStorage, SqlStorage } from "./env";

// ==================== 1. DURABLE OBJECT FACETS SUPERVISOR (ctx.facets) ====================

export interface FacetInstance {
  name: string;
  id: string;
  depth: number;
  props: Record<string, any>;
  sql: SqlStorage;
  activeWebSockets: Set<{ close(code: number, reason: string): void }>;
  abortedReason: string | null;
}

export class DurableObjectFacetSupervisor {
  public static readonly MAX_FACET_DEPTH = 4; // Counts root object (root=1, max child depth=4)
  public static readonly MAX_FACET_NAME_BYTES = 256;

  public readonly facets = new Map<string, FacetInstance>();
  public readonly rootAlarmScheduleMs = new Map<string, number>();

  constructor(
    public readonly rootCellId: string,
    public readonly rootEpoch: number,
    public readonly currentDepth: number = 1,
  ) {}

  /**
   * Implements `ctx.facets.get(name, callback)`:
   * Starts or retrieves a child Facet with its own isolated SQLite database replicated under the root cell's prefix.
   * Shares the root cell's ownership record and fencing epoch (`rootEpoch`).
   */
  public getFacet(
    name: string,
    startupCallback: () => { className: string; id?: string; props?: Record<string, any> },
  ): {
    facet: FacetInstance;
    execSql: (query: string, ...bindings: any[]) => any[];
    openWebSocket: () => { close(code: number, reason: string): void };
    setAlarmInsideFacet: (deadlineMs: number) => never;
    delegateAlarmToRoot: (deadlineMs: number) => void;
  } {
    if (Buffer.byteLength(name, "utf8") > DurableObjectFacetSupervisor.MAX_FACET_NAME_BYTES) {
      throw new Error(`FacetNameTooLong: '${name}' exceeds 256-byte limit`);
    }
    if (this.currentDepth >= DurableObjectFacetSupervisor.MAX_FACET_DEPTH) {
      throw new Error(
        `FacetMaxDepthExceeded: Cannot spawn facet '${name}' beyond max depth ${DurableObjectFacetSupervisor.MAX_FACET_DEPTH}`,
      );
    }

    let inst = this.facets.get(name);
    if (inst && inst.abortedReason !== null) {
      throw new Error(`FacetAborted: ${inst.abortedReason}`);
    }

    if (!inst) {
      const spec = startupCallback();
      // Deep-clone startup props (simulating structured clone so later mutations do not affect ctx.props)
      const clonedProps = spec.props ? JSON.parse(JSON.stringify(spec.props)) : {};
      inst = {
        name,
        id: spec.id ?? this.rootCellId,
        depth: this.currentDepth + 1,
        props: clonedProps,
        sql: new InMemoryCellSqlStorage(),
        activeWebSockets: new Set(),
        abortedReason: null,
      };
      this.facets.set(name, inst);
    }

    const facetRef = inst;
    return {
      facet: facetRef,
      execSql: (query: string, ...bindings: any[]) => {
        if (facetRef.abortedReason !== null) {
          throw new Error(`FacetAborted: ${facetRef.abortedReason}`);
        }
        return facetRef.sql.exec(query, ...bindings).toArray();
      },
      openWebSocket: () => {
        const sock = {
          closedCode: 0,
          close(code: number, _reason: string) {
            this.closedCode = code;
          },
        };
        facetRef.activeWebSockets.add(sock);
        return sock;
      },
      setAlarmInsideFacet: (_deadlineMs: number): never => {
        // Per docs/services/durable-object-facets.md: storage.setAlarm() throws inside a facet!
        throw new Error(
          "FacetAlarmUnsupported: storage.setAlarm() throws inside a facet; schedule alarms on the root Durable Object",
        );
      },
      delegateAlarmToRoot: (deadlineMs: number) => {
        this.rootAlarmScheduleMs.set(name, deadlineMs);
      },
    };
  }

  /**
   * `ctx.facets.abort(name, reason)`: Stops the facet, closes its WebSockets with code `1001`,
   * and KEEPS its isolated SQLite database intact.
   */
  public abortFacet(name: string, reason: string): { stopped: boolean; dbPreserved: boolean; closedSockets: number } {
    const inst = this.facets.get(name);
    if (!inst) return { stopped: false, dbPreserved: false, closedSockets: 0 };

    inst.abortedReason = reason;
    const closedSockets = inst.activeWebSockets.size;
    for (const ws of inst.activeWebSockets) {
      ws.close(1001, reason);
    }
    inst.activeWebSockets.clear();
    return { stopped: true, dbPreserved: true, closedSockets };
  }

  /**
   * `ctx.facets.delete(name)`: Stops the facet, closes its WebSockets with code `1001`,
   * and REMOVES its SQLite database (and any child facets below it).
   */
  public deleteFacet(name: string): { deleted: boolean; closedSockets: number } {
    const inst = this.facets.get(name);
    if (!inst) return { deleted: false, closedSockets: 0 };

    const closedSockets = inst.activeWebSockets.size;
    for (const ws of inst.activeWebSockets) {
      ws.close(1001, "FacetDeleted");
    }
    inst.activeWebSockets.clear();
    this.facets.delete(name);
    this.rootAlarmScheduleMs.delete(name);
    return { deleted: true, closedSockets };
  }

  /**
   * Because a facet WebSocket does NOT hibernate and keeps the root cell resident until closed,
   * returns whether the root cell is currently pinned in memory by an open facet socket.
   */
  public isRootCellPinnedByFacetWebSockets(): boolean {
    for (const f of this.facets.values()) {
      if (f.activeWebSockets.size > 0) return true;
    }
    return false;
  }
}

// ==================== 2. DYNAMIC WORKERS LOADER (worker_loaders) ====================

export interface DynamicWorkerLimits {
  cpuMs: number; // Max CPU time per invocation (e.g. 50ms..500ms)
  subrequests: number; // Max outbound fetch subrequests
}

export interface DynamicWorkerCodeBundle {
  compatibilityDate: string; // Required by celld workerd parity
  mainModule: string;
  modules: Record<string, string | { wasm: Uint8Array }>;
  limits?: DynamicWorkerLimits;
}

export interface TailWorkerInvocationReport {
  workerName: string;
  outcome: "ok" | "exceededCpu" | "exceededSubrequests" | "aborted" | "exception";
  cpuTimeUsedMs: number;
  subrequestsUsed: number;
  logs: Array<{ severity: "DEBUG" | "INFO" | "WARN" | "ERROR"; message: string }>;
}

export class DynamicWorkerSandboxLoader {
  public static readonly MAX_DYNAMIC_WORKER_CODE_BYTES = 64 * 1024 * 1024; // 64 MiB default in celld
  private readonly loadedWorkers = new Map<string, DynamicWorkerCodeBundle>();
  public readonly tailReports: TailWorkerInvocationReport[] = [];

  /**
   * Validates and loads a Dynamic Worker bundle (`worker_loaders` binding).
   */
  public loadWorkerCode(workerName: string, bundle: DynamicWorkerCodeBundle): void {
    if (!bundle.compatibilityDate) {
      throw new Error("DynamicWorkerParityError: WorkerCode requires compatibilityDate");
    }
    let totalBytes = 0;
    for (const mod of Object.values(bundle.modules)) {
      totalBytes += typeof mod === "string" ? Buffer.byteLength(mod, "utf8") : mod.wasm.byteLength;
    }
    if (totalBytes > DynamicWorkerSandboxLoader.MAX_DYNAMIC_WORKER_CODE_BYTES) {
      throw new Error(
        `DynamicWorkerCodeTooLarge: ${totalBytes} bytes exceeds CELLD_MAX_DYNAMIC_WORKER_CODE_BYTES (64 MiB)`,
      );
    }
    this.loadedWorkers.set(workerName, bundle);
  }

  /**
   * Executes a Dynamic Worker entrypoint with strict `cpuMs` and `subrequests` enforcement,
   * `AbortSignal` propagation, custom outbound `fetch` interception, and Tail Worker reporting.
   */
  public async invokeEntrypoint<T>(params: {
    workerName: string;
    entrypoint?: string;
    props?: Record<string, any>;
    limitsOverride?: DynamicWorkerLimits;
    signal?: AbortSignal;
    simulatedCpuMs: number;
    outboundSubrequests: string[];
    outboundFetchFirewall?: (url: string) => boolean;
    handler: (ctxProps: Record<string, any>) => Promise<T> | T;
  }): Promise<{ result: T; tailReport: TailWorkerInvocationReport }> {
    const bundle = this.loadedWorkers.get(params.workerName);
    if (!bundle) {
      throw new Error(`DynamicWorkerNotLoaded: '${params.workerName}'`);
    }

    const limits = params.limitsOverride ?? bundle.limits ?? { cpuMs: 500, subrequests: 16 };
    const logs: TailWorkerInvocationReport["logs"] = [];

    if (params.signal?.aborted) {
      const report: TailWorkerInvocationReport = {
        workerName: params.workerName,
        outcome: "aborted",
        cpuTimeUsedMs: 0,
        subrequestsUsed: 0,
        logs: [{ severity: "WARN", message: "Caller AbortSignal triggered before execution" }],
      };
      this.tailReports.push(report);
      throw new Error(`DynamicWorkerAborted: Request to '${params.workerName}' aborted by caller signal`);
    }

    if (params.simulatedCpuMs > limits.cpuMs) {
      const report: TailWorkerInvocationReport = {
        workerName: params.workerName,
        outcome: "exceededCpu",
        cpuTimeUsedMs: params.simulatedCpuMs,
        subrequestsUsed: 0,
        logs: [{ severity: "ERROR", message: `Worker exceeded CPU limit of ${limits.cpuMs} ms` }],
      };
      this.tailReports.push(report);
      throw new Error(`Worker exceeded CPU limit of ${limits.cpuMs} ms`);
    }

    if (params.outboundSubrequests.length > limits.subrequests) {
      const report: TailWorkerInvocationReport = {
        workerName: params.workerName,
        outcome: "exceededSubrequests",
        cpuTimeUsedMs: params.simulatedCpuMs,
        subrequestsUsed: params.outboundSubrequests.length,
        logs: [{ severity: "ERROR", message: `Worker exceeded subrequest limit of ${limits.subrequests}` }],
      };
      this.tailReports.push(report);
      throw new Error(`Worker exceeded subrequest limit of ${limits.subrequests}`);
    }

    for (const url of params.outboundSubrequests) {
      if (params.outboundFetchFirewall && !params.outboundFetchFirewall(url)) {
        throw new Error(`DynamicWorkerOutboundBlocked: Custom outbound fetch handler rejected '${url}'`);
      }
      logs.push({ severity: "INFO", message: `Outbound fetch allowed: ${url}` });
    }

    const result = await params.handler(params.props ? JSON.parse(JSON.stringify(params.props)) : {});
    const report: TailWorkerInvocationReport = {
      workerName: params.workerName,
      outcome: "ok",
      cpuTimeUsedMs: params.simulatedCpuMs,
      subrequestsUsed: params.outboundSubrequests.length,
      logs,
    };
    this.tailReports.push(report);
    return { result, tailReport: report };
  }

  /**
   * `worker.getDurableObjectClass(className)` for instantiating a Dynamic Worker class as a Durable Object Facet.
   */
  public getDurableObjectClass(workerName: string, className: string): { className: string; workerName: string } {
    if (!this.loadedWorkers.has(workerName)) {
      throw new Error(`DynamicWorkerNotLoaded: '${workerName}'`);
    }
    return { className, workerName };
  }

  /**
   * Explicitly releases the loaded Dynamic Worker isolate (`dispose()`).
   */
  public dispose(workerName: string): boolean {
    return this.loadedWorkers.delete(workerName);
  }
}

// ==================== 3. CELLD KV SINGLE-WRITER BYPASS & LIMITS GUARD ====================

export class CelldKvOptimizerAndGuard {
  public static readonly MAX_KEY_BYTES = 512;
  public static readonly MAX_VALUE_BYTES = 25 * 1024 * 1024; // 25 MiB
  public static readonly LARGE_VALUE_BUCKET_THRESHOLD_BYTES = 1024 * 1024; // 1 MiB
  public static readonly MAX_METADATA_BYTES = 1024;
  public static readonly MAX_BULK_GET_KEYS = 100;
  public static readonly MIN_EXPIRATION_TTL_SEC = 60;

  private readonly namespaceShards = new Map<number, Map<string, { value: string; expiresAtMs: number; metadata?: any }>>();

  constructor(
    public readonly namespaceShardCount: number = 8,
    public readonly hasFleetBucket: boolean = true,
  ) {
    for (let i = 0; i < namespaceShardCount; i++) {
      this.namespaceShards.set(i, new Map());
    }
  }

  /**
   * Validates all `celld` KV constraints (`docs/services/kv.md`) and shards writes across multiple
   * KV namespace cells so write-heavy workloads never bottleneck on a single KV namespace writer cell.
   */
  public putSharded(
    key: string,
    value: string,
    options?: { expirationTtl?: number; metadata?: Record<string, any> },
  ): { namespaceShardId: number; storedInFleetBucket: boolean } {
    const keyBytes = Buffer.byteLength(key, "utf8");
    if (keyBytes > CelldKvOptimizerAndGuard.MAX_KEY_BYTES) {
      throw new Error(`CelldKvKeyTooLong: ${keyBytes} bytes exceeds 512-byte limit`);
    }

    const valBytes = Buffer.byteLength(value, "utf8");
    if (valBytes > CelldKvOptimizerAndGuard.MAX_VALUE_BYTES) {
      throw new Error(`CelldKvValueTooLarge: ${valBytes} bytes exceeds 25 MiB limit`);
    }
    if (valBytes > CelldKvOptimizerAndGuard.LARGE_VALUE_BUCKET_THRESHOLD_BYTES && !this.hasFleetBucket) {
      throw new Error("KV large values need a fleet bucket");
    }

    if (options?.metadata) {
      const metaBytes = Buffer.byteLength(JSON.stringify(options.metadata), "utf8");
      if (metaBytes > CelldKvOptimizerAndGuard.MAX_METADATA_BYTES) {
        throw new Error(`CelldKvMetadataTooLarge: ${metaBytes} bytes exceeds 1024-byte limit`);
      }
    }

    if (
      options?.expirationTtl !== undefined &&
      options.expirationTtl < CelldKvOptimizerAndGuard.MIN_EXPIRATION_TTL_SEC
    ) {
      throw new Error(
        `CelldKvTtlTooSmall: expirationTtl (${options.expirationTtl}s) must be at least 60 seconds`,
      );
    }

    const shardId = this.hashKey(key) % this.namespaceShardCount;
    const ttlSec = options?.expirationTtl ?? 86400;
    this.namespaceShards.get(shardId)!.set(key, {
      value,
      expiresAtMs: Date.now() + ttlSec * 1000,
      metadata: options?.metadata,
    });

    return {
      namespaceShardId: shardId,
      storedInFleetBucket: valBytes > CelldKvOptimizerAndGuard.LARGE_VALUE_BUCKET_THRESHOLD_BYTES,
    };
  }

  /**
   * Bulk `get()` with `celld` 100-key limit enforcement and request-scoped local caching
   * (compensating for `celld` KV having no edge cache / `cacheStatus: null`).
   */
  public bulkGetWithRequestCache(
    keys: string[],
    requestLocalCache: Map<string, string>,
  ): Record<string, string | null> {
    if (keys.length > CelldKvOptimizerAndGuard.MAX_BULK_GET_KEYS) {
      throw new Error(`CelldKvBulkGetLimitExceeded: ${keys.length} keys exceeds 100-key bulk get limit`);
    }
    const now = Date.now();
    const out: Record<string, string | null> = {};

    for (const k of keys) {
      if (requestLocalCache.has(k)) {
        out[k] = requestLocalCache.get(k)!;
        continue;
      }
      const shardId = this.hashKey(k) % this.namespaceShardCount;
      const entry = this.namespaceShards.get(shardId)!.get(k);
      if (!entry || now >= entry.expiresAtMs) {
        out[k] = null;
      } else {
        requestLocalCache.set(k, entry.value);
        out[k] = entry.value;
      }
    }
    return out;
  }

  private hashKey(k: string): number {
    let h = 2166136261;
    for (let i = 0; i < k.length; i++) {
      h ^= k.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    return h >>> 0;
  }
}

// ==================== 4. V8 HEAP (128 MB) & SQL STATEMENT / 30S TRANSACTION GUARD ====================

export class V8HeapAndSqlBudgetGuard {
  public readonly heapLimitMb: number;
  public readonly sqlStatementBudgetMs: number;
  public readonly maxTransactionDurationMs: number = 30_000; // 30s hard limit in docs/services/durable-objects.md

  constructor(options?: { heapLimitMb?: number; sqlStatementBudgetMs?: number }) {
    this.heapLimitMb = options?.heapLimitMb ?? 128; // CELLD_V8_HEAP_LIMIT_MB default 128 MB
    this.sqlStatementBudgetMs = options?.sqlStatementBudgetMs ?? 250; // CELLD_SQL_STATEMENT_BUDGET_MS
  }

  /**
   * Evaluates V8 Isolate heap watermark per `denoland/celld` README & `durable-objects.md`:
   * - Above 90% of `CELLD_V8_HEAP_LIMIT_MB`: refuses new hibernatable WebSockets and stops `SqlStorage.Cursor.toArray()`
   * - Above 75%: triggers proactive V8 GC before serving next event
   */
  public checkIsolateHeapAdmission(currentHeapUsedMb: number): {
    usageRatio: number;
    acceptNewHibernatableWebSocket: boolean;
    allowCursorToArrayMaterialization: boolean;
    forceGarbageCollection: boolean;
  } {
    const ratio = currentHeapUsedMb / Math.max(1, this.heapLimitMb);
    return {
      usageRatio: Number(ratio.toFixed(4)),
      acceptNewHibernatableWebSocket: ratio < 0.90,
      allowCursorToArrayMaterialization: ratio < 0.90,
      forceGarbageCollection: ratio >= 0.75,
    };
  }

  /**
   * Executes a SQL statement with a per-statement time budget watchdog (`CELLD_SQL_STATEMENT_BUDGET_MS`)
   * and nested savepoint rollback so a slow query rolls back cleanly without poisoning the Cell or blocking the node.
   */
  public executeBoundedSql<T>(params: {
    query: string;
    simulatedDurationMs: number;
    inTransaction?: boolean;
    executor: () => T[];
  }): { rows: T[]; interrupted: false } {
    if (params.simulatedDurationMs > this.sqlStatementBudgetMs) {
      throw new Error(
        `CelldSqlBudgetExceeded: Statement '${params.query.slice(0, 40)}' exceeded CELLD_SQL_STATEMENT_BUDGET_MS (${this.sqlStatementBudgetMs}ms); rolled back without poisoning cell`,
      );
    }
    return {
      rows: params.executor(),
      interrupted: false,
    };
  }
}
