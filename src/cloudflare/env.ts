import * as fs from "node:fs";
import * as path from "node:path";
/**
 * @fileoverview M7 TASK-25, M59-1 & M59-2: Cloudflare Workers & denoland/celld Unified Runtime Bindings & SQL/KV/R2 Layer
 * with Composite Primary Key Support, `DELETE FROM` Disk Durability Flush, `SELECT ... LIMIT` Enforcement & KV `expirationTtl` Expiration.
 */
export interface DurableObjectIdLike {
  toString(): string;
  name?: string;
}

export interface DurableObjectStubLike {
  id: DurableObjectIdLike;
  fetch(input: string | URL | Request, init?: RequestInit): Promise<Response>;
}

export interface DurableObjectNamespaceLike {
  idFromName(name: string): DurableObjectIdLike;
  get(id: DurableObjectIdLike): DurableObjectStubLike;
}

export interface SqlCursorLike<T = Record<string, unknown>> {
  toArray(): T[];
  one(): T | null;
}

export interface SqlStorageLike {
  exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): SqlCursorLike<T>;
}

export interface DurableObjectStorageLike {
  sql: SqlStorageLike;
  get<T = unknown>(key: string): Promise<T | undefined>;
  put<T = unknown>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  setAlarm(scheduledTime: number | Date): Promise<void>;
  getAlarm(): Promise<number | null>;
}

export interface DurableObjectFacetsLike {
  get<T = DurableObjectStubLike>(
    facetName: string,
    initOrFactory?: () => { class: unknown; id?: string }
  ): T;
  abort(facetName: string, error?: Error): void;
  delete(facetName: string): Promise<void>;
}

export interface DurableObjectStateLike {
  id: DurableObjectIdLike;
  storage: DurableObjectStorageLike;
  facets?: DurableObjectFacetsLike;
  blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T>;
}

export interface KvNamespaceLike {
  get(key: string): Promise<string | null>;
  put(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>;
  delete(key: string): Promise<void>;
}

export interface R2ObjectBodyLike {
  key: string;
  etag: string;
  size: number;
  customMetadata: Record<string, string>;
  arrayBuffer(): Promise<ArrayBuffer>;
  text(): Promise<string>;
}

export interface R2BucketLike {
  put(
    key: string,
    value: Uint8Array | ArrayBuffer | string,
    options?: { customMetadata?: Record<string, string>; onlyIf?: { etagMatches?: string; etagDoesNotMatch?: string } }
  ): Promise<{ key: string; etag: string; size: number } | null>;
  get(key: string): Promise<R2ObjectBodyLike | null>;
  delete(key: string): Promise<void>;
  list(options?: { prefix?: string }): Promise<{ objects: Array<{ key: string; etag: string; size: number }> }>;
}

export interface QueueMessageLike<T = unknown> {
  id: string;
  timestamp: Date;
  body: T;
  attempts: number;
  ack(): void;
  retry(options?: { delaySeconds?: number }): void;
}

export interface QueueBatchLike<T = unknown> {
  queue: string;
  messages: Array<QueueMessageLike<T>>;
}

export interface QueueProducerLike<T = unknown> {
  send(body: T, options?: { contentType?: string; delaySeconds?: number }): Promise<void>;
}

export interface DynamicWorkerLoaderModuleSpec {
  compatibilityDate: string;
  mainModule: string;
  modules: Record<string, string | { js?: string; cjs?: string; text?: string; json?: unknown }>;
  env?: Record<string, unknown>;
  globalOutbound?: unknown | null;
}

export interface DynamicWorkerEntrypointLike {
  fetch(input: string | URL | Request, init?: RequestInit): Promise<Response>;
}

export interface DynamicWorkerStubLike {
  getEntrypoint(name?: string): DynamicWorkerEntrypointLike;
}

export interface DynamicWorkerLoaderBindingLike {
  get(
    id: string,
    loaderCallback: () => Promise<DynamicWorkerLoaderModuleSpec>
  ): DynamicWorkerStubLike;
}

export interface CloudflareCelldEnv {
  ANSTS_BODY_AUTH_SECRET?: string;
  ANSTS_BODY_CELL?: DurableObjectNamespaceLike;
  ANSTS_SCHEDULER_CELL: DurableObjectNamespaceLike;
  ANSTS_GATEWAY_CELL?: DurableObjectNamespaceLike;
  ANSTS_HARNESS_CELL: DurableObjectNamespaceLike;
  ANSTS_TUNNEL_CELL?: DurableObjectNamespaceLike;
  /** Operator write secret. Empty or unset fail-closes gateway lease release. Never a default. */
  ANSTS_CONSOLE_RBAC_SECRET?: string;
  ANSTS_IDEMPOTENCY_KV: KvNamespaceLike;
  ANSTS_CHECKPOINT_BUCKET: R2BucketLike;
  ANSTS_TASK_DISPATCH_QUEUE: QueueProducerLike;
  ANSTS_WEBHOOK_OUTBOX_QUEUE: QueueProducerLike;
  ANSTS_SANDBOX_LOADER?: DynamicWorkerLoaderBindingLike;
  CELLD_REGION?: string;
}

export class InMemoryCellSqlStorage implements SqlStorageLike {
  private tables: Map<string, Map<string, Record<string, unknown>>> = new Map();
  private primaryKeysByTable: Map<string, string[]> = new Map();
  private txSnapshot: Map<string, Map<string, Record<string, unknown>>> | null = null;
  public readonly storageFilePath: string | null;

  constructor(storageFilePath?: string) {
    this.storageFilePath = storageFilePath ?? null;
    if (this.storageFilePath) {
      this.rehydrateFromDisk();
    }
  }

  private rehydrateFromDisk(): void {
    if (!this.storageFilePath || !fs.existsSync(this.storageFilePath)) return;
    try {
      const raw = JSON.parse(fs.readFileSync(this.storageFilePath, "utf-8")) as {
        tables?: Record<string, Record<string, Record<string, unknown>>>;
        primaryKeysByTable?: Record<string, string[]>;
      };
      if (raw.primaryKeysByTable) {
        for (const [tblName, pkCols] of Object.entries(raw.primaryKeysByTable)) {
          if (Array.isArray(pkCols)) this.primaryKeysByTable.set(tblName, pkCols);
        }
      }
      if (raw.tables) {
        for (const [tblName, rowsObj] of Object.entries(raw.tables)) {
          const rowMap = new Map<string, Record<string, unknown>>();
          for (const [pk, r] of Object.entries(rowsObj)) {
            rowMap.set(pk, r);
          }
          this.tables.set(tblName, rowMap);
        }
      }
    } catch {
      // ignore corrupted or partial file
    }
  }

  private flushToDisk(): void {
    if (!this.storageFilePath || this.txSnapshot) return;
    try {
      fs.mkdirSync(path.dirname(this.storageFilePath), { recursive: true });
      const serialized: Record<string, Record<string, Record<string, unknown>>> = {};
      for (const [tblName, rowMap] of this.tables.entries()) {
        const rowsObj: Record<string, Record<string, unknown>> = {};
        for (const [pk, r] of rowMap.entries()) {
          rowsObj[pk] = r;
        }
        serialized[tblName] = rowsObj;
      }
      const pkSerialized: Record<string, string[]> = {};
      for (const [tblName, pkCols] of this.primaryKeysByTable.entries()) {
        pkSerialized[tblName] = pkCols;
      }
      const tmpPath = this.storageFilePath + ".tmp";
      fs.writeFileSync(
        tmpPath,
        JSON.stringify({ updatedAtMs: Date.now(), primaryKeysByTable: pkSerialized, tables: serialized })
      );
      fs.renameSync(tmpPath, this.storageFilePath);
    } catch {
      // ignore on read-only fs
    }
  }

  public exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): SqlCursorLike<T> {
    const q = query.trim().replace(/\s+/g, " ");
    const upper = q.toUpperCase();

    if (upper === "BEGIN" || upper === "BEGIN TRANSACTION") {
      this.txSnapshot = new Map();
      for (const [tName, rows] of this.tables.entries()) {
        const clonedRows = new Map<string, Record<string, unknown>>();
        for (const [pk, r] of rows.entries()) {
          clonedRows.set(pk, { ...r });
        }
        this.txSnapshot.set(tName, clonedRows);
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper === "COMMIT" || upper === "COMMIT TRANSACTION") {
      this.txSnapshot = null;
      this.flushToDisk();
      return { toArray: () => [], one: () => null };
    }

    if (upper === "ROLLBACK" || upper === "ROLLBACK TRANSACTION") {
      if (this.txSnapshot) {
        this.tables = this.txSnapshot;
        this.txSnapshot = null;
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("CREATE TABLE") || upper.startsWith("CREATE INDEX") || upper.startsWith("PRAGMA")) {
      const m = q.match(/CREATE TABLE(?: IF NOT EXISTS)?\s+([a-zA-Z0-9_]+)\s*\((.+)\)/i);
      if (m) {
        const tbl = m[1];
        const bodyDef = m[2];
        if (!this.tables.has(tbl)) {
          this.tables.set(tbl, new Map());
        }
        // [M59-1] Parse composite PRIMARY KEY (col1, col2, ...) if present so multi-column PK rows never overwrite one another
        const compositePkMatch = bodyDef.match(/PRIMARY\s+KEY\s*\(([^)]+)\)/i);
        if (compositePkMatch) {
          const pkCols = compositePkMatch[1].split(",").map((c) => c.trim());
          this.primaryKeysByTable.set(tbl, pkCols);
        } else {
          const inlinePkMatch = bodyDef.match(/([a-zA-Z0-9_]+)\s+[a-zA-Z0-9_]+\s+PRIMARY\s+KEY/i);
          if (inlinePkMatch) {
            this.primaryKeysByTable.set(tbl, [inlinePkMatch[1].trim()]);
          }
        }
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("INSERT") || upper.startsWith("REPLACE")) {
      const m = q.match(/(?:INSERT(?: OR REPLACE)?|REPLACE)\s+INTO\s+([a-zA-Z0-9_]+)\s*\(([^)]+)\)/i);
      if (m) {
        const tblName = m[1];
        const cols = m[2].split(",").map((c) => c.trim());
        let tbl = this.tables.get(tblName);
        if (!tbl) {
          tbl = new Map();
          this.tables.set(tblName, tbl);
        }
        const row: Record<string, unknown> = {};
        cols.forEach((col, i) => {
          row[col] = bindings[i];
        });
        const pkCols = this.primaryKeysByTable.get(tblName);
        const pk =
          pkCols && pkCols.length > 0 && pkCols.every((c) => c in row)
            ? pkCols.map((c) => String(row[c])).join("::")
            : String(bindings[0] ?? `${tbl.size + 1}`);
        tbl.set(pk, row);
        this.flushToDisk();
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("UPDATE ")) {
      const m = q.match(/UPDATE\s+([a-zA-Z0-9_]+)\s+SET\s+(.+?)\s+WHERE\s+(.+)$/i);
      if (m) {
        const tblName = m[1];
        const tbl = this.tables.get(tblName);
        const setClause = m[2];
        const whereClause = m[3];
        if (tbl) {
          const setCols = setClause.split(",").map((part) => part.split("=")[0].trim());
          const whereCols = whereClause.split(/\s+AND\s+/i).map((c) => c.split("=")[0].trim());
          const whereBindings = bindings.slice(setCols.length);
          for (const existing of tbl.values()) {
            const matched = whereCols.every((col, idx) => String(existing[col]) === String(whereBindings[idx]));
            if (matched) {
              setCols.forEach((c, idx) => {
                existing[c] = bindings[idx];
              });
            }
          }
        }
      }
      this.flushToDisk();
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("DELETE FROM")) {
      const m = q.match(/DELETE FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+(.+))?/i);
      if (m) {
        const tbl = this.tables.get(m[1]);
        if (tbl) {
          if (m[2] && bindings.length > 0) {
            const condCols = m[2].split(/\s+AND\s+/i).map((c) => c.split("=")[0].trim());
            for (const [pk, row] of tbl.entries()) {
              const matchAll = condCols.every((col, idx) => String(row[col]) === String(bindings[idx]));
              if (matchAll) tbl.delete(pk);
            }
          } else {
            tbl.clear();
          }
          // [M59-1] Flush DELETE mutations to disk so deleted rows never resurrect on restart
          this.flushToDisk();
        }
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("SELECT ")) {
      const mTable = q.match(/FROM\s+([a-zA-Z0-9_]+)/i);
      if (mTable) {
        const tbl = this.tables.get(mTable[1]);
        if (!tbl) return { toArray: () => [], one: () => null };
        let rows = Array.from(tbl.values());

        const mWhere = q.match(/WHERE\s+(.+?)(?:\s+ORDER BY|\s+LIMIT|$)/i);
        let whereBindingsConsumed = 0;
        if (mWhere && bindings.length > 0) {
          const condCols = mWhere[1].split(/\s+AND\s+/i).map((c) => c.split("=")[0].trim());
          whereBindingsConsumed = condCols.length;
          rows = rows.filter((r) =>
            condCols.every((col, idx) => String(r[col]) === String(bindings[idx]))
          );
        }

        const mOrder = q.match(/ORDER BY\s+([a-zA-Z0-9_]+)(?:\s+(ASC|DESC))?/i);
        if (mOrder) {
          const orderCol = mOrder[1];
          const desc = (mOrder[2] ?? "ASC").toUpperCase() === "DESC";
          rows.sort((a, b) => {
            const va = a[orderCol] as number | string;
            const vb = b[orderCol] as number | string;
            if (va === vb) return 0;
            const cmp = va > vb ? 1 : -1;
            return desc ? -cmp : cmp;
          });
        }

        // [M59-1] Enforce LIMIT N (literal or bound parameter `?`)
        const mLimit = q.match(/LIMIT\s+(\?|\d+)/i);
        if (mLimit) {
          const rawLim =
            mLimit[1] === "?" ? Number(bindings[whereBindingsConsumed] ?? rows.length) : Number(mLimit[1]);
          if (Number.isFinite(rawLim) && rawLim >= 0) {
            rows = rows.slice(0, Math.floor(rawLim));
          }
        }

        if (upper.includes("COUNT(*)")) {
          const cntRow = { cnt: rows.length, "COUNT(*)": rows.length } as unknown as T;
          return { toArray: () => [cntRow], one: () => cntRow };
        }
        return {
          toArray: () => rows as unknown as T[],
          one: () => (rows[0] as unknown as T) ?? null,
        };
      }
    }

    return { toArray: () => [], one: () => null };
  }
}

export class MockDurableObjectState implements DurableObjectStateLike {
  public readonly id: DurableObjectIdLike;
  public readonly storage: DurableObjectStorageLike;
  private readonly kvMap: Map<string, unknown> = new Map();
  private alarmTime: number | null = null;

  constructor(idStr = "cell-default-01") {
    this.id = { toString: () => idStr, name: idStr };
    const sql = new InMemoryCellSqlStorage();
    this.storage = {
      sql,
      get: async <T = unknown>(key: string) => this.kvMap.get(key) as T | undefined,
      put: async <T = unknown>(key: string, value: T) => {
        this.kvMap.set(key, value);
      },
      delete: async (key: string) => this.kvMap.delete(key),
      setAlarm: async (scheduledTime: number | Date) => {
        this.alarmTime = typeof scheduledTime === "number" ? scheduledTime : scheduledTime.getTime();
      },
      getAlarm: async () => this.alarmTime,
    };
  }

  public async blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T> {
    return callback();
  }
}

export class InMemoryKvNamespace implements KvNamespaceLike {
  private readonly store: Map<string, { value: string; expiresAtMs: number | null }> = new Map();

  public async get(key: string, nowMs = Date.now()): Promise<string | null> {
    const entry = this.store.get(key);
    if (!entry) return null;
    if (entry.expiresAtMs !== null && nowMs >= entry.expiresAtMs) {
      this.store.delete(key);
      return null;
    }
    return entry.value;
  }

  public async put(
    key: string,
    value: string,
    options?: { expirationTtl?: number; nowMs?: number }
  ): Promise<void> {
    const nowMs = options?.nowMs ?? Date.now();
    const ttlSec = options?.expirationTtl;
    const expiresAtMs =
      ttlSec !== undefined && Number.isFinite(ttlSec) && ttlSec > 0 ? nowMs + ttlSec * 1000 : null;
    this.store.set(key, { value, expiresAtMs });
  }

  public async delete(key: string): Promise<void> {
    this.store.delete(key);
  }
}

export class InMemoryR2Bucket implements R2BucketLike {
  private readonly objects: Map<string, { bytes: Uint8Array; etag: string; customMetadata: Record<string, string> }> =
    new Map();
  private seq = 0;

  public async put(
    key: string,
    value: Uint8Array | ArrayBuffer | string,
    options?: { customMetadata?: Record<string, string>; onlyIf?: { etagMatches?: string; etagDoesNotMatch?: string } }
  ): Promise<{ key: string; etag: string; size: number } | null> {
    const existing = this.objects.get(key);
    if (options?.onlyIf?.etagDoesNotMatch === "*" && existing) {
      return null;
    }
    if (options?.onlyIf?.etagMatches && (!existing || existing.etag !== options.onlyIf.etagMatches)) {
      return null;
    }
    this.seq += 1;
    const bytes =
      typeof value === "string"
        ? new TextEncoder().encode(value)
        : value instanceof Uint8Array
          ? value
          : new Uint8Array(value);
    const etag = `"etag-${this.seq}-${bytes.byteLength}"`;
    this.objects.set(key, {
      bytes,
      etag,
      customMetadata: options?.customMetadata ?? {},
    });
    return { key, etag, size: bytes.byteLength };
  }

  public async get(key: string): Promise<R2ObjectBodyLike | null> {
    const obj = this.objects.get(key);
    if (!obj) return null;
    return {
      key,
      etag: obj.etag,
      size: obj.bytes.byteLength,
      customMetadata: obj.customMetadata,
      arrayBuffer: async () => obj.bytes.slice().buffer as ArrayBuffer,
      text: async () => new TextDecoder().decode(obj.bytes),
    };
  }

  public async delete(key: string): Promise<void> {
    this.objects.delete(key);
  }

  public async list(options?: { prefix?: string }): Promise<{ objects: Array<{ key: string; etag: string; size: number }> }> {
    const prefix = options?.prefix ?? "";
    const out: Array<{ key: string; etag: string; size: number }> = [];
    for (const [k, v] of this.objects.entries()) {
      if (k.startsWith(prefix)) {
        out.push({ key: k, etag: v.etag, size: v.bytes.byteLength });
      }
    }
    return { objects: out };
  }
}

export class InMemoryQueueProducer<T = unknown> implements QueueProducerLike<T> {
  public readonly sentMessages: Array<{ body: T; delaySeconds?: number }> = [];
  public async send(body: T, options?: { delaySeconds?: number }): Promise<void> {
    this.sentMessages.push({ body, delaySeconds: options?.delaySeconds });
  }
}

export class DurableDiskCellSqlStorage extends InMemoryCellSqlStorage {
  constructor(storagePathOrDir: string, cellName?: string) {
    const resolvedPath = cellName
      ? path.join(storagePathOrDir, `${cellName}.sqlite.json`)
      : fs.existsSync(storagePathOrDir) && fs.statSync(storagePathOrDir).isDirectory()
        ? path.join(storagePathOrDir, "cell-default.sqlite.json")
        : storagePathOrDir;
    super(resolvedPath);
  }
}
