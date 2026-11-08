/**
 * @fileoverview M7 TASK-25: Cloudflare Workers & denoland/celld Unified Runtime Bindings & SQL/KV/R2 Mock Layer.
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
  ANSTS_SCHEDULER_CELL: DurableObjectNamespaceLike;
  ANSTS_HARNESS_CELL: DurableObjectNamespaceLike;
  ANSTS_IDEMPOTENCY_KV: KvNamespaceLike;
  ANSTS_CHECKPOINT_BUCKET: R2BucketLike;
  ANSTS_TASK_DISPATCH_QUEUE: QueueProducerLike;
  ANSTS_WEBHOOK_OUTBOX_QUEUE: QueueProducerLike;
  ANSTS_SANDBOX_LOADER?: DynamicWorkerLoaderBindingLike;
  CELLD_REGION?: string;
}

export class InMemoryCellSqlStorage implements SqlStorageLike {
  private readonly tables: Map<string, Map<string, Record<string, unknown>>> = new Map();

  public exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): SqlCursorLike<T> {
    const q = query.trim().replace(/\s+/g, " ");
    const upper = q.toUpperCase();

    if (upper.startsWith("CREATE TABLE") || upper.startsWith("CREATE INDEX") || upper.startsWith("PRAGMA")) {
      const m = q.match(/CREATE TABLE(?: IF NOT EXISTS)?\s+([a-zA-Z0-9_]+)/i);
      if (m) {
        const tbl = m[1];
        if (!this.tables.has(tbl)) {
          this.tables.set(tbl, new Map());
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
        const pk = String(bindings[0] ?? `${tbl.size + 1}`);
        tbl.set(pk, row);
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("UPDATE ")) {
      const m = q.match(/UPDATE\s+([a-zA-Z0-9_]+)\s+SET\s+(.+?)\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*\?/i);
      if (m) {
        const tbl = this.tables.get(m[1]);
        const setClause = m[2];
        const pkValue = String(bindings[bindings.length - 1]);
        if (tbl && tbl.has(pkValue)) {
          const existing = tbl.get(pkValue)!;
          const setCols = setClause.split(",").map((part) => part.split("=")[0].trim());
          setCols.forEach((c, idx) => {
            if (idx < bindings.length - 1) {
              existing[c] = bindings[idx];
            }
          });
        }
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("DELETE FROM")) {
      const m = q.match(/DELETE FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*\?)?/i);
      if (m) {
        const tbl = this.tables.get(m[1]);
        if (tbl) {
          if (m[2] && bindings.length > 0) {
            tbl.delete(String(bindings[0]));
          } else {
            tbl.clear();
          }
        }
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("SELECT ")) {
      const m = q.match(/FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*\?)?/i);
      if (m) {
        const tbl = this.tables.get(m[1]);
        if (!tbl) return { toArray: () => [], one: () => null };
        let rows = Array.from(tbl.values());
        if (m[2] && bindings.length > 0) {
          const col = m[2];
          rows = rows.filter((r) => String(r[col]) === String(bindings[0]));
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
  private readonly store: Map<string, string> = new Map();
  public async get(key: string): Promise<string | null> {
    return this.store.get(key) ?? null;
  }
  public async put(key: string, value: string): Promise<void> {
    this.store.set(key, value);
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
      arrayBuffer: async () => obj.bytes.buffer.slice(obj.bytes.byteOffset, obj.bytes.byteOffset + obj.bytes.byteLength),
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
