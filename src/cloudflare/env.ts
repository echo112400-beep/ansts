/**
 * Cloudflare Workers & denoland/celld Runtime Bindings, Durable Object Base Interfaces,
 * and In-Process Simulator Mocks for Local CI Verification.
 */

export interface SqlStorageCursor<T = Record<string, any>> {
  toArray(): T[];
  one(): T | null;
}

export interface SqlStorage {
  exec<T = Record<string, any>>(query: string, ...bindings: any[]): SqlStorageCursor<T>;
}

export interface CompanionContainer {
  running: boolean;
  allocatedCpuCores: number;
  start(): Promise<void>;
  stop(): Promise<void>;
  fetch(req: RequestInfo, init?: RequestInit): Promise<Response>;
}

export interface DurableObjectStorage {
  sql: SqlStorage;
  get<T = any>(key: string): Promise<T | undefined>;
  put<T = any>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  setAlarm(scheduledTime: number | Date): Promise<void>;
  getAlarm(): Promise<number | null>;
}

export interface DurableObjectState {
  id: { toString(): string; name?: string };
  storage: DurableObjectStorage;
  container?: CompanionContainer;
  acceptWebSocket(ws: WebSocketLike, tags?: string[]): void;
  getWebSockets(tag?: string): WebSocketLike[];
  waitUntil(promise: Promise<any>): void;
}

export interface WebSocketLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  serializeAttachment?(attachment: any): void;
  deserializeAttachment?(): any;
}

export interface DurableObjectStub {
  fetch(input: RequestInfo, init?: RequestInit): Promise<Response>;
}

export interface DurableObjectNamespace {
  idFromName(name: string): { toString(): string; name: string };
  get(id: { toString(): string; name?: string }): DurableObjectStub;
}

export interface KVNamespaceBinding {
  get(key: string, type?: "text" | "json"): Promise<any>;
  put(key: string, value: string, options?: { expirationTtl?: number; metadata?: any }): Promise<void>;
  delete(key: string): Promise<void>;
}

export interface R2ObjectBody {
  key: string;
  body: string;
  customMetadata?: Record<string, string>;
  text(): Promise<string>;
}

export interface R2BucketBinding {
  put(
    key: string,
    value: string,
    options?: { onlyIf?: { etagDoesNotMatch?: string }; customMetadata?: Record<string, string> },
  ): Promise<{ key: string } | null>;
  get(key: string): Promise<R2ObjectBody | null>;
  list(options?: { prefix?: string }): Promise<{ objects: Array<{ key: string }> }>;
}

export interface D1PreparedStatement {
  bind(...values: any[]): D1PreparedStatement;
  run(): Promise<{ success: boolean }>;
  all<T = Record<string, any>>(): Promise<{ results: T[] }>;
}

export interface D1DatabaseBinding {
  prepare(query: string): D1PreparedStatement;
}

export interface QueueMessage<T = any> {
  id: string;
  timestamp: Date;
  body: T;
  ack(): void;
  retry(): void;
}

export interface MessageBatch<T = any> {
  queue: string;
  messages: Array<QueueMessage<T>>;
}

export interface QueueBinding {
  send(message: any): Promise<void>;
}

export interface CloudflareCelldEnv {
  SCHEDULER_CELL: DurableObjectNamespace;
  AGENT_HARNESS_CELL: DurableObjectNamespace;
  ANSTS_D1: D1DatabaseBinding;
  ANSTS_KV: KVNamespaceBinding;
  ANSTS_CHECKPOINTS_R2: R2BucketBinding;
  ANSTS_EVENTS_QUEUE: QueueBinding;
  PRIMARY_MODEL_POOL?: string;
  FAST_UTILITY_POOL?: string;
  FALLBACK_MODEL_POOL?: string;
  WEBHOOK_HMAC_SECRET?: string;
}

/**
 * Lightweight SQLite/KV/R2/Container simulator for testing Durable Objects & Workers
 * in Node.js CI while remaining 100% API-compatible with `celld` and Cloudflare Workers.
 */
export class InMemoryCellSqlStorage implements SqlStorage {
  public tables: Map<string, Array<Record<string, any>>> = new Map();

  public exec<T = Record<string, any>>(query: string, ...bindings: any[]): SqlStorageCursor<T> {
    const q = query.trim();
    const upper = q.toUpperCase();

    if (upper.startsWith("CREATE TABLE")) {
      const match = q.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)/i);
      if (match) {
        const tableName = match[1];
        if (!this.tables.has(tableName)) {
          this.tables.set(tableName, []);
        }
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("INSERT INTO")) {
      const match = q.match(/INSERT\s+INTO\s+([a-zA-Z0-9_]+)\s*\(([^)]+)\)/i);
      if (match) {
        const tableName = match[1];
        const cols = match[2].split(",").map((c) => c.trim());
        const row: Record<string, any> = {};
        cols.forEach((col, idx) => {
          row[col] = bindings[idx];
        });
        const rows = this.tables.get(tableName) ?? [];
        rows.push(row);
        this.tables.set(tableName, rows);
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("DELETE FROM")) {
      const match = q.match(/DELETE\s+FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+([a-zA-Z0-9_]+)\s*>\s*\?)?/i);
      if (match) {
        const tableName = match[1];
        const col = match[2];
        const rows = this.tables.get(tableName) ?? [];
        if (col && bindings.length > 0) {
          this.tables.set(
            tableName,
            rows.filter((r) => !(r[col] > bindings[0])),
          );
        } else {
          this.tables.set(tableName, []);
        }
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("SELECT")) {
      const match = q.match(/FROM\s+([a-zA-Z0-9_]+)/i);
      const tableName = match ? match[1] : "";
      const rows = (this.tables.get(tableName) ?? []) as T[];
      return {
        toArray: () => [...rows],
        one: () => (rows.length > 0 ? rows[rows.length - 1] : null),
      };
    }

    return { toArray: () => [], one: () => null };
  }
}

export class MockDurableObjectState implements DurableObjectState {
  public id: { toString(): string; name?: string };
  public storage: DurableObjectStorage;
  public container: CompanionContainer;
  private kv: Map<string, any> = new Map();
  private sqlStore = new InMemoryCellSqlStorage();
  private alarmTs: number | null = null;
  private websockets: Array<{ ws: WebSocketLike; tags: string[]; attachment?: any }> = [];

  constructor(name: string = "default-cell") {
    this.id = { toString: () => `cell_${name}`, name };
    this.storage = {
      sql: this.sqlStore,
      get: async <T = any>(key: string) => this.kv.get(key) as T | undefined,
      put: async <T = any>(key: string, value: T) => {
        this.kv.set(key, value);
      },
      delete: async (key: string) => this.kv.delete(key),
      setAlarm: async (time: number | Date) => {
        this.alarmTs = typeof time === "number" ? time : time.getTime();
      },
      getAlarm: async () => this.alarmTs,
    };
    this.container = {
      running: false,
      allocatedCpuCores: 0.1,
      start: async () => {
        this.container.running = true;
        this.container.allocatedCpuCores = 8.0;
      },
      stop: async () => {
        this.container.running = false;
        this.container.allocatedCpuCores = 0.0;
      },
      fetch: async (req: RequestInfo) => {
        const url = typeof req === "string" ? req : (req as Request).url;
        return new Response(
          JSON.stringify({ container_executed: true, url, burst_cores: this.container.allocatedCpuCores }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      },
    };
  }

  public acceptWebSocket(ws: WebSocketLike, tags: string[] = []): void {
    this.websockets.push({ ws, tags });
  }

  public getWebSockets(tag?: string): WebSocketLike[] {
    return this.websockets
      .filter((item) => !tag || item.tags.includes(tag))
      .map((item) => item.ws);
  }

  public waitUntil(_promise: Promise<any>): void {}
}
