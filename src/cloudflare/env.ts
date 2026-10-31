/**
 * Cloudflare Workers & denoland/celld Runtime Bindings, Durable Object Base Interfaces,
 * and In-Process Simulator Mocks for Local CI Verification.
 */

import {
  PersistenceCellBindings,
  PersistenceObjectStorage,
  PersistenceSqlCursor,
  PersistenceSqlStorage,
} from "../persistence/contracts";

export interface SqlStorageCursor<T = Record<string, any>> extends PersistenceSqlCursor<T> {}

export interface SqlStorage extends PersistenceSqlStorage {}

export interface CompanionContainer {
  running: boolean;
  allocatedCpuCores: number;
  start(): Promise<void>;
  stop(): Promise<void>;
  fetch(req: RequestInfo, init?: RequestInit): Promise<Response>;
}

export interface DurableObjectStorage extends PersistenceObjectStorage {
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

export interface CloudflareCelldEnv extends PersistenceCellBindings {
  SCHEDULER_CELL: DurableObjectNamespace;
  AGENT_HARNESS_CELL: DurableObjectNamespace;
  TENANT_LEDGER_CELL: DurableObjectNamespace;
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
 * Lightweight in-memory subset for request and protocol tests. Its transaction rollback and
 * sync counter are simulations only; they do not model SQLite recovery, Celld replication,
 * bucket confirmation, or the durability proof used by a real fleet.
 */
export class InMemoryCellSqlStorage implements SqlStorage {
  public tables: Map<string, Array<Record<string, any>>> = new Map();

  public transactionSync<T>(callback: () => T): T {
    const before = new Map(
      Array.from(this.tables.entries(), ([name, rows]) => [name, rows.map((row) => ({ ...row }))]),
    );
    try {
      const result = callback();
      if (result && (typeof result === "object" || typeof result === "function") && "then" in result) {
        throw new TypeError("InMemoryCellSqlStorage.transactionSync callback cannot return a Promise.");
      }
      return result;
    } catch (error) {
      this.tables.clear();
      for (const [name, rows] of before) this.tables.set(name, rows);
      throw error;
    }
  }

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

    if (upper.startsWith("UPDATE")) {
      const match = q.match(/^UPDATE\s+([a-zA-Z0-9_]+)\s+SET\s+(.+?)\s+WHERE\s+(.+)$/i);
      if (match) {
        const [, tableName, setSql, whereSql] = match;
        const setFields = setSql.split(",").map((part) => part.trim().match(/^([a-zA-Z0-9_]+)\s*=\s*\?$/));
        if (setFields.every(Boolean)) {
          const setValues = bindings.slice(0, setFields.length);
          const whereBindings = bindings.slice(setFields.length);
          const rows = this.tables.get(tableName) ?? [];
          for (const row of rows) {
            if (!this.matchesWhere(row, whereSql, whereBindings)) continue;
            setFields.forEach((field, index) => {
              row[field![1]] = setValues[index];
            });
          }
        }
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("SELECT")) {
      const match = q.match(
        /^SELECT\s+(.+?)\s+FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?(?:\s+LIMIT\s+(\?|[0-9]+))?$/i,
      );
      if (!match) return { toArray: () => [], one: () => null };
      const [, columns, tableName, whereSql, orderSql, limitSql] = match;
      let rows = [...(this.tables.get(tableName) ?? [])];
      let whereBindings = bindings;
      if (whereSql) {
        const conditionCount = whereSql.split(/\s+AND\s+/i).length;
        whereBindings = bindings.slice(0, conditionCount);
        rows = rows.filter((row) => this.matchesWhere(row, whereSql, whereBindings));
      }
      if (orderSql) {
        const orderFields = orderSql.split(",").map((field) => {
          const parsed = field.trim().match(/^([a-zA-Z0-9_]+)(?:\s+(ASC|DESC))?$/i);
          return parsed ? { column: parsed[1], descending: parsed[2]?.toUpperCase() === "DESC" } : null;
        });
        if (orderFields.every(Boolean)) {
          rows.sort((left, right) => {
            for (const field of orderFields) {
              const column = field!.column;
              if (left[column] === right[column]) continue;
              const order = left[column] < right[column] ? -1 : 1;
              return field!.descending ? -order : order;
            }
            return 0;
          });
        }
      }
      if (limitSql) {
        const limit = limitSql === "?" ? Number(bindings[whereBindings.length]) : Number(limitSql);
        rows = rows.slice(0, limit);
      }
      if (columns.trim() !== "*") {
        const selected = columns.split(",").map((col) => col.trim().match(/^([a-zA-Z0-9_]+)(?:\s+AS\s+([a-zA-Z0-9_]+))?$/i));
        if (selected.every(Boolean)) {
          rows = rows.map((row) => {
            const result: Record<string, any> = {};
            for (const column of selected) result[column![2] ?? column![1]] = row[column![1]];
            return result;
          });
        }
      }
      const resultRows = rows as T[];
      return {
        toArray: () => [...resultRows],
        one: () => (resultRows.length > 0 ? resultRows[resultRows.length - 1] : null),
      };
    }

    return { toArray: () => [], one: () => null };
  }

  private matchesWhere(row: Record<string, any>, whereSql: string, bindings: any[]): boolean {
    const clauses = whereSql.split(/\s+AND\s+/i);
    let bindingIndex = 0;
    for (const clause of clauses) {
      const match = clause.trim().match(/^([a-zA-Z0-9_]+)\s*(=|<=|>=|<>|<|>)\s*\?$/i);
      if (!match) return false;
      const [, column, operator] = match;
      const left = row[column];
      const right = bindings[bindingIndex++];
      if (operator === "=" && left !== right) return false;
      if (operator === "<>" && left === right) return false;
      if (operator === "<" && !(left < right)) return false;
      if (operator === ">" && !(left > right)) return false;
      if (operator === "<=" && !(left <= right)) return false;
      if (operator === ">=" && !(left >= right)) return false;
    }
    return true;
  }
}

export class MockDurableObjectState implements DurableObjectState {
  public id: { toString(): string; name?: string };
  public storage: DurableObjectStorage;
  public container: CompanionContainer;
  public durableSyncCalls = 0;
  private kv: Map<string, any> = new Map();
  private sqlStore = new InMemoryCellSqlStorage();
  private alarmTs: number | null = null;
  private websockets: Array<{ ws: WebSocketLike; tags: string[]; attachment?: any }> = [];

  constructor(name: string = "default-cell") {
    this.id = { toString: () => `cell_${name}`, name };
    this.storage = {
      sql: this.sqlStore,
      // This is a no-op proof counter in the simulator; it does not model Celld replication or bucket durability.
      sync: async () => {
        this.durableSyncCalls += 1;
      },
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
