/** Stable names for the persistence owners defined by the Celld design. */
export const PERSISTENCE_CELL_TYPES = [
  "tenant-ledger",
  "task",
  "scheduler-pool",
  "node-capacity",
  "dag",
  "provider-quota",
  "routing-config",
  "tenant-directory",
] as const;

export type PersistenceCellType = (typeof PERSISTENCE_CELL_TYPES)[number];

/** The persisted identity is the exact logical key routed to one Cell namespace. */
export interface PersistenceCellIdentity {
  type: PersistenceCellType;
  key: string;
}

export interface PersistenceSqlCursor<T> {
  toArray(): T[];
  one(): T | null;
}

/** Only synchronous SQL belongs inside a Cell transaction. */
export interface PersistenceSqlStorage {
  exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): PersistenceSqlCursor<T>;
  /** Legacy test adapter only; Celld exposes transactions on DurableObjectStorage. */
  transactionSync?<T>(callback: () => T): T;
}

/** storage.sync is Celld-specific and waits for the configured durability proof. */
export interface PersistenceObjectStorage {
  sql: PersistenceSqlStorage;
  transactionSync?<T>(callback: () => T): T;
  sync?(): Promise<void>;
}

export interface PersistenceCellStub {
  fetch(input: RequestInfo, init?: RequestInit): Promise<Response>;
}

export interface PersistenceCellId {
  toString(): string;
  name?: string;
}

export interface PersistenceCellNamespace {
  idFromName(name: string): PersistenceCellId;
  get(id: PersistenceCellId): PersistenceCellStub;
}

export interface PersistenceCellBindings {
  TENANT_LEDGER_CELL?: PersistenceCellNamespace;
  TASK_CELL?: PersistenceCellNamespace;
  SCHEDULER_POOL_CELL?: PersistenceCellNamespace;
  NODE_CAPACITY_CELL?: PersistenceCellNamespace;
  DAG_CELL?: PersistenceCellNamespace;
  PROVIDER_QUOTA_CELL?: PersistenceCellNamespace;
  ROUTING_CONFIG_CELL?: PersistenceCellNamespace;
  TENANT_DIRECTORY_CELL?: PersistenceCellNamespace;
}

export interface CellOperationCommand {
  operationId: string;
  operationType: string;
  resourceId: string;
  tenantId: string;
  payload: unknown;
  aggregateType: string;
  aggregateId: string;
  expectedVersion: number;
}

export interface CellOutboxDraft {
  eventId: string;
  eventType: string;
  tenantId: string;
  payload: unknown;
}

export interface CellOutboxEvent extends CellOutboxDraft {
  operationId: string;
  payloadDigest: string;
  aggregateType: string;
  aggregateId: string;
  aggregateVersion: number;
  createdAt: number;
}

export interface CellInboxEvent {
  eventId: string;
  eventType: string;
  tenantId: string;
  aggregateType: string;
  aggregateId: string;
  aggregateVersion: number;
  payloadDigest: string;
  payload: unknown;
}

export interface CellMigration {
  version: number;
  description: string;
  up(sql: PersistenceSqlStorage): void;
}

export type CellOperationOutcome = "APPLIED" | "REPLAYED";
