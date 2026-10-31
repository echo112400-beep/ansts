import {
  CellOperationCommand,
  CellOperationOutcome,
  CellOutboxDraft,
  PersistenceObjectStorage,
  PersistenceSqlStorage,
} from "./contracts";
import { confirmCellWrite, withCellTransaction } from "./transactions";
import { digestJson } from "./digest";
import {
  assertCellOutboxReplay,
  prepareCellOutboxEvent,
  PreparedCellOutboxEvent,
  stageCellOutboxEvent,
} from "./outbox";

export class CellOperationConflictError extends Error {
  public readonly code = "CELL_OPERATION_CONFLICT";

  constructor(operationId: string) {
    super(`Operation ${operationId} was already used with different content or resource identity.`);
    this.name = "CellOperationConflictError";
  }
}

export class CellVersionConflictError extends Error {
  public readonly code = "CELL_VERSION_CONFLICT";

  constructor(aggregateType: string, aggregateId: string, expected: number, actual: number) {
    super(`Aggregate ${aggregateType}/${aggregateId} has version ${actual}; expected ${expected}.`);
    this.name = "CellVersionConflictError";
  }
}

export interface CellOperationResult<T> {
  outcome: CellOperationOutcome;
  aggregateVersion: number;
  result: T;
}

interface OperationRow {
  operation_id: string;
  operation_type: string;
  resource_id: string;
  tenant_id: string;
  payload_digest: string;
  aggregate_type: string;
  aggregate_id: string;
  expected_version: number;
  aggregate_version: number;
  result_json: string;
}

interface VersionRow {
  version: number;
}

/**
 * Atomically applies a local owner mutation, aggregate version, operation result, and outbox drafts.
 * `apply` is synchronous by design: it must not wait for a network or another Cell.
 */
export async function executeCellOperation<T>(
  storage: PersistenceObjectStorage,
  command: CellOperationCommand,
  apply: (sql: PersistenceSqlStorage) => T,
  outboxEvents: CellOutboxDraft[] = [],
): Promise<CellOperationResult<T>> {
  validateCommand(command);
  const payloadDigest = (await digestJson(command.payload)).sha256;
  const preparedEvents: PreparedCellOutboxEvent[] = await Promise.all(
    outboxEvents.map((draft) => prepareCellOutboxEvent(command, draft)),
  );
  const committed = withCellTransaction(storage.sql, () => {
    const existing = storage.sql
      .exec<OperationRow>("SELECT * FROM cell_operations WHERE operation_id = ?", command.operationId)
      .toArray()[0];
    if (existing) {
      assertSameOperation(existing, command, payloadDigest);
      assertCellOutboxReplay(storage.sql, command.operationId, preparedEvents, Number(existing.aggregate_version));
      return {
        outcome: "REPLAYED" as const,
        aggregateVersion: Number(existing.aggregate_version),
        result: JSON.parse(existing.result_json) as T,
      };
    }

    const currentVersion = readAggregateVersion(storage.sql, command.aggregateType, command.aggregateId);
    if (currentVersion !== command.expectedVersion) {
      throw new CellVersionConflictError(command.aggregateType, command.aggregateId, command.expectedVersion, currentVersion);
    }

    const result = apply(storage.sql);
    if (isThenable(result)) {
      throw new TypeError("Cell operation callbacks must be synchronous; external I/O belongs after commit.");
    }
    const resultJson = JSON.stringify(result);
    if (resultJson === undefined) {
      throw new TypeError("Cell operation results must be JSON serializable.");
    }
    const nextVersion = currentVersion + 1;
    if (currentVersion === 0) {
      storage.sql.exec(
        "INSERT INTO cell_aggregate_versions (aggregate_type, aggregate_id, version) VALUES (?, ?, ?)",
        command.aggregateType,
        command.aggregateId,
        nextVersion,
      );
    } else {
      storage.sql.exec(
        "UPDATE cell_aggregate_versions SET version = ? WHERE aggregate_type = ? AND aggregate_id = ?",
        nextVersion,
        command.aggregateType,
        command.aggregateId,
      );
    }
    storage.sql.exec(
      `INSERT INTO cell_operations (
        operation_id, operation_type, resource_id, tenant_id, payload_digest,
        aggregate_type, aggregate_id, expected_version, aggregate_version, result_json, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      command.operationId,
      command.operationType,
      command.resourceId,
      command.tenantId,
      payloadDigest,
      command.aggregateType,
      command.aggregateId,
      command.expectedVersion,
      nextVersion,
      resultJson,
      Date.now(),
    );
    for (const event of preparedEvents) {
      stageCellOutboxEvent(storage.sql, event, nextVersion);
    }
    return { outcome: "APPLIED" as const, aggregateVersion: nextVersion, result };
  });

  await confirmCellWrite(storage);
  return committed;
}

function readAggregateVersion(sql: PersistenceSqlStorage, aggregateType: string, aggregateId: string): number {
  const row = sql
    .exec<VersionRow>(
      "SELECT version FROM cell_aggregate_versions WHERE aggregate_type = ? AND aggregate_id = ?",
      aggregateType,
      aggregateId,
    )
    .toArray()[0];
  return row ? Number(row.version) : 0;
}

function assertSameOperation(row: OperationRow, command: CellOperationCommand, payloadDigest: string): void {
  if (
    row.operation_type !== command.operationType ||
    row.resource_id !== command.resourceId ||
    row.tenant_id !== command.tenantId ||
    row.payload_digest !== payloadDigest ||
    row.aggregate_type !== command.aggregateType ||
    row.aggregate_id !== command.aggregateId ||
    Number(row.expected_version) !== command.expectedVersion
  ) {
    throw new CellOperationConflictError(command.operationId);
  }
}

function validateCommand(command: CellOperationCommand): void {
  validateText(command.operationId, "operationId");
  validateText(command.operationType, "operationType");
  validateText(command.resourceId, "resourceId");
  validateText(command.tenantId, "tenantId");
  validateText(command.aggregateType, "aggregateType");
  validateText(command.aggregateId, "aggregateId");
  if (!Number.isSafeInteger(command.expectedVersion) || command.expectedVersion < 0) {
    throw new TypeError("expectedVersion must be a non-negative safe integer.");
  }
}

function validateText(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0) throw new TypeError(`${label} must be non-empty.`);
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return Boolean(value && (typeof value === "object" || typeof value === "function") && "then" in value);
}
