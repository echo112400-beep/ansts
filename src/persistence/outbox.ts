import { CellOutboxDraft, CellOutboxEvent, PersistenceObjectStorage, PersistenceSqlStorage } from "./contracts";
import { confirmCellWrite, withCellTransaction } from "./transactions";
import { digestJson } from "./digest";

export class CellOutboxConflictError extends Error {
  public readonly code = "CELL_OUTBOX_CONFLICT";

  constructor(eventId: string) {
    super(`Outbox event ${eventId} was staged with different identity or content.`);
    this.name = "CellOutboxConflictError";
  }
}

interface OutboxRow {
  event_id: string;
  operation_id: string;
  event_type: string;
  tenant_id: string;
  aggregate_type: string;
  aggregate_id: string;
  aggregate_version: number;
  payload_digest: string;
  payload_json: string;
  status: "PENDING" | "DELIVERED";
  attempts: number;
  next_attempt_at: number;
  created_at: number;
  delivered_at: number | null;
  last_error: string | null;
}

export interface PendingCellOutboxEvent extends CellOutboxEvent {
  attempts: number;
  nextAttemptAt: number;
  lastError: string | null;
}

export interface PreparedCellOutboxEvent extends Omit<CellOutboxDraft, "payload"> {
  operationId: string;
  aggregateType: string;
  aggregateId: string;
  payload: unknown;
  payloadJson: string;
  payloadDigest: string;
  createdAt: number;
}

/** Compute the shared canonical payload identity before opening the SQLite transaction. */
export async function prepareCellOutboxEvent(
  command: { operationId: string; aggregateType: string; aggregateId: string },
  draft: CellOutboxDraft,
): Promise<PreparedCellOutboxEvent> {
  validateDraft(draft);
  const { canonicalJson, sha256 } = await digestJson(draft.payload);
  return {
    ...draft,
    operationId: command.operationId,
    aggregateType: command.aggregateType,
    aggregateId: command.aggregateId,
    payloadJson: canonicalJson,
    payloadDigest: sha256,
    createdAt: Date.now(),
  };
}

/** Stage an event inside the caller's business SQL transaction. */
export function stageCellOutboxEvent(
  sql: PersistenceSqlStorage,
  event: PreparedCellOutboxEvent,
  aggregateVersion: number,
): void {
  validateEvent(event);

  const existing = sql.exec<OutboxRow>("SELECT * FROM cell_outbox WHERE event_id = ?", event.eventId).toArray()[0];
  if (existing) {
    if (!sameEvent(existing, event, aggregateVersion)) throw new CellOutboxConflictError(event.eventId);
    return;
  }
  sql.exec(
    `INSERT INTO cell_outbox (
      event_id, operation_id, event_type, tenant_id, aggregate_type, aggregate_id,
      aggregate_version, payload_digest, payload_json, status, attempts, next_attempt_at,
      created_at, delivered_at, last_error
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    event.eventId,
    event.operationId,
    event.eventType,
    event.tenantId,
    event.aggregateType,
    event.aggregateId,
    aggregateVersion,
    event.payloadDigest,
    event.payloadJson,
    "PENDING",
    0,
    event.createdAt,
    event.createdAt,
    null,
    null,
  );
}

/** Verify that a retried operation proposes the exact outbox envelope it committed earlier. */
export function assertCellOutboxReplay(
  sql: PersistenceSqlStorage,
  operationId: string,
  events: PreparedCellOutboxEvent[],
  aggregateVersion: number,
): void {
  const rows = sql.exec<OutboxRow>("SELECT * FROM cell_outbox WHERE operation_id = ?", operationId).toArray();
  if (rows.length !== events.length) {
    throw new CellOutboxConflictError(events[0]?.eventId ?? operationId);
  }
  for (const event of events) {
    const row = rows.find((candidate) => candidate.event_id === event.eventId);
    if (!row || !sameEvent(row, event, aggregateVersion)) throw new CellOutboxConflictError(event.eventId);
  }
}

/** Find the durable pending work after activation; callers may safely repeat this read. */
export function listDueCellOutbox(
  sql: PersistenceSqlStorage,
  now: number = Date.now(),
  limit: number = 100,
): PendingCellOutboxEvent[] {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new RangeError("Outbox limit must be 1..1000.");
  const rows = sql
    .exec<OutboxRow>(
      `SELECT * FROM cell_outbox
       WHERE status = ? AND next_attempt_at <= ?
       ORDER BY next_attempt_at ASC, created_at ASC
       LIMIT ?`,
      "PENDING",
      now,
      limit,
    )
    .toArray();
  return rows.map((row) => ({
    eventId: row.event_id,
    operationId: row.operation_id,
    eventType: row.event_type,
    tenantId: row.tenant_id,
    aggregateType: row.aggregate_type,
    aggregateId: row.aggregate_id,
    aggregateVersion: Number(row.aggregate_version),
    payloadDigest: row.payload_digest,
    payload: JSON.parse(row.payload_json),
    createdAt: Number(row.created_at),
    attempts: Number(row.attempts),
    nextAttemptAt: Number(row.next_attempt_at),
    lastError: row.last_error,
  }));
}

export async function markCellOutboxDelivered(
  storage: PersistenceObjectStorage,
  eventId: string,
  deliveredAt: number = Date.now(),
): Promise<boolean> {
  const changed = withCellTransaction(storage.sql, () => {
    const row = storage.sql.exec<OutboxRow>("SELECT * FROM cell_outbox WHERE event_id = ?", eventId).toArray()[0];
    if (!row || row.status === "DELIVERED") return false;
    storage.sql.exec(
      "UPDATE cell_outbox SET status = ?, delivered_at = ?, last_error = ? WHERE event_id = ?",
      "DELIVERED",
      deliveredAt,
      null,
      eventId,
    );
    return true;
  });
  if (changed) await confirmCellWrite(storage);
  return changed;
}

export async function recordCellOutboxFailure(
  storage: PersistenceObjectStorage,
  eventId: string,
  nextAttemptAt: number,
  error: unknown,
): Promise<boolean> {
  if (!Number.isSafeInteger(nextAttemptAt) || nextAttemptAt < 0) throw new TypeError("nextAttemptAt must be a valid timestamp.");
  const changed = withCellTransaction(storage.sql, () => {
    const row = storage.sql.exec<OutboxRow>("SELECT * FROM cell_outbox WHERE event_id = ?", eventId).toArray()[0];
    if (!row || row.status === "DELIVERED") return false;
    const lastError = String(error instanceof Error ? error.message : error).slice(0, 1000);
    storage.sql.exec(
      "UPDATE cell_outbox SET attempts = ?, next_attempt_at = ?, last_error = ? WHERE event_id = ?",
      Number(row.attempts) + 1,
      nextAttemptAt,
      lastError,
      eventId,
    );
    return true;
  });
  if (changed) await confirmCellWrite(storage);
  return changed;
}

function sameEvent(row: OutboxRow, event: PreparedCellOutboxEvent, aggregateVersion: number): boolean {
  return (
    row.operation_id === event.operationId &&
    row.event_type === event.eventType &&
    row.tenant_id === event.tenantId &&
    row.aggregate_type === event.aggregateType &&
    row.aggregate_id === event.aggregateId &&
    Number(row.aggregate_version) === aggregateVersion &&
    row.payload_digest === event.payloadDigest
  );
}

function validateDraft(event: CellOutboxDraft): void {
  for (const [field, value] of Object.entries({
    eventId: event.eventId,
    eventType: event.eventType,
    tenantId: event.tenantId,
  })) {
    if (typeof value !== "string" || value.trim().length === 0) throw new TypeError(`${field} must be non-empty.`);
  }
}

function validateEvent(event: PreparedCellOutboxEvent): void {
  for (const [field, value] of Object.entries({
    eventId: event.eventId,
    operationId: event.operationId,
    eventType: event.eventType,
    tenantId: event.tenantId,
    aggregateType: event.aggregateType,
    aggregateId: event.aggregateId,
  })) {
    if (typeof value !== "string" || value.trim().length === 0) throw new TypeError(`${field} must be non-empty.`);
  }
  if (!/^[a-f0-9]{64}$/i.test(event.payloadDigest)) throw new TypeError("payloadDigest must be a SHA-256 digest.");
  if (!Number.isSafeInteger(event.createdAt) || event.createdAt < 0) throw new TypeError("createdAt must be a valid timestamp.");
}
