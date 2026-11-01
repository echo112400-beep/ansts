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

export interface CellOutboxCursor {
  aggregateVersion: number;
  createdAt: number;
  eventId: string;
}

export interface ReplayedCellOutboxEvent extends CellOutboxEvent {
  createdAt: number;
  status: "PENDING" | "DELIVERED";
  cursor: string;
}

export interface CellOutboxReplayPage {
  events: ReplayedCellOutboxEvent[];
  nextCursor: string | null;
  hasMore: boolean;
  cursorExpired: boolean;
}

export function encodeCellOutboxCursor(cursor: CellOutboxCursor): string {
  if (!Number.isSafeInteger(cursor.aggregateVersion) || cursor.aggregateVersion < 1 || !Number.isSafeInteger(cursor.createdAt) || cursor.createdAt < 0 || !cursor.eventId) {
    throw new TypeError("Outbox cursor identity is invalid.");
  }
  return `${cursor.aggregateVersion}~${cursor.createdAt}~${encodeURIComponent(cursor.eventId)}`;
}

export function decodeCellOutboxCursor(value: string): CellOutboxCursor {
  const match = value.match(/^(\d+)~(\d+)~(.+)$/);
  if (!match) throw new TypeError("Last-Event-ID must be a valid ANSTS task cursor.");
  let eventId: string;
  try { eventId = decodeURIComponent(match[3]); } catch { throw new TypeError("Last-Event-ID has an invalid event identity."); }
  const aggregateVersion = Number(match[1]);
  const createdAt = Number(match[2]);
  if (!Number.isSafeInteger(aggregateVersion) || aggregateVersion < 1 || !Number.isSafeInteger(createdAt) || createdAt < 0 || !eventId) {
    throw new TypeError("Last-Event-ID has an invalid position.");
  }
  return { aggregateVersion, createdAt, eventId };
}

/** Return a bounded, tenant-scoped replay page from the authoritative Cell outbox. */
export function listCellOutboxReplay(
  sql: PersistenceSqlStorage,
  input: { tenantId: string; aggregateType: string; aggregateId: string; cutoff: number; after?: CellOutboxCursor; limit?: number },
): CellOutboxReplayPage {
  const limit = input.limit ?? 100;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new RangeError("Outbox replay limit must be 1..1000.");
  if (input.after && input.after.createdAt < input.cutoff) return { events: [], nextCursor: null, hasMore: false, cursorExpired: true };
  const rows = sql.exec<OutboxRow>(
    `SELECT * FROM cell_outbox
     WHERE tenant_id = ? AND aggregate_type = ? AND aggregate_id = ? AND created_at >= ?
     ORDER BY aggregate_version ASC, created_at ASC, event_id ASC`,
    input.tenantId, input.aggregateType, input.aggregateId, input.cutoff,
  ).toArray();
  const after = input.after;
  const remaining = rows.filter((row) => !after
    || Number(row.aggregate_version) > after.aggregateVersion
    || (Number(row.aggregate_version) === after.aggregateVersion
      && (Number(row.created_at) > after.createdAt
        || (Number(row.created_at) === after.createdAt && row.event_id > after.eventId))));
  const selected = remaining.slice(0, limit + 1);
  const hasMore = selected.length > limit;
  const pageRows = selected.slice(0, limit);
  const events = pageRows.map((row) => {
    const cursor = encodeCellOutboxCursor({ aggregateVersion: Number(row.aggregate_version), createdAt: Number(row.created_at), eventId: row.event_id });
    return {
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
      status: row.status,
      cursor,
    };
  });
  return { events, nextCursor: events.at(-1)?.cursor ?? null, hasMore, cursorExpired: false };
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
  const result = withCellTransaction(storage, () => {
    const row = storage.sql.exec<OutboxRow>("SELECT * FROM cell_outbox WHERE event_id = ?", eventId).toArray()[0];
    if (!row) return { exists: false, changed: false };
    if (row.status === "DELIVERED") return { exists: true, changed: false };
    storage.sql.exec(
      "UPDATE cell_outbox SET status = ?, delivered_at = ?, last_error = ? WHERE event_id = ?",
      "DELIVERED",
      deliveredAt,
      null,
      eventId,
    );
    return { exists: true, changed: true };
  });
  // A prior call may have committed locally and then lost its sync acknowledgement. Reconfirm
  // the durable row on an idempotent retry even though no SQL field changes this time.
  if (result.exists) await confirmCellWrite(storage);
  return result.changed;
}

/** Acknowledge one bounded consumer batch in one durable SQLite transaction. */
export async function markCellOutboxBatchDelivered(
  storage: PersistenceObjectStorage,
  input: { eventIds: string[]; tenantId: string; aggregateType: string; aggregateId: string },
  deliveredAt: number = Date.now(),
): Promise<number> {
  if (!Array.isArray(input.eventIds) || input.eventIds.length < 1 || input.eventIds.length > 1000 ||
      input.eventIds.some((eventId) => typeof eventId !== "string" || !eventId.trim()) ||
      new Set(input.eventIds).size !== input.eventIds.length) {
    throw new TypeError("Outbox acknowledgment requires 1..1000 unique event IDs.");
  }
  if (!Number.isSafeInteger(deliveredAt) || deliveredAt < 0) throw new TypeError("deliveredAt must be a valid timestamp.");
  const result = withCellTransaction(storage, () => {
    let changed = 0;
    for (const eventId of input.eventIds) {
      const row = storage.sql.exec<OutboxRow>("SELECT * FROM cell_outbox WHERE event_id = ?", eventId).toArray()[0];
      if (!row || row.tenant_id !== input.tenantId || row.aggregate_type !== input.aggregateType || row.aggregate_id !== input.aggregateId) {
        throw new CellOutboxConflictError(eventId);
      }
      if (row.status === "DELIVERED") continue;
      storage.sql.exec("UPDATE cell_outbox SET status = ?, delivered_at = ?, last_error = ? WHERE event_id = ?",
        "DELIVERED", deliveredAt, null, eventId);
      changed += 1;
    }
    return { changed, exists: true };
  });
  if (result.exists) await confirmCellWrite(storage);
  return result.changed;
}

/** Drop expired delivered event bodies while retaining identity/digest tombstones for operation replay. */
export async function pruneDeliveredCellOutboxPayloads(
  storage: PersistenceObjectStorage,
  input: { tenantId: string; aggregateType: string; aggregateId: string; cutoff: number },
): Promise<number> {
  if (!Number.isSafeInteger(input.cutoff) || input.cutoff < 0) throw new TypeError("Outbox retention cutoff must be a valid timestamp.");
  const expired = storage.sql.exec<OutboxRow>("SELECT * FROM cell_outbox WHERE tenant_id = ? AND aggregate_type = ? AND aggregate_id = ? AND status = ?",
    input.tenantId, input.aggregateType, input.aggregateId, "DELIVERED")
    .toArray().filter((row) => Number(row.created_at) < input.cutoff && row.payload_json !== "{}");
  if (expired.length === 0) return 0;
  withCellTransaction(storage, () => {
    for (const row of expired) {
      storage.sql.exec("UPDATE cell_outbox SET payload_json = ? WHERE event_id = ? AND status = ?",
        "{}", row.event_id, "DELIVERED");
    }
  });
  await confirmCellWrite(storage);
  return expired.length;
}

export async function recordCellOutboxFailure(
  storage: PersistenceObjectStorage,
  eventId: string,
  nextAttemptAt: number,
  error: unknown,
): Promise<boolean> {
  if (!Number.isSafeInteger(nextAttemptAt) || nextAttemptAt < 0) throw new TypeError("nextAttemptAt must be a valid timestamp.");
  const changed = withCellTransaction(storage, () => {
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
