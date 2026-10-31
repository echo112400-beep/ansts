import { CellInboxEvent, PersistenceObjectStorage, PersistenceSqlStorage } from "./contracts";
import { confirmCellWrite, withCellTransaction } from "./transactions";
import { digestJson } from "./digest";

export class CellInboxConflictError extends Error {
  public readonly code = "CELL_INBOX_CONFLICT";

  constructor(eventId: string) {
    super(`Inbox event ${eventId} was redelivered with different identity or content.`);
    this.name = "CellInboxConflictError";
  }
}

interface InboxRow {
  event_id: string;
  event_type: string;
  tenant_id: string;
  aggregate_type: string;
  aggregate_id: string;
  aggregate_version: number;
  payload_digest: string;
}

export type InboxOutcome = "APPLIED" | "REPLAYED";

/** Apply a delivered event and record its inbox identity in the same local transaction. */
export async function consumeCellInboxEvent(
  storage: PersistenceObjectStorage,
  event: CellInboxEvent,
  apply: (sql: PersistenceSqlStorage, event: CellInboxEvent) => void,
): Promise<InboxOutcome> {
  validateEvent(event);
  const actualDigest = (await digestJson(event.payload)).sha256;
  if (actualDigest !== event.payloadDigest.toLowerCase()) {
    throw new CellInboxConflictError(event.eventId);
  }
  const outcome = withCellTransaction(storage.sql, () => {
    const existing = storage.sql.exec<InboxRow>("SELECT * FROM cell_inbox WHERE event_id = ?", event.eventId).toArray()[0];
    if (existing) {
      if (!sameEvent(existing, event)) throw new CellInboxConflictError(event.eventId);
      return "REPLAYED" as const;
    }

    const result = apply(storage.sql, event);
    if (isThenable(result)) {
      throw new TypeError("Inbox handlers must be synchronous; perform external I/O after the local transaction.");
    }
    const now = Date.now();
    storage.sql.exec(
      `INSERT INTO cell_inbox (
        event_id, event_type, tenant_id, aggregate_type, aggregate_id,
        aggregate_version, payload_digest, received_at, processed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      event.eventId,
      event.eventType,
      event.tenantId,
      event.aggregateType,
      event.aggregateId,
      event.aggregateVersion,
      event.payloadDigest,
      now,
      now,
    );
    return "APPLIED" as const;
  });

  await confirmCellWrite(storage);
  return outcome;
}

function sameEvent(row: InboxRow, event: CellInboxEvent): boolean {
  return (
    row.event_type === event.eventType &&
    row.tenant_id === event.tenantId &&
    row.aggregate_type === event.aggregateType &&
    row.aggregate_id === event.aggregateId &&
    Number(row.aggregate_version) === event.aggregateVersion &&
    row.payload_digest === event.payloadDigest
  );
}

function validateEvent(event: CellInboxEvent): void {
  for (const [field, value] of Object.entries({
    eventId: event.eventId,
    eventType: event.eventType,
    tenantId: event.tenantId,
    aggregateType: event.aggregateType,
    aggregateId: event.aggregateId,
  })) {
    if (typeof value !== "string" || value.trim().length === 0) throw new TypeError(`${field} must be non-empty.`);
  }
  if (!Number.isSafeInteger(event.aggregateVersion) || event.aggregateVersion < 0) {
    throw new TypeError("aggregateVersion must be a non-negative safe integer.");
  }
  if (!/^[a-f0-9]{64}$/i.test(event.payloadDigest)) {
    throw new TypeError("payloadDigest must be a 64-character SHA-256 digest.");
  }
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return Boolean(value && (typeof value === "object" || typeof value === "function") && "then" in value);
}
