import { createHmac, timingSafeEqual } from "node:crypto";
import { joinedTransactions } from "../finops/finops_page.js";
import { ControlError } from "./contracts.js";
import { ExecutionInventory } from "./execution_inventory.js";
import type { DirectoryStorage } from "./tenant_directory.js";

const tables = {
  scheduler: {
    ansts_tasks: ["task_id", "tenant_id", "priority", "state", "fencing_token", "assigned_node_id", "payload_json"],
    ansts_nodes: ["node_id", "heartbeat_json", "updated_at"],
    ansts_claims: ["task_id", "holder_id", "claimed_at"],
    ansts_pilot_submissions: ["reservation_id", "task_id", "input_digest", "expires_at"],
    ansts_pilot_submission_accepted: ["reservation_id"],
    scheduler_runtime_config: ["id", "payload_json"],
    execution_inventory_heads: ["tenant_id", "payload_json"],
    execution_inventory_events: ["tenant_id", "ordinal", "task_id", "payload_json"],
  },
  capacity: {
    capacity_inventory: ["id", "payload_json"],
    capacity_requests: ["pool_id", "id", "tenant_id", "resources", "enqueued_at", "priority", "state"],
    capacity_request_grants: ["pool_id", "request_id", "host_id", "policy_version"],
    capacity_grant_bindings: ["pool_id", "request_id", "tenant_id", "reservation_id", "attempt_id", "generation", "fencing"],
    capacity_grant_expiry: ["pool_id", "request_id", "expires_at"],
    capacity_queue_policy: ["pool_id", "version", "wait_ms", "weights_json"],
    capacity_allocation_control: ["pool_id", "version", "enabled"],
    capacity_grant_epochs: ["pool_id", "request_id", "daemon_id", "epoch"],
    capacity_guaranteed_shares: ["pool_id", "tenant_id", "resources_json"],
    capacity_queue_priorities: ["pool_id", "priorities_json"],
  },
} as const;

type Plane = keyof typeof tables;

function schemaFor(plane: Plane): [string, readonly string[]][] {
  return Object.entries(tables[plane]);
}
type Value = string | number | null;
type Rows = Record<string, Value[][]>;
interface Scope { plane: Plane; name: string }
interface InventoryHead { tenantId: string; complete: boolean; headDigest: string }
export interface ControlCheckpoint {
  version: 1;
  scope: Scope;
  sourceId: string;
  alarmAt: number | null;
  rows: Rows;
  heads: InventoryHead[];
}
export interface SignedControlCheckpoint {
  checkpoint: ControlCheckpoint;
  signature: string;
}

const maxRows = 100_000;
export const maxControlCheckpointBytes = 32 * 1024 * 1024;

function signature(payload: string, key: string) {
  if (key.length < 32) throw new ControlError("CHECKPOINT_SIGNING_UNAVAILABLE", 503);
  return createHmac("sha256", key).update("ansts-control-checkpoint-v1\n").update(payload).digest();
}

function captureRows(storage: DirectoryStorage, plane: Plane): Rows {
  let count = 0;
  return Object.fromEntries(schemaFor(plane).map(([table, columns]) => {
    const fields = columns.join(", ");
    const records = storage.sql.exec<Record<string, Value>>(
      `SELECT ${fields} FROM ${table} ORDER BY ${fields} LIMIT ?`, maxRows - count + 1,
    ).toArray();
    count += records.length;
    if (count > maxRows) throw new ControlError("CHECKPOINT_TOO_LARGE", 413);
    return [table, records.map(row => columns.map(column => row[column]))];
  }));
}

function captureHeads(storage: DirectoryStorage, scope: Scope, objectId: string, rows: Rows): InventoryHead[] {
  if (scope.plane !== "scheduler") return [];
  const inventory = new ExecutionInventory(storage, "SCHEDULER", objectId, () => []);
  return rows.execution_inventory_heads.map(([tenant]) => {
    if (typeof tenant !== "string") throw new ControlError("CHECKPOINT_INVALID", 422);
    const { complete, headDigest } = inventory.snapshot(tenant);
    if (!complete) throw new ControlError("CHECKPOINT_INVENTORY_INCOMPLETE", 409);
    return { tenantId: tenant, complete, headDigest };
  });
}

/** Capture from a fenced object whose business state does not change while reading its alarm and SQL. */
export function signControlCheckpoint(storage: DirectoryStorage, scope: Scope, sourceId: string, alarmAt: number | null, key: string): SignedControlCheckpoint {
  const joined = joinedTransactions(storage);
  const checkpoint = joined.transactionSync((): ControlCheckpoint => {
    const rows = captureRows(joined, scope.plane);
    return { version: 1, scope: { plane: scope.plane, name: scope.name }, sourceId, alarmAt, rows, heads: captureHeads(joined, scope, sourceId, rows) };
  });
  const payload = JSON.stringify(checkpoint);
  if (Buffer.byteLength(payload) > maxControlCheckpointBytes) throw new ControlError("CHECKPOINT_TOO_LARGE", 413);
  return { checkpoint, signature: signature(payload, key).toString("base64url") };
}

function verifyCheckpoint(archive: SignedControlCheckpoint, expected: Scope & { sourceId: string }, key: string) {
  if (!archive?.checkpoint || typeof archive.signature !== "string" || !/^[A-Za-z0-9_-]{43}$/.test(archive.signature))
    throw new ControlError("CHECKPOINT_INVALID", 422);
  const checkpoint = archive.checkpoint;
  const payload = JSON.stringify(checkpoint);
  if (Buffer.byteLength(payload) > maxControlCheckpointBytes) throw new ControlError("CHECKPOINT_TOO_LARGE", 413);
  const supplied = Buffer.from(archive.signature, "base64url");
  if (supplied.length !== 32 || !timingSafeEqual(supplied, signature(payload, key)))
    throw new ControlError("CHECKPOINT_SIGNATURE_INVALID", 401);
  if (checkpoint.version !== 1 || checkpoint.scope?.plane !== expected.plane ||
      checkpoint.scope.name !== expected.name || checkpoint.sourceId !== expected.sourceId)
    throw new ControlError("CHECKPOINT_SCOPE_MISMATCH", 409);
  if (checkpoint.alarmAt !== null && (!Number.isSafeInteger(checkpoint.alarmAt) || checkpoint.alarmAt < 0))
    throw new ControlError("CHECKPOINT_INVALID", 422);
  const schema = schemaFor(expected.plane);
  if (!checkpoint.rows || Object.keys(checkpoint.rows).length !== schema.length || !Array.isArray(checkpoint.heads))
    throw new ControlError("CHECKPOINT_INVALID", 422);
  let count = 0;
  for (const [table, columns] of schema) {
    const rows = checkpoint.rows[table];
    if (!Array.isArray(rows)) throw new ControlError("CHECKPOINT_INVALID", 422);
    count += rows.length;
    if (count > maxRows || rows.some(row => !Array.isArray(row) || row.length !== columns.length ||
        row.some(value => value !== null && typeof value !== "string" && (typeof value !== "number" || !Number.isFinite(value)))))
      throw new ControlError("CHECKPOINT_INVALID", 422);
  }
  return checkpoint;
}

/** Restore initialized table schemas in a dedicated fenced object. Alarm activation is a separate operation. */
export function restoreControlCheckpoint(storage: DirectoryStorage, objectId: string, archive: SignedControlCheckpoint, expected: Scope & { sourceId: string }, key: string) {
  const checkpoint = verifyCheckpoint(archive, expected, key);
  const joined = joinedTransactions(storage);
  return joined.transactionSync(() => {
    for (const [table, columns] of schemaFor(expected.plane)) {
      if (storage.sql.exec(`SELECT 1 FROM ${table} LIMIT 1`).toArray().length)
        throw new ControlError("CHECKPOINT_DESTINATION_INITIALIZED", 409);
      const insert = `INSERT INTO ${table} (${columns.join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`;
      for (const row of checkpoint.rows[table]) storage.sql.exec(insert, ...row);
    }
    const rows = captureRows(storage, expected.plane);
    const heads = captureHeads(joined, checkpoint.scope, objectId, rows);
    if (JSON.stringify(rows) !== JSON.stringify(checkpoint.rows) || JSON.stringify(heads) !== JSON.stringify(checkpoint.heads))
      throw new ControlError("CHECKPOINT_READBACK_MISMATCH", 409);
    return { alarmAt: checkpoint.alarmAt, counts: Object.fromEntries(Object.entries(rows).map(([table, values]) => [table, values.length])) };
  });
}
