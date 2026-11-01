import { strict as assert } from "assert";
import { test } from "node:test";

import { MockDurableObjectState } from "../src/cloudflare/env";
import {
  consumeCellInboxEvent,
  confirmCellWrite,
  digestJson,
  executeCellOperation,
  initializeCellProtocol,
  listDueCellOutbox,
  markCellOutboxBatchDelivered,
  markCellOutboxDelivered,
  pruneDeliveredCellOutboxPayloads,
  recordCellOutboxFailure,
  CellDurabilityUnavailableError,
} from "../src/persistence";

test("Cell operations replay by stable identity and reject a changed digest", async () => {
  const storage = new MockDurableObjectState("protocol-test").storage;
  initializeCellProtocol(storage.sql, { type: "scheduler-pool", key: "us-east:gpu" });

  let applies = 0;
  const operation = {
    operationId: "op-42",
    operationType: "dispatch.reserve",
    resourceId: "task-42",
    tenantId: "tenant-7",
    payload: { action: "reserve", taskId: "task-42" },
    aggregateType: "dispatch",
    aggregateId: "task-42",
    expectedVersion: 0,
  };
  const apply = () => {
    applies += 1;
    return { dispatchId: "dispatch-42", state: "RESERVED" };
  };

  const [first, retry] = await Promise.all([
    executeCellOperation(storage, operation, apply),
    executeCellOperation(storage, operation, apply),
  ]);

  assert.deepEqual([first.outcome, retry.outcome].sort(), ["APPLIED", "REPLAYED"]);
  assert.deepEqual(first.result, { dispatchId: "dispatch-42", state: "RESERVED" });
  assert.deepEqual(retry.result, first.result);
  assert.equal(applies, 1);
  await assert.rejects(
    executeCellOperation(storage, { ...operation, payload: { action: "release", taskId: "task-42" } }, apply),
    { code: "CELL_OPERATION_CONFLICT" },
  );
  await assert.rejects(executeCellOperation(storage, { ...operation, expectedVersion: 1 }, apply), {
    code: "CELL_OPERATION_CONFLICT",
  });
});

test("a failed operation transaction leaves its aggregate version available for retry", async () => {
  const storage = new MockDurableObjectState("protocol-rollback").storage;
  initializeCellProtocol(storage.sql, { type: "task", key: "task-rollback" }, [
    {
      version: 2,
      description: "test effect table",
      up(sql) {
        sql.exec("CREATE TABLE IF NOT EXISTS protocol_test_effects (effect_id TEXT PRIMARY KEY)");
      },
    },
  ]);
  const operation = {
    operationId: "op-fail-then-retry",
    operationType: "task.transition",
    resourceId: "task-rollback",
    tenantId: "tenant-7",
    payload: { state: "READY" },
    aggregateType: "task",
    aggregateId: "task-rollback",
    expectedVersion: 0,
  };

  await assert.rejects(executeCellOperation(storage, operation, () => {
    storage.sql.exec("INSERT INTO protocol_test_effects (effect_id) VALUES (?)", "partial");
    throw new Error("abort local transaction");
  }));
  const retry = await executeCellOperation(storage, operation, (sql) => {
    assert.deepEqual(sql.exec("SELECT * FROM protocol_test_effects").toArray(), []);
    return { state: "READY" };
  });
  assert.equal(retry.outcome, "APPLIED");
  assert.equal(retry.aggregateVersion, 1);
});

test("Cell inbox delivery is idempotent and conflicting event reuse is rejected", async () => {
  const state = new MockDurableObjectState("protocol-inbox");
  initializeCellProtocol(state.storage.sql, { type: "task", key: "task-inbox" });
  const payload = { status: "COMPLETED", turnSeq: 5 };
  const event = {
    eventId: "event-task-5",
    eventType: "task.turn.completed",
    tenantId: "tenant-7",
    aggregateType: "task",
    aggregateId: "task-inbox",
    aggregateVersion: 5,
    payloadDigest: (await digestJson(payload)).sha256,
    payload,
  };
  let applies = 0;
  const apply = () => {
    applies += 1;
  };

  assert.equal(await consumeCellInboxEvent(state.storage, event, apply), "APPLIED");
  assert.equal(await consumeCellInboxEvent(state.storage, event, apply), "REPLAYED");
  assert.equal(applies, 1);
  await assert.rejects(
    consumeCellInboxEvent(state.storage, { ...event, tenantId: "tenant-other" }, apply),
    { code: "CELL_INBOX_CONFLICT" },
  );
});

test("outbox work remains queryable until acknowledged and preserves event identity", async () => {
  const state = new MockDurableObjectState("protocol-outbox");
  initializeCellProtocol(state.storage.sql, { type: "tenant-ledger", key: "tenant-7" });
  const operation = {
    operationId: "admit-7",
    operationType: "task.admit",
    resourceId: "task-7",
    tenantId: "tenant-7",
    payload: { taskId: "task-7", input: "hello" },
    aggregateType: "tenant-admissions",
    aggregateId: "tenant-7",
    expectedVersion: 0,
  };
  const event = {
    eventId: "event-admit-7",
    eventType: "task.admitted",
    tenantId: "tenant-7",
    payload: { taskId: "task-7" },
  };

  const first = await executeCellOperation(state.storage, operation, () => ({ status: "ACCEPTED" }), [event]);
  const replay = await executeCellOperation(state.storage, operation, () => {
    throw new Error("a replay must not reapply the mutation");
  }, [event]);
  assert.equal(first.outcome, "APPLIED");
  assert.equal(replay.outcome, "REPLAYED");

  const due = listDueCellOutbox(state.storage.sql, Number.MAX_SAFE_INTEGER);
  assert.equal(due.length, 1);
  assert.equal(due[0].eventId, "event-admit-7");
  assert.equal(due[0].operationId, "admit-7");
  assert.equal(due[0].aggregateVersion, 1);
  assert.deepEqual(due[0].payload, event.payload);

  await assert.rejects(
    executeCellOperation(state.storage, operation, () => ({ status: "ACCEPTED" }), [
      { ...event, eventType: "task.changed" },
    ]),
    { code: "CELL_OUTBOX_CONFLICT" },
  );

  assert.equal(await recordCellOutboxFailure(state.storage, "event-admit-7", 0, new Error("temporary outage")), true);
  const retried = listDueCellOutbox(state.storage.sql, 1)[0];
  assert.equal(retried.attempts, 1);
  assert.equal(retried.lastError, "temporary outage");
  assert.equal(await markCellOutboxDelivered(state.storage, "event-admit-7", 2), true);
  const syncsAfterDelivery = state.durableSyncCalls;
  assert.equal(await markCellOutboxDelivered(state.storage, "event-admit-7", 3), false);
  assert.equal(state.durableSyncCalls, syncsAfterDelivery + 1, "idempotent delivery retry must reconfirm the committed row");
  await executeCellOperation(state.storage, { ...operation, operationId: "admit-8", resourceId: "task-8", expectedVersion: 1 },
    () => ({ status: "ACCEPTED" }), [{ ...event, eventId: "event-admit-8", payload: { taskId: "task-8" } }]);
  assert.equal(await markCellOutboxBatchDelivered(state.storage, {
    eventIds: ["event-admit-8"], tenantId: "tenant-7", aggregateType: "tenant-admissions", aggregateId: "tenant-7",
  }, 4), 1);
  await assert.rejects(markCellOutboxBatchDelivered(state.storage, {
    eventIds: ["event-admit-8"], tenantId: "tenant-other", aggregateType: "tenant-admissions", aggregateId: "tenant-7",
  }, 5), { code: "CELL_OUTBOX_CONFLICT" });
  state.storage.sql.exec("UPDATE cell_outbox SET created_at = ? WHERE event_id = ?", 1, "event-admit-8");
  await executeCellOperation(state.storage, { ...operation, operationId: "admit-9", resourceId: "task-9", expectedVersion: 2 },
    () => ({ status: "ACCEPTED" }), [{ ...event, eventId: "event-admit-9", payload: { taskId: "task-9" } }]);
  state.storage.sql.exec("UPDATE cell_outbox SET created_at = ?, next_attempt_at = ? WHERE event_id = ?", 1, 1, "event-admit-9");
  const beforePrune = state.storage.sql.exec<{ event_id: string; status: string; created_at: number; payload_json: string }>(
    "SELECT event_id, status, created_at, payload_json FROM cell_outbox WHERE event_id = ?", "event-admit-8").toArray()[0];
  assert.deepEqual(beforePrune, { event_id: "event-admit-8", status: "DELIVERED", created_at: 1, payload_json: JSON.stringify({ taskId: "task-8" }) });
  const deliveredRows = state.storage.sql.exec<{ event_id: string }>("SELECT * FROM cell_outbox WHERE tenant_id = ? AND aggregate_type = ? AND aggregate_id = ? AND status = ?",
    "tenant-7", "tenant-admissions", "tenant-7", "DELIVERED").toArray();
  assert.ok(deliveredRows.some((row: any) => row.event_id === "event-admit-8"));
  assert.equal(Number((deliveredRows as any[]).find((row) => row.event_id === "event-admit-8").created_at), 1);
  assert.equal(await pruneDeliveredCellOutboxPayloads(state.storage, {
    tenantId: "tenant-7", aggregateType: "tenant-admissions", aggregateId: "tenant-7", cutoff: 2,
  }), 1);
  const prunedPayload = state.storage.sql.exec<{ payload_json: string; status: string }>(
    "SELECT payload_json, status FROM cell_outbox WHERE event_id = ?", "event-admit-8").toArray()[0];
  assert.deepEqual(JSON.parse(prunedPayload.payload_json), {}, "expired delivered payload is discarded but identity metadata remains");
  assert.equal(prunedPayload.status, "DELIVERED");
  assert.equal(listDueCellOutbox(state.storage.sql, 10).some((item) => item.eventId === "event-admit-9"), true,
    "expired pending delivery remains available for replay");
  const replayAfterPayloadRetention = await executeCellOperation(state.storage, { ...operation, operationId: "admit-8", resourceId: "task-8", expectedVersion: 1 },
    () => { throw new Error("retained operation must not reapply"); }, [{ ...event, eventId: "event-admit-8", payload: { taskId: "task-8" } }]);
  assert.equal(replayAfterPayloadRetention.outcome, "REPLAYED");
  await markCellOutboxBatchDelivered(state.storage, {
    eventIds: ["event-admit-9"], tenantId: "tenant-7", aggregateType: "tenant-admissions", aggregateId: "tenant-7",
  }, 6);
  assert.deepEqual(listDueCellOutbox(state.storage.sql, 10), []);
  assert.ok(state.durableSyncCalls >= 3);
});

test("Cell schema migration is versioned and rejects a namespace identity change", () => {
  const storage = new MockDurableObjectState("protocol-schema").storage.sql;
  const identity = { type: "routing-config" as const, key: "routes-v1" };
  let applied = 0;
  const ownerMigrations = [
    {
      version: 2,
      description: "initial route policy tables",
      up() {
        applied += 1;
      },
    },
  ];

  assert.equal(initializeCellProtocol(storage, identity, ownerMigrations), 2);
  assert.equal(initializeCellProtocol(storage, identity, ownerMigrations), 2);
  assert.equal(applied, 1);
  assert.throws(
    () => initializeCellProtocol(storage, { ...identity, key: "routes-renamed" }, ownerMigrations),
    { code: "CELL_IDENTITY_MISMATCH" },
  );
  assert.throws(() => initializeCellProtocol(storage, identity), { code: "CELL_SCHEMA_INVALID" });
});

test("canonical payload digests ignore object key insertion order", async () => {
  const left = await digestJson({ task: "task-1", policy: { model: "model-a", effort: "high" } });
  const right = await digestJson({ policy: { effort: "high", model: "model-a" }, task: "task-1" });
  assert.equal(left.canonicalJson, right.canonicalJson);
  assert.equal(left.sha256, right.sha256);
});

test("durable confirmation fails explicitly when the runtime exposes no Celld proof method", async () => {
  const storage = new MockDurableObjectState("protocol-no-proof").storage;
  storage.sync = undefined;
  await assert.rejects(confirmCellWrite(storage), CellDurabilityUnavailableError);
});
