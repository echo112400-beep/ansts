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
  markCellOutboxDelivered,
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
