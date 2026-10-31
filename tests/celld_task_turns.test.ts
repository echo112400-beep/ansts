import { strict as assert } from "assert";
import { test } from "node:test";

import { FenceToken, ToolEffectClass } from "../src/models";
import { MockDurableObjectState } from "../src/cloudflare/env";
import { SnapshotBlockStore, sha256Hex } from "../src/checkpoint/snapshot_block_store";
import { initializeCellProtocol } from "../src/persistence/schema";
import { taskCoreMigration } from "../src/persistence/task-core";
import { TaskOperations, stableTaskOperationId, taskOperationsMigration } from "../src/persistence/task-operations";
import { TaskTurns, taskTurnMigrations } from "../src/persistence/task-turns";

class TestSnapshotBlockStore implements SnapshotBlockStore {
  public readonly objects = new Map<string, Uint8Array>();

  public async putIfAbsent(key: string, bytes: Uint8Array): Promise<"CREATED" | "EXISTS"> {
    if (this.objects.has(key)) return "EXISTS";
    this.objects.set(key, new Uint8Array(bytes));
    return "CREATED";
  }

  public async get(key: string): Promise<Uint8Array | null> {
    const bytes = this.objects.get(key);
    return bytes ? new Uint8Array(bytes) : null;
  }

  public async delete(key: string): Promise<void> {
    this.objects.delete(key);
  }
}

function createTask(taskId: string, options: { retentionMs?: number; graceMs?: number; modelCallChunkBytes?: number } = {}) {
  const state = new MockDurableObjectState(taskId);
  initializeCellProtocol(state.storage.sql, { type: "task", key: taskId }, [
    taskCoreMigration,
    taskOperationsMigration,
    ...taskTurnMigrations,
  ]);
  state.storage.sql.exec(`INSERT INTO task_execution (
    task_id, tenant_id, region, resource_pool, lifecycle_state, business_epoch, execution_version,
    current_dispatch_id, pending_dispatch_id, current_node_id, node_incarnation, last_turn_seq,
    task_spec_json, execution_policy_json, progress_json, heartbeat_at, suspected_at, suspect_reason, updated_at
  ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  taskId, "tenant-turns", "default", "default", "RUNNING", 3, 0,
  "dispatch-turns", null, "node-turns", "incarnation-turns", 0,
  "{}", "{}", "{}", null, null, null, Date.now());
  const operations = new TaskOperations(state.storage);
  const blockStore = new TestSnapshotBlockStore();
  const turns = new TaskTurns(state.storage, blockStore, operations, {
    snapshotRetentionMs: options.retentionMs,
    gcGracePeriodMs: options.graceMs,
    modelCallChunkBytes: options.modelCallChunkBytes,
  });
  return { state, operations, blockStore, turns, taskId };
}

async function bytes(text: string): Promise<Uint8Array> {
  return new TextEncoder().encode(text);
}

async function beginWithChunks(
  turns: TaskTurns,
  taskId: string,
  turnSeq: number,
  uploadId: string,
  inputs: string[],
  epoch = 3,
): Promise<void> {
  const blocks = await Promise.all(inputs.map(bytes));
  const chunks = await Promise.all(blocks.map(async (data, index) => ({ index, sha256: await sha256Hex(data), sizeBytes: data.byteLength })));
  await turns.beginSnapshotUpload({ taskId, tenantId: "tenant-turns", fence: new FenceToken(epoch, turnSeq, taskId), uploadId, chunks });
  for (let index = 0; index < blocks.length; index += 1) {
    await turns.uploadSnapshotChunk({ taskId, tenantId: "tenant-turns", uploadId, index, bytes: blocks[index] });
  }
}

test("resumable snapshot chunks commit response, manifest, references, head and outbox atomically", async () => {
  const { state, blockStore, taskId } = createTask("task-turn-atomic");
  const turns = new TaskTurns(state.storage, blockStore, new TaskOperations(state.storage));
  const first = await bytes("workspace page one");
  const second = await bytes("workspace page two");
  const chunks = await Promise.all([first, second].map(async (data, index) => ({ index, sha256: await sha256Hex(data), sizeBytes: data.byteLength })));
  await turns.beginSnapshotUpload({ taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: "upload-1", chunks });
  await turns.uploadSnapshotChunk({ taskId, tenantId: "tenant-turns", uploadId: "upload-1", index: 0, bytes: first });

  await assert.rejects(turns.assertSnapshotReady({
    uploadId: "upload-1", taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId),
  }), { code: "SNAPSHOT_UPLOAD_INCOMPLETE" });
  assert.equal(state.storage.sql.exec("SELECT * FROM task_turns").toArray().length, 0);
  assert.equal(state.storage.sql.exec<{ last_turn_seq: number }>("SELECT last_turn_seq FROM task_execution WHERE task_id = ?", taskId).toArray()[0].last_turn_seq, 0);

  await turns.uploadSnapshotChunk({ taskId, tenantId: "tenant-turns", uploadId: "upload-1", index: 1, bytes: second });
  await turns.assertSnapshotReady({ uploadId: "upload-1", taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId) });
  const committed = await turns.commitTurn({
    taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: "upload-1",
    requestDigest: await turnRequestDigest(taskId, 1),
    response: { role: "assistant", content: "done" }, progress: { cursor: "next" }, settlementRefs: ["settlement-1"],
  });
  assert.deepEqual(committed.response, { role: "assistant", content: "done" });
  assert.equal(state.storage.sql.exec<{ last_turn_seq: number; progress_json: string }>(
    "SELECT last_turn_seq, progress_json FROM task_execution WHERE task_id = ?", taskId,
  ).toArray()[0].last_turn_seq, 1);
  assert.equal(JSON.parse(state.storage.sql.exec<{ progress_json: string }>("SELECT progress_json FROM task_execution WHERE task_id = ?", taskId).toArray()[0].progress_json).cursor, "next");
  assert.equal(state.storage.sql.exec("SELECT * FROM cell_outbox WHERE event_type = ?", "task.turn.committed").toArray().length, 1);
  assert.equal(state.storage.sql.exec("SELECT * FROM task_snapshot_refs WHERE ref_kind = ?", "SNAPSHOT_BLOCK").toArray().length, 2);
  assert.equal(state.durableSyncCalls > 0, true);
  const replay = await turns.commitTurn({
    taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: "upload-1",
    requestDigest: await turnRequestDigest(taskId, 1), response: { role: "assistant", content: "done" },
    progress: { cursor: "next" }, settlementRefs: ["settlement-1"],
  });
  assert.equal(replay.turnId, committed.turnId);
  await assert.rejects(turns.commitTurn({
    taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: "upload-1",
    requestDigest: await turnRequestDigest(taskId, 1), response: { role: "assistant", content: "changed" },
    progress: { cursor: "next" }, settlementRefs: ["settlement-1"],
  }), { code: "TASK_TURN_CONFLICT" });

  // A fresh manager instance recovers from owner SQL and block storage, without prior JS state.
  const reactivated = new TaskTurns(state.storage, blockStore, new TaskOperations(state.storage));
  const restored = await reactivated.recoverLatest(taskId);
  assert.equal(restored.turnSeq, 1);
  assert.deepEqual(restored.snapshotBlocks.map((chunk) => new TextDecoder().decode(chunk)), ["workspace page one", "workspace page two"]);
  assert.deepEqual(restored.settlementRefs, ["settlement-1"]);
});

test("snapshot upload resumes under a new business epoch and commit rechecks the current fence", async () => {
  const { state, turns, taskId } = createTask("task-turn-resume");
  await beginWithChunks(turns, taskId, 1, "upload-resume", ["durable chunk"]);
  state.storage.sql.exec("UPDATE task_execution SET business_epoch = ? WHERE task_id = ?", 4, taskId);
  await turns.beginSnapshotUpload({
    taskId, tenantId: "tenant-turns", fence: new FenceToken(4, 1, taskId), uploadId: "upload-resume",
    chunks: [{ index: 0, sha256: await sha256Hex(await bytes("durable chunk")), sizeBytes: (await bytes("durable chunk")).byteLength }],
  });
  const turn = await turns.commitTurn({
    taskId, tenantId: "tenant-turns", fence: new FenceToken(4, 1, taskId), uploadId: "upload-resume",
    requestDigest: await turnRequestDigest(taskId, 1), response: "resumed",
  });
  assert.equal(turn.businessEpoch, 4);
  await assert.rejects(turns.beginSnapshotUpload({
    taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 2, taskId), uploadId: "stale-upload", chunks: [],
  }), { code: "STALE_EXECUTION_EPOCH" });
});

test("snapshot chunk upload verifies TaskCell task and tenant ownership before object access", async () => {
  const { turns, blockStore, taskId } = createTask("task-turn-upload-owner");
  const content = await bytes("owner scoped");
  await turns.beginSnapshotUpload({
    taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: "upload-owner",
    chunks: [{ index: 0, sha256: await sha256Hex(content), sizeBytes: content.byteLength }],
  });
  await assert.rejects(turns.uploadSnapshotChunk({
    taskId, tenantId: "tenant-other", uploadId: "upload-owner", index: 0, bytes: content,
  }), { code: "SNAPSHOT_UPLOAD_NOT_FOUND" });
  assert.equal(blockStore.objects.size, 0);
  await turns.uploadSnapshotChunk({ taskId, tenantId: "tenant-turns", uploadId: "upload-owner", index: 0, bytes: content });
  assert.equal(blockStore.objects.size, 1);
});

test("recovery overlays later TaskOperations results, compensation state and staged model usage", async () => {
  const { turns, operations, taskId } = createTask("task-turn-overlay", { modelCallChunkBytes: 5 });
  await beginWithChunks(turns, taskId, 1, "upload-overlay", ["base snapshot"]);
  await turns.commitTurn({ taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: "upload-overlay", requestDigest: await turnRequestDigest(taskId, 1), response: "base" });

  const call = await turns.recordModelCallResult({
    taskId, tenantId: "tenant-turns", callId: "call-2", turnSeq: 2,
    response: { text: "a later provider response that spans several blocks" },
    usage: { inputTokens: 12, outputTokens: 7, settlementId: "settlement-later" },
    routeRefs: { routeVersion: "route-v9", model: "model-x" },
  });
  assert.ok(call.responseSha256);
  const stored = await turns.getModelCallResult(taskId, "call-2");
  assert.deepEqual(stored.response, { text: "a later provider response that spans several blocks" });
  await assert.rejects(turns.recordModelCallResult({
    taskId, tenantId: "tenant-turns", callId: "call-2", turnSeq: 2, response: { text: "conflict" },
    usage: { inputTokens: 12, outputTokens: 7, settlementId: "settlement-later" }, routeRefs: { routeVersion: "route-v9", model: "model-x" },
  }), { code: "MODEL_CALL_RESULT_CONFLICT" });

  const fence = new FenceToken(3, 2, taskId);
  const operationId = stableTaskOperationId(taskId, "tool-call-2");
  await operations.prepareOperation({
    operationId, tenantId: "tenant-turns", fence, toolName: "billing/refund",
    effectClass: ToolEffectClass.SAGA_COMPENSATABLE, args: { invoice: "inv-2" },
    compensation: { toolName: "billing/refund-compensate", args: { invoice: "inv-2" } },
  });
  const attempt = await operations.beginAttempt({ operationId, attemptId: "tool-attempt-1", fence });
  await operations.recordOperationOutcome({ operationId, attemptId: attempt.attemptId, fence, outcome: "SUCCEEDED", result: { refunded: true } });

  const restored = await turns.recoverLatest(taskId);
  assert.equal(restored.laterOperationOutcomes[0].status, "SUCCEEDED");
  assert.deepEqual(restored.laterOperationOutcomes[0].result, { refunded: true });
  assert.equal(restored.laterCompensations[0].status, "PENDING");
  assert.equal(restored.laterModelCalls[0].callId, "call-2");
  assert.equal(restored.laterModelCalls[0].usage && (restored.laterModelCalls[0].usage as any).outputTokens, 7);
  assert.deepEqual(restored.laterSettlementRefs, ["settlement-later"]);
});

test("committing a Turn requires every staged model result reference", async () => {
  const { state, turns, taskId } = createTask("task-turn-model-ref");
  await turns.recordModelCallResult({
    taskId, tenantId: "tenant-turns", callId: "call-current", turnSeq: 1,
    response: { text: "model answer" }, usage: { verified: true, reference: "usage-1" }, routeRefs: { priceVersion: "price-1" },
  });
  await beginWithChunks(turns, taskId, 1, "upload-model-ref", ["workspace"]);
  const request = {
    taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: "upload-model-ref",
    requestDigest: await turnRequestDigest(taskId, 1), response: "assistant output",
  };
  await assert.rejects(turns.commitTurn(request), { code: "MODEL_CALL_RESULT_REFERENCE_MISSING" });
  assert.equal(state.storage.sql.exec("SELECT * FROM task_turns").toArray().length, 0);
  const committed = await turns.commitTurn({ ...request, modelCallRefs: ["call-current"] });
  assert.deepEqual(committed.modelCallRefs, ["call-current"]);
  const restored = await turns.recoverLatest(taskId);
  assert.deepEqual(restored.committedModelCalls[0].response, { text: "model answer" });
});

test("latest committed Turn restore reports missing/corrupt blocks instead of falling back", async () => {
  const { turns, blockStore, taskId } = createTask("task-turn-corrupt");
  await beginWithChunks(turns, taskId, 1, "upload-old", ["old workspace"]);
  await turns.commitTurn({ taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: "upload-old", requestDigest: await turnRequestDigest(taskId, 1), response: "old" });
  await beginWithChunks(turns, taskId, 2, "upload-new", ["new workspace"]);
  await turns.commitTurn({ taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 2, taskId), uploadId: "upload-new", requestDigest: await turnRequestDigest(taskId, 2), response: "new" });

  const latest = await turns.recoverLatest(taskId);
  blockStore.objects.delete(latest.manifest.blocks[0].objectKey);
  await assert.rejects(turns.recoverLatest(taskId), { code: "SNAPSHOT_BLOCK_MISSING" });
});

test("Turn commit leaves no visible Turn or outbox when a verified upload later reads back corrupt", async () => {
  const { state, turns, blockStore, taskId } = createTask("task-turn-corrupt-before-commit");
  await beginWithChunks(turns, taskId, 1, "upload-corrupt-before-commit", ["verified at upload"]);
  const manifest = JSON.parse(state.storage.sql.exec<{ expected_manifest_json: string }>(
    "SELECT expected_manifest_json FROM task_turn_uploads WHERE upload_id = ?", "upload-corrupt-before-commit",
  ).toArray()[0].expected_manifest_json) as { blocks: Array<{ objectKey: string }> };
  blockStore.objects.set(manifest.blocks[0].objectKey, new TextEncoder().encode("x".repeat("verified at upload".length)));

  await assert.rejects(turns.commitTurn({
    taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: "upload-corrupt-before-commit",
    requestDigest: await turnRequestDigest(taskId, 1), response: "must not commit",
  }), { code: "SNAPSHOT_BLOCK_DIGEST_MISMATCH" });
  assert.equal(state.storage.sql.exec("SELECT * FROM task_turns").toArray().length, 0);
  assert.equal(state.storage.sql.exec("SELECT * FROM cell_outbox WHERE event_type = ?", "task.turn.committed").toArray().length, 0);
  assert.equal(state.storage.sql.exec<{ last_turn_seq: number }>(
    "SELECT last_turn_seq FROM task_execution WHERE task_id = ?", taskId,
  ).toArray()[0].last_turn_seq, 0);
});

test("GC tombstones old references before deleting them after the grace period", async () => {
  const { state, turns, blockStore, taskId } = createTask("task-turn-gc-delete", { retentionMs: 0, graceMs: 100 });
  await seedTwoTurns(turns, taskId, "gc-delete");
  const oldBlockKey = state.storage.sql.exec<{ object_key: string }>(
    "SELECT object_key FROM task_snapshot_refs WHERE ref_kind = ? AND turn_seq = ?", "SNAPSHOT_BLOCK", 1,
  ).toArray()[0].object_key;
  state.storage.sql.exec("UPDATE task_execution SET lifecycle_state = ? WHERE task_id = ?", "SUCCEEDED", taskId);

  const tombstoned = await turns.collectGarbage(Date.now() + 1000);
  assert.equal(tombstoned.tombstoned, 1);
  assert.equal(tombstoned.deleted, 0);
  assert.equal(blockStore.objects.has(oldBlockKey), true);

  const deleted = await turns.collectGarbage(Date.now() + 1100);
  assert.equal(deleted.deleted, 1);
  assert.equal(blockStore.objects.has(oldBlockKey), false);
  assert.equal((await turns.recoverLatest(taskId)).turnSeq, 2);
});

test("GC reclaims incomplete upload fragments only after a terminal task and grace period", async () => {
  const { state, turns, blockStore, taskId } = createTask("task-turn-gc-orphan", { retentionMs: 0, graceMs: 100 });
  await beginWithChunks(turns, taskId, 1, "upload-gc-base", ["base workspace"]);
  await turns.commitTurn({ taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: "upload-gc-base", requestDigest: await turnRequestDigest(taskId, 1), response: "base" });
  const orphanBytes = await bytes("incomplete workspace");
  await turns.beginSnapshotUpload({
    taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 2, taskId), uploadId: "upload-gc-orphan",
    chunks: [{ index: 0, sha256: await sha256Hex(orphanBytes), sizeBytes: orphanBytes.byteLength }],
  });
  const orphanChunk = await turns.uploadSnapshotChunk({ taskId, tenantId: "tenant-turns", uploadId: "upload-gc-orphan", index: 0, bytes: orphanBytes });
  state.storage.sql.exec("UPDATE task_execution SET lifecycle_state = ? WHERE task_id = ?", "FAILED", taskId);

  await turns.collectGarbage(Date.now() + 1000);
  assert.equal(blockStore.objects.has(orphanChunk.objectKey), true);
  await turns.collectGarbage(Date.now() + 1100);
  assert.equal(blockStore.objects.has(orphanChunk.objectKey), false);
});

test("GC preserves active tasks, unknown references and pending compensation references", async () => {
  const active = createTask("task-turn-gc-active", { retentionMs: 0, graceMs: 0 });
  await seedTwoTurns(active.turns, active.taskId, "gc-active");
  assert.equal((await active.turns.collectGarbage(Date.now() + 1000)).tombstoned, 0);

  const unknown = createTask("task-turn-gc-unknown", { retentionMs: 0, graceMs: 0 });
  await seedTwoTurns(unknown.turns, unknown.taskId, "gc-unknown");
  const oldUnknownRef = unknown.state.storage.sql.exec<{ ref_id: string; object_key: string }>(
    "SELECT ref_id, object_key FROM task_snapshot_refs WHERE ref_kind = ? AND turn_seq = ?", "SNAPSHOT_BLOCK", 1,
  ).toArray()[0];
  await unknown.turns.markReferenceUnknown(oldUnknownRef.ref_id, "retention owner is unknown");
  unknown.state.storage.sql.exec("UPDATE task_execution SET lifecycle_state = ? WHERE task_id = ?", "SUCCEEDED", unknown.taskId);
  assert.equal((await unknown.turns.collectGarbage(Date.now() + 1000)).tombstoned, 0);
  assert.equal(unknown.blockStore.objects.has(oldUnknownRef.object_key), true);

  const pending = createTask("task-turn-gc-compensation", { retentionMs: 0, graceMs: 0 });
  await seedTwoTurns(pending.turns, pending.taskId, "gc-compensation");
  await pending.operations.prepareOperation({
    operationId: stableTaskOperationId(pending.taskId, "gc-pending-tool"), tenantId: "tenant-turns",
    fence: new FenceToken(3, 3, pending.taskId), toolName: "billing/charge",
    effectClass: ToolEffectClass.SAGA_COMPENSATABLE, args: { invoice: "inv-gc" },
    compensation: { toolName: "billing/undo-charge", args: { invoice: "inv-gc" } },
  });
  pending.state.storage.sql.exec("UPDATE task_execution SET lifecycle_state = ? WHERE task_id = ?", "SUCCEEDED", pending.taskId);
  assert.equal((await pending.turns.collectGarbage(Date.now() + 1000)).tombstoned, 0);
});

async function seedTwoTurns(turns: TaskTurns, taskId: string, label: string): Promise<void> {
  await beginWithChunks(turns, taskId, 1, `upload-${label}-1`, [`${label} workspace one`]);
  await turns.commitTurn({ taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 1, taskId), uploadId: `upload-${label}-1`, requestDigest: await turnRequestDigest(taskId, 1), response: `${label} turn one` });
  await beginWithChunks(turns, taskId, 2, `upload-${label}-2`, [`${label} workspace two`]);
  await turns.commitTurn({ taskId, tenantId: "tenant-turns", fence: new FenceToken(3, 2, taskId), uploadId: `upload-${label}-2`, requestDigest: await turnRequestDigest(taskId, 2), response: `${label} turn two` });
}

async function turnRequestDigest(taskId: string, turnSeq: number): Promise<string> {
  return sha256Hex(new TextEncoder().encode(`${taskId}:${turnSeq}:request`));
}

