import { strict as assert } from "node:assert";
import { test } from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { TaskTurns, taskTurnMigrations } from "../src/persistence/task-turns";
import { TaskCore, taskCoreMigration } from "../src/persistence/task-core";
import { TaskOperations, taskOperationsMigration, stableTaskOperationId } from "../src/persistence/task-operations";
import { initializeCellProtocol } from "../src/persistence/schema";
import { sha256Hex, SnapshotBlockStore } from "../src/checkpoint/snapshot_block_store";
import { FenceToken, ToolEffectClass } from "../src/models";

let DatabaseSync: any;
try { ({ DatabaseSync } = require("node:sqlite")); }
catch (error) { if ((error as NodeJS.ErrnoException).code !== "ERR_UNKNOWN_BUILTIN_MODULE") throw error; }
const sqliteUnavailable = !DatabaseSync && "Requires Node with node:sqlite.";

// Real owner SQL and root-storage transaction API; only bucket proof and object-store
// failures are controlled test boundaries. This is not real Celld/bucket DR evidence.
function fixture() {
  const directory = mkdtempSync(path.join(tmpdir(), "ansts-turn-integrity-"));
  const objects = new Map<string, Uint8Array>();
  const store: SnapshotBlockStore = {
    async putIfAbsent(key, bytes) { if (objects.has(key)) return "EXISTS"; objects.set(key, new Uint8Array(bytes)); return "CREATED"; },
    async get(key) { const bytes = objects.get(key); return bytes ? new Uint8Array(bytes) : null; },
    async delete(key) { objects.delete(key); },
  };
  let db: any;
  let rejectSync = false;
  function activate() {
    db = new DatabaseSync(path.join(directory, "task.sqlite"));
    db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
    const storage: any = {
      sql: { exec(query: string, ...values: unknown[]) {
        const stmt = db.prepare(query);
        const rows = stmt.columns().length ? stmt.all(...values) : (stmt.run(...values), []);
        return { toArray: () => rows, one: () => rows[0] ?? null };
      } },
      transactionSync(callback: () => unknown) {
        db.exec("BEGIN IMMEDIATE");
        try { const result = callback(); db.exec("COMMIT"); return result; }
        catch (error) { db.exec("ROLLBACK"); throw error; }
      },
      async sync() { if (rejectSync) throw new Error("bucket sync unavailable"); },
    };
    initializeCellProtocol(storage, { type: "task", key: "task-integrity" }, [taskCoreMigration, taskOperationsMigration, ...taskTurnMigrations]);
    const operations = new TaskOperations(storage);
    return { core: new TaskCore(storage), operations, turns: new TaskTurns(storage, store, operations, { snapshotRetentionMs: 0, gcGracePeriodMs: 0 }) };
  }
  return {
    activate, objects,
    failSync(value: boolean) { rejectSync = value; },
    reopen() { db.close(); return activate(); },
    cleanup() { db?.close(); rmSync(directory, { recursive: true, force: true }); },
  };
}

async function start(core: TaskCore) {
  const request = { task_id: "task-integrity", tenant_id: "tenant-integrity", dispatch_id: "dispatch-integrity",
    task_spec: { metadata: { task_id: "task-integrity", tenant_id: "tenant-integrity" }, spec: { prompt: "durable workspace" } },
    kind: "ordinary" as const, node_id: "node", node_incarnation: "boot", reservation_id: "reservation", reservation_token: "reserved" };
  await core.prepareDispatch(request);
  await core.recordReservation(request);
  const authorization = await core.authorizeDispatch(request);
  await core.markStarted(authorization, "started");
}

async function upload(turns: TaskTurns, seq: number, text: string) {
  const data = new TextEncoder().encode(text);
  const session = await turns.beginSnapshotUpload({ taskId: "task-integrity", tenantId: "tenant-integrity",
    fence: new FenceToken(1, seq, "task-integrity"), uploadId: `upload-${seq}`,
    chunks: [{ index: 0, sizeBytes: data.byteLength, sha256: await sha256Hex(data) }] });
  await turns.uploadSnapshotChunk({ taskId: "task-integrity", tenantId: "tenant-integrity", uploadId: session.uploadId, index: 0, bytes: data });
  return session;
}

function commit(turns: TaskTurns, seq: number, modelCallRefs: string[] = []) {
  return turns.commitTurn({ taskId: "task-integrity", tenantId: "tenant-integrity", fence: new FenceToken(1, seq, "task-integrity"),
    uploadId: `upload-${seq}`, requestDigest: "a".repeat(64), response: { output: `turn-${seq}` }, modelCallRefs });
}

test("a missing staged model-result block prevents Turn acknowledgment", { skip: sqliteUnavailable }, async () => {
  const f = fixture();
  try {
    const { core, turns } = f.activate();
    await start(core);
    await upload(turns, 1, "workspace");
    await turns.recordModelCallResult({ taskId: "task-integrity", tenantId: "tenant-integrity", callId: "model-call", turnSeq: 1,
      response: { text: "provider output" }, usage: { inputTokens: 7, outputTokens: 3 }, routeRefs: { routingVersion: 1 } });
    for (const key of f.objects.keys()) if (key.startsWith("task-model-call-results/")) f.objects.delete(key);
    await assert.rejects(commit(turns, 1, ["model-call"]), { code: "MODEL_CALL_RESULT_BLOCK_MISSING" });
    await assert.rejects(turns.recoverLatest("task-integrity"), { code: "NO_COMMITTED_TURN" });
    assert.equal(core.get("task-integrity")?.last_turn_seq, 0);
  } finally { f.cleanup(); }
});

test("GC retry keeps blocks when its earlier tombstone still cannot be durably confirmed", { skip: sqliteUnavailable }, async () => {
  const f = fixture();
  try {
    const { core, turns } = f.activate();
    await start(core);
    const old = await upload(turns, 1, "old workspace");
    await commit(turns, 1);
    await upload(turns, 2, "latest workspace");
    await commit(turns, 2);
    await core.finish("task-integrity", "tenant-integrity", 1, "SUCCEEDED");
    f.failSync(true);
    const now = Date.now() + 1000;
    await assert.rejects(turns.collectGarbage(now), /bucket sync unavailable/);
    await turns.collectGarbage(now + 10).catch(() => undefined);
    assert.equal(f.objects.has(old.chunks[0].objectKey), true, "physical deletion must wait for a confirmed tombstone on every retry");
    f.failSync(false);
    await turns.collectGarbage(now + 20);
    assert.equal(f.objects.has(old.chunks[0].objectKey), false);
    assert.equal((await turns.recoverLatest("task-integrity")).turnSeq, 2);
  } finally { f.cleanup(); }
});

test("replaying a staged model result reconfirms a previously lost durable acknowledgment", { skip: sqliteUnavailable }, async () => {
  const f = fixture();
  try {
    const { core, turns } = f.activate();
    await start(core);
    const request = { taskId: "task-integrity", tenantId: "tenant-integrity", callId: "model-call", turnSeq: 1,
      response: { text: "preserved output" }, usage: { inputTokens: 7, outputTokens: 3 }, routeRefs: { routingVersion: 1 } };
    f.failSync(true);
    await assert.rejects(turns.recordModelCallResult(request), /bucket sync unavailable/);
    await assert.rejects(turns.getConfirmedModelCallResult("task-integrity", "model-call"), /bucket sync unavailable/);
    await assert.rejects(turns.recordModelCallResult(request), /bucket sync unavailable/);
    f.failSync(false);
    assert.deepEqual((await turns.recordModelCallResult(request)).response, { text: "preserved output" });
  } finally { f.cleanup(); }
});

test("file reopen resumes partial upload and recovers the latest Turn with later effects and usage", { skip: sqliteUnavailable }, async () => {
  const f = fixture();
  try {
    let owners = f.activate();
    await start(owners.core);
    const blocks = [new TextEncoder().encode("workspace page one"), new TextEncoder().encode("workspace page two")];
    const request = { taskId: "task-integrity", tenantId: "tenant-integrity", uploadId: "upload-1", fence: new FenceToken(1, 1, "task-integrity"),
      chunks: await Promise.all(blocks.map(async (bytes, index) => ({ index, sizeBytes: bytes.byteLength, sha256: await sha256Hex(bytes) }))) };
    const original = await owners.turns.beginSnapshotUpload(request);
    await owners.turns.uploadSnapshotChunk({ ...request, index: 0, bytes: blocks[0] });
    owners = f.reopen();
    const resumed = await owners.turns.beginSnapshotUpload(request);
    assert.deepEqual(resumed, original);
    await owners.turns.uploadSnapshotChunk({ ...request, index: 1, bytes: blocks[1] });
    await commit(owners.turns, 1);
    await owners.turns.recordModelCallResult({ taskId: "task-integrity", tenantId: "tenant-integrity", callId: "later-call", turnSeq: 2,
      response: { text: "later model output" }, usage: { inputTokens: 7, outputTokens: 3, settlementId: "later-settlement" }, routeRefs: { routingVersion: 1 } });
    const operationId = stableTaskOperationId("task-integrity", "later-tool");
    const fence = new FenceToken(1, 2, "task-integrity");
    await owners.operations.prepareOperation({ operationId, tenantId: "tenant-integrity", fence,
      toolName: "orders/create", effectClass: ToolEffectClass.SAGA_COMPENSATABLE, args: { order: "order-1" },
      compensation: { toolName: "orders/cancel", args: { order: "order-1" } } });
    const attempt = await owners.operations.beginAttempt({ operationId, attemptId: "later-attempt", fence });
    await owners.operations.recordOperationOutcome({ operationId, attemptId: attempt.attemptId, fence, outcome: "SUCCEEDED", result: { order: "order-1", created: true } });
    owners = f.reopen();
    const recovered = await owners.turns.recoverLatest("task-integrity");
    assert.equal(recovered.turnSeq, 1);
    assert.deepEqual(recovered.response, { output: "turn-1" });
    assert.deepEqual(recovered.snapshotBlocks.map((bytes) => new TextDecoder().decode(bytes)), ["workspace page one", "workspace page two"]);
    assert.deepEqual(recovered.laterOperationOutcomes[0].result, { order: "order-1", created: true });
    assert.equal(recovered.laterCompensations[0].status, "PENDING");
    assert.deepEqual(recovered.laterModelCalls[0].response, { text: "later model output" });
    assert.deepEqual(recovered.laterSettlementRefs, ["later-settlement"]);
    f.objects.delete(original.chunks[1].objectKey);
    await assert.rejects(owners.turns.recoverLatest("task-integrity"), { code: "SNAPSHOT_BLOCK_MISSING" });
  } finally { f.cleanup(); }
});
