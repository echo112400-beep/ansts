/**
 * @fileoverview M7 Cloudflare Workers & denoland/celld Integration Suite (5 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  MockDurableObjectState,
  InMemoryR2Bucket,
  AnstsSchedulerCell,
  AgentTaskHarnessCell,
  NodeDaemon,
  TaskPriority,
  TaskSpec,
} from "../src/index.js";

describe("M7 Cloudflare Workers & denoland/celld Integration Suite", () => {
  it("1. AnstsSchedulerCell persists submitted tasks into embedded SQLite and sets DO alarm", async () => {
    const state = new MockDurableObjectState("sched-cell-1");
    const cell = new AnstsSchedulerCell(state);
    const task = new TaskSpec({ task_id: "cf-task-1", tenant_id: "t1", priority: TaskPriority.P1_HIGH });
    await cell.submitTask(task);
    const alarm = await state.storage.getAlarm();
    assert.ok(alarm !== null);
  });

  it("2. AnstsSchedulerCell rehydrates task queue from SQLite after cell migration/restart", async () => {
    const state = new MockDurableObjectState("sched-cell-rehydrate");
    const cell1 = new AnstsSchedulerCell(state);
    await cell1.submitTask(new TaskSpec({ task_id: "cf-rehydrate-1", tenant_id: "t1" }));

    const cell2 = new AnstsSchedulerCell(state);
    assert.ok(cell2.scheduler.allTasks.has("cf-rehydrate-1"));
  });

  it("3. AnstsSchedulerCell alarm() dispatches queued tasks to registered nodes", async () => {
    const state = new MockDurableObjectState("sched-cell-alarm");
    const cell = new AnstsSchedulerCell(state);
    cell.registerNode(new NodeDaemon("cf-node-1").emitHeartbeat());
    await cell.submitTask(new TaskSpec({ task_id: "cf-alarm-task", tenant_id: "t1" }));
    const res = await cell.alarm();
    assert.equal(res.placements.length, 1);
  });

  it("4. AgentTaskHarnessCell writes turn LTX snapshot to R2/RustFS bucket and SQLite", async () => {
    const state = new MockDurableObjectState("harness-cell-1");
    const bucket = new InMemoryR2Bucket();
    const harness = new AgentTaskHarnessCell(state, bucket, "task-h-1");

    const rec = await harness.executeAndCheckpointTurn({
      turnIndex: 1,
      fencingToken: 5,
      toolName: "bash",
      toolOutput: { exitCode: 0 },
    });
    const obj = await bucket.get(rec.r2SnapshotKey);
    assert.ok(obj !== null);
    assert.equal(harness.getTurnHistory().length, 1);
  });

  it("5. AgentTaskHarnessCell rejects stale fencing token writes", async () => {
    const state = new MockDurableObjectState("harness-cell-fence");
    const bucket = new InMemoryR2Bucket();
    const harness = new AgentTaskHarnessCell(state, bucket, "task-h-fence");
    await harness.executeAndCheckpointTurn({
      turnIndex: 1,
      fencingToken: 10,
      toolName: "bash",
      toolOutput: {},
    });
    await assert.rejects(async () => {
      await harness.executeAndCheckpointTurn({
        turnIndex: 2,
        fencingToken: 9,
        toolName: "bash",
        toolOutput: {},
      });
    }, /Stale fencing token/);
  });

  it("6. AnstsSchedulerCell /claim arbitrates a placed task to exactly one gateway", async () => {
    const state = new MockDurableObjectState("sched-cell-claim");
    const cell = new AnstsSchedulerCell(state);
    cell.registerNode(new NodeDaemon("cf-node-claim").emitHeartbeat());
    await cell.submitTask(new TaskSpec({ task_id: "cf-claim-1", tenant_id: "t1" }));
    await cell.alarm();

    const claim = (taskId: string, holderId: string) =>
      cell.fetch(
        new Request("https://cell.internal/v1/scheduler/claim", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ task_id: taskId, holder_id: holderId }),
        })
      );

    const first = await claim("cf-claim-1", "gw-01");
    assert.equal(first.status, 200);
    assert.equal(((await first.json()) as { claimed: boolean }).claimed, true);

    // A sibling gateway must lose the arbitration instead of executing the same task twice.
    const second = await claim("cf-claim-1", "gw-02");
    assert.equal(second.status, 409);
    const secondBody = (await second.json()) as { claimed: boolean; holder_id: string };
    assert.equal(secondBody.claimed, false);
    assert.equal(secondBody.holder_id, "gw-01");

    // Re-claiming by the winner is idempotent, which is what a restarted gateway relies on.
    const again = await claim("cf-claim-1", "gw-01");
    assert.equal(again.status, 200);
    assert.equal(((await again.json()) as { claimed: boolean }).claimed, true);

    const missing = await claim("cf-claim-unknown", "gw-01");
    assert.equal(missing.status, 404);
  });
});
