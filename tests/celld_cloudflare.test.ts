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
});
