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
  TaskState,
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

  it("7. AnstsSchedulerCell /purge removes finished tasks and their claims, and only those", async () => {
    const state = new MockDurableObjectState("sched-cell-purge");
    const cell = new AnstsSchedulerCell(state);
    const finished = new TaskSpec({ task_id: "cf-purge-done", tenant_id: "t1" });
    const running = new TaskSpec({ task_id: "cf-purge-running", tenant_id: "t1" });
    await cell.submitTask(finished);
    await cell.submitTask(running);
    // Submitting normalizes the state, so the terminal/in-flight states the purge must judge are set afterwards.
    cell.scheduler.allTasks.get("cf-purge-done")!.state = TaskState.DEAD_LETTER;
    cell.scheduler.allTasks.get("cf-purge-running")!.state = TaskState.RUNNING_ACT;
    const claimResp = await cell.fetch(
      new Request("https://cell.internal/v1/scheduler/claim", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "cf-purge-done", holder_id: "gw-01" }),
      })
    );
    assert.equal(claimResp.status, 200);

    const purge = (body: Record<string, unknown>) =>
      cell.fetch(
        new Request("https://cell.internal/v1/scheduler/purge", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        })
      );

    // dry_run must report the candidate without deleting it.
    const preview = await purge({ dry_run: true });
    const previewBody = (await preview.json()) as { purged: number; task_ids: string[] };
    assert.equal(previewBody.purged, 1);
    assert.deepEqual(previewBody.task_ids, ["cf-purge-done"]);
    assert.equal(cell.scheduler.allTasks.has("cf-purge-done"), true);

    // An in-flight state is rejected rather than quietly skipped.
    const rejected = await purge({ states: ["RUNNING_ACT"] });
    assert.equal(rejected.status, 400);
    assert.match(((await rejected.json()) as { message: string }).message, /not terminal/);

    const applied = await purge({});
    const appliedBody = (await applied.json()) as { purged: number; remaining: number };
    assert.equal(appliedBody.purged, 1);
    assert.equal(appliedBody.remaining, 1);
    assert.equal(cell.scheduler.allTasks.has("cf-purge-done"), false);
    assert.equal(cell.scheduler.allTasks.has("cf-purge-running"), true);

    // The purged task's claim went with it, so a later claim cannot resurrect the record.
    const reclaim = await cell.fetch(
      new Request("https://cell.internal/v1/scheduler/claim", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "cf-purge-done", holder_id: "gw-01" }),
      })
    );
    assert.equal(reclaim.status, 404);
  });
});
