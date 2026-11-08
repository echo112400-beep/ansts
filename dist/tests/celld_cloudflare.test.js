"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M7 Cloudflare Workers & denoland/celld Integration Suite (5 tests).
 */







(0, _nodeTest.describe)("M7 Cloudflare Workers & denoland/celld Integration Suite", () => {
  (0, _nodeTest.it)("1. AnstsSchedulerCell persists submitted tasks into embedded SQLite and sets DO alarm", async () => {
    const state = new _index.MockDurableObjectState("sched-cell-1");
    const cell = new _index.AnstsSchedulerCell(state);
    const task = new _index.TaskSpec({ task_id: "cf-task-1", tenant_id: "t1", priority: _index.TaskPriority.P1_HIGH });
    await cell.submitTask(task);
    const alarm = await state.storage.getAlarm();
    assert.ok(alarm !== null);
  });

  (0, _nodeTest.it)("2. AnstsSchedulerCell rehydrates task queue from SQLite after cell migration/restart", async () => {
    const state = new _index.MockDurableObjectState("sched-cell-rehydrate");
    const cell1 = new _index.AnstsSchedulerCell(state);
    await cell1.submitTask(new _index.TaskSpec({ task_id: "cf-rehydrate-1", tenant_id: "t1" }));

    const cell2 = new _index.AnstsSchedulerCell(state);
    assert.ok(cell2.scheduler.allTasks.has("cf-rehydrate-1"));
  });

  (0, _nodeTest.it)("3. AnstsSchedulerCell alarm() dispatches queued tasks to registered nodes", async () => {
    const state = new _index.MockDurableObjectState("sched-cell-alarm");
    const cell = new _index.AnstsSchedulerCell(state);
    cell.registerNode(new _index.NodeDaemon("cf-node-1").emitHeartbeat());
    await cell.submitTask(new _index.TaskSpec({ task_id: "cf-alarm-task", tenant_id: "t1" }));
    const res = await cell.alarm();
    assert.equal(res.placements.length, 1);
  });

  (0, _nodeTest.it)("4. AgentTaskHarnessCell writes turn LTX snapshot to R2/RustFS bucket and SQLite", async () => {
    const state = new _index.MockDurableObjectState("harness-cell-1");
    const bucket = new _index.InMemoryR2Bucket();
    const harness = new _index.AgentTaskHarnessCell(state, bucket, "task-h-1");

    const rec = await harness.executeAndCheckpointTurn({
      turnIndex: 1,
      fencingToken: 5,
      toolName: "bash",
      toolOutput: { exitCode: 0 }
    });
    const obj = await bucket.get(rec.r2SnapshotKey);
    assert.ok(obj !== null);
    assert.equal(harness.getTurnHistory().length, 1);
  });

  (0, _nodeTest.it)("5. AgentTaskHarnessCell rejects stale fencing token writes", async () => {
    const state = new _index.MockDurableObjectState("harness-cell-fence");
    const bucket = new _index.InMemoryR2Bucket();
    const harness = new _index.AgentTaskHarnessCell(state, bucket, "task-h-fence");
    await harness.executeAndCheckpointTurn({
      turnIndex: 1,
      fencingToken: 10,
      toolName: "bash",
      toolOutput: {}
    });
    await assert.rejects(async () => {
      await harness.executeAndCheckpointTurn({
        turnIndex: 2,
        fencingToken: 9,
        toolName: "bash",
        toolOutput: {}
      });
    }, /Stale fencing token/);
  });
});