"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
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