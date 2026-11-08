"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M14 100% Component Persistence & Cold-Start Rehydration Suite (2 tests).
 */





(0, _nodeTest.describe)("M14 Full Component Persistence & Cold-Start Rehydration Suite", () => {
  (0, _nodeTest.it)("1. UnifiedComponentPersistenceStore snapshots and rehydrates CoreScheduler5D state across restart", () => {
    const sql = new _index.InMemoryCellSqlStorage();
    const store = new _index.UnifiedComponentPersistenceStore(sql);

    const sched1 = new _index.CoreScheduler5D();
    sched1.submitTask(
    new _index.TaskSpec({ task_id: "persist-t1", tenant_id: "t1", priority: _index.TaskPriority.P1_HIGH, prompt: "Saved" }));

    const snap = store.snapshotSchedulerState(sched1);
    assert.equal(snap.tasksSaved, 1);

    const sched2 = new _index.CoreScheduler5D();
    const restoredCount = store.rehydrateSchedulerFromSql(sched2);
    assert.equal(restoredCount, 1);
    assert.equal(sched2.allTasks.get("persist-t1")?.prompt, "Saved");
  });

  (0, _nodeTest.it)("2. UnifiedComponentPersistenceStore persists Escrows, Router State, Saga Steps, Approvals, FinOps & MCP Tools", () => {
    const sql = new _index.InMemoryCellSqlStorage();
    const store = new _index.UnifiedComponentPersistenceStore(sql);

    store.saveComponentRecord("persisted_escrows", "parent_task_id", "p1", { escrowedSlots: 3 });
    store.saveComponentRecord("persisted_router_state", "endpoint_id", "ep1", { ttft: 120 });
    store.saveComponentRecord("persisted_saga_steps", "step_id", "s1", { tool: "git_commit" });
    store.saveComponentRecord("persisted_approval_tickets", "ticket_id", "tk1", { status: "PENDING" });
    store.saveComponentRecord("persisted_finops_ledgers", "tenant_id", "ten1", { spendUsd: 4.2 });
    store.saveComponentRecord("persisted_mcp_tools", "tool_name", "mcp_search", { server: "s1" });

    assert.equal(store.loadComponentRecords("persisted_escrows").length, 1);
    assert.equal(store.loadComponentRecords("persisted_router_state").length, 1);
    assert.equal(store.loadComponentRecords("persisted_saga_steps").length, 1);
    assert.equal(store.loadComponentRecords("persisted_approval_tickets").length, 1);
    assert.equal(store.loadComponentRecords("persisted_finops_ledgers").length, 1);
    assert.equal(store.loadComponentRecords("persisted_mcp_tools").length, 1);
  });
});