"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M14 100% Component Persistence & Cold-Start Rehydration Suite (2 tests).
 */





(0, _nodeTest.describe)("M14 Full Component Persistence & Cold-Start Rehydration Suite", () => {
  (0, _nodeTest.it)("1. UnifiedComponentPersistenceStore snapshots and rehydrates CoreScheduler5D state across restart", () => {
    const sql = new _index.InMemoryCellSqlStorage();
    const store = new _index.UnifiedComponentPersistenceStore(sql);

    const sched1 = new _index.CoreScheduler5D();
    sched1.submitTask(
      new _index.TaskSpec({ task_id: "persist-t1", tenant_id: "t1", priority: _index.TaskPriority.P1_HIGH, prompt: "Saved" })
    );
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