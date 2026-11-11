"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview M14 100% Component Persistence & Cold-Start Rehydration Suite (2 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("M14 Full Component Persistence & Cold-Start Rehydration Suite", () => {
    (0, node_test_1.it)("1. UnifiedComponentPersistenceStore snapshots and rehydrates CoreScheduler5D state across restart", () => {
        const sql = new index_js_1.InMemoryCellSqlStorage();
        const store = new index_js_1.UnifiedComponentPersistenceStore(sql);
        const sched1 = new index_js_1.CoreScheduler5D();
        sched1.submitTask(new index_js_1.TaskSpec({ task_id: "persist-t1", tenant_id: "t1", priority: index_js_1.TaskPriority.P1_HIGH, prompt: "Saved" }));
        const snap = store.snapshotSchedulerState(sched1);
        assert.equal(snap.tasksSaved, 1);
        const sched2 = new index_js_1.CoreScheduler5D();
        const restoredCount = store.rehydrateSchedulerFromSql(sched2);
        assert.equal(restoredCount, 1);
        assert.equal(sched2.allTasks.get("persist-t1")?.prompt, "Saved");
    });
    (0, node_test_1.it)("2. UnifiedComponentPersistenceStore persists Escrows, Router State, Saga Steps, Approvals, FinOps & MCP Tools", () => {
        const sql = new index_js_1.InMemoryCellSqlStorage();
        const store = new index_js_1.UnifiedComponentPersistenceStore(sql);
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
