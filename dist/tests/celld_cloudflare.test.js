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
 * @fileoverview M7 Cloudflare Workers & denoland/celld Integration Suite (5 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("M7 Cloudflare Workers & denoland/celld Integration Suite", () => {
    (0, node_test_1.it)("1. AnstsSchedulerCell persists submitted tasks into embedded SQLite and sets DO alarm", async () => {
        const state = new index_js_1.MockDurableObjectState("sched-cell-1");
        const cell = new index_js_1.AnstsSchedulerCell(state);
        const task = new index_js_1.TaskSpec({ task_id: "cf-task-1", tenant_id: "t1", priority: index_js_1.TaskPriority.P1_HIGH });
        await cell.submitTask(task);
        const alarm = await state.storage.getAlarm();
        assert.ok(alarm !== null);
    });
    (0, node_test_1.it)("2. AnstsSchedulerCell rehydrates task queue from SQLite after cell migration/restart", async () => {
        const state = new index_js_1.MockDurableObjectState("sched-cell-rehydrate");
        const cell1 = new index_js_1.AnstsSchedulerCell(state);
        await cell1.submitTask(new index_js_1.TaskSpec({ task_id: "cf-rehydrate-1", tenant_id: "t1" }));
        const cell2 = new index_js_1.AnstsSchedulerCell(state);
        assert.ok(cell2.scheduler.allTasks.has("cf-rehydrate-1"));
    });
    (0, node_test_1.it)("3. AnstsSchedulerCell alarm() dispatches queued tasks to registered nodes", async () => {
        const state = new index_js_1.MockDurableObjectState("sched-cell-alarm");
        const cell = new index_js_1.AnstsSchedulerCell(state);
        cell.registerNode(new index_js_1.NodeDaemon("cf-node-1").emitHeartbeat());
        await cell.submitTask(new index_js_1.TaskSpec({ task_id: "cf-alarm-task", tenant_id: "t1" }));
        const res = await cell.alarm();
        assert.equal(res.placements.length, 1);
    });
    (0, node_test_1.it)("4. AgentTaskHarnessCell writes turn LTX snapshot to R2/RustFS bucket and SQLite", async () => {
        const state = new index_js_1.MockDurableObjectState("harness-cell-1");
        const bucket = new index_js_1.InMemoryR2Bucket();
        const harness = new index_js_1.AgentTaskHarnessCell(state, bucket, "task-h-1");
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
    (0, node_test_1.it)("5. AgentTaskHarnessCell rejects stale fencing token writes", async () => {
        const state = new index_js_1.MockDurableObjectState("harness-cell-fence");
        const bucket = new index_js_1.InMemoryR2Bucket();
        const harness = new index_js_1.AgentTaskHarnessCell(state, bucket, "task-h-fence");
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
