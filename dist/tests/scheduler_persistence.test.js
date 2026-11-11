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
 * @fileoverview feat/celld-v25 Single-Write-Path Scheduler Persistence & Queue Deduplication Suite (`b566a53`).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("feat/celld-v25 Single-Write-Path Scheduler Persistence Suite", () => {
    (0, node_test_1.it)("Commits tasks via single write path and deduplicates repeated idempotency keys", () => {
        const sql = new index_js_1.InMemoryCellSqlStorage();
        const store = new index_js_1.SchedulerV25SingleWritePathStore(sql);
        const task = new index_js_1.TaskSpec({
            task_id: "v25-task-001",
            tenant_id: "tenant-alpha",
            priority: index_js_1.TaskPriority.P1_HIGH,
            idempotency_key: "idem-v25-001",
            prompt: "Verify v2.5 single write path",
        });
        const first = store.commitTaskSingleWritePath(task);
        const second = store.commitTaskSingleWritePath(task);
        assert.equal(first.deduplicated, false);
        assert.equal(second.deduplicated, true);
        assert.equal(store.listPersistedTasks().length, 1);
    });
});
