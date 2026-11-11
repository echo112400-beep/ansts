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
 * @fileoverview M9 Horizontal Scale-Out Architecture Suite (3 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("M9 Horizontal Scale-Out Suite", () => {
    (0, node_test_1.it)("1. PartitionedSchedulerRouter distributes tasks across 64 consistent-hash shards", async () => {
        const router = new index_js_1.PartitionedSchedulerRouter(64);
        const seenShards = new Set();
        for (let i = 0; i < 64; i++) {
            const { shardId } = await router.routeSubmitTask(new index_js_1.TaskSpec({ task_id: `t-${i}`, tenant_id: `tenant-${i}` }));
            seenShards.add(shardId);
        }
        assert.ok(seenShards.size > 15);
    });
    (0, node_test_1.it)("2. DynamicQueueAutoSharder scales queue shards dynamically with incoming write RPS", () => {
        const sharder = new index_js_1.DynamicQueueAutoSharder(16, 4, 128);
        const up = sharder.adjustShards(18_000, 450);
        assert.equal(up.newShards, 40);
        assert.equal(up.rebalanced, true);
    });
    (0, node_test_1.it)("3. BrainBodyContainerPoolScaler decouples lightweight V8 Isolate memory from heavy ACT containers", () => {
        const scaler = new index_js_1.BrainBodyContainerPoolScaler();
        const plan = scaler.computeTargetSandboxContainers({
            activeThinkCells: 1000,
            activeActCells: 50,
            warmStandbyRatio: 0.1,
        });
        assert.equal(plan.requiredHeavyActContainers, 50);
        assert.equal(plan.warmBufferContainers, 100);
        assert.equal(plan.totalContainerPoolSize, 150);
    });
});
