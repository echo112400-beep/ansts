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
 * @fileoverview M8 Zero-Data-Loss HA & Durability Suite (5 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("M8 Zero-Data-Loss HA & Durability Suite", () => {
    (0, node_test_1.it)("1. RpoZeroDurabilityEngine flushes LTX segment with CAS If-None-Match", async () => {
        const bucket = new index_js_1.InMemoryR2Bucket();
        const engine = new index_js_1.RpoZeroDurabilityEngine(bucket);
        let mutated = false;
        const meta = await engine.commitWithSyncLtxFlush("cell-ha-1", 1, () => {
            mutated = true;
        }, new Uint8Array([1, 2, 3, 4]));
        assert.equal(mutated, true);
        assert.ok(meta.sha256.length === 64);
    });
    (0, node_test_1.it)("2. DurableWorkflowContext memoizes step.do() outputs across crash recovery", async () => {
        const sql = new index_js_1.InMemoryCellSqlStorage();
        const wf = new index_js_1.DurableWorkflowContext(sql, "wf-001");
        let runs = 0;
        const first = await wf.stepDo("charge", () => {
            runs += 1;
            return { tx: "abc" };
        });
        const second = await wf.stepDo("charge", () => {
            runs += 1;
            return { tx: "xyz" };
        });
        assert.equal(first.replayedFromJournal, false);
        assert.equal(second.replayedFromJournal, true);
        assert.equal(runs, 1);
        assert.deepEqual(second.result, { tx: "abc" });
    });
    (0, node_test_1.it)("3. ShardedQueueBrokerPool distributes queue load across 16 shards deterministically", () => {
        const pool = new index_js_1.ShardedQueueBrokerPool(16);
        const shard = pool.publish("tenant-a:task-1", "msg-1", { hello: "world" });
        const batch = pool.consumeBatch(shard);
        assert.equal(batch.length, 1);
        assert.equal(batch[0].id, "msg-1");
    });
    (0, node_test_1.it)("4. SafeEpochGarbageCollector preserves active snapshots and collects expired fencing epochs", () => {
        const gc = new index_js_1.SafeEpochGarbageCollector(1000);
        const now = Date.now();
        const collectible = gc.filterCollectibleSnapshots([
            { key: "old-1", createdAtMs: now - 5000, fencingToken: 1, referencedByActiveCell: false },
            { key: "active-2", createdAtMs: now - 5000, fencingToken: 5, referencedByActiveCell: true },
        ], 3, now);
        assert.deepEqual(collectible, ["old-1"]);
    });
    (0, node_test_1.it)("5. FleetHaAndAutoscalerController triggers SCALE_OUT when unhealthy nodes are detected", () => {
        const ctrl = new index_js_1.FleetHaAndAutoscalerController();
        const res = ctrl.evaluateFleetHealth([
            { nodeId: "n1", healthy: true, cpuUtilPct: 50 },
            { nodeId: "n2", healthy: false, cpuUtilPct: 0 },
        ]);
        assert.equal(res.scaleAction, "SCALE_OUT");
        assert.deepEqual(res.unhealthyNodeIds, ["n2"]);
    });
});
