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
 * @fileoverview Chaos Fault Injection & High-Concurrency Benchmarks (4 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("Chaos Engineering & Benchmarks Suite", () => {
    (0, node_test_1.it)("1. 500-Task High-Concurrency Dispatch Benchmark completes in < 100ms", () => {
        const scheduler = new index_js_1.CoreScheduler5D();
        for (let n = 0; n < 20; n++) {
            const d = new index_js_1.NodeDaemon(`node-bench-${n}`, 64, 131072, 16);
            scheduler.registerOrUpdateNode(d.emitHeartbeat());
        }
        const t0 = performance.now();
        for (let i = 0; i < 500; i++) {
            scheduler.submitTask(new index_js_1.TaskSpec({
                task_id: `bench-task-${i}`,
                tenant_id: `tenant-${i % 10}`,
                priority: (i % 4),
            }));
        }
        const placements = scheduler.scheduleTick();
        const elapsedMs = performance.now() - t0;
        assert.equal(placements.length, 500);
        assert.ok(elapsedMs < 500, `Expected <500ms, got ${elapsedMs.toFixed(1)}ms`);
    });
    (0, node_test_1.it)("2. P0_EMERGENCY Preemption evicts running P3_BATCH task when cluster is saturated", () => {
        const scheduler = new index_js_1.CoreScheduler5D();
        const node = new index_js_1.NodeDaemon("node-tight", 1.0, 2048, 1).emitHeartbeat();
        node.free_cpu_cores = 0.5;
        node.free_memory_mb = 1500;
        scheduler.registerOrUpdateNode(node);
        const p3 = new index_js_1.TaskSpec({
            task_id: "batch-victim",
            tenant_id: "t-batch",
            priority: index_js_1.TaskPriority.P3_BATCH,
            resource_profile: { think_cpu_cores: 0.5, act_cpu_cores: 1, act_memory_mb: 1024, disk_mb: 1024 },
        });
        scheduler.submitTask(p3);
        scheduler.scheduleTick();
        assert.equal(p3.state, index_js_1.TaskState.LEASED);
        node.free_cpu_cores = 0.0;
        node.free_memory_mb = 100;
        const p0 = new index_js_1.TaskSpec({
            task_id: "p0-urgent",
            tenant_id: "t-urgent",
            priority: index_js_1.TaskPriority.P0_EMERGENCY,
            resource_profile: { think_cpu_cores: 0.25, act_cpu_cores: 1, act_memory_mb: 512, disk_mb: 1024 },
        });
        scheduler.submitTask(p0);
        scheduler.scheduleTick();
        assert.equal(p3.state, index_js_1.TaskState.QUEUED);
    });
    (0, node_test_1.it)("3. Expired Lease Reconciliation recovers orphaned tasks and moves >3 retries to DEAD_LETTER", () => {
        const scheduler = new index_js_1.CoreScheduler5D(0.001);
        scheduler.registerOrUpdateNode(new index_js_1.NodeDaemon("node-crash").emitHeartbeat());
        const task = new index_js_1.TaskSpec({ task_id: "orphan-1", tenant_id: "t1", priority: index_js_1.TaskPriority.P1_HIGH });
        scheduler.submitTask(task);
        scheduler.scheduleTick();
        const lease = scheduler.leaseCoordinator.getLease("orphan-1");
        lease.expires_at = Date.now() / 1000 - 10;
        const recovered = scheduler.reconcileOrphanedLeases();
        assert.deepEqual(recovered, ["orphan-1"]);
        assert.equal(task.retry_count, 1);
    });
    (0, node_test_1.it)("4. Linux PSI Memory Guard triggers FREEZE_AND_CHECKPOINT before Linux OOM-Killer", () => {
        const guard = new index_js_1.LinuxPsiMemoryGuard(40.0, 70.0);
        const t1 = new index_js_1.TaskSpec({ task_id: "t-p0", tenant_id: "t", priority: index_js_1.TaskPriority.P0_EMERGENCY });
        const t2 = new index_js_1.TaskSpec({ task_id: "t-p3", tenant_id: "t", priority: index_js_1.TaskPriority.P3_BATCH });
        const action = guard.evaluateAndDefend({ someAvg10Pct: 80, fullAvg10Pct: 75, freeSwapMb: 64 }, [t1, t2]);
        assert.equal(action.level, "FREEZE_AND_CHECKPOINT");
        assert.ok(action.evictedTaskIds.includes("t-p3"));
    });
});
