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
 * @fileoverview M51 End-to-End & Unit Chaos Verification: Scheduler Kernel Anti-Stale, Deadlock-Free & Starvation-Proof Hardening Suite.
 * Verifies all 5 kernel fixes:
 * 1. [Stale-1] Node Heartbeat TTL Expiration (`evictStaleNodes` & `scoreNodeForTask` rejecting silent dead nodes).
 * 2. [Stale-2] Orphan Lease Expiration Reclaims Node CPU, Memory & `running_tasks` (`reconcileOrphanedLeases`).
 * 3. [Deadlock-1] Intra-Tenant Head-of-Line (HoL) Blocking Elimination (`deferredTasks` allowing small tasks behind an unplaceable large task to be scheduled in the same tick).
 * 4. [Deadlock-2] Cascading DAG `DEAD_LETTER` Propagation (`cascadeDeadLetterToDependents` preventing downstream `SUBMITTED` child tasks from hanging forever).
 * 5. [Starvation-1] Fair Least-Preempted Victim Rotation, Preemption Cooldown Immunity & Preemption Priority Boost (`preempted_count` * 120 boost in `HierarchicalQueueManager`).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const scheduler_js_1 = require("../src/scheduler.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M51 Scheduler Kernel Anti-Stale, Deadlock-Free & Starvation-Proof Hardening Suite", () => {
    (0, node_test_1.it)("verifies Stale-1 heartbeat TTL, Stale-2 lease capacity reclaim, Deadlock-1 HoL bypass, Deadlock-2 cascading DAG dead-letter, Starvation-1 fair preemption, and Management CLI/API", async () => {
        const nowSec = 1_700_000_000;
        const sched = new scheduler_js_1.CoreScheduler5D(10.0, 30.0, 3, 5.0);
        // 1. [Stale-1] Silent dead node with 45s-old heartbeat is evicted and never receives tasks
        sched.registerOrUpdateNode(new models_js_1.NodeHeartbeat({
            node_id: "node-silent-dead",
            total_cpu_cores: 64,
            free_cpu_cores: 64,
            total_memory_mb: 131072,
            free_memory_mb: 131072,
            timestamp: nowSec - 45,
        }));
        sched.registerOrUpdateNode(new models_js_1.NodeHeartbeat({
            node_id: "node-healthy-4c",
            total_cpu_cores: 4,
            free_cpu_cores: 4,
            total_memory_mb: 8192,
            free_memory_mb: 8192,
            timestamp: nowSec,
        }));
        // 2. [Deadlock-1] Submit an unplaceable 16-core P0 task followed by a placeable 0.25-core P2 task for the SAME tenant
        const hugeP0 = new models_js_1.TaskSpec({
            task_id: "task-huge-p0",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P0_EMERGENCY,
        });
        sched.submitTask(hugeP0);
        hugeP0.resource_profile.think_cpu_cores = 16.0;
        const smallP2 = new models_js_1.TaskSpec({
            task_id: "task-small-p2",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P2_STANDARD,
        });
        sched.submitTask(smallP2);
        const placements = sched.scheduleTick(nowSec);
        assert.equal(sched.nodes.get("node-silent-dead").healthy, false, "Stale node must be marked unhealthy");
        assert.equal(placements.length, 1, "Small task behind unplaceable huge task must be scheduled in same tick");
        assert.equal(placements[0][0], "task-small-p2");
        assert.equal(placements[0][1], "node-healthy-4c");
        assert.equal(hugeP0.state, models_js_1.TaskState.QUEUED, "Unplaceable huge task must remain queued after tick");
        // 3. [Stale-2 & Deadlock-2] Register a 2-level downstream DAG chain (smallP2 -> childA -> grandchildB), then expire smallP2 past retry limit
        const childA = new models_js_1.TaskSpec({
            task_id: "task-dag-child-a",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P2_STANDARD,
            dependencies: ["task-small-p2"],
        });
        const grandchildB = new models_js_1.TaskSpec({
            task_id: "task-dag-grandchild-b",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P2_STANDARD,
            dependencies: ["task-dag-child-a"],
        });
        sched.submitTask(childA);
        sched.submitTask(grandchildB);
        assert.equal(childA.state, models_js_1.TaskState.SUBMITTED);
        assert.equal(grandchildB.state, models_js_1.TaskState.SUBMITTED);
        const healthyNode = sched.nodes.get("node-healthy-4c");
        assert.equal(healthyNode.running_tasks, 1);
        assert.ok(healthyNode.free_cpu_cores < 4.0);
        smallP2.retry_count = 3;
        sched.reconcileOrphanedLeases(nowSec + 15);
        // Verify Stale-2: node capacity & running_tasks fully reclaimed
        assert.equal(healthyNode.running_tasks, 0);
        assert.equal(healthyNode.free_cpu_cores, 4.0);
        assert.equal(healthyNode.free_memory_mb, 8192);
        // Verify Deadlock-2: both childA and transitive grandchildB cascaded to DEAD_LETTER
        assert.equal(smallP2.state, models_js_1.TaskState.DEAD_LETTER);
        assert.equal(childA.state, models_js_1.TaskState.DEAD_LETTER);
        assert.equal(childA.dead_letter_reason, "UPSTREAM_DEPENDENCY_DEAD_LETTER:task-small-p2");
        assert.equal(grandchildB.state, models_js_1.TaskState.DEAD_LETTER);
        assert.equal(grandchildB.dead_letter_reason, "UPSTREAM_DEPENDENCY_DEAD_LETTER:task-dag-child-a");
        // 4. [Starvation-1] Verify fair victim rotation, cooldown immunity, and preemption priority boost
        const schedFair = new scheduler_js_1.CoreScheduler5D(15.0, 30.0, 2, 5.0);
        schedFair.registerOrUpdateNode(new models_js_1.NodeHeartbeat({
            node_id: "node-2c",
            total_cpu_cores: 2.0,
            free_cpu_cores: 2.0,
            total_memory_mb: 8192,
            free_memory_mb: 8192,
            timestamp: nowSec,
        }));
        const p3First = new models_js_1.TaskSpec({ task_id: "p3-first", tenant_id: "Northstar", priority: models_js_1.TaskPriority.P3_BATCH });
        const p3Second = new models_js_1.TaskSpec({ task_id: "p3-second", tenant_id: "Northstar", priority: models_js_1.TaskPriority.P3_BATCH });
        schedFair.submitTask(p3First);
        schedFair.submitTask(p3Second);
        p3First.resource_profile.think_cpu_cores = 1.0;
        p3Second.resource_profile.think_cpu_cores = 1.0;
        schedFair.scheduleTick(nowSec);
        // Urgent #1 preempts one of the P3 tasks
        const u1 = new models_js_1.TaskSpec({ task_id: "p0-u1", tenant_id: "Northstar", priority: models_js_1.TaskPriority.P0_EMERGENCY });
        schedFair.submitTask(u1);
        u1.resource_profile.think_cpu_cores = 1.0;
        schedFair.scheduleTick(nowSec + 1);
        // Urgent #2 must preempt the OTHER P3 task (fair rotation, not repeatedly starving the same task)
        const u2 = new models_js_1.TaskSpec({ task_id: "p0-u2", tenant_id: "Northstar", priority: models_js_1.TaskPriority.P0_EMERGENCY });
        schedFair.submitTask(u2);
        u2.resource_profile.think_cpu_cores = 1.0;
        schedFair.scheduleTick(nowSec + 2);
        assert.equal(p3First.preempted_count, 1);
        assert.equal(p3Second.preempted_count, 1);
        // Verify preemption compensation boost (+120 per preemption) promotes a twice-preempted P3_BATCH (50 + 240 = 290) above an un-preempted P2_STANDARD (150)
        p3First.preempted_count = 2;
        const boostedScore = schedFair.queueManager.computeEffectiveScore(p3First, nowSec + 2);
        const p2StandardTask = new models_js_1.TaskSpec({ task_id: "p2-normal", tenant_id: "Northstar", priority: models_js_1.TaskPriority.P2_STANDARD });
        const p2Score = schedFair.queueManager.computeEffectiveScore(p2StandardTask, nowSec + 2);
        assert.ok(boostedScore > p2Score, `Boosted P3 score (${boostedScore}) must exceed normal P2 score (${p2Score})`);
        // 5. Verify Management Plane Endpoint & CLI (`POST /v1beta1/management/scheduler/kernel-audit` & `anstsctl mgmt-scheduler-audit`)
        const tmpDir = `/tmp/ansts-m51-${process.pid}-${Date.now()}`;
        fs.mkdirSync(tmpDir, { recursive: true });
        const server = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-01",
            nodeRole: "gateway",
            storageDir: tmpDir,
        });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-scheduler-audit"], baseUrl);
            assert.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            assert.equal(body.stale1HeartbeatTtlEvicted, true);
            assert.equal(body.stale2LeaseCapacityReclaimed, true);
            assert.equal(body.deadlock1HolBypassPlacedSmallTask, true);
            assert.equal(body.deadlock2DagCascadedDeadLetter, true);
            assert.equal(body.starvation1FairPreemptionRotation, true);
            assert.equal(body.allInvariantsVerified, true);
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
