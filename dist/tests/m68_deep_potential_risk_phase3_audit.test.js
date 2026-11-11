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
 * @fileoverview M68 E2E Verification Suite: Deep Potential Risk Identification & Elimination Suite Phase III
 * Covers:
 * - [M68-1] `TunnelGatewayCell` Timed-Out Command Queue/Idempotency Eviction, Non-Regressive Epoch/Generation Registration Guard & Bounded Idempotency Cache
 * - [M68-2] `FinOpsBudgetController` UTC-Day Rollover Idempotency Key Pruning & Policy Parameter Sanitization
 * - [M68-3] `HierarchicalQueueManager` Eager Tombstone/Empty-Heap Cleanup & `LeaseCoordinator` `advanceTurnFence` Epoch Watermark Sync + Fence Purge
 * - [M68-4] `CoreScheduler5D` Cascade-Dead-Letter Active Lease/Resource Reclamation + Terminal Content-Fingerprint Release & `NodeDaemon` Checkpoint Purge
 * - [M68-5] `SagaCompensatorEngine` No-Handler Step Rollback Idempotency Invalidation + Step Purge & `StagingCommitAndApprovalGate` Completed Task Staging Cleanup
 * - [M68-6] Management Plane Endpoint (`POST /v1beta1/management/system/deep-risk-phase3-audit`) & CLI (`anstsctl mgmt-deep-risk-phase3-audit`)
 */
const assert = __importStar(require("node:assert/strict"));
const node_test_1 = require("node:test");
const env_js_1 = require("../src/cloudflare/env.js");
const tunnel_gateway_cell_js_1 = require("../src/cloudflare/tunnel_gateway_cell.js");
const budget_controller_js_1 = require("../src/finops/budget_controller.js");
const queue_manager_js_1 = require("../src/queue_manager.js");
const lease_coordinator_js_1 = require("../src/lease_coordinator.js");
const scheduler_js_1 = require("../src/scheduler.js");
const node_daemon_js_1 = require("../src/node_daemon.js");
const saga_gateway_js_1 = require("../src/saga_gateway.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M68 Deep Potential Risk Identification & Elimination Suite Phase III", () => {
    (0, node_test_1.it)("[M68-1] TunnelGatewayCell evicts timed-out unpolled commands from host queue/idempotency/SQLite, guards non-regressive registration, and bounds idempotency cache", async () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const cell = new tunnel_gateway_cell_js_1.TunnelGatewayCell(sql, 3);
        const r1 = cell.registerTunnel({
            hostId: "body-m68-01",
            gatewayId: "gw-01",
            role: "PRIMARY",
            connGeneration: 4,
            epoch: 3,
        });
        assert.equal(r1.epoch, 3);
        assert.equal(r1.connGeneration, 4);
        // Out-of-order stale registration packet with older epoch or generation must not downgrade the binding
        const regressedEpoch = cell.registerTunnel({
            hostId: "body-m68-01",
            gatewayId: "gw-01",
            role: "PRIMARY",
            connGeneration: 9,
            epoch: 2,
        });
        assert.equal(regressedEpoch.epoch, 3);
        assert.equal(regressedEpoch.connGeneration, 4);
        const regressedGen = cell.registerTunnel({
            hostId: "body-m68-01",
            gatewayId: "gw-01",
            role: "PRIMARY",
            connGeneration: 2,
            epoch: 3,
        });
        assert.equal(regressedGen.epoch, 3);
        assert.equal(regressedGen.connGeneration, 4);
        // Enqueue a command with short timeout that times out BEFORE host polls
        const timedOutAttempt = cell.enqueueCommandForHost("body-m68-01", { cmd: "sleep 10" }, 30, { idempotencyKey: "idem-timeout-01" });
        await assert.rejects(timedOutAttempt.completionPromise, /timed out/);
        // Subsequent poll must NOT receive the timed-out ghost command!
        const polledAfterTimeout = cell.pollPendingCommands("body-m68-01");
        assert.equal(polledAfterTimeout.length, 0);
        // Subsequent retry with the same idempotencyKey must cleanly enqueue a fresh command
        const retryAttempt = cell.enqueueCommandForHost("body-m68-01", { cmd: "echo recovered" }, 2000, { idempotencyKey: "idem-timeout-01" });
        assert.equal(retryAttempt.deduplicated, false);
        assert.notEqual(retryAttempt.command.commandId, timedOutAttempt.command.commandId);
        const polledRetry = cell.pollPendingCommands("body-m68-01");
        assert.equal(polledRetry.length, 1);
        cell.acknowledgeCommand(polledRetry[0].commandId, { ok: true });
        const retryRes = await retryAttempt.completionPromise;
        assert.equal(retryRes.ok, true);
        // Bounded idempotency cache (maxRetainedIdempotentCommands = 3)
        for (let i = 1; i <= 5; i++) {
            const enq = cell.enqueueCommandForHost("body-m68-01", { step: i }, 2000, {
                idempotencyKey: `idem-cap-${i}`,
            });
            const p = cell.pollPendingCommands("body-m68-01");
            cell.acknowledgeCommand(p[0].commandId, { stepDone: i });
            await enq.completionPromise;
        }
        // Oldest key `idem-timeout-01` and `idem-cap-1`, `idem-cap-2` were evicted; `idem-cap-5` remains cached
        const hit5 = cell.enqueueCommandForHost("body-m68-01", { step: 5 }, 2000, {
            idempotencyKey: "idem-cap-5",
        });
        assert.equal(hit5.deduplicated, true);
    });
    (0, node_test_1.it)("[M68-2] FinOpsBudgetController prunes tenant idempotency keys on UTC day rollover and sanitizes invalid policy parameters", () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const finops = new budget_controller_js_1.FinOpsBudgetController(sql);
        finops.setPolicy({
            tenantId: "Northstar",
            dailyBudgetUsd: -50,
            softDowngradeThresholdRatio: 2.5,
            preferredModel: "DeepSeek-R1",
            fallbackEconomyModel: "Qwen-3.8",
        });
        const sanitized = finops.getPolicy("Northstar");
        assert.equal(sanitized.dailyBudgetUsd, 0);
        assert.equal(sanitized.softDowngradeThresholdRatio, 1);
        finops.setPolicy({
            tenantId: "Northstar",
            dailyBudgetUsd: 100,
            softDowngradeThresholdRatio: 0.8,
            preferredModel: "DeepSeek-R1",
            fallbackEconomyModel: "Qwen-3.8",
        });
        // Day 1: record spend with daily recurring job idempotency key
        const s1 = finops.recordSpend("Northstar", 15, "2026-11-10T10:00:00Z", "daily-batch-rollup");
        const s1Dup = finops.recordSpend("Northstar", 15, "2026-11-10T11:00:00Z", "daily-batch-rollup");
        assert.equal(s1, 15);
        assert.equal(s1Dup, 15);
        assert.equal(finops.getSeenIdempotencyKeyCount("Northstar"), 1);
        // Day 2 rollover: spend resets to 0 AND yesterday's idempotency keys for Northstar are pruned
        const s2 = finops.recordSpend("Northstar", 20, "2026-11-11T00:05:00Z", "daily-batch-rollup");
        assert.equal(s2, 20);
        assert.equal(finops.getSeenIdempotencyKeyCount("Northstar"), 1);
    });
    (0, node_test_1.it)("[M68-3] HierarchicalQueueManager eagerly cleans up removed tasks & empty tenant heaps, and LeaseCoordinator syncs highestTokenByTask on advanceTurnFence", () => {
        const qm = new queue_manager_js_1.HierarchicalQueueManager(-1, Number.NaN);
        const t1 = new models_js_1.TaskSpec({ task_id: "t-qm-1", tenant_id: "ephemeral-tenant-A" });
        const t2 = new models_js_1.TaskSpec({ task_id: "t-qm-2", tenant_id: "ephemeral-tenant-B" });
        qm.enqueue(t1);
        qm.enqueue(t2);
        assert.equal(qm.activeTenantHeapCount(), 2);
        // Eagerly remove t1 -> ephemeral-tenant-A's empty heap must be deleted immediately
        qm.removeQueuedTask("t-qm-1");
        assert.equal(qm.activeTenantHeapCount(), 1);
        const popped = qm.popNext();
        assert.equal(popped?.task_id, "t-qm-2");
        assert.equal(qm.activeTenantHeapCount(), 0);
        qm.completeTask(t2);
        assert.equal(qm.tenantActiveCount.has("ephemeral-tenant-B"), false);
        // LeaseCoordinator: advanceTurnFence with a higher epoch must sync highestTokenByTask
        const lc = new lease_coordinator_js_1.LeaseCoordinator(15);
        const l1 = lc.acquireLease("t-fence-1", "node-1");
        assert.equal(l1.fencing_token, 1);
        const advanced = lc.advanceTurnFence("t-fence-1", new models_js_1.FenceToken(5, 2, "t-fence-1"));
        assert.equal(advanced.epoch, 5);
        assert.equal(advanced.turnSeq, 3);
        assert.equal(lc.getHighestFencingToken("t-fence-1"), 5);
        // Subsequent acquireLease must issue epoch 6 (never regressing to 2!)
        const l2 = lc.acquireLease("t-fence-1", "node-2");
        assert.equal(l2.fencing_token, 6);
        assert.equal(lc.purgeCompletedTaskFence("t-fence-1"), true);
        assert.equal(lc.getHighestFencingToken("t-fence-1"), 0);
    });
    (0, node_test_1.it)("[M68-4] CoreScheduler5D reclaims active lease/node resources on cascadeDeadLetterToDependents, releases terminal content fingerprints, and NodeDaemon purges checkpoints", () => {
        const sched = new scheduler_js_1.CoreScheduler5D();
        sched.registerOrUpdateNode(new models_js_1.NodeHeartbeat({
            node_id: "node-m68-01",
            total_cpu_cores: 16,
            free_cpu_cores: 16,
            total_memory_mb: 32768,
            free_memory_mb: 32768,
        }));
        const parent = new models_js_1.TaskSpec({
            task_id: "t-parent-m68",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P1_HIGH,
            prompt: "parent workflow",
        });
        const child = new models_js_1.TaskSpec({
            task_id: "t-child-m68",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P1_HIGH,
            prompt: "child workflow",
        });
        sched.submitTask(parent, { mergeDuplicateInFlight: true });
        sched.submitTask(child, { mergeDuplicateInFlight: true });
        const placements = sched.scheduleTick();
        assert.equal(placements.length, 2);
        const nodeBefore = sched.nodes.get("node-m68-01");
        assert.equal(nodeBefore.running_tasks, 2);
        // Wire child to depend on parent while child is already LEASED, then cascade dead-letter from parent
        child.dependencies.push("t-parent-m68");
        const cascaded = sched.cascadeDeadLetterToDependents("t-parent-m68");
        assert.deepEqual(cascaded, ["t-child-m68"]);
        assert.equal(child.state, models_js_1.TaskState.DEAD_LETTER);
        assert.equal(sched.leaseCoordinator.getLease("t-child-m68"), undefined);
        assert.equal(nodeBefore.running_tasks, 1);
        // Complete parent and verify its content fingerprint is released so a future identical submission creates a new task
        const parentToken = parent.lease_fencing_token;
        assert.equal(sched.completeTaskExecution("t-parent-m68", "node-m68-01", parentToken), true);
        assert.equal(sched.completeTaskExecution("t-parent-m68", "node-m68-01", parentToken), false);
        const resubmitted = new models_js_1.TaskSpec({
            task_id: "t-parent-m68-resubmit",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P1_HIGH,
            prompt: "parent workflow",
        });
        const subRes = sched.submitTask(resubmitted, { mergeDuplicateInFlight: true });
        assert.equal(subRes.task_id, "t-parent-m68-resubmit");
        // NodeDaemon checkpoint purge
        const daemon = new node_daemon_js_1.NodeDaemon("node-m68-01", -4, 0, -2);
        assert.equal(daemon.totalCpu, 32.0);
        assert.equal(daemon.totalMemMb, 65536);
        assert.equal(daemon.warmPoolSize, 8);
        sched.scheduleTick();
        daemon.writeTurnCheckpoint(sched, resubmitted, 1, "sha256:t1", "s3://ckpt/1");
        assert.equal(daemon.purgeTaskCheckpoints("t-parent-m68-resubmit"), 1);
        assert.equal(daemon.latestCheckpoint("t-parent-m68-resubmit"), null);
    });
    (0, node_test_1.it)("[M68-5] SagaCompensatorEngine invalidates idempotency keys on rollback even when no compensation handler is registered, and StagingGate purges completed task staging", () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const gw = new saga_gateway_js_1.SagaToolGateway(undefined, undefined, sql);
        let execCount = 0;
        // Register a COMPENSATABLE tool WITHOUT a compensateHandler
        gw.registerTool("stateless_side_effect", models_js_1.ToolCallEffectClass.COMPENSATABLE, (args) => {
            execCount += 1;
            return { run: execCount, echo: args.v };
        });
        const [r1, d1] = gw.invokeTool("t-m68-saga", 2, "stateless_side_effect", { v: "alpha" });
        assert.equal(d1, false);
        assert.equal(r1.run, 1);
        // Roll back turn >= 2: even without a compensateHandler, step must be marked compensated and idempotency key invalidated!
        const rb = gw.rollbackAndInvalidateTask("t-m68-saga", 2);
        assert.equal(rb.rolledBackSteps.length, 1);
        assert.equal(rb.invalidatedIdemKeys.length >= 1, true);
        // Re-executing turn 2 after breakpoint recovery must execute fresh rather than returning stale cached result
        const [r2, d2] = gw.invokeTool("t-m68-saga", 2, "stateless_side_effect", { v: "alpha" });
        assert.equal(d2, false);
        assert.equal(r2.run, 2);
        assert.equal(gw.compensator.purgeTaskSteps("t-m68-saga"), 2);
        assert.equal(gw.compensator.getTaskSteps("t-m68-saga").length, 0);
        // StagingCommitAndApprovalGate completed task staging cleanup
        const task = new models_js_1.TaskSpec({ task_id: "t-m68-stage", tenant_id: "Northstar" });
        gw.stagingGate.stageEffect({
            effectId: "eff-m68-1",
            taskId: "t-m68-stage",
            turnIndex: 1,
            toolName: "stateless_side_effect",
            args: { v: "staged" },
        });
        const ticket = gw.stagingGate.requestHumanApproval(task, "stateless_side_effect", { v: "staged" });
        gw.stagingGate.resolveApproval(ticket.ticketId, true, "chen", task);
        gw.stagingGate.commitStagedEffects("t-m68-stage", (tName, tArgs, tTurn) => gw.invokeTool("t-m68-stage", tTurn, tName, tArgs)[0]);
        const purged = gw.stagingGate.purgeCompletedTaskStaging("t-m68-stage");
        assert.equal(purged.purgedEffects, 1);
        assert.equal(purged.purgedTickets, 1);
        assert.equal(gw.stagingGate.getStagedEffects("t-m68-stage").length, 0);
    });
    (0, node_test_1.it)("[M68-6] End-to-End Management Plane Endpoint (POST /v1beta1/management/system/deep-risk-phase3-audit) & CLI (anstsctl mgmt-deep-risk-phase3-audit)", async () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m68-live-"));
        const server = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            apiPort: 0,
            dashboardPort: 0,
            storageDir: tmpDir,
            tickIntervalMs: 60_000,
        });
        const { apiPort } = await server.start();
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-deep-risk-phase3-audit"], baseUrl);
            assert.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            assert.equal(body.allM68DeepRiskPhase3ChecksPassed, true);
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
