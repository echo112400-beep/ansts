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
 * @fileoverview M61 Full-Subsystem Persistence, Idempotent CAS/Push Retry,
 * Breakpoint State Rollback & Coalescing Duplicate Arbitration Suite.
 *
 * Covers:
 * 1. [M61-1] `SharedBlackboardAndGitArbiter` & `TopologyCasCoordinator`: SQLite persistence,
 *    idempotent CAS retry on network timeout, and duplicate same-agent patch coalescing (preventing self-conflict).
 * 2. [M61-2] `StagingCommitAndApprovalGate`, `DagEscrowCoordinator` & `FinOpsBudgetController`:
 *    SQLite persistence, idempotent effect staging, coalesced pending human approval tickets,
 *    idempotent parent/child escrow reservation, and idempotent FinOps spend/refund accounting.
 * 3. [M61-3] `SyntacticAndVelocityBreaker`: In-place same-turn retry coalescing (preventing false
 *    `SEMANTIC_DEAD_LOOP` & token double-counting on single-turn retries) & SQLite persistence.
 * 4. [M61-4] `TunnelGatewayCell` & `AgentTaskHarnessCell`: Idempotent/coalesced reverse-tunnel command
 *    queueing with SQLite persistence, idempotent harness turn checkpoint retry & R2-to-SQL breakpoint reconciliation.
 * 5. [M61-5] `LayerFsWorkspaceDriver` & `LayerFsRemoteStoreSynchronizer`: Branch breakpoint rollback
 *    (`rollbackBranchToCommitOrStep`) & idempotent same-payload remote store push retry.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const env_js_1 = require("../src/cloudflare/env.js");
const blackboard_and_git_arbiter_js_1 = require("../src/a2a/blackboard_and_git_arbiter.js");
const topology_cas_coordinator_js_1 = require("../src/cluster/topology_cas_coordinator.js");
const staging_js_1 = require("../src/saga/staging.js");
const escrow_js_1 = require("../src/dag/escrow.js");
const budget_controller_js_1 = require("../src/finops/budget_controller.js");
const velocity_breaker_js_1 = require("../src/harness/velocity_breaker.js");
const tunnel_gateway_cell_js_1 = require("../src/cloudflare/tunnel_gateway_cell.js");
const harness_cell_js_1 = require("../src/cloudflare/harness_cell.js");
const layerfs_workspace_driver_js_1 = require("../src/runtime/layerfs_workspace_driver.js");
const layerfs_remote_sync_js_1 = require("../src/checkpoint/layerfs_remote_sync.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M61 Subsystem Persistence, Idempotent CAS/Push Retry, Breakpoint Rollback & Coalescing Suite", () => {
    (0, node_test_1.it)("1. Verifies M61-1 through M61-5 across A2A Blackboard, Topology CAS, Staging/Escrow/FinOps, VelocityBreaker, Tunnel/HarnessCell & LayerFS, plus live HTTP & CLI", async () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        // =========================================================================
        // 1. [M61-1] SharedBlackboardAndGitArbiter & TopologyCasCoordinator
        // =========================================================================
        const bb1 = new blackboard_and_git_arbiter_js_1.SharedBlackboardAndGitArbiter(sql);
        const put1 = bb1.putCas("design/spec", { modules: ["a", "b"], rev: 1 }, 0, "agent-1");
        assert.equal(put1.committed, true);
        assert.equal(put1.deduplicated, false);
        assert.equal(put1.entry.version, 1);
        // Retry with expectedVersion=0 and canonically identical value from same authorAgentId must succeed idempotently
        const put1Retry = bb1.putCas("design/spec", { rev: 1, modules: ["a", "b"] }, 0, "agent-1");
        assert.equal(put1Retry.committed, true);
        assert.equal(put1Retry.deduplicated, true);
        assert.equal(put1Retry.entry.version, 1);
        // Duplicate patch entries from the same agent in one arbitration batch must coalesce instead of self-conflicting
        const arbRes = bb1.arbitrateGitPatchTouches([
            { agentId: "agent-1", touchedFiles: ["src/index.ts"], priorityScore: 100 },
            { agentId: "agent-1", touchedFiles: ["src/index.ts", "src/models.ts"], priorityScore: 150 },
            { agentId: "agent-2", touchedFiles: ["src/index.ts"], priorityScore: 90 },
        ]);
        assert.deepEqual(arbRes.acceptedAgentIds, ["agent-1"]);
        assert.deepEqual(arbRes.conflictedAgentIds, ["agent-2"]);
        assert.deepEqual(arbRes.coalescedDuplicateAgents, ["agent-1"]);
        assert.equal(bb1.getAgentConflictCount("agent-1"), 0);
        assert.equal(bb1.getAgentConflictCount("agent-2"), 1);
        // Cold-start rehydration of Blackboard & conflict counters
        const bb2 = new blackboard_and_git_arbiter_js_1.SharedBlackboardAndGitArbiter(sql);
        assert.equal(bb2.get("design/spec")?.version, 1);
        assert.equal(bb2.getAgentConflictCount("agent-2"), 1);
        // TopologyCasCoordinator idempotent CAS retry
        const top = new topology_cas_coordinator_js_1.TopologyCasCoordinator(sql);
        const t1 = top.updateTopologyCas(0, ["brain-02", "brain-01"]);
        assert.equal(t1.generation, 1);
        assert.equal(t1.deduplicated, false);
        const t1Retry = top.updateTopologyCas(0, ["brain-01", "brain-02"]);
        assert.equal(t1Retry.generation, 1);
        assert.equal(t1Retry.deduplicated, true);
        // =========================================================================
        // 2. [M61-2] StagingCommitAndApprovalGate, DagEscrowCoordinator & FinOpsBudgetController
        // =========================================================================
        const staging1 = new staging_js_1.StagingCommitAndApprovalGate(sql);
        const task = new models_js_1.TaskSpec({
            task_id: "task-m61-parent",
            tenant_id: "tenant-m61",
            expected_SubAgent_count: 2,
        });
        const effA = staging1.stageEffect({
            effectId: "eff-1",
            taskId: task.task_id,
            turnIndex: 2,
            toolName: "delete_bucket",
            args: { bucket: "logs", force: true },
        });
        const effB = staging1.stageEffect({
            effectId: "eff-2-dup",
            taskId: task.task_id,
            turnIndex: 2,
            toolName: "delete_bucket",
            args: { force: true, bucket: "logs" },
        });
        assert.equal(effA.effectId, effB.effectId, "Identical staged effect on retry must coalesce");
        assert.equal(staging1.getStagedEffects(task.task_id).length, 1);
        const tkA = staging1.requestHumanApproval(task, "delete_bucket", { bucket: "logs", force: true });
        const tkB = staging1.requestHumanApproval(task, "delete_bucket", { force: true, bucket: "logs" });
        assert.equal(tkA.ticketId, tkB.ticketId, "Duplicate pending approval requests must coalesce");
        // Resolve approval idempotently, and reject conflicting terminal state change
        const resolved1 = staging1.resolveApproval(tkA.ticketId, true, "admin-1", task);
        const resolvedRetry = staging1.resolveApproval(tkA.ticketId, true, "admin-1", task);
        assert.equal(resolved1.status, "APPROVED");
        assert.equal(resolvedRetry.status, "APPROVED");
        assert.throws(() => staging1.resolveApproval(tkA.ticketId, false, "admin-2", task), /already in terminal status/);
        const escrow1 = new escrow_js_1.DagEscrowCoordinator(sql);
        escrow1.reserveParentEscrow(task, 5, 0);
        // Retrying reserveParentEscrow when currentTenantUsed=4 must not fail or reset usedSlots
        const child1 = new models_js_1.TaskSpec({ task_id: "task-m61-child-1", tenant_id: "tenant-m61" });
        const reg1 = escrow1.registerChildSubAgent(task, child1);
        const reg1Retry = escrow1.registerChildSubAgent(task, child1);
        assert.equal(reg1.usedEscrowSlot, true);
        assert.equal(reg1Retry.deduplicated, true);
        const escRetry = escrow1.reserveParentEscrow(task, 5, 4);
        assert.equal(escRetry.usedSlots, 1, "Idempotent reserveParentEscrow must preserve usedSlots");
        const fin1 = new budget_controller_js_1.FinOpsBudgetController(sql);
        fin1.configureTenant({
            tenantId: "tenant-m61",
            dailyBudgetUsd: 20,
            softDowngradeThresholdRatio: 0.8,
            preferredModel: "claude-3-7-sonnet",
            fallbackEconomyModel: "deepseek-chat",
        });
        fin1.recordSpend("tenant-m61", 6.5, "2026-11-10", "turn-1-bill");
        const spendRetry = fin1.recordSpend("tenant-m61", 6.5, "2026-11-10", "turn-1-bill");
        assert.equal(spendRetry, 6.5, "Duplicate spend with same idempotencyKey must not double-charge");
        fin1.refundSpend("tenant-m61", 1.5, "2026-11-10", "turn-1-refund");
        const refundRetry = fin1.refundSpend("tenant-m61", 1.5, "2026-11-10", "turn-1-refund");
        assert.equal(refundRetry, 5.0, "Duplicate refund with same idempotencyKey must not double-refund");
        // Rehydrate FinOps & Escrow from SQLite
        const escrow2 = new escrow_js_1.DagEscrowCoordinator(sql);
        assert.equal(escrow2.getTenantEscrowedSlots("tenant-m61"), 2);
        const fin2 = new budget_controller_js_1.FinOpsBudgetController(sql);
        assert.equal(fin2.recordSpend("tenant-m61", 6.5, "2026-11-10", "turn-1-bill"), 5.0);
        // =========================================================================
        // 3. [M61-3] SyntacticAndVelocityBreaker Same-Turn Retry Coalescing & SQLite
        // =========================================================================
        const breaker1 = new velocity_breaker_js_1.SyntacticAndVelocityBreaker(3, 10_000, sql);
        // Retrying turnIndex=1 three times must coalesce in place and neither trip SEMANTIC_DEAD_LOOP nor exceed 10,000 token budget
        breaker1.recordAndCheckTurn("task-vb-1", {
            turnIndex: 1,
            toolName: "grep_code",
            argsHash: "hash-a",
            tokensConsumed: 4500,
            timestampMs: 2000,
        });
        breaker1.recordAndCheckTurn("task-vb-1", {
            turnIndex: 1,
            toolName: "grep_code",
            argsHash: "hash-a",
            tokensConsumed: 4500,
            timestampMs: 2010,
        });
        const thirdRetrySameTurn = breaker1.recordAndCheckTurn("task-vb-1", {
            turnIndex: 1,
            toolName: "grep_code",
            argsHash: "hash-a",
            tokensConsumed: 4500,
            timestampMs: 2020,
        });
        assert.equal(thirdRetrySameTurn.tripped, false);
        assert.equal(thirdRetrySameTurn.coalescedSameTurnRetry, true);
        // Distinct turns 2 and 3 with the same tool/argsHash MUST trip SEMANTIC_DEAD_LOOP
        breaker1.recordAndCheckTurn("task-vb-1", {
            turnIndex: 2,
            toolName: "grep_code",
            argsHash: "hash-a",
            tokensConsumed: 1000,
            timestampMs: 2030,
        });
        const turn3Distinct = breaker1.recordAndCheckTurn("task-vb-1", {
            turnIndex: 3,
            toolName: "grep_code",
            argsHash: "hash-a",
            tokensConsumed: 1000,
            timestampMs: 2040,
        });
        assert.equal(turn3Distinct.tripped, true);
        assert.equal(turn3Distinct.reason, "SEMANTIC_DEAD_LOOP");
        // =========================================================================
        // 4. [M61-4] TunnelGatewayCell Coalescing & AgentTaskHarnessCell Reconciliation
        // =========================================================================
        const tgw = new tunnel_gateway_cell_js_1.TunnelGatewayCell(sql);
        tgw.registerTunnel({
            hostId: "body-host-01",
            gatewayId: "gw-primary",
            role: "PRIMARY",
            connGeneration: 1,
            epoch: 1,
        });
        const cmdA = tgw.enqueueCommandForHost("body-host-01", { action: "exec_step", step: 1 }, 5000, { idempotencyKey: "cmd-key-1" });
        const cmdB = tgw.enqueueCommandForHost("body-host-01", { action: "exec_step", step: 1 }, 5000, { idempotencyKey: "cmd-key-1" });
        assert.equal(cmdA.deduplicated, false);
        assert.equal(cmdB.deduplicated, true);
        assert.equal(cmdA.command.commandId, cmdB.command.commandId);
        const polled = tgw.pollPendingCommands("body-host-01", "gw-primary");
        assert.equal(polled.length, 1, "Coalesced command must only be polled once by Body host");
        tgw.acknowledgeCommand(cmdA.command.commandId, { status: "done", output: 42 });
        const [outA, outB] = await Promise.all([cmdA.completionPromise, cmdB.completionPromise]);
        assert.equal(outA.output, 42);
        assert.equal(outB.output, 42);
        // Subsequent retry after completion returns cached result immediately
        const cmdC = tgw.enqueueCommandForHost("body-host-01", { action: "exec_step", step: 1 }, 5000, { idempotencyKey: "cmd-key-1" });
        assert.equal(cmdC.deduplicated, true);
        assert.equal((await cmdC.completionPromise).output, 42);
        // AgentTaskHarnessCell R2-to-SQL breakpoint reconciliation & idempotent retry
        const r2 = new env_js_1.InMemoryR2Bucket();
        const doState = new env_js_1.MockDurableObjectState("harness-cell-m61");
        await r2.put("cells/task-h-m61/turn-1-f3.ltx", JSON.stringify({
            taskId: "task-h-m61",
            turnIndex: 1,
            fencingToken: 3,
            toolName: "write_patch",
            toolOutput: { patched: true, files: 2 },
        }), { customMetadata: { taskId: "task-h-m61", turnIndex: "1", fencingToken: "3" } });
        const hCell = new harness_cell_js_1.AgentTaskHarnessCell(doState, r2, "task-h-m61");
        const reconciled = await hCell.reconcileBreakpointFromR2AndSql();
        assert.equal(reconciled.reconciledTurnsCount, 1);
        assert.equal(reconciled.highestTurnIndex, 1);
        assert.equal(reconciled.highestFencingToken, 3);
        const retryTurn = await hCell.executeAndCheckpointTurn({
            turnIndex: 1,
            fencingToken: 3,
            toolName: "write_patch",
            toolOutput: { files: 2, patched: true },
            enforceTurnMonotonicity: true,
        });
        assert.equal(retryTurn.deduplicated, true);
        // =========================================================================
        // 5. [M61-5] LayerFsWorkspaceDriver Breakpoint Rollback & Remote Sync Idempotent Push
        // =========================================================================
        const lfs = new layerfs_workspace_driver_js_1.LayerFsWorkspaceDriver();
        const c1 = lfs.executeInProcessFileTool({
            branchName: "main",
            agentId: "agent-1",
            stepId: "turn-1-commit",
            operation: "write",
            filePath: "src/service.ts",
            newContent: "export const v = 1;",
        });
        lfs.executeInProcessFileTool({
            branchName: "main",
            agentId: "agent-1",
            stepId: "turn-2-aborted",
            operation: "write",
            filePath: "src/service.ts",
            newContent: "export const v = 999;",
        });
        const rolledBack = lfs.rollbackBranchToCommitOrStep("main", "turn-1-commit");
        assert.equal(rolledBack.restoredCommitId, c1.commitId);
        const readBack = lfs.executeInProcessFileTool({
            branchName: "main",
            agentId: "agent-1",
            stepId: "read-check",
            operation: "read",
            filePath: "src/service.ts",
        });
        assert.equal(readBack.readResult, "export const v = 1;");
        const syncer = new layerfs_remote_sync_js_1.LayerFsRemoteStoreSynchronizer(new env_js_1.InMemoryR2Bucket(), "proj-m61");
        const snapshotBytes = new Uint8Array([1, 2, 3, 4, 5]);
        const p1 = await syncer.pushStoreSnapshot({
            machineId: "node-1",
            basedOnGeneration: 0,
            storeSqliteBytes: snapshotBytes,
            mainHeadCommitId: c1.commitId,
            idempotencyKey: "sync-push-idem-1",
        });
        assert.equal(p1.remoteGeneration, 1);
        assert.equal(p1.deduplicated, false);
        const p1Retry = await syncer.pushStoreSnapshot({
            machineId: "node-1",
            basedOnGeneration: 0,
            storeSqliteBytes: snapshotBytes,
            mainHeadCommitId: c1.commitId,
            idempotencyKey: "sync-push-idem-1",
        });
        assert.equal(p1Retry.remoteGeneration, 1);
        assert.equal(p1Retry.deduplicated, true);
        // =========================================================================
        // 6. [M61-6] Live HTTP Endpoint & CLI Verification
        // =========================================================================
        const server = new server_main_js_1.AnstsLocalAndComposeServer({ nodeId: "gw-m61-test" });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-subsystem-idempotency-audit"], baseUrl);
            assert.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            assert.equal(body.allM61RisksEliminated, true);
            assert.equal(body.blackboardAndTopology1.blackboardAndTopologyVerified, true);
            assert.equal(body.stagingEscrowFinOps2.stagingEscrowFinOpsVerified, true);
            assert.equal(body.velocityBreakerCoalesce3.velocityBreakerCoalesceVerified, true);
            assert.equal(body.tunnelAndHarness4.tunnelAndHarnessVerified, true);
            assert.equal(body.layerFsRollbackAndPush5.layerFsRollbackAndPushVerified, true);
        }
        finally {
            await server.stop();
        }
    });
});
