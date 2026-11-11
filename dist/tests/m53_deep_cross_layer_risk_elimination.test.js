"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview M53 Deep Cross-Layer Risk Elimination Verification Suite:
 * Verifies all 5 deep cross-layer risks identified and resolved in M53:
 * 1. [Saga-Stale-1] `SagaToolGateway.rollbackAndInvalidateTask` invalidates `executionJournal` idempotency cache for rolled-back turns and `SagaCompensatorEngine.rollbackTaskWithReport` isolates compensation handler exceptions so remaining LIFO steps always execute.
 * 2. [FinOps-Starvation-2] `FinOpsBudgetController` automatically resets daily tenant spend across UTC day boundaries (preventing permanent multi-day HTTP 402 starvation) and supports `refundSpend` on aborted/rolled-back turns.
 * 3. [WarmPool-Leak-3] `WarmSandboxPoolManager.acquireSandbox` prevents duplicate task allocation from leaking MicroVMs, `reclaimOrphanedSandboxes` sweeps zombie VMs, and `LayerFsNumberedSandboxMultiplexer.mountWorkspaceIntoSandbox` is idempotent when a workspace is already mounted.
 * 4. [Wal-Stale-4] `TurnWalEngine.appendTurnDelta` enforces dual-tuple `(fencingToken, turnIndex)` monotonicity when `enforceTurnMonotonicity: true` and `verifyTaskWalChainIntegrity` validates full-chain CRC32 + monotonic order.
 * 5. [DrainToken-Deadlock-5] `CelldStateAwareFleetAutoscaler.initiateNodeShutdown` automatically expires and reclaims a stale `activeDrainTokenHolder` after `shutdownTotalMs` TTL so a crashed draining node never deadlocks fleet rolling shutdowns.
 */
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const saga_gateway_js_1 = require("../src/saga_gateway.js");
const models_js_1 = require("../src/models.js");
const budget_controller_js_1 = require("../src/finops/budget_controller.js");
const warm_pool_js_1 = require("../src/runtime/warm_pool.js");
const wal_engine_js_1 = require("../src/checkpoint/wal_engine.js");
const lease_coordinator_js_1 = require("../src/lease_coordinator.js");
const manual_scale_controller_js_1 = require("../src/cluster/manual_scale_controller.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M53: Deep Cross-Layer Risk Elimination (Saga, FinOps, WarmPool/Multiplexer, WAL & Drain-Token TTL)", () => {
    (0, node_test_1.it)("validates all 5 cross-layer unit invariants & live HTTP/CLI risk-elimination audit", async () => {
        // 1. [Saga-Stale-1] Post-rollback idempotency cache invalidation & fault-tolerant LIFO compensation
        const saga = new saga_gateway_js_1.SagaToolGateway();
        let turn1Executions = 0;
        const lifoOrder = [];
        const firstOut = saga.executeTool({
            taskId: "task-m53-1",
            turnIndex: 1,
            toolName: "mutate_resource",
            effectClass: models_js_1.ToolCallEffectClass.COMPENSATABLE,
            args: { target: "alpha" },
            actionFn: () => {
                turn1Executions += 1;
                return { attempt: turn1Executions };
            },
            compensationFn: () => {
                lifoOrder.push(1);
            },
        });
        strict_1.default.equal(firstOut.attempt, 1);
        saga.executeTool({
            taskId: "task-m53-1",
            turnIndex: 2,
            toolName: "flaky_webhook",
            effectClass: models_js_1.ToolCallEffectClass.COMPENSATABLE,
            args: { target: "beta" },
            actionFn: () => ({ ok: true }),
            compensationFn: () => {
                lifoOrder.push(2);
                throw new Error("Simulated transient webhook compensation failure");
            },
        });
        const rbReport = saga.rollbackAndInvalidateTask("task-m53-1", 1);
        strict_1.default.deepEqual(lifoOrder, [2, 1], "Turn 1 compensation must still execute even if Turn 2 compensation threw");
        strict_1.default.equal(rbReport.compensationErrors.length, 1);
        strict_1.default.equal(rbReport.invalidatedIdemKeys.length, 2);
        const reexecutedOut = saga.executeTool({
            taskId: "task-m53-1",
            turnIndex: 1,
            toolName: "mutate_resource",
            effectClass: models_js_1.ToolCallEffectClass.COMPENSATABLE,
            args: { target: "alpha" },
            actionFn: () => {
                turn1Executions += 1;
                return { attempt: turn1Executions };
            },
        });
        strict_1.default.equal(reexecutedOut.attempt, 2, "Re-executed turn after rollback must not return stale cached attempt=1");
        // 2. [FinOps-Starvation-2] UTC daily window auto-reset & spend refund
        const finops = new budget_controller_js_1.FinOpsBudgetController();
        finops.configureTenant({
            tenantId: "Northstar-M53",
            dailyBudgetUsd: 100,
            softDowngradeThresholdRatio: 0.8,
            primaryModel: "DeepSeek-R1",
            fallbackEconomyModel: "Qwen-3.8",
        });
        const day1Utc = Date.UTC(2026, 10, 10, 22, 0, 0);
        finops.recordSpend("Northstar-M53", 110, day1Utc);
        strict_1.default.equal(finops.evaluateBudgetAndModel("Northstar-M53", day1Utc).allowed, false);
        // Refund $25 from rolled-back turn -> $85 spent (allowed with soft downgrade)
        const remainingAfterRefund = finops.refundSpend("Northstar-M53", 25, day1Utc);
        strict_1.default.equal(remainingAfterRefund, 85);
        const afterRefundEval = finops.evaluateBudgetAndModel("Northstar-M53", day1Utc);
        strict_1.default.equal(afterRefundEval.allowed, true);
        strict_1.default.equal(afterRefundEval.downgraded, true);
        // Advance to next UTC day -> spend resets to $0 and primary model is restored
        const day2Utc = Date.UTC(2026, 10, 11, 0, 1, 0);
        const day2Eval = finops.evaluateBudgetAndModel("Northstar-M53", day2Utc);
        strict_1.default.equal(day2Eval.allowed, true);
        strict_1.default.equal(day2Eval.downgraded, false);
        strict_1.default.equal(day2Eval.selectedModel, "DeepSeek-R1");
        strict_1.default.equal(day2Eval.windowReset, true);
        // 3. [WarmPool-Leak-3] Idempotent sandbox allocation, orphan VM sweep & multiplexer idempotent mount
        const pool = new warm_pool_js_1.WarmSandboxPoolManager("node-m53", 4);
        const vmA = pool.acquireSandbox("task-m53-sb");
        const vmB = pool.acquireSandbox("task-m53-sb");
        strict_1.default.equal(vmA.vmId, vmB.vmId);
        strict_1.default.equal(pool.stats().activeCount, 1);
        pool.acquireSandbox("task-m53-ghost");
        strict_1.default.equal(pool.stats().activeCount, 2);
        const sweptVms = pool.reclaimOrphanedSandboxes(new Set(["task-m53-sb"]));
        strict_1.default.equal(sweptVms.length, 1);
        strict_1.default.equal(pool.stats().activeCount, 1);
        const mux = new warm_pool_js_1.LayerFsNumberedSandboxMultiplexer();
        for (let i = 1; i <= 16; i++) {
            mux.mountWorkspaceIntoSandbox(`ws-${i}`, 1000);
        }
        const remount = mux.mountWorkspaceIntoSandbox("ws-1", 2000);
        strict_1.default.equal(remount.sandboxNumber, 1);
        strict_1.default.equal(remount.spawnedNewSandbox, false);
        // 4. [Wal-Stale-4] Dual-tuple (fencingToken, turnIndex) monotonicity & chain integrity check
        const wal = new wal_engine_js_1.TurnWalEngine();
        wal.appendTurnDelta({
            taskId: "task-m53-wal",
            turnIndex: 1,
            fencingToken: 3,
            changedFiles: ["a.ts"],
            sqliteWalPages: 4,
            highestSubmittedFencingToken: 3,
            enforceTurnMonotonicity: true,
        });
        wal.appendTurnDelta({
            taskId: "task-m53-wal",
            turnIndex: 2,
            fencingToken: 3,
            changedFiles: ["b.ts"],
            sqliteWalPages: 6,
            highestSubmittedFencingToken: 3,
            enforceTurnMonotonicity: true,
        });
        strict_1.default.throws(() => wal.appendTurnDelta({
            taskId: "task-m53-wal",
            turnIndex: 2,
            fencingToken: 3,
            changedFiles: ["dup.ts"],
            sqliteWalPages: 2,
            highestSubmittedFencingToken: 3,
            enforceTurnMonotonicity: true,
        }), lease_coordinator_js_1.StaleFenceError);
        const chainIntegrity = wal.verifyTaskWalChainIntegrity("task-m53-wal");
        strict_1.default.equal(chainIntegrity.valid, true);
        strict_1.default.equal(chainIntegrity.frameCount, 2);
        // 5. [DrainToken-Deadlock-5] Drain-token lease TTL expiration on crashed draining node
        const autoscaler = new manual_scale_controller_js_1.CelldStateAwareFleetAutoscaler(40_000);
        const firstDrain = autoscaler.initiateNodeShutdown({
            nodeId: "node-dead-drainer",
            systemdTimeoutStopSec: 60,
            nowMs: 10_000,
        });
        strict_1.default.equal(firstDrain.claimedDrainToken, true);
        const secondEarly = autoscaler.initiateNodeShutdown({
            nodeId: "node-next-drainer",
            systemdTimeoutStopSec: 60,
            nowMs: 25_000,
        });
        strict_1.default.equal(secondEarly.claimedDrainToken, false);
        const secondAfterTtl = autoscaler.initiateNodeShutdown({
            nodeId: "node-next-drainer",
            systemdTimeoutStopSec: 60,
            nowMs: 51_000,
        });
        strict_1.default.equal(secondAfterTtl.claimedDrainToken, true);
        strict_1.default.equal(secondAfterTtl.expiredStaleHolder, "node-dead-drainer");
        // 6. Verify live HTTP endpoint & CLI `mgmt-risk-audit`
        const server = new server_main_js_1.AnstsLocalAndComposeServer({ nodeId: "gw-m53-test", apiPort: 0, dashboardPort: 0 });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-risk-audit"], baseUrl);
            strict_1.default.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            strict_1.default.equal(body.allM53RisksEliminated, true);
            strict_1.default.equal(body.sagaStale1.sagaFaultTolerantLifoAndCacheInvalidated, true);
            strict_1.default.equal(body.finopsStarvation2.finopsUtcResetAndRefundVerified, true);
            strict_1.default.equal(body.warmPoolLeak3.idempotentSameVmReturned, true);
            strict_1.default.equal(body.warmPoolLeak3.orphanVmReclaimedCleanly, true);
            strict_1.default.equal(body.warmPoolLeak3.multiplexerIdempotentMountVerified, true);
            strict_1.default.equal(body.walStale4.walMonotonicAndChainIntegrityVerified, true);
            strict_1.default.equal(body.drainTokenDeadlock5.drainTokenTtlReclaimed, true);
        }
        finally {
            await server.stop();
        }
    });
});
