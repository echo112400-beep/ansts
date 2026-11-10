"use strict";








var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var _saga_gateway = require("../src/saga_gateway.js");
var _models = require("../src/models.js");
var _budget_controller = require("../src/finops/budget_controller.js");
var _warm_pool = require("../src/runtime/warm_pool.js");
var _wal_engine = require("../src/checkpoint/wal_engine.js");
var _lease_coordinator = require("../src/lease_coordinator.js");
var _manual_scale_controller = require("../src/cluster/manual_scale_controller.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M53 Deep Cross-Layer Risk Elimination Verification Suite:
 * Verifies all 5 deep cross-layer risks identified and resolved in M53:
 * 1. [Saga-Stale-1] `SagaToolGateway.rollbackAndInvalidateTask` invalidates `executionJournal` idempotency cache for rolled-back turns and `SagaCompensatorEngine.rollbackTaskWithReport` isolates compensation handler exceptions so remaining LIFO steps always execute.
 * 2. [FinOps-Starvation-2] `FinOpsBudgetController` automatically resets daily tenant spend across UTC day boundaries (preventing permanent multi-day HTTP 402 starvation) and supports `refundSpend` on aborted/rolled-back turns.
 * 3. [WarmPool-Leak-3] `WarmSandboxPoolManager.acquireSandbox` prevents duplicate task allocation from leaking MicroVMs, `reclaimOrphanedSandboxes` sweeps zombie VMs, and `LayerFsNumberedSandboxMultiplexer.mountWorkspaceIntoSandbox` is idempotent when a workspace is already mounted.
 * 4. [Wal-Stale-4] `TurnWalEngine.appendTurnDelta` enforces dual-tuple `(fencingToken, turnIndex)` monotonicity when `enforceTurnMonotonicity: true` and `verifyTaskWalChainIntegrity` validates full-chain CRC32 + monotonic order.
 * 5. [DrainToken-Deadlock-5] `CelldStateAwareFleetAutoscaler.initiateNodeShutdown` automatically expires and reclaims a stale `activeDrainTokenHolder` after `shutdownTotalMs` TTL so a crashed draining node never deadlocks fleet rolling shutdowns.
 */(0, _nodeTest.describe)("M53: Deep Cross-Layer Risk Elimination (Saga, FinOps, WarmPool/Multiplexer, WAL & Drain-Token TTL)", () => {(0, _nodeTest.it)("validates all 5 cross-layer unit invariants & live HTTP/CLI risk-elimination audit", async () => {// 1. [Saga-Stale-1] Post-rollback idempotency cache invalidation & fault-tolerant LIFO compensation
    const saga = new _saga_gateway.SagaToolGateway();let turn1Executions = 0;const lifoOrder = [];const firstOut = saga.executeTool({
      taskId: "task-m53-1",
      turnIndex: 1,
      toolName: "mutate_resource",
      effectClass: _models.ToolCallEffectClass.COMPENSATABLE,
      args: { target: "alpha" },
      actionFn: () => {
        turn1Executions += 1;
        return { attempt: turn1Executions };
      },
      compensationFn: () => {
        lifoOrder.push(1);
      }
    });
    _strict.default.equal(firstOut.attempt, 1);

    saga.executeTool({
      taskId: "task-m53-1",
      turnIndex: 2,
      toolName: "flaky_webhook",
      effectClass: _models.ToolCallEffectClass.COMPENSATABLE,
      args: { target: "beta" },
      actionFn: () => ({ ok: true }),
      compensationFn: () => {
        lifoOrder.push(2);
        throw new Error("Simulated transient webhook compensation failure");
      }
    });

    const rbReport = saga.rollbackAndInvalidateTask("task-m53-1", 1);
    _strict.default.deepEqual(lifoOrder, [2, 1], "Turn 1 compensation must still execute even if Turn 2 compensation threw");
    _strict.default.equal(rbReport.compensationErrors.length, 1);
    _strict.default.equal(rbReport.invalidatedIdemKeys.length, 2);

    const reexecutedOut = saga.executeTool({
      taskId: "task-m53-1",
      turnIndex: 1,
      toolName: "mutate_resource",
      effectClass: _models.ToolCallEffectClass.COMPENSATABLE,
      args: { target: "alpha" },
      actionFn: () => {
        turn1Executions += 1;
        return { attempt: turn1Executions };
      }
    });
    _strict.default.equal(reexecutedOut.attempt, 2, "Re-executed turn after rollback must not return stale cached attempt=1");

    // 2. [FinOps-Starvation-2] UTC daily window auto-reset & spend refund
    const finops = new _budget_controller.FinOpsBudgetController();
    finops.configureTenant({
      tenantId: "Northstar-M53",
      dailyBudgetUsd: 100,
      softDowngradeThresholdRatio: 0.8,
      primaryModel: "DeepSeek-R1",
      fallbackEconomyModel: "Qwen-3.8"
    });
    const day1Utc = Date.UTC(2026, 10, 10, 22, 0, 0);
    finops.recordSpend("Northstar-M53", 110, day1Utc);
    _strict.default.equal(finops.evaluateBudgetAndModel("Northstar-M53", day1Utc).allowed, false);

    // Refund $25 from rolled-back turn -> $85 spent (allowed with soft downgrade)
    const remainingAfterRefund = finops.refundSpend("Northstar-M53", 25, day1Utc);
    _strict.default.equal(remainingAfterRefund, 85);
    const afterRefundEval = finops.evaluateBudgetAndModel("Northstar-M53", day1Utc);
    _strict.default.equal(afterRefundEval.allowed, true);
    _strict.default.equal(afterRefundEval.downgraded, true);

    // Advance to next UTC day -> spend resets to $0 and primary model is restored
    const day2Utc = Date.UTC(2026, 10, 11, 0, 1, 0);
    const day2Eval = finops.evaluateBudgetAndModel("Northstar-M53", day2Utc);
    _strict.default.equal(day2Eval.allowed, true);
    _strict.default.equal(day2Eval.downgraded, false);
    _strict.default.equal(day2Eval.selectedModel, "DeepSeek-R1");
    _strict.default.equal(day2Eval.windowReset, true);

    // 3. [WarmPool-Leak-3] Idempotent sandbox allocation, orphan VM sweep & multiplexer idempotent mount
    const pool = new _warm_pool.WarmSandboxPoolManager("node-m53", 4);
    const vmA = pool.acquireSandbox("task-m53-sb");
    const vmB = pool.acquireSandbox("task-m53-sb");
    _strict.default.equal(vmA.vmId, vmB.vmId);
    _strict.default.equal(pool.stats().activeCount, 1);

    pool.acquireSandbox("task-m53-ghost");
    _strict.default.equal(pool.stats().activeCount, 2);
    const sweptVms = pool.reclaimOrphanedSandboxes(new Set(["task-m53-sb"]));
    _strict.default.equal(sweptVms.length, 1);
    _strict.default.equal(pool.stats().activeCount, 1);

    const mux = new _warm_pool.LayerFsNumberedSandboxMultiplexer();
    for (let i = 1; i <= 16; i++) {
      mux.mountWorkspaceIntoSandbox(`ws-${i}`, 1000);
    }
    const remount = mux.mountWorkspaceIntoSandbox("ws-1", 2000);
    _strict.default.equal(remount.sandboxNumber, 1);
    _strict.default.equal(remount.spawnedNewSandbox, false);

    // 4. [Wal-Stale-4] Dual-tuple (fencingToken, turnIndex) monotonicity & chain integrity check
    const wal = new _wal_engine.TurnWalEngine();
    wal.appendTurnDelta({
      taskId: "task-m53-wal",
      turnIndex: 1,
      fencingToken: 3,
      changedFiles: ["a.ts"],
      sqliteWalPages: 4,
      highestSubmittedFencingToken: 3,
      enforceTurnMonotonicity: true
    });
    wal.appendTurnDelta({
      taskId: "task-m53-wal",
      turnIndex: 2,
      fencingToken: 3,
      changedFiles: ["b.ts"],
      sqliteWalPages: 6,
      highestSubmittedFencingToken: 3,
      enforceTurnMonotonicity: true
    });
    _strict.default.throws(
    () =>
    wal.appendTurnDelta({
      taskId: "task-m53-wal",
      turnIndex: 2,
      fencingToken: 3,
      changedFiles: ["dup.ts"],
      sqliteWalPages: 2,
      highestSubmittedFencingToken: 3,
      enforceTurnMonotonicity: true
    }),
    _lease_coordinator.StaleFenceError);

    const chainIntegrity = wal.verifyTaskWalChainIntegrity("task-m53-wal");
    _strict.default.equal(chainIntegrity.valid, true);
    _strict.default.equal(chainIntegrity.frameCount, 2);

    // 5. [DrainToken-Deadlock-5] Drain-token lease TTL expiration on crashed draining node
    const autoscaler = new _manual_scale_controller.CelldStateAwareFleetAutoscaler(40_000);
    const firstDrain = autoscaler.initiateNodeShutdown({
      nodeId: "node-dead-drainer",
      systemdTimeoutStopSec: 60,
      nowMs: 10_000
    });
    _strict.default.equal(firstDrain.claimedDrainToken, true);

    const secondEarly = autoscaler.initiateNodeShutdown({
      nodeId: "node-next-drainer",
      systemdTimeoutStopSec: 60,
      nowMs: 25_000
    });
    _strict.default.equal(secondEarly.claimedDrainToken, false);

    const secondAfterTtl = autoscaler.initiateNodeShutdown({
      nodeId: "node-next-drainer",
      systemdTimeoutStopSec: 60,
      nowMs: 51_000
    });
    _strict.default.equal(secondAfterTtl.claimedDrainToken, true);
    _strict.default.equal(secondAfterTtl.expiredStaleHolder, "node-dead-drainer");

    // 6. Verify live HTTP endpoint & CLI `mgmt-risk-audit`
    const server = new _server_main.AnstsLocalAndComposeServer({ nodeId: "gw-m53-test", apiPort: 0, dashboardPort: 0 });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-risk-audit"], baseUrl);
      _strict.default.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);
      _strict.default.equal(body.allM53RisksEliminated, true);
      _strict.default.equal(body.sagaStale1.sagaFaultTolerantLifoAndCacheInvalidated, true);
      _strict.default.equal(body.finopsStarvation2.finopsUtcResetAndRefundVerified, true);
      _strict.default.equal(body.warmPoolLeak3.idempotentSameVmReturned, true);
      _strict.default.equal(body.warmPoolLeak3.orphanVmReclaimedCleanly, true);
      _strict.default.equal(body.warmPoolLeak3.multiplexerIdempotentMountVerified, true);
      _strict.default.equal(body.walStale4.walMonotonicAndChainIntegrityVerified, true);
      _strict.default.equal(body.drainTokenDeadlock5.drainTokenTtlReclaimed, true);
    } finally {
      await server.stop();
    }
  });
});