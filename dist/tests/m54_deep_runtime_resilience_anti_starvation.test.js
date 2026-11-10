"use strict";



var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var _env = require("../src/cloudflare/env.js");
var _harness_cell = require("../src/cloudflare/harness_cell.js");
var _breaker_limiter = require("../src/router/breaker_limiter.js");
var _staging = require("../src/saga/staging.js");
var _models = require("../src/models.js");
var _hot_cell_overload_guard = require("../src/cloudflare/hot_cell_overload_guard.js");
var _outbox_webhook = require("../src/telemetry/outbox_webhook.js");
var _sse_stream = require("../src/gateway/sse_stream.js");
var _blackboard_and_git_arbiter = require("../src/a2a/blackboard_and_git_arbiter.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M54 Deep Runtime Resilience, Cold-Start Fencing, Half-Open Stampede Guard,
 * Approval TTL, Hot-Cell Leak Reclamation, Outbox/SSE Resilience & A2A Anti-Starvation Suite.
 */(0, _nodeTest.describe)("M54: Deep Runtime Resilience, Half-Open Stampede Guard, Staging Approval TTL, Hot-Cell Leak Reclamation, Outbox/SSE & A2A Anti-Starvation", () => {(0, _nodeTest.it)("validates all 6 deep runtime invariants & live HTTP/CLI deep-runtime-audit", async () => {
    // 1. [HarnessCell-ColdStart-Stale-1] Cold-start SQLite fencing watermark rehydration & sorted history
    const r2 = new _env.InMemoryR2Bucket();
    const doState = new _env.MockDurableObjectState("AgentTaskHarnessCell:m54-unit");
    const cellA = new _harness_cell.AgentTaskHarnessCell(doState, r2, "task-m54-cell");
    await cellA.executeAndCheckpointTurn({
      turnIndex: 2,
      fencingToken: 9,
      toolName: "bash",
      toolOutput: { step: 2 }
    });
    await cellA.executeAndCheckpointTurn({
      turnIndex: 1,
      fencingToken: 9,
      toolName: "bash",
      toolOutput: { step: 1 }
    });

    // Simulate cold-start on new cell instance over same SQLite storage
    const cellB = new _harness_cell.AgentTaskHarnessCell(doState, r2, "task-m54-cell");
    _strict.default.equal(cellB.getHighestFencingToken(), 9);
    await _strict.default.rejects(
    async () => {
      await cellB.executeAndCheckpointTurn({
        turnIndex: 3,
        fencingToken: 5, // stale token < 9
        toolName: "zombie_tool",
        toolOutput: {}
      });
    },
    /Stale fencing token 5 < highest 9/);

    const history = cellB.getTurnHistory();
    _strict.default.deepEqual(
    history.map((h) => h.turnIndex),
    [1, 2],
    "getTurnHistory must return turns sorted ascending by turnIndex");


    // 2. [CircuitBreaker-HalfOpen-Stampede-2] Single-probe Half-Open stampede guard & stuck-probe timeout recovery
    const breaker = new _breaker_limiter.ThreeStateCircuitBreaker(2, 10_000, 5_000);
    const t0 = 5_000_000;
    breaker.recordFailure(t0);
    breaker.recordFailure(t0 + 10);
    _strict.default.equal(breaker.getState(t0 + 20), "OPEN");

    // Advance past 10s cooldown -> HALF_OPEN: first caller acquires single probe slot, concurrent callers are blocked
    const firstProbe = breaker.tryAcquireRequestSlot(t0 + 10_500);
    _strict.default.equal(firstProbe.admitted, true);
    _strict.default.equal(firstProbe.isHalfOpenProbe, true);

    const concurrentCaller = breaker.tryAcquireRequestSlot(t0 + 10_600);
    _strict.default.equal(concurrentCaller.admitted, false, "Concurrent caller during HALF_OPEN probe must be blocked to prevent stampede");

    // Simulate first probe hanging beyond 5s probeTimeoutMs -> next caller at +6s reclaims probe slot
    const recoveredProbe = breaker.tryAcquireRequestSlot(t0 + 16_000);
    _strict.default.equal(recoveredProbe.admitted, true);
    breaker.recordSuccess();
    _strict.default.equal(breaker.getState(t0 + 16_100), "CLOSED");

    // 3. [Staging-ApprovalTimeout-Deadlock-3] Auto-abort staged effects on rejection & expireStaleApprovalTickets TTL sweep
    const staging = new _staging.StagingCommitAndApprovalGate();
    const task1 = new _models.TaskSpec({ task_id: "task-m54-stg-1", tenant_id: "Northstar" });
    staging.stageEffect({
      effectId: "eff-1",
      taskId: task1.task_id,
      turnIndex: 1,
      toolName: "delete_prod_bucket",
      args: {}
    });
    const tkt1 = staging.requestHumanApproval(task1, "delete_prod_bucket", {}, t0);
    staging.resolveApproval(tkt1.ticketId, false, "chen", task1);
    _strict.default.equal(task1.state, _models.TaskState.DEAD_LETTER);
    _strict.default.equal(staging.getStagedEffects(task1.task_id)[0].aborted, true);

    const task2 = new _models.TaskSpec({ task_id: "task-m54-stg-2", tenant_id: "Northstar" });
    staging.stageEffect({
      effectId: "eff-2",
      taskId: task2.task_id,
      turnIndex: 1,
      toolName: "modify_iam",
      args: {}
    });
    const tkt2 = staging.requestHumanApproval(task2, "modify_iam", {}, t0);
    const expired = staging.expireStaleApprovalTickets(
    new Map([[task2.task_id, task2]]),
    t0 + 4_000_000,
    3_600_000);

    _strict.default.deepEqual(expired, [tkt2.ticketId]);
    _strict.default.equal(task2.state, _models.TaskState.DEAD_LETTER);
    _strict.default.equal(staging.getStagedEffects(task2.task_id)[0].aborted, true);

    // 4. [HotCell-InFlight-Leak-Deadlock-4] Reclaim hung in-flight cell fetch slots
    const guard = new _hot_cell_overload_guard.HotCellOverloadAndQueueGuard({ maxCellConcurrentRequests: 2 });
    guard.tryEnterCellFetch("cell-hot-1", t0);
    guard.tryEnterCellFetch("cell-hot-1", t0);
    _strict.default.equal(guard.tryEnterCellFetch("cell-hot-1", t0 + 50).admitted, false);

    const reclaimed = guard.reclaimStaleInFlightFetches(t0 + 35_000, 30_000);
    _strict.default.equal(reclaimed.reclaimedCount, 2);
    _strict.default.equal(guard.tryEnterCellFetch("cell-hot-1", t0 + 35_100).admitted, true);

    // 5. [Outbox-Sse-Resource-Starvation-5] Outbox per-message exception isolation & SSE evicted Last-Event-ID fallback replay
    const outbox = new _outbox_webhook.TransactionalOutboxDispatcher("sec-m54", 2);
    outbox.enqueueOutbox("m-bad", "task-1", "http://bad", { a: 1 });
    outbox.enqueueOutbox("m-good", "task-1", "http://good", { a: 2 });
    const res = outbox.processPending((m) => {
      if (m.messageId === "m-bad") throw new Error("Socket hang up");
      return true;
    });
    _strict.default.equal(res.deliveredCount, 1);
    _strict.default.equal(res.failedAttemptCount, 1);

    const sse = new _sse_stream.SseEventStreamGateway(2);
    sse.publish({ id: "e1", event: "msg", taskId: "t-sse", turnIndex: 1, timestampMs: t0, data: {} });
    sse.publish({ id: "e2", event: "msg", taskId: "t-sse", turnIndex: 2, timestampMs: t0 + 1, data: {} });
    sse.publish({ id: "e3", event: "msg", taskId: "t-sse", turnIndex: 3, timestampMs: t0 + 2, data: {} });
    const sub = sse.subscribe("t-sse", "sub-1", "e1");
    _strict.default.equal(sub.replayGapDetected, true);
    sse.unsubscribe("t-sse", "sub-1");
    _strict.default.equal(sse.gcCompletedTaskStreams(new Set()), 1);

    // 6. [A2a-GitArbiter-Starvation-6] Conflict-aging priority boost prevents low-priority agent starvation
    const arbiter = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter();
    const round1 = arbiter.arbitrateGitPatchTouches([
    { agentId: "agent-p1", touchedFiles: ["shared.ts"], priorityScore: 220 },
    { agentId: "agent-p3", touchedFiles: ["shared.ts"], priorityScore: 100 }]);

    _strict.default.deepEqual(round1.acceptedAgentIds, ["agent-p1"]);
    _strict.default.deepEqual(round1.conflictedAgentIds, ["agent-p3"]);

    const round2 = arbiter.arbitrateGitPatchTouches([
    { agentId: "agent-p1", touchedFiles: ["shared.ts"], priorityScore: 220 },
    { agentId: "agent-p3", touchedFiles: ["shared.ts"], priorityScore: 100 } // 100 + 150 aging boost = 250 > 220
    ]);
    _strict.default.deepEqual(round2.acceptedAgentIds, ["agent-p3"]);

    // 7. Verify live HTTP endpoint & CLI `mgmt-runtime-audit`
    const server = new _server_main.AnstsLocalAndComposeServer({ nodeId: "gw-m54-test" });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-runtime-audit"], baseUrl);
      _strict.default.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);
      _strict.default.equal(body.allM54RisksEliminated, true);
      _strict.default.equal(body.harnessColdStartStale1.harnessColdStartFencingVerified, true);
      _strict.default.equal(body.circuitBreakerStampede2.halfOpenStampedeAndTimeoutVerified, true);
      _strict.default.equal(body.stagingApprovalDeadlock3.stagingExpirationSweepVerified, true);
      _strict.default.equal(body.hotCellInFlightLeak4.hotCellReclamationVerified, true);
      _strict.default.equal(body.outboxAndSseResilience5.outboxIsolationAndReplayVerified, true);
      _strict.default.equal(body.outboxAndSseResilience5.sseEvictedFallbackVerified, true);
      _strict.default.equal(body.a2aGitArbiterStarvation6.a2aAntiStarvationVerified, true);
    } finally {
      await server.stop();
    }
  });
});