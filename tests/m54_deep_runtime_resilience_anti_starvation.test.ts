/**
 * @fileoverview M54 Deep Runtime Resilience, Cold-Start Fencing, Half-Open Stampede Guard,
 * Approval TTL, Hot-Cell Leak Reclamation, Outbox/SSE Resilience & A2A Anti-Starvation Suite.
 */
import "./console_write_auth.js";
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { InMemoryR2Bucket, MockDurableObjectState } from "../src/cloudflare/env.js";
import { AgentTaskHarnessCell } from "../src/cloudflare/harness_cell.js";
import { ThreeStateCircuitBreaker } from "../src/router/breaker_limiter.js";
import { StagingCommitAndApprovalGate } from "../src/saga/staging.js";
import { TaskSpec, TaskState } from "../src/models.js";
import { HotCellOverloadAndQueueGuard } from "../src/cloudflare/hot_cell_overload_guard.js";
import { TransactionalOutboxDispatcher } from "../src/telemetry/outbox_webhook.js";
import { SseEventStreamGateway } from "../src/gateway/sse_stream.js";
import { SharedBlackboardAndGitArbiter } from "../src/a2a/blackboard_and_git_arbiter.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M54: Deep Runtime Resilience, Half-Open Stampede Guard, Staging Approval TTL, Hot-Cell Leak Reclamation, Outbox/SSE & A2A Anti-Starvation", () => {
  it("validates all 6 deep runtime invariants & live HTTP/CLI deep-runtime-audit", async () => {
    // 1. [HarnessCell-ColdStart-Stale-1] Cold-start SQLite fencing watermark rehydration & sorted history
    const r2 = new InMemoryR2Bucket();
    const doState = new MockDurableObjectState("AgentTaskHarnessCell:m54-unit");
    const cellA = new AgentTaskHarnessCell(doState, r2, "task-m54-cell");
    await cellA.executeAndCheckpointTurn({
      turnIndex: 2,
      fencingToken: 9,
      toolName: "bash",
      toolOutput: { step: 2 },
    });
    await cellA.executeAndCheckpointTurn({
      turnIndex: 1,
      fencingToken: 9,
      toolName: "bash",
      toolOutput: { step: 1 },
    });

    // Simulate cold-start on new cell instance over same SQLite storage
    const cellB = new AgentTaskHarnessCell(doState, r2, "task-m54-cell");
    assert.equal(cellB.getHighestFencingToken(), 9);
    await assert.rejects(
      async () => {
        await cellB.executeAndCheckpointTurn({
          turnIndex: 3,
          fencingToken: 5, // stale token < 9
          toolName: "zombie_tool",
          toolOutput: {},
        });
      },
      /Stale fencing token 5 < highest 9/
    );
    const history = cellB.getTurnHistory();
    assert.deepEqual(
      history.map((h) => h.turnIndex),
      [1, 2],
      "getTurnHistory must return turns sorted ascending by turnIndex"
    );

    // 2. [CircuitBreaker-HalfOpen-Stampede-2] Single-probe Half-Open stampede guard & stuck-probe timeout recovery
    const breaker = new ThreeStateCircuitBreaker(2, 10_000, 5_000);
    const t0 = 5_000_000;
    breaker.recordFailure(t0);
    breaker.recordFailure(t0 + 10);
    assert.equal(breaker.getState(t0 + 20), "OPEN");

    // Advance past 10s cooldown -> HALF_OPEN: first caller acquires single probe slot, concurrent callers are blocked
    const firstProbe = breaker.tryAcquireRequestSlot(t0 + 10_500);
    assert.equal(firstProbe.admitted, true);
    assert.equal(firstProbe.isHalfOpenProbe, true);

    const concurrentCaller = breaker.tryAcquireRequestSlot(t0 + 10_600);
    assert.equal(concurrentCaller.admitted, false, "Concurrent caller during HALF_OPEN probe must be blocked to prevent stampede");

    // Simulate first probe hanging beyond 5s probeTimeoutMs -> next caller at +6s reclaims probe slot
    const recoveredProbe = breaker.tryAcquireRequestSlot(t0 + 16_000);
    assert.equal(recoveredProbe.admitted, true);
    breaker.recordSuccess();
    assert.equal(breaker.getState(t0 + 16_100), "CLOSED");

    // 3. [Staging-ApprovalTimeout-Deadlock-3] Auto-abort staged effects on rejection & expireStaleApprovalTickets TTL sweep
    const staging = new StagingCommitAndApprovalGate();
    const task1 = new TaskSpec({ task_id: "task-m54-stg-1", tenant_id: "Northstar" });
    staging.stageEffect({
      effectId: "eff-1",
      taskId: task1.task_id,
      turnIndex: 1,
      toolName: "delete_prod_bucket",
      args: {},
    });
    const tkt1 = staging.requestHumanApproval(task1, "delete_prod_bucket", {}, t0);
    staging.resolveApproval(tkt1.ticketId, false, "chen", task1);
    assert.equal(task1.state, TaskState.DEAD_LETTER);
    assert.equal(staging.getStagedEffects(task1.task_id)[0].aborted, true);

    const task2 = new TaskSpec({ task_id: "task-m54-stg-2", tenant_id: "Northstar" });
    staging.stageEffect({
      effectId: "eff-2",
      taskId: task2.task_id,
      turnIndex: 1,
      toolName: "modify_iam",
      args: {},
    });
    const tkt2 = staging.requestHumanApproval(task2, "modify_iam", {}, t0);
    const expired = staging.expireStaleApprovalTickets(
      new Map([[task2.task_id, task2]]),
      t0 + 4_000_000,
      3_600_000
    );
    assert.deepEqual(expired, [tkt2.ticketId]);
    assert.equal(task2.state, TaskState.DEAD_LETTER);
    assert.equal(staging.getStagedEffects(task2.task_id)[0].aborted, true);

    // 4. [HotCell-InFlight-Leak-Deadlock-4] Reclaim hung in-flight cell fetch slots
    const guard = new HotCellOverloadAndQueueGuard({ maxCellConcurrentRequests: 2 });
    guard.tryEnterCellFetch("cell-hot-1", t0);
    guard.tryEnterCellFetch("cell-hot-1", t0);
    assert.equal(guard.tryEnterCellFetch("cell-hot-1", t0 + 50).admitted, false);

    const reclaimed = guard.reclaimStaleInFlightFetches(t0 + 35_000, 30_000);
    assert.equal(reclaimed.reclaimedCount, 2);
    assert.equal(guard.tryEnterCellFetch("cell-hot-1", t0 + 35_100).admitted, true);

    // 5. [Outbox-Sse-Resource-Starvation-5] Outbox per-message exception isolation & SSE evicted Last-Event-ID fallback replay
    const outbox = new TransactionalOutboxDispatcher("sec-m54", 2);
    outbox.enqueueOutbox("m-bad", "task-1", "http://bad", { a: 1 });
    outbox.enqueueOutbox("m-good", "task-1", "http://good", { a: 2 });
    const res = outbox.processPending((m) => {
      if (m.messageId === "m-bad") throw new Error("Socket hang up");
      return true;
    });
    assert.equal(res.deliveredCount, 1);
    assert.equal(res.failedAttemptCount, 1);

    const sse = new SseEventStreamGateway(2);
    sse.publish({ id: "e1", event: "msg", taskId: "t-sse", turnIndex: 1, timestampMs: t0, data: {} });
    sse.publish({ id: "e2", event: "msg", taskId: "t-sse", turnIndex: 2, timestampMs: t0 + 1, data: {} });
    sse.publish({ id: "e3", event: "msg", taskId: "t-sse", turnIndex: 3, timestampMs: t0 + 2, data: {} });
    const sub = sse.subscribe("t-sse", "sub-1", "e1");
    assert.equal(sub.replayGapDetected, true);
    sse.unsubscribe("t-sse", "sub-1");
    assert.equal(sse.gcCompletedTaskStreams(new Set()), 1);

    // 6. [A2a-GitArbiter-Starvation-6] Conflict-aging priority boost prevents low-priority agent starvation
    const arbiter = new SharedBlackboardAndGitArbiter();
    const round1 = arbiter.arbitrateGitPatchTouches([
      { agentId: "agent-p1", touchedFiles: ["shared.ts"], priorityScore: 220 },
      { agentId: "agent-p3", touchedFiles: ["shared.ts"], priorityScore: 100 },
    ]);
    assert.deepEqual(round1.acceptedAgentIds, ["agent-p1"]);
    assert.deepEqual(round1.conflictedAgentIds, ["agent-p3"]);

    const round2 = arbiter.arbitrateGitPatchTouches([
      { agentId: "agent-p1", touchedFiles: ["shared.ts"], priorityScore: 220 },
      { agentId: "agent-p3", touchedFiles: ["shared.ts"], priorityScore: 100 }, // 100 + 150 aging boost = 250 > 220
    ]);
    assert.deepEqual(round2.acceptedAgentIds, ["agent-p3"]);

    // 7. Verify live HTTP endpoint & CLI `mgmt-runtime-audit`
    const server = new AnstsLocalAndComposeServer({ nodeId: "gw-m54-test" });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await runAnstsCtlRemote(["mgmt-runtime-audit"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);
      assert.equal(body.allM54RisksEliminated, true);
      assert.equal(body.harnessColdStartStale1.harnessColdStartFencingVerified, true);
      assert.equal(body.circuitBreakerStampede2.halfOpenStampedeAndTimeoutVerified, true);
      assert.equal(body.stagingApprovalDeadlock3.stagingExpirationSweepVerified, true);
      assert.equal(body.hotCellInFlightLeak4.hotCellReclamationVerified, true);
      assert.equal(body.outboxAndSseResilience5.outboxIsolationAndReplayVerified, true);
      assert.equal(body.outboxAndSseResilience5.sseEvictedFallbackVerified, true);
      assert.equal(body.a2aGitArbiterStarvation6.a2aAntiStarvationVerified, true);
    } finally {
      await server.stop();
    }
  });
});
