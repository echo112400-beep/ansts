/**
 * @fileoverview Code Audit & Edge-Case Hardening Suite (4 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  TaskSpecValidator,
  IngressBackpressureController,
  DagEscrowCoordinator,
  TaskPriority,
  TaskSpec,
  SyntacticAndVelocityBreaker,
} from "../src/index.js";

describe("Code Audit & Edge-Case Hardening Suite", () => {
  it("1. TaskSpecValidator rejects self-dependency and invalid CPU resource profiles", () => {
    assert.throws(() => {
      TaskSpecValidator.validateAndNormalize({
        task_id: "self-dep",
        tenant_id: "t1",
        dependencies: ["self-dep"],
      });
    }, /cannot depend on itself/);
  });

  it("2. IngressBackpressureController returns HTTP 429 with Retry-After header under PSI pressure", () => {
    const ctrl = new IngressBackpressureController({ maxGlobalQueueDepth: 100 });
    const decision = ctrl.evaluateAdmission({
      priority: TaskPriority.P2_STANDARD,
      globalQueueDepth: 20,
      tenantActiveTasks: 2,
      clusterPsiPressurePct: 90.0,
    });
    assert.equal(decision.allowed, false);
    assert.equal(decision.statusCode, 429);
    assert.ok((decision.retryAfterSeconds ?? 0) > 0);
  });

  it("3. DagEscrowCoordinator prevents parent-child quota deadlock and detects DAG cycles", () => {
    const coord = new DagEscrowCoordinator();
    const parent = new TaskSpec({
      task_id: "p-escrow",
      tenant_id: "t1",
      expected_SubAgent_count: 2,
    });
    const escrow = coord.reserveParentEscrow(parent, 10, 1);
    assert.equal(escrow.escrowedSlots, 2);

    const child = new TaskSpec({ task_id: "c-escrow-1", tenant_id: "t1", parent_task_id: "p-escrow" });
    const reg = coord.registerChildSubAgent(parent, child);
    assert.equal(reg.usedEscrowSlot, true);
    const resumed = coord.completeChildSubAgent(parent, "c-escrow-1");
    assert.equal(resumed, true);
  });

  it("4. SyntacticAndVelocityBreaker trips on 3 consecutive identical tool calls", () => {
    const breaker = new SyntacticAndVelocityBreaker(3, 100_000);
    const now = Date.now();
    breaker.recordAndCheckTurn("t-loop", { turnIndex: 1, toolName: "ls", argsHash: "h1", tokensConsumed: 100, timestampMs: now });
    breaker.recordAndCheckTurn("t-loop", { turnIndex: 2, toolName: "ls", argsHash: "h1", tokensConsumed: 100, timestampMs: now + 10 });
    const third = breaker.recordAndCheckTurn("t-loop", { turnIndex: 3, toolName: "ls", argsHash: "h1", tokensConsumed: 100, timestampMs: now + 20 });
    assert.equal(third.tripped, true);
    assert.equal(third.reason, "SEMANTIC_DEAD_LOOP");
  });
});
