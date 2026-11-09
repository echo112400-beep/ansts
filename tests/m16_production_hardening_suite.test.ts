/**
 * @fileoverview feat/celld-v25 M16 Production Hardening Suite (Canary Rollout, Priority Load Shedder, Preflight Gate & Chaos Proxy).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  CanaryRolloutController,
  PriorityLoadShedder,
  SandboxPreflightReadinessGate,
  NetworkChaosFaultInjectionProxy,
  TaskPriority,
} from "../src/index.js";

describe("feat/celld-v25 M16 Production Hardening Suite", () => {
  it("1. CanaryRolloutController promotes healthy canary and rolls back on elevated error rate", () => {
    const canary = new CanaryRolloutController();
    const step1 = canary.evaluateCanaryStep({ errorRatePct: 0.2, p99LatencyMs: 250 });
    assert.equal(step1.state, "CANARY_ACTIVE");
    assert.ok(step1.trafficWeightPct > 5);

    const bad = canary.evaluateCanaryStep({ errorRatePct: 4.5, p99LatencyMs: 2000 });
    assert.equal(bad.state, "ROLLED_BACK");
    assert.equal(bad.trafficWeightPct, 0);
  });

  it("2. PriorityLoadShedder progressively sheds P3 -> P2 -> P1 as cluster approaches saturation", () => {
    const shedder = new PriorityLoadShedder();
    assert.equal(shedder.shouldAdmitUnderLoad(TaskPriority.P3_BATCH, 0.85), false);
    assert.equal(shedder.shouldAdmitUnderLoad(TaskPriority.P2_STANDARD, 0.85), true);
    assert.equal(shedder.shouldAdmitUnderLoad(TaskPriority.P2_STANDARD, 0.92), false);
    assert.equal(shedder.shouldAdmitUnderLoad(TaskPriority.P0_EMERGENCY, 0.99), true);
  });

  it("3. SandboxPreflightReadinessGate & NetworkChaosFaultInjectionProxy validate host readiness and partition faults", () => {
    const gate = new SandboxPreflightReadinessGate();
    assert.equal(gate.evaluateHostReadiness({ cgroupV2Mounted: false, freeDiskGb: 50 }).ready, false);
    assert.equal(gate.evaluateHostReadiness({ cgroupV2Mounted: true, freeDiskGb: 20 }).ready, true);

    const chaos = new NetworkChaosFaultInjectionProxy();
    chaos.injectPartition("celld-brain-02");
    assert.throws(() => chaos.assertReachable("celld-brain-02"), /simulated network partition/);
    chaos.healPartition("celld-brain-02");
    assert.doesNotThrow(() => chaos.assertReachable("celld-brain-02"));
  });
});
