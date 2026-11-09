/**
 * @fileoverview feat/celld-v25 M17 Reliability, Capacity SLA, Topology CAS & Multi-Node Dev Server Suite.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  CapacitySlaEnforcer,
  TopologyCasCoordinator,
  AutomatedChaosDrillRunner,
  CrossRegionAsyncReplicator,
  MultiNodeDevServer,
} from "../src/index.js";

describe("feat/celld-v25 M17 Reliability, Capacity & Console Suite", () => {
  it("1. CapacitySlaEnforcer detects SLA breaches and reserves emergency headroom", () => {
    const sla = new CapacitySlaEnforcer();
    const res = sla.evaluateSlaCompliance({
      p0QueueWaitP99Ms: 310,
      warmPoolHitRatio: 0.92,
      freeClusterCpuRatio: 0.12,
    });
    assert.equal(res.compliant, false);
    assert.deepEqual(res.breachedMetrics, ["p0QueueWaitP99Ms"]);
    assert.equal(res.reserveEmergencyHeadroom, true);
  });

  it("2. TopologyCasCoordinator & CrossRegionAsyncReplicator enforce CAS membership updates", () => {
    const cas = new TopologyCasCoordinator();
    const u1 = cas.updateTopologyCas(0, ["brain-01", "brain-02", "brain-03"]);
    assert.equal(u1.generation, 1);
    assert.throws(() => cas.updateTopologyCas(0, ["brain-01"]), /Topology CAS conflict/);

    const rep = new CrossRegionAsyncReplicator();
    const r = rep.replicateLtxObject("eu-central-1", "ltx-001");
    assert.equal(r.replicatedCount, 1);

    const drill = new AutomatedChaosDrillRunner();
    assert.equal(
      drill.runSplitBrainAndRecoveryDrill({ oldNodeFencingToken: 2, newNodeFencingToken: 3 }).rpoZeroVerified,
      true
    );
  });

  it("3. MultiNodeDevServer initializes role-aware brain/body/gateway cluster instance", async () => {
    const srv = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "brain-node-01",
      nodeRole: "brain",
      workerNodeCount: 1,
    });
    assert.equal(srv.nodeId, "brain-node-01");
    assert.equal(srv.nodeRole, "brain");
    const { apiPort } = await srv.start(0, 0);
    const res = await fetch(`http://127.0.0.1:${apiPort}/state`);
    assert.equal(res.status, 200);
    await srv.stop();
  });
});
