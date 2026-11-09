/**
 * @fileoverview feat/celld-v25 M32 Predictive EWMA Sandbox Prewarmer & Dual-Homed Edge Tunnel Failover Suite.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  PredictiveSandboxPrewarmer,
  EdgeTunnelHaRouter,
} from "../src/index.js";

describe("feat/celld-v25 M32 Predictive Prewarmer & Edge Tunnel HA Suite", () => {
  it("1. PredictiveSandboxPrewarmer scales warm pool size ahead of burst traffic", () => {
    const prewarmer = new PredictiveSandboxPrewarmer(0.5);
    const pred = prewarmer.recordArrivalSampleAndPredict(25.0, 4.0);
    assert.ok(pred.predictedConcurrency >= 50);
    assert.ok(pred.recommendedWarmPoolSize >= 60);
  });

  it("2. EdgeTunnelHaRouter routes via PRIMARY gateway and automatically fails over to SECONDARY", () => {
    const router = new EdgeTunnelHaRouter();
    router.tunnelCell.registerTunnel({
      hostId: "body-host-01",
      gatewayId: "celld-gw-01",
      role: "PRIMARY",
      connGeneration: 1,
      epoch: 1,
    });
    router.tunnelCell.registerTunnel({
      hostId: "body-host-01",
      gatewayId: "celld-gw-02",
      role: "SECONDARY",
      connGeneration: 1,
      epoch: 1,
    });

    const r1 = router.routeRpcToBodyHost("body-host-01", { cmd: "run" });
    assert.equal(r1.routedViaGatewayId, "celld-gw-01");
    assert.equal(r1.role, "PRIMARY");

    router.tunnelCell.markGatewayHealth("body-host-01", "celld-gw-01", false);
    const r2 = router.routeRpcToBodyHost("body-host-01", { cmd: "run" });
    assert.equal(r2.routedViaGatewayId, "celld-gw-02");
    assert.equal(r2.role, "SECONDARY");
  });
});
