"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview feat/celld-v25 M32 Predictive EWMA Sandbox Prewarmer & Dual-Homed Edge Tunnel Failover Suite.
 */


(0, _nodeTest.describe)("feat/celld-v25 M32 Predictive Prewarmer & Edge Tunnel HA Suite", () => {
  (0, _nodeTest.it)("1. PredictiveSandboxPrewarmer scales warm pool size ahead of burst traffic", () => {
    const prewarmer = new _index.PredictiveSandboxPrewarmer(0.5);
    const pred = prewarmer.recordArrivalSampleAndPredict(25.0, 4.0);
    assert.ok(pred.predictedConcurrency >= 50);
    assert.ok(pred.recommendedWarmPoolSize >= 60);
  });

  (0, _nodeTest.it)("2. EdgeTunnelHaRouter routes via PRIMARY gateway and automatically fails over to SECONDARY", () => {
    const router = new _index.EdgeTunnelHaRouter();
    router.tunnelCell.registerTunnel({
      hostId: "body-host-01",
      gatewayId: "celld-gw-01",
      role: "PRIMARY",
      connGeneration: 1,
      epoch: 1
    });
    router.tunnelCell.registerTunnel({
      hostId: "body-host-01",
      gatewayId: "celld-gw-02",
      role: "SECONDARY",
      connGeneration: 1,
      epoch: 1
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