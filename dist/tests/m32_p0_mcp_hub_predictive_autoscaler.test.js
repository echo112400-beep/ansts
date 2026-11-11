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
 * @fileoverview feat/celld-v25 M32 Predictive EWMA Sandbox Prewarmer & Dual-Homed Edge Tunnel Failover Suite.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("feat/celld-v25 M32 Predictive Prewarmer & Edge Tunnel HA Suite", () => {
    (0, node_test_1.it)("1. PredictiveSandboxPrewarmer scales warm pool size ahead of burst traffic", () => {
        const prewarmer = new index_js_1.PredictiveSandboxPrewarmer(0.5);
        const pred = prewarmer.recordArrivalSampleAndPredict(25.0, 4.0);
        assert.ok(pred.predictedConcurrency >= 50);
        assert.ok(pred.recommendedWarmPoolSize >= 60);
    });
    (0, node_test_1.it)("2. EdgeTunnelHaRouter routes via PRIMARY gateway and automatically fails over to SECONDARY", () => {
        const router = new index_js_1.EdgeTunnelHaRouter();
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
