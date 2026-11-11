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
 * @fileoverview feat/celld-v25 M17 Reliability, Capacity SLA, Topology CAS & Multi-Node Dev Server Suite.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("feat/celld-v25 M17 Reliability, Capacity & Console Suite", () => {
    (0, node_test_1.it)("1. CapacitySlaEnforcer detects SLA breaches and reserves emergency headroom", () => {
        const sla = new index_js_1.CapacitySlaEnforcer();
        const res = sla.evaluateSlaCompliance({
            p0QueueWaitP99Ms: 310,
            warmPoolHitRatio: 0.92,
            freeClusterCpuRatio: 0.12,
        });
        assert.equal(res.compliant, false);
        assert.deepEqual(res.breachedMetrics, ["p0QueueWaitP99Ms"]);
        assert.equal(res.reserveEmergencyHeadroom, true);
    });
    (0, node_test_1.it)("2. TopologyCasCoordinator & CrossRegionAsyncReplicator enforce CAS membership updates", () => {
        const cas = new index_js_1.TopologyCasCoordinator();
        const u1 = cas.updateTopologyCas(0, ["brain-01", "brain-02", "brain-03"]);
        assert.equal(u1.generation, 1);
        assert.throws(() => cas.updateTopologyCas(0, ["brain-01"]), /Topology CAS conflict/);
        const rep = new index_js_1.CrossRegionAsyncReplicator();
        const r = rep.replicateLtxObject("eu-central-1", "ltx-001");
        assert.equal(r.replicatedCount, 1);
        const drill = new index_js_1.AutomatedChaosDrillRunner();
        assert.equal(drill.runSplitBrainAndRecoveryDrill({ oldNodeFencingToken: 2, newNodeFencingToken: 3 }).rpoZeroVerified, true);
    });
    (0, node_test_1.it)("3. MultiNodeDevServer initializes role-aware brain/body/gateway cluster instance", async () => {
        const srv = new index_js_1.MultiNodeDevServer({
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
