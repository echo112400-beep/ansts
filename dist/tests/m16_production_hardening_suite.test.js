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
 * @fileoverview feat/celld-v25 M16 Production Hardening Suite (Canary Rollout, Priority Load Shedder, Preflight Gate & Chaos Proxy).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("feat/celld-v25 M16 Production Hardening Suite", () => {
    (0, node_test_1.it)("1. CanaryRolloutController promotes healthy canary and rolls back on elevated error rate", () => {
        const canary = new index_js_1.CanaryRolloutController();
        const step1 = canary.evaluateCanaryStep({ errorRatePct: 0.2, p99LatencyMs: 250 });
        assert.equal(step1.state, "CANARY_ACTIVE");
        assert.ok(step1.trafficWeightPct > 5);
        const bad = canary.evaluateCanaryStep({ errorRatePct: 4.5, p99LatencyMs: 2000 });
        assert.equal(bad.state, "ROLLED_BACK");
        assert.equal(bad.trafficWeightPct, 0);
    });
    (0, node_test_1.it)("2. PriorityLoadShedder progressively sheds P3 -> P2 -> P1 as cluster approaches saturation", () => {
        const shedder = new index_js_1.PriorityLoadShedder();
        assert.equal(shedder.shouldAdmitUnderLoad(index_js_1.TaskPriority.P3_BATCH, 0.85), false);
        assert.equal(shedder.shouldAdmitUnderLoad(index_js_1.TaskPriority.P2_STANDARD, 0.85), true);
        assert.equal(shedder.shouldAdmitUnderLoad(index_js_1.TaskPriority.P2_STANDARD, 0.92), false);
        assert.equal(shedder.shouldAdmitUnderLoad(index_js_1.TaskPriority.P0_EMERGENCY, 0.99), true);
    });
    (0, node_test_1.it)("3. SandboxPreflightReadinessGate & NetworkChaosFaultInjectionProxy validate host readiness and partition faults", () => {
        const gate = new index_js_1.SandboxPreflightReadinessGate();
        assert.equal(gate.evaluateHostReadiness({ cgroupV2Mounted: false, freeDiskGb: 50 }).ready, false);
        assert.equal(gate.evaluateHostReadiness({ cgroupV2Mounted: true, freeDiskGb: 20 }).ready, true);
        const chaos = new index_js_1.NetworkChaosFaultInjectionProxy();
        chaos.injectPartition("celld-brain-02");
        assert.throws(() => chaos.assertReachable("celld-brain-02"), /simulated network partition/);
        chaos.healPartition("celld-brain-02");
        assert.doesNotThrow(() => chaos.assertReachable("celld-brain-02"));
    });
});
