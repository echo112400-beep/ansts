"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview feat/celld-v25 M16 Production Hardening Suite (Canary Rollout, Priority Load Shedder, Preflight Gate & Chaos Proxy).
 */





(0, _nodeTest.describe)("feat/celld-v25 M16 Production Hardening Suite", () => {
  (0, _nodeTest.it)("1. CanaryRolloutController promotes healthy canary and rolls back on elevated error rate", () => {
    const canary = new _index.CanaryRolloutController();
    const step1 = canary.evaluateCanaryStep({ errorRatePct: 0.2, p99LatencyMs: 250 });
    assert.equal(step1.state, "CANARY_ACTIVE");
    assert.ok(step1.trafficWeightPct > 5);

    const bad = canary.evaluateCanaryStep({ errorRatePct: 4.5, p99LatencyMs: 2000 });
    assert.equal(bad.state, "ROLLED_BACK");
    assert.equal(bad.trafficWeightPct, 0);
  });

  (0, _nodeTest.it)("2. PriorityLoadShedder progressively sheds P3 -> P2 -> P1 as cluster approaches saturation", () => {
    const shedder = new _index.PriorityLoadShedder();
    assert.equal(shedder.shouldAdmitUnderLoad(_index.TaskPriority.P3_BATCH, 0.85), false);
    assert.equal(shedder.shouldAdmitUnderLoad(_index.TaskPriority.P2_STANDARD, 0.85), true);
    assert.equal(shedder.shouldAdmitUnderLoad(_index.TaskPriority.P2_STANDARD, 0.92), false);
    assert.equal(shedder.shouldAdmitUnderLoad(_index.TaskPriority.P0_EMERGENCY, 0.99), true);
  });

  (0, _nodeTest.it)("3. SandboxPreflightReadinessGate & NetworkChaosFaultInjectionProxy validate host readiness and partition faults", () => {
    const gate = new _index.SandboxPreflightReadinessGate();
    assert.equal(gate.evaluateHostReadiness({ cgroupV2Mounted: false, freeDiskGb: 50 }).ready, false);
    assert.equal(gate.evaluateHostReadiness({ cgroupV2Mounted: true, freeDiskGb: 20 }).ready, true);

    const chaos = new _index.NetworkChaosFaultInjectionProxy();
    chaos.injectPartition("celld-brain-02");
    assert.throws(() => chaos.assertReachable("celld-brain-02"), /simulated network partition/);
    chaos.healPartition("celld-brain-02");
    assert.doesNotThrow(() => chaos.assertReachable("celld-brain-02"));
  });
});