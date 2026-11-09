"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview feat/celld-v25 M17 Reliability, Capacity SLA, Topology CAS & Multi-Node Dev Server Suite.
 */





(0, _nodeTest.describe)("feat/celld-v25 M17 Reliability, Capacity & Console Suite", () => {
  (0, _nodeTest.it)("1. CapacitySlaEnforcer detects SLA breaches and reserves emergency headroom", () => {
    const sla = new _index.CapacitySlaEnforcer();
    const res = sla.evaluateSlaCompliance({
      p0QueueWaitP99Ms: 310,
      warmPoolHitRatio: 0.92,
      freeClusterCpuRatio: 0.12
    });
    assert.equal(res.compliant, false);
    assert.deepEqual(res.breachedMetrics, ["p0QueueWaitP99Ms"]);
    assert.equal(res.reserveEmergencyHeadroom, true);
  });

  (0, _nodeTest.it)("2. TopologyCasCoordinator & CrossRegionAsyncReplicator enforce CAS membership updates", () => {
    const cas = new _index.TopologyCasCoordinator();
    const u1 = cas.updateTopologyCas(0, ["brain-01", "brain-02", "brain-03"]);
    assert.equal(u1.generation, 1);
    assert.throws(() => cas.updateTopologyCas(0, ["brain-01"]), /Topology CAS conflict/);

    const rep = new _index.CrossRegionAsyncReplicator();
    const r = rep.replicateLtxObject("eu-central-1", "ltx-001");
    assert.equal(r.replicatedCount, 1);

    const drill = new _index.AutomatedChaosDrillRunner();
    assert.equal(
    drill.runSplitBrainAndRecoveryDrill({ oldNodeFencingToken: 2, newNodeFencingToken: 3 }).rpoZeroVerified,
    true);

  });

  (0, _nodeTest.it)("3. MultiNodeDevServer initializes role-aware brain/body/gateway cluster instance", async () => {
    const srv = new _index.MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "brain-node-01",
      nodeRole: "brain",
      workerNodeCount: 1
    });
    assert.equal(srv.nodeId, "brain-node-01");
    assert.equal(srv.nodeRole, "brain");
    const { apiPort } = await srv.start(0, 0);
    const res = await fetch(`http://127.0.0.1:${apiPort}/state`);
    assert.equal(res.status, 200);
    await srv.stop();
  });
});