"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M9 Horizontal Scale-Out Architecture Suite (3 tests).
 */




(0, _nodeTest.describe)("M9 Horizontal Scale-Out Suite", () => {
  (0, _nodeTest.it)("1. PartitionedSchedulerRouter distributes tasks across 64 consistent-hash shards", async () => {
    const router = new _index.PartitionedSchedulerRouter(64);
    const seenShards = new Set();
    for (let i = 0; i < 64; i++) {
      const { shardId } = await router.routeSubmitTask(
      new _index.TaskSpec({ task_id: `t-${i}`, tenant_id: `tenant-${i}` }));

      seenShards.add(shardId);
    }
    assert.ok(seenShards.size > 15);
  });

  (0, _nodeTest.it)("2. DynamicQueueAutoSharder scales queue shards dynamically with incoming write RPS", () => {
    const sharder = new _index.DynamicQueueAutoSharder(16, 4, 128);
    const up = sharder.adjustShards(18_000, 450);
    assert.equal(up.newShards, 40);
    assert.equal(up.rebalanced, true);
  });

  (0, _nodeTest.it)("3. BrainBodyContainerPoolScaler decouples lightweight V8 Isolate memory from heavy ACT containers", () => {
    const scaler = new _index.BrainBodyContainerPoolScaler();
    const plan = scaler.computeTargetSandboxContainers({
      activeThinkCells: 1000,
      activeActCells: 50,
      warmStandbyRatio: 0.1
    });
    assert.equal(plan.requiredHeavyActContainers, 50);
    assert.equal(plan.warmBufferContainers, 100);
    assert.equal(plan.totalContainerPoolSize, 150);
  });
});