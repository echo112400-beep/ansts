"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M9 Horizontal Scale-Out Architecture Suite (3 tests).
 */




(0, _nodeTest.describe)("M9 Horizontal Scale-Out Suite", () => {
  (0, _nodeTest.it)("1. PartitionedSchedulerRouter distributes tasks across 64 consistent-hash shards", async () => {
    const router = new _index.PartitionedSchedulerRouter(64);
    const seenShards = new Set();
    for (let i = 0; i < 64; i++) {
      const { shardId } = await router.routeSubmitTask(
        new _index.TaskSpec({ task_id: `t-${i}`, tenant_id: `tenant-${i}` })
      );
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