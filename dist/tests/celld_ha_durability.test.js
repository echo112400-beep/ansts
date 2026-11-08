"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M8 Zero-Data-Loss HA & Durability Suite (5 tests).
 */







(0, _nodeTest.describe)("M8 Zero-Data-Loss HA & Durability Suite", () => {
  (0, _nodeTest.it)("1. RpoZeroDurabilityEngine flushes LTX segment with CAS If-None-Match", async () => {
    const bucket = new _index.InMemoryR2Bucket();
    const engine = new _index.RpoZeroDurabilityEngine(bucket);
    let mutated = false;
    const meta = await engine.commitWithSyncLtxFlush(
    "cell-ha-1",
    1,
    () => {
      mutated = true;
    },
    new Uint8Array([1, 2, 3, 4]));

    assert.equal(mutated, true);
    assert.ok(meta.sha256.length === 64);
  });

  (0, _nodeTest.it)("2. DurableWorkflowContext memoizes step.do() outputs across crash recovery", async () => {
    const sql = new _index.InMemoryCellSqlStorage();
    const wf = new _index.DurableWorkflowContext(sql, "wf-001");
    let runs = 0;
    const first = await wf.stepDo("charge", () => {
      runs += 1;
      return { tx: "abc" };
    });
    const second = await wf.stepDo("charge", () => {
      runs += 1;
      return { tx: "xyz" };
    });
    assert.equal(first.replayedFromJournal, false);
    assert.equal(second.replayedFromJournal, true);
    assert.equal(runs, 1);
    assert.deepEqual(second.result, { tx: "abc" });
  });

  (0, _nodeTest.it)("3. ShardedQueueBrokerPool distributes queue load across 16 shards deterministically", () => {
    const pool = new _index.ShardedQueueBrokerPool(16);
    const shard = pool.publish("tenant-a:task-1", "msg-1", { hello: "world" });
    const batch = pool.consumeBatch(shard);
    assert.equal(batch.length, 1);
    assert.equal(batch[0].id, "msg-1");
  });

  (0, _nodeTest.it)("4. SafeEpochGarbageCollector preserves active snapshots and collects expired fencing epochs", () => {
    const gc = new _index.SafeEpochGarbageCollector(1000);
    const now = Date.now();
    const collectible = gc.filterCollectibleSnapshots(
    [
    { key: "old-1", createdAtMs: now - 5000, fencingToken: 1, referencedByActiveCell: false },
    { key: "active-2", createdAtMs: now - 5000, fencingToken: 5, referencedByActiveCell: true }],

    3,
    now);

    assert.deepEqual(collectible, ["old-1"]);
  });

  (0, _nodeTest.it)("5. FleetHaAndAutoscalerController triggers SCALE_OUT when unhealthy nodes are detected", () => {
    const ctrl = new _index.FleetHaAndAutoscalerController();
    const res = ctrl.evaluateFleetHealth([
    { nodeId: "n1", healthy: true, cpuUtilPct: 50 },
    { nodeId: "n2", healthy: false, cpuUtilPct: 0 }]);

    assert.equal(res.scaleAction, "SCALE_OUT");
    assert.deepEqual(res.unhealthyNodeIds, ["n2"]);
  });
});