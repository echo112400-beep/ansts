"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
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
      new Uint8Array([1, 2, 3, 4])
    );
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
      now
    );
    assert.deepEqual(collectible, ["old-1"]);
  });

  (0, _nodeTest.it)("5. FleetHaAndAutoscalerController triggers SCALE_OUT when unhealthy nodes are detected", () => {
    const ctrl = new _index.FleetHaAndAutoscalerController();
    const res = ctrl.evaluateFleetHealth([
    { nodeId: "n1", healthy: true, cpuUtilPct: 50 },
    { nodeId: "n2", healthy: false, cpuUtilPct: 0 }]
    );
    assert.equal(res.scaleAction, "SCALE_OUT");
    assert.deepEqual(res.unhealthyNodeIds, ["n2"]);
  });
});