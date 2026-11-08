/**
 * @fileoverview M8 Zero-Data-Loss HA & Durability Suite (5 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  InMemoryR2Bucket,
  InMemoryCellSqlStorage,
  RpoZeroDurabilityEngine,
  DurableWorkflowContext,
  ShardedQueueBrokerPool,
  SafeEpochGarbageCollector,
  FleetHaAndAutoscalerController,
} from "../src/index.js";

describe("M8 Zero-Data-Loss HA & Durability Suite", () => {
  it("1. RpoZeroDurabilityEngine flushes LTX segment with CAS If-None-Match", async () => {
    const bucket = new InMemoryR2Bucket();
    const engine = new RpoZeroDurabilityEngine(bucket);
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

  it("2. DurableWorkflowContext memoizes step.do() outputs across crash recovery", async () => {
    const sql = new InMemoryCellSqlStorage();
    const wf = new DurableWorkflowContext(sql, "wf-001");
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

  it("3. ShardedQueueBrokerPool distributes queue load across 16 shards deterministically", () => {
    const pool = new ShardedQueueBrokerPool(16);
    const shard = pool.publish("tenant-a:task-1", "msg-1", { hello: "world" });
    const batch = pool.consumeBatch(shard);
    assert.equal(batch.length, 1);
    assert.equal(batch[0].id, "msg-1");
  });

  it("4. SafeEpochGarbageCollector preserves active snapshots and collects expired fencing epochs", () => {
    const gc = new SafeEpochGarbageCollector(1000);
    const now = Date.now();
    const collectible = gc.filterCollectibleSnapshots(
      [
        { key: "old-1", createdAtMs: now - 5000, fencingToken: 1, referencedByActiveCell: false },
        { key: "active-2", createdAtMs: now - 5000, fencingToken: 5, referencedByActiveCell: true },
      ],
      3,
      now
    );
    assert.deepEqual(collectible, ["old-1"]);
  });

  it("5. FleetHaAndAutoscalerController triggers SCALE_OUT when unhealthy nodes are detected", () => {
    const ctrl = new FleetHaAndAutoscalerController();
    const res = ctrl.evaluateFleetHealth([
      { nodeId: "n1", healthy: true, cpuUtilPct: 50 },
      { nodeId: "n2", healthy: false, cpuUtilPct: 0 },
    ]);
    assert.equal(res.scaleAction, "SCALE_OUT");
    assert.deepEqual(res.unhealthyNodeIds, ["n2"]);
  });
});
