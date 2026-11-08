/**
 * @fileoverview M9 Horizontal Scale-Out Architecture Suite (3 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  PartitionedSchedulerRouter,
  DynamicQueueAutoSharder,
  BrainBodyContainerPoolScaler,
  TaskSpec,
} from "../src/index.js";

describe("M9 Horizontal Scale-Out Suite", () => {
  it("1. PartitionedSchedulerRouter distributes tasks across 64 consistent-hash shards", async () => {
    const router = new PartitionedSchedulerRouter(64);
    const seenShards = new Set<number>();
    for (let i = 0; i < 64; i++) {
      const { shardId } = await router.routeSubmitTask(
        new TaskSpec({ task_id: `t-${i}`, tenant_id: `tenant-${i}` })
      );
      seenShards.add(shardId);
    }
    assert.ok(seenShards.size > 15);
  });

  it("2. DynamicQueueAutoSharder scales queue shards dynamically with incoming write RPS", () => {
    const sharder = new DynamicQueueAutoSharder(16, 4, 128);
    const up = sharder.adjustShards(18_000, 450);
    assert.equal(up.newShards, 40);
    assert.equal(up.rebalanced, true);
  });

  it("3. BrainBodyContainerPoolScaler decouples lightweight V8 Isolate memory from heavy ACT containers", () => {
    const scaler = new BrainBodyContainerPoolScaler();
    const plan = scaler.computeTargetSandboxContainers({
      activeThinkCells: 1000,
      activeActCells: 50,
      warmStandbyRatio: 0.1,
    });
    assert.equal(plan.requiredHeavyActContainers, 50);
    assert.equal(plan.warmBufferContainers, 100);
    assert.equal(plan.totalContainerPoolSize, 150);
  });
});
