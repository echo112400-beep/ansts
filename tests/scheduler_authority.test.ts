/**
 * @fileoverview feat/celld-v25 Scheduler Authority & 256-Slot HaSchedulerShardCoordinator Suite.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import { HaSchedulerShardCoordinator } from "../src/index.js";

describe("feat/celld-v25 Scheduler Shard Authority Suite", () => {
  it("Maps tasks across 256 virtual slots to registered celld-brain-01..03 nodes with monotonic epochs", () => {
    const coord = new HaSchedulerShardCoordinator(256, 8);
    assert.equal(coord.registerBrainShardNode("brain-node-01"), 1);
    assert.equal(coord.registerBrainShardNode("brain-node-02"), 1);
    assert.equal(coord.registerBrainShardNode("brain-node-03"), 1);

    const res = coord.resolveBrainForTask("tenant-alpha", "task-distributed-001");
    assert.ok(res.slotIndex >= 0 && res.slotIndex < 256);
    assert.ok(["brain-node-01", "brain-node-02", "brain-node-03"].includes(res.brainNodeId!));
    assert.equal(res.epoch, 1);
  });
});
