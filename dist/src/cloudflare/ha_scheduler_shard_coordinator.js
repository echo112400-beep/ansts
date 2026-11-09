"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.HaSchedulerShardCoordinator = void 0;



var _manual_scale_controller = require("../cluster/manual_scale_controller.js");
var _topology_cas_coordinator = require("../cluster/topology_cas_coordinator.js"); /**
 * @fileoverview M25 High-Availability 256-Slot Scheduler Shard Coordinator with Epoch Lease Fencing
 * and Automatic Unhealthy Brain Shard Eviction & Slot Rebalancing.
 */class HaSchedulerShardCoordinator {ring;
  topologyCas;
  shardEpochs = new Map();
  activeBrains = new Set();

  constructor(totalSlots = 256, vnodes = 8) {
    this.ring = new _manual_scale_controller.VirtualHashSlotRingManager(totalSlots, vnodes);
    this.topologyCas = new _topology_cas_coordinator.TopologyCasCoordinator();
  }

  registerBrainShardNode(brainNodeId) {
    this.ring.addNode(brainNodeId);
    this.activeBrains.add(brainNodeId);
    const nextEpoch = (this.shardEpochs.get(brainNodeId) ?? 0) + 1;
    this.shardEpochs.set(brainNodeId, nextEpoch);
    this.topologyCas.forceSyncMembers(Array.from(this.activeBrains));
    return nextEpoch;
  }

  evictUnhealthyBrainShardNode(failedBrainNodeId)




  {
    this.ring.removeNode(failedBrainNodeId);
    this.activeBrains.delete(failedBrainNodeId);
    const bumpedEpochs = {};
    for (const surviving of this.activeBrains) {
      const nextEpoch = (this.shardEpochs.get(surviving) ?? 1) + 1;
      this.shardEpochs.set(surviving, nextEpoch);
      bumpedEpochs[surviving] = nextEpoch;
    }
    const casRes = this.topologyCas.forceSyncMembers(Array.from(this.activeBrains));
    return {
      evictedNodeId: failedBrainNodeId,
      survivingBrains: casRes.members,
      newTopologyGeneration: casRes.generation,
      bumpedEpochs
    };
  }

  resolveBrainForTask(tenantId, taskId)



  {
    const { slotIndex, nodeId } = this.ring.getOwnerNodeForKey(`${tenantId}:${taskId}`);
    return {
      slotIndex,
      brainNodeId: nodeId,
      epoch: nodeId ? this.shardEpochs.get(nodeId) ?? 1 : 0
    };
  }
}exports.HaSchedulerShardCoordinator = HaSchedulerShardCoordinator;