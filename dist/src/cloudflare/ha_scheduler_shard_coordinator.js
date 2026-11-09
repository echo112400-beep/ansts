"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.HaSchedulerShardCoordinator = void 0;


var _manual_scale_controller = require("../cluster/manual_scale_controller.js"); /**
 * @fileoverview M25 High-Availability 256-Slot Scheduler Shard Coordinator with Epoch Lease Fencing.
 */class HaSchedulerShardCoordinator {
  ring;
  shardEpochs = new Map();

  constructor(totalSlots = 256, vnodes = 8) {
    this.ring = new _manual_scale_controller.VirtualHashSlotRingManager(totalSlots, vnodes);
  }

  registerBrainShardNode(brainNodeId) {
    this.ring.addNode(brainNodeId);
    const nextEpoch = (this.shardEpochs.get(brainNodeId) ?? 0) + 1;
    this.shardEpochs.set(brainNodeId, nextEpoch);
    return nextEpoch;
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