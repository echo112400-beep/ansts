"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.HaSchedulerShardCoordinator = void 0;



var _manual_scale_controller = require("../cluster/manual_scale_controller.js");
var _topology_cas_coordinator = require("../cluster/topology_cas_coordinator.js"); /**
 * @fileoverview M25, M55-2 & M64-4: High-Availability 256-Slot Scheduler Shard Coordinator with Epoch Lease Fencing,
 * SQLite-Backed Topology CAS Coordination, Automatic Unhealthy Brain Shard Eviction & Last-Brain Survival Guard.
 */class HaSchedulerShardCoordinator {
  ring;
  topologyCas;
  shardEpochs = new Map();
  activeBrains = new Set();

  constructor(totalSlots = 256, vnodes = 8, sql) {
    this.ring = new _manual_scale_controller.VirtualHashSlotRingManager(totalSlots, vnodes);
    this.topologyCas = new _topology_cas_coordinator.TopologyCasCoordinator(sql);
    for (const member of this.topologyCas.getMembers()) {
      this.ring.addNode(member);
      this.activeBrains.add(member);
      this.shardEpochs.set(member, Math.max(1, this.topologyCas.getGeneration()));
    }
  }

  registerBrainShardNode(brainNodeId) {
    this.ring.addNode(brainNodeId);
    this.activeBrains.add(brainNodeId);
    const nextEpoch = (this.shardEpochs.get(brainNodeId) ?? 0) + 1;
    this.shardEpochs.set(brainNodeId, nextEpoch);
    this.topologyCas.forceSyncMembers(Array.from(this.activeBrains));
    return nextEpoch;
  }

  /**
   * [M55-2] Evicts an unhealthy Brain shard node and rebalances its slots onto surviving Brain shards.
   * If `failedBrainNodeId` is the LAST remaining Brain shard in `activeBrains`, triggers the
   * Last-Brain Survival Guard: retains the last Brain node in the ring (bumping its epoch to fence stale leases)
   * so the 256-slot hash ring never becomes completely empty/ownerless.
   */
  evictUnhealthyBrainShardNode(failedBrainNodeId)





  {
    if (this.activeBrains.size <= 1 && this.activeBrains.has(failedBrainNodeId)) {
      const nextEpoch = (this.shardEpochs.get(failedBrainNodeId) ?? 1) + 1;
      this.shardEpochs.set(failedBrainNodeId, nextEpoch);
      const casRes = this.topologyCas.forceSyncMembers(Array.from(this.activeBrains));
      return {
        evictedNodeId: failedBrainNodeId,
        survivingBrains: casRes.members,
        newTopologyGeneration: casRes.generation,
        bumpedEpochs: { [failedBrainNodeId]: nextEpoch },
        lastBrainSurvivalGuardTriggered: true
      };
    }

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
      bumpedEpochs,
      lastBrainSurvivalGuardTriggered: false
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