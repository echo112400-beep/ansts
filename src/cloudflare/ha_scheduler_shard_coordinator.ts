/**
 * @fileoverview M25 High-Availability 256-Slot Scheduler Shard Coordinator with Epoch Lease Fencing
 * and Automatic Unhealthy Brain Shard Eviction & Slot Rebalancing.
 */
import { VirtualHashSlotRingManager } from "../cluster/manual_scale_controller.js";
import { TopologyCasCoordinator } from "../cluster/topology_cas_coordinator.js";

export class HaSchedulerShardCoordinator {
  public readonly ring: VirtualHashSlotRingManager;
  public readonly topologyCas: TopologyCasCoordinator;
  private readonly shardEpochs: Map<string, number> = new Map();
  private readonly activeBrains: Set<string> = new Set();

  constructor(totalSlots = 256, vnodes = 8) {
    this.ring = new VirtualHashSlotRingManager(totalSlots, vnodes);
    this.topologyCas = new TopologyCasCoordinator();
  }

  public registerBrainShardNode(brainNodeId: string): number {
    this.ring.addNode(brainNodeId);
    this.activeBrains.add(brainNodeId);
    const nextEpoch = (this.shardEpochs.get(brainNodeId) ?? 0) + 1;
    this.shardEpochs.set(brainNodeId, nextEpoch);
    this.topologyCas.forceSyncMembers(Array.from(this.activeBrains));
    return nextEpoch;
  }

  public evictUnhealthyBrainShardNode(failedBrainNodeId: string): {
    evictedNodeId: string;
    survivingBrains: string[];
    newTopologyGeneration: number;
    bumpedEpochs: Record<string, number>;
  } {
    this.ring.removeNode(failedBrainNodeId);
    this.activeBrains.delete(failedBrainNodeId);
    const bumpedEpochs: Record<string, number> = {};
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
    };
  }

  public resolveBrainForTask(tenantId: string, taskId: string): {
    slotIndex: number;
    brainNodeId: string | null;
    epoch: number;
  } {
    const { slotIndex, nodeId } = this.ring.getOwnerNodeForKey(`${tenantId}:${taskId}`);
    return {
      slotIndex,
      brainNodeId: nodeId,
      epoch: nodeId ? (this.shardEpochs.get(nodeId) ?? 1) : 0,
    };
  }
}
