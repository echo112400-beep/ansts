/**
 * @fileoverview M25 & M55-2: High-Availability 256-Slot Scheduler Shard Coordinator with Epoch Lease Fencing,
 * Automatic Unhealthy Brain Shard Eviction & Last-Brain Survival Guard (preventing empty-ring deadlocks).
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

  /**
   * [M55-2] Evicts an unhealthy Brain shard node and rebalances its slots onto surviving Brain shards.
   * If `failedBrainNodeId` is the LAST remaining Brain shard in `activeBrains`, triggers the
   * Last-Brain Survival Guard: retains the last Brain node in the ring (bumping its epoch to fence stale leases)
   * so the 256-slot hash ring never becomes completely empty/ownerless.
   */
  public evictUnhealthyBrainShardNode(failedBrainNodeId: string): {
    evictedNodeId: string;
    survivingBrains: string[];
    newTopologyGeneration: number;
    bumpedEpochs: Record<string, number>;
    lastBrainSurvivalGuardTriggered?: boolean;
  } {
    if (this.activeBrains.size <= 1 && this.activeBrains.has(failedBrainNodeId)) {
      const nextEpoch = (this.shardEpochs.get(failedBrainNodeId) ?? 1) + 1;
      this.shardEpochs.set(failedBrainNodeId, nextEpoch);
      const casRes = this.topologyCas.forceSyncMembers(Array.from(this.activeBrains));
      return {
        evictedNodeId: failedBrainNodeId,
        survivingBrains: casRes.members,
        newTopologyGeneration: casRes.generation,
        bumpedEpochs: { [failedBrainNodeId]: nextEpoch },
        lastBrainSurvivalGuardTriggered: true,
      };
    }

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
      lastBrainSurvivalGuardTriggered: false,
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
