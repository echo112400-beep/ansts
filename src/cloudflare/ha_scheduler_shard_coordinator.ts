/**
 * @fileoverview M25 High-Availability 256-Slot Scheduler Shard Coordinator with Epoch Lease Fencing.
 */
import { VirtualHashSlotRingManager } from "../cluster/manual_scale_controller.js";

export class HaSchedulerShardCoordinator {
  public readonly ring: VirtualHashSlotRingManager;
  private readonly shardEpochs: Map<string, number> = new Map();

  constructor(totalSlots = 256, vnodes = 8) {
    this.ring = new VirtualHashSlotRingManager(totalSlots, vnodes);
  }

  public registerBrainShardNode(brainNodeId: string): number {
    this.ring.addNode(brainNodeId);
    const nextEpoch = (this.shardEpochs.get(brainNodeId) ?? 0) + 1;
    this.shardEpochs.set(brainNodeId, nextEpoch);
    return nextEpoch;
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
