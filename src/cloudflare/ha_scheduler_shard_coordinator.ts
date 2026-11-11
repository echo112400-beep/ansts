/**
 * @fileoverview M25, M55-2, M64-4 & M69-5: High-Availability 256-Slot Scheduler Shard Coordinator with Epoch Lease Fencing,
 * SQLite-Backed Topology CAS & Per-Brain `shardEpochs` Persistence, Automatic Unhealthy Brain Shard Eviction & Last-Brain Survival Guard.
 */
import { VirtualHashSlotRingManager } from "../cluster/manual_scale_controller.js";
import { TopologyCasCoordinator } from "../cluster/topology_cas_coordinator.js";
import { SqlStorageLike } from "./env.js";

export class HaSchedulerShardCoordinator {
  public readonly ring: VirtualHashSlotRingManager;
  public readonly topologyCas: TopologyCasCoordinator;
  private readonly shardEpochs: Map<string, number> = new Map();
  private readonly activeBrains: Set<string> = new Map<string, boolean>() as unknown as Set<string>;
  private readonly sql?: SqlStorageLike;

  constructor(totalSlots = 256, vnodes = 8, sql?: SqlStorageLike) {
    this.ring = new VirtualHashSlotRingManager(totalSlots, vnodes);
    this.topologyCas = new TopologyCasCoordinator(sql);
    this.activeBrains = new Set<string>();
    this.sql = sql;
    if (this.sql) {
      this.sql.exec(
        "CREATE TABLE IF NOT EXISTS v25_shard_epochs (brain_node_id TEXT PRIMARY KEY, epoch INTEGER)"
      );
      for (const r of this.sql.exec<Record<string, unknown>>("SELECT * FROM v25_shard_epochs").toArray()) {
        const bId = String(r.brain_node_id ?? "");
        const ep = Number(r.epoch ?? 1);
        if (bId && Number.isFinite(ep) && ep >= 1) {
          this.shardEpochs.set(bId, Math.floor(ep));
        }
      }
    }
    for (const member of this.topologyCas.getMembers()) {
      this.ring.addNode(member);
      this.activeBrains.add(member);
      const persistedEpoch = this.shardEpochs.get(member) ?? 0;
      const effectiveEpoch = Math.max(1, persistedEpoch, this.topologyCas.getGeneration());
      this.shardEpochs.set(member, effectiveEpoch);
      this.persistBrainEpoch(member, effectiveEpoch);
    }
  }

  private persistBrainEpoch(brainNodeId: string, epoch: number): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO v25_shard_epochs (brain_node_id, epoch) VALUES (?, ?)",
      brainNodeId,
      epoch
    );
  }

  public registerBrainShardNode(brainNodeId: string): number {
    this.ring.addNode(brainNodeId);
    this.activeBrains.add(brainNodeId);
    const nextEpoch = (this.shardEpochs.get(brainNodeId) ?? 0) + 1;
    this.shardEpochs.set(brainNodeId, nextEpoch);
    this.persistBrainEpoch(brainNodeId, nextEpoch);
    this.topologyCas.forceSyncMembers(Array.from(this.activeBrains));
    return nextEpoch;
  }

  /**
   * [M55-2 & M69-5] Evicts an unhealthy Brain shard node and rebalances its slots onto surviving Brain shards.
   * If `failedBrainNodeId` is the LAST remaining Brain shard in `activeBrains`, triggers the
   * Last-Brain Survival Guard: retains the last Brain node in the ring (bumping its epoch to fence stale leases)
   * and persists all bumped `shardEpochs` in SQLite (`v25_shard_epochs`).
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
      this.persistBrainEpoch(failedBrainNodeId, nextEpoch);
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
      this.persistBrainEpoch(surviving, nextEpoch);
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
