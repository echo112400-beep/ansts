"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.HaSchedulerShardCoordinator = void 0;



var _manual_scale_controller = require("../cluster/manual_scale_controller.js");
var _topology_cas_coordinator = require("../cluster/topology_cas_coordinator.js"); /**
 * @fileoverview M25, M55-2, M64-4 & M69-5: High-Availability 256-Slot Scheduler Shard Coordinator with Epoch Lease Fencing,
 * SQLite-Backed Topology CAS & Per-Brain `shardEpochs` Persistence, Automatic Unhealthy Brain Shard Eviction & Last-Brain Survival Guard.
 */class HaSchedulerShardCoordinator {
  ring;
  topologyCas;
  shardEpochs = new Map();
  activeBrains = new Map();
  sql;

  constructor(totalSlots = 256, vnodes = 8, sql) {
    this.ring = new _manual_scale_controller.VirtualHashSlotRingManager(totalSlots, vnodes);
    this.topologyCas = new _topology_cas_coordinator.TopologyCasCoordinator(sql);
    this.activeBrains = new Set();
    this.sql = sql;
    if (this.sql) {
      this.sql.exec(
      "CREATE TABLE IF NOT EXISTS v25_shard_epochs (brain_node_id TEXT PRIMARY KEY, epoch INTEGER)");

      for (const r of this.sql.exec("SELECT * FROM v25_shard_epochs").toArray()) {
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

  persistBrainEpoch(brainNodeId, epoch) {
    if (!this.sql) return;
    this.sql.exec(
    "INSERT OR REPLACE INTO v25_shard_epochs (brain_node_id, epoch) VALUES (?, ?)",
    brainNodeId,
    epoch);

  }

  registerBrainShardNode(brainNodeId) {
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
  evictUnhealthyBrainShardNode(failedBrainNodeId)





  {
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
        lastBrainSurvivalGuardTriggered: true
      };
    }

    this.ring.removeNode(failedBrainNodeId);
    this.activeBrains.delete(failedBrainNodeId);
    const bumpedEpochs = {};
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