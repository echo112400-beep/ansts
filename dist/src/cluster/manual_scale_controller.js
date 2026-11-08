"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.VirtualHashSlotRingManager = exports.ManualFleetScaleController = exports.CelldStateAwareFleetAutoscaler = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M13 TASK-62: Non-K8s Bare-Metal/VM Systemd & Docker Compose Manual Fleet Scale-Out/Scale-In Controller.
 */ /**
 * Consistent Hash Virtual Slot Ring Manager (`VirtualHashSlotRingManager`) for Non-K8s Bare-Metal Manual Scale-Out/In.
 * Maps keys onto a ring of `totalVirtualSlots` (default 1024) with configurable virtual nodes per physical node.
 */


/**
 * Persisted (S3 LTX / SQLite) ledger snapshot handed over during the 3-phase online slot migration.
 */























class VirtualHashSlotRingManager {
  totalVirtualSlots;
  vnodesPerNode;
  ring = [];
  activeNodeIds = new Set();
  /** Authoritative L2 slot ledger: `slotIndex -> owner/generation/migration phase` (256 slots for scheduler meshes). */
  slots = new Map();
  /** Monotonic topology generation, bumped on every slot cutover / failover reassignment. */
  generation = 1;



  constructor(totalVirtualSlotsOrShardIds = 1024, vnodesPerNode = 16) {
    if (Array.isArray(totalVirtualSlotsOrShardIds)) {
      this.totalVirtualSlots = 256;
      this.vnodesPerNode = vnodesPerNode;
      this.initializeSlotLedger(totalVirtualSlotsOrShardIds);
      return;
    }
    this.totalVirtualSlots = totalVirtualSlotsOrShardIds;
    this.vnodesPerNode = vnodesPerNode;
  }

  /**
   * Seeds the 256-slot ledger by round-robin distributing slots over the initial scheduler shards.
   * Each shard is also registered as a consistent-hash ring node so `getOwnerNodeForKey` keeps working.
   */
  initializeSlotLedger(shardIds) {
    for (const shardId of shardIds) {
      this.addNode(shardId);
    }
    if (shardIds.length === 0) return;
    for (let slotIndex = 0; slotIndex < this.totalVirtualSlots; slotIndex++) {
      this.slots.set(slotIndex, {
        slotIndex,
        ownerShardId: shardIds[slotIndex % shardIds.length],
        generation: this.generation,
        phase: "STEADY",
        targetShardId: null,
        lastSnapshot: null,
        state: "STEADY",
        epoch: this.generation
      });
    }
  }

  requireSlot(slotIndex) {
    const entry = this.slots.get(slotIndex);
    if (!entry) {
      throw new Error(`Slot ${slotIndex} not present in this slot ledger`);
    }
    return entry;
  }

  /** Deterministic `SHA256(tenantId) % 256` slot lookup used by the scheduler mesh router. */
  resolveOwnerShardForTenant(tenantId) {
    return this.resolveOwnerShardForKey(tenantId);
  }

  resolveOwnerShardForKey(routingKey) {
    let ledgerSize = this.slots.size;
    if (ledgerSize === 0) {
      throw new Error("Slot ledger is empty; construct the manager with initial shard ids");
    }
    const digest = crypto.createHash("sha256").update(routingKey, "utf-8").digest();
    return this.requireSlot(digest[0] % ledgerSize);
  }

  /** Phase 1 (`STEADY -> DRAINING`): new requests for this slot are micro-buffered by the router. */
  beginSlotDrain(slotIndex, targetShardId) {
    const entry = this.requireSlot(slotIndex);
    entry.targetShardId = targetShardId;
    entry.phase = "DRAINING";
    entry.state = entry.phase;
    entry.epoch = entry.generation;
    return entry;
  }

  /** Phase 2 (`DRAINING -> SNAPSHOT_TRANSFERRED`): the ledger snapshot has been copied to the target shard. */
  transferSlotSnapshot(snapshot) {
    const entry = this.requireSlot(snapshot.slotIndex);
    entry.targetShardId = snapshot.toShardId;
    entry.lastSnapshot = snapshot;
    entry.phase = "SNAPSHOT_TRANSFERRED";
    entry.state = entry.phase;
    entry.epoch = entry.generation;
    return entry;
  }

  /** Phase 3 (`-> STEADY`): atomically cuts the slot over to its target shard and bumps the generation. */
  commitSlotCutover(slotIndex) {
    const entry = this.requireSlot(slotIndex);
    const nextOwner = entry.targetShardId ?? entry.lastSnapshot?.toShardId;
    if (!nextOwner) {
      throw new Error(`Slot ${slotIndex} has no cutover target; call beginSlotDrain first`);
    }
    entry.ownerShardId = nextOwner;
    entry.phase = "STEADY";
    entry.state = entry.phase;
    entry.targetShardId = null;
    entry.lastSnapshot = null;
    this.generation += 1;
    entry.generation = this.generation;
    entry.epoch = entry.generation;
    return entry;
  }

  static hashString(input) {
    return crypto.createHash("sha256").update(input, "utf-8").digest().readUInt32BE(0);
  }

  addNode(nodeId) {
    if (this.activeNodeIds.has(nodeId)) return;
    this.activeNodeIds.add(nodeId);
    for (let v = 0; v < this.vnodesPerNode; v++) {
      const tokenHash = VirtualHashSlotRingManager.hashString(`${nodeId}#vnode-${v}`);
      this.ring.push({ tokenHash, nodeId });
    }
    this.ring.sort((a, b) => a.tokenHash - b.tokenHash);
  }

  removeNode(nodeId) {
    if (!this.activeNodeIds.has(nodeId)) return;
    this.activeNodeIds.delete(nodeId);
    for (let i = this.ring.length - 1; i >= 0; i--) {
      if (this.ring[i].nodeId === nodeId) {
        this.ring.splice(i, 1);
      }
    }
  }

  getOwnerNodeForSlot(slotIndex) {
    if (this.ring.length === 0) return null;
    const slotHash = VirtualHashSlotRingManager.hashString(`slot:${slotIndex % this.totalVirtualSlots}`);
    let low = 0;
    let high = this.ring.length - 1;
    while (low <= high) {
      const mid = low + high >> 1;
      if (this.ring[mid].tokenHash >= slotHash) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    const chosenIdx = low < this.ring.length ? low : 0;
    return this.ring[chosenIdx].nodeId;
  }

  getOwnerNodeForKey(key) {
    const slotIndex = VirtualHashSlotRingManager.hashString(key) % this.totalVirtualSlots;
    return {
      slotIndex,
      nodeId: this.getOwnerNodeForSlot(slotIndex)
    };
  }

  computeSlotDistribution() {
    const counts = new Map();
    for (const n of this.activeNodeIds) counts.set(n, 0);
    if (this.ring.length === 0) return counts;
    for (let s = 0; s < this.totalVirtualSlots; s++) {
      const owner = this.getOwnerNodeForSlot(s);
      counts.set(owner, (counts.get(owner) ?? 0) + 1);
    }
    return counts;
  }

  /** Counts how many ledger slots each shard currently owns (slot-ledger view). */
  computeLedgerSlotDistribution() {
    const counts = new Map();
    for (const entry of this.slots.values()) {
      counts.set(entry.ownerShardId, (counts.get(entry.ownerShardId) ?? 0) + 1);
    }
    return counts;
  }
}exports.VirtualHashSlotRingManager = VirtualHashSlotRingManager;












class ManualFleetScaleController {
  nodes = new Map();
  slotRing = new VirtualHashSlotRingManager(1024, 16);

  registerBareMetalNode(node) {
    const full = {
      ...node,
      state: "ACTIVE",
      runningCells: node.runningCells ? [...node.runningCells] : []
    };
    this.nodes.set(node.nodeId, full);
    this.slotRing.addNode(node.nodeId);
    return full;
  }

  scaleOutNode(nodeId, ipAddress, role = "worker")


  {
    const newNode = this.registerBareMetalNode({
      nodeId,
      ipAddress,
      role,
      systemdUnit: role === "control-plane" ? "celld.service" : "ansts-node-daemon.service"
    });
    const rebalancedCells = [];
    for (const existing of this.nodes.values()) {
      if (existing.nodeId !== nodeId && existing.state === "ACTIVE" && existing.runningCells.length > 2) {
        const moved = existing.runningCells.pop();
        newNode.runningCells.push(moved);
        rebalancedCells.push(moved);
      }
    }
    return { node: newNode, rebalancedCells };
  }

  cordonAndDrainNode(nodeId)


  {
    const source = this.nodes.get(nodeId);
    if (!source) {
      throw new Error(`Node '${nodeId}' not found in ManualFleetScaleController`);
    }
    source.state = "CORDONED";
    const activeTargets = Array.from(this.nodes.values()).filter(
      (n) => n.nodeId !== nodeId && n.state === "ACTIVE" && n.role === source.role
    );
    if (activeTargets.length === 0 && source.runningCells.length > 0) {
      throw new Error(`Cannot drain node '${nodeId}': no other ACTIVE nodes with role '${source.role}'`);
    }

    source.state = "DRAINING";
    const drainedCells = [];
    let idx = 0;
    while (source.runningCells.length > 0) {
      const cellId = source.runningCells.shift();
      const target = activeTargets[idx % activeTargets.length];
      target.runningCells.push(cellId);
      drainedCells.push({ cellId, targetNodeId: target.nodeId });
      idx += 1;
    }
    source.state = "OFFLINE";
    this.slotRing.removeNode(nodeId);
    return { drainedCells, finalState: source.state };
  }

  listNodes() {
    return Array.from(this.nodes.values());
  }
}


/**
 * M16 TASK-78: Real-Time `GET /state` & `fleet/capacity-v1.json` Autoscaler Evaluator
 * and Paced Drain-Token Shutdown Coordinator (`CELLD_SHUTDOWN_TOTAL_MS=40000`, `handoff=preserve`).
 */exports.ManualFleetScaleController = ManualFleetScaleController;














class CelldStateAwareFleetAutoscaler {
  activeDrainTokenHolder = null;
  shutdownTotalMs;
  drainTokenWaitMs;
  handoffNoProgressMs;

  constructor(shutdownTotalMs = 40_000) {
    this.shutdownTotalMs = shutdownTotalMs;
    this.drainTokenWaitMs = Math.floor(shutdownTotalMs * 3 / 4);
    this.handoffNoProgressMs = Math.floor(shutdownTotalMs * 5 / 8);
  }

  evaluateFleetStateSamples(samples)




  {
    const anyRestoring = samples.some((s) => s.restoring > 0);
    const isolateMaintenanceWarning = samples.some(
      (s) => (s.live_empty_isolates_sec ?? 0) > 30 || (s.retiring_isolates_sec ?? 0) > 30
    );

    if (samples.some((s) => s.capacity_waiting > 0 || s.pressured)) {
      return {
        decision: "ADD_NODE",
        rollingUpgradeNextNodeAllowed: !anyRestoring,
        isolateMaintenanceWarning,
        reason: "Positive capacity_waiting or pressured node lease detected in /state"
      };
    }

    const allHaveHeadroomAndSmallBacklog =
    samples.length > 1 && samples.every((s) => s.memory_headroom && s.restoring <= 1);
    if (allHaveHeadroomAndSmallBacklog) {
      return {
        decision: "SCALE_DOWN_SAFE",
        rollingUpgradeNextNodeAllowed: !anyRestoring,
        isolateMaintenanceWarning,
        reason: "All survivor nodes report memory_headroom=true and low restoring backlog"
      };
    }

    return {
      decision: "HOLD_STEADY",
      rollingUpgradeNextNodeAllowed: !anyRestoring,
      isolateMaintenanceWarning,
      reason: "Fleet balanced within normal residency and memory watermarks"
    };
  }

  initiateNodeShutdown(params)









  {
    if (params.systemdTimeoutStopSec <= this.shutdownTotalMs / 1000) {
      throw new Error(
        `systemd TimeoutStopSec (${params.systemdTimeoutStopSec}s) must exceed CELLD_SHUTDOWN_TOTAL_MS (${this.shutdownTotalMs / 1000}s)`
      );
    }
    const claimedDrainToken = this.activeDrainTokenHolder === null || this.activeDrainTokenHolder === params.nodeId;
    if (claimedDrainToken) {
      this.activeDrainTokenHolder = params.nodeId;
    }
    return {
      claimedDrainToken,
      ownershipPreservedForSameNodeRestart: params.handoffMode === "preserve",
      maxConcurrentReleases: 128,
      drainTokenWaitMs: this.drainTokenWaitMs,
      handoffNoProgressMs: this.handoffNoProgressMs
    };
  }

  releaseDrainToken(nodeId) {
    if (this.activeDrainTokenHolder === nodeId) {
      this.activeDrainTokenHolder = null;
    }
  }
}exports.CelldStateAwareFleetAutoscaler = CelldStateAwareFleetAutoscaler;