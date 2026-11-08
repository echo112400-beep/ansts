/**
 * @fileoverview M13 TASK-62: Non-K8s Bare-Metal/VM Systemd & Docker Compose Manual Fleet Scale-Out/Scale-In Controller.
 */
import * as crypto from "node:crypto";

/**
 * Consistent Hash Virtual Slot Ring Manager (`VirtualHashSlotRingManager`) for Non-K8s Bare-Metal Manual Scale-Out/In.
 * Maps keys onto a ring of `totalVirtualSlots` (default 1024) with configurable virtual nodes per physical node.
 */
export type SlotMigrationPhase = "STEADY" | "DRAINING" | "SNAPSHOT_TRANSFERRED";

/**
 * Persisted (S3 LTX / SQLite) ledger snapshot handed over during the 3-phase online slot migration.
 */
export interface SlotLedgerSnapshot {
  slotIndex: number;
  fromShardId: string;
  toShardId: string;
  generation: number;
  tenantWdrfShares: Record<string, number>;
  queuedTaskIds: string[];
  dagEscrowVoucherIds: string[];
}

export interface VirtualSlotLedgerEntry {
  slotIndex: number;
  ownerShardId: string;
  generation: number;
  phase: SlotMigrationPhase;
  targetShardId: string | null;
  lastSnapshot: SlotLedgerSnapshot | null;
  /** Alias of `phase`, kept in sync by the ledger mutators (console 256-slot heatmap reads it). */
  state?: SlotMigrationPhase;
  /** Alias of `generation`, kept in sync by the ledger mutators (console 256-slot heatmap reads it). */
  epoch?: number;
}

export class VirtualHashSlotRingManager {
  public readonly totalVirtualSlots: number;
  public readonly vnodesPerNode: number;
  private readonly ring: Array<{ tokenHash: number; nodeId: string }> = [];
  private readonly activeNodeIds: Set<string> = new Set();
  /** Authoritative L2 slot ledger: `slotIndex -> owner/generation/migration phase` (256 slots for scheduler meshes). */
  public readonly slots: Map<number, VirtualSlotLedgerEntry> = new Map();
  /** Monotonic topology generation, bumped on every slot cutover / failover reassignment. */
  public generation = 1;

  constructor(totalVirtualSlots?: number, vnodesPerNode?: number);
  constructor(shardIds: string[]);
  constructor(totalVirtualSlotsOrShardIds: number | string[] = 1024, vnodesPerNode = 16) {
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
  private initializeSlotLedger(shardIds: string[]): void {
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
        epoch: this.generation,
      });
    }
  }

  private requireSlot(slotIndex: number): VirtualSlotLedgerEntry {
    const entry = this.slots.get(slotIndex);
    if (!entry) {
      throw new Error(`Slot ${slotIndex} not present in this slot ledger`);
    }
    return entry;
  }

  /** Deterministic `SHA256(tenantId) % 256` slot lookup used by the scheduler mesh router. */
  public resolveOwnerShardForTenant(tenantId: string): VirtualSlotLedgerEntry {
    return this.resolveOwnerShardForKey(tenantId);
  }

  public resolveOwnerShardForKey(routingKey: string): VirtualSlotLedgerEntry {
    let ledgerSize = this.slots.size;
    if (ledgerSize === 0) {
      throw new Error("Slot ledger is empty; construct the manager with initial shard ids");
    }
    const digest = crypto.createHash("sha256").update(routingKey, "utf-8").digest();
    return this.requireSlot(digest[0] % ledgerSize);
  }

  /** Phase 1 (`STEADY -> DRAINING`): new requests for this slot are micro-buffered by the router. */
  public beginSlotDrain(slotIndex: number, targetShardId: string): VirtualSlotLedgerEntry {
    const entry = this.requireSlot(slotIndex);
    entry.targetShardId = targetShardId;
    entry.phase = "DRAINING";
    entry.state = entry.phase;
    entry.epoch = entry.generation;
    return entry;
  }

  /** Phase 2 (`DRAINING -> SNAPSHOT_TRANSFERRED`): the ledger snapshot has been copied to the target shard. */
  public transferSlotSnapshot(snapshot: SlotLedgerSnapshot): VirtualSlotLedgerEntry {
    const entry = this.requireSlot(snapshot.slotIndex);
    entry.targetShardId = snapshot.toShardId;
    entry.lastSnapshot = snapshot;
    entry.phase = "SNAPSHOT_TRANSFERRED";
    entry.state = entry.phase;
    entry.epoch = entry.generation;
    return entry;
  }

  /** Phase 3 (`-> STEADY`): atomically cuts the slot over to its target shard and bumps the generation. */
  public commitSlotCutover(slotIndex: number): VirtualSlotLedgerEntry {
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

  private static hashString(input: string): number {
    return crypto.createHash("sha256").update(input, "utf-8").digest().readUInt32BE(0);
  }

  public addNode(nodeId: string): void {
    if (this.activeNodeIds.has(nodeId)) return;
    this.activeNodeIds.add(nodeId);
    for (let v = 0; v < this.vnodesPerNode; v++) {
      const tokenHash = VirtualHashSlotRingManager.hashString(`${nodeId}#vnode-${v}`);
      this.ring.push({ tokenHash, nodeId });
    }
    this.ring.sort((a, b) => a.tokenHash - b.tokenHash);
  }

  public removeNode(nodeId: string): void {
    if (!this.activeNodeIds.has(nodeId)) return;
    this.activeNodeIds.delete(nodeId);
    for (let i = this.ring.length - 1; i >= 0; i--) {
      if (this.ring[i].nodeId === nodeId) {
        this.ring.splice(i, 1);
      }
    }
  }

  public getOwnerNodeForSlot(slotIndex: number): string | null {
    if (this.ring.length === 0) return null;
    const slotHash = VirtualHashSlotRingManager.hashString(`slot:${slotIndex % this.totalVirtualSlots}`);
    let low = 0;
    let high = this.ring.length - 1;
    while (low <= high) {
      const mid = (low + high) >> 1;
      if (this.ring[mid].tokenHash >= slotHash) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    const chosenIdx = low < this.ring.length ? low : 0;
    return this.ring[chosenIdx].nodeId;
  }

  public getOwnerNodeForKey(key: string): { slotIndex: number; nodeId: string | null } {
    const slotIndex = VirtualHashSlotRingManager.hashString(key) % this.totalVirtualSlots;
    return {
      slotIndex,
      nodeId: this.getOwnerNodeForSlot(slotIndex),
    };
  }

  public computeSlotDistribution(): Map<string, number> {
    const counts = new Map<string, number>();
    for (const n of this.activeNodeIds) counts.set(n, 0);
    if (this.ring.length === 0) return counts;
    for (let s = 0; s < this.totalVirtualSlots; s++) {
      const owner = this.getOwnerNodeForSlot(s)!;
      counts.set(owner, (counts.get(owner) ?? 0) + 1);
    }
    return counts;
  }

  /** Counts how many ledger slots each shard currently owns (slot-ledger view). */
  public computeLedgerSlotDistribution(): Map<string, number> {
    const counts = new Map<string, number>();
    for (const entry of this.slots.values()) {
      counts.set(entry.ownerShardId, (counts.get(entry.ownerShardId) ?? 0) + 1);
    }
    return counts;
  }
}

export type FleetNodeState = "ACTIVE" | "CORDONED" | "DRAINING" | "OFFLINE";

export interface BareMetalFleetNode {
  nodeId: string;
  ipAddress: string;
  role: "control-plane" | "worker" | "storage";
  state: FleetNodeState;
  runningCells: string[];
  systemdUnit: string;
}

export class ManualFleetScaleController {
  private readonly nodes: Map<string, BareMetalFleetNode> = new Map();
  public readonly slotRing: VirtualHashSlotRingManager = new VirtualHashSlotRingManager(1024, 16);

  public registerBareMetalNode(node: Omit<BareMetalFleetNode, "state" | "runningCells"> & { runningCells?: string[] }): BareMetalFleetNode {
    const full: BareMetalFleetNode = {
      ...node,
      state: "ACTIVE",
      runningCells: node.runningCells ? [...node.runningCells] : [],
    };
    this.nodes.set(node.nodeId, full);
    this.slotRing.addNode(node.nodeId);
    return full;
  }

  public scaleOutNode(nodeId: string, ipAddress: string, role: "control-plane" | "worker" | "storage" = "worker"): {
    node: BareMetalFleetNode;
    rebalancedCells: string[];
  } {
    const newNode = this.registerBareMetalNode({
      nodeId,
      ipAddress,
      role,
      systemdUnit: role === "control-plane" ? "celld.service" : "ansts-node-daemon.service",
    });
    const rebalancedCells: string[] = [];
    for (const existing of this.nodes.values()) {
      if (existing.nodeId !== nodeId && existing.state === "ACTIVE" && existing.runningCells.length > 2) {
        const moved = existing.runningCells.pop()!;
        newNode.runningCells.push(moved);
        rebalancedCells.push(moved);
      }
    }
    return { node: newNode, rebalancedCells };
  }

  public cordonAndDrainNode(nodeId: string): {
    drainedCells: Array<{ cellId: string; targetNodeId: string }>;
    finalState: FleetNodeState;
  } {
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
    const drainedCells: Array<{ cellId: string; targetNodeId: string }> = [];
    let idx = 0;
    while (source.runningCells.length > 0) {
      const cellId = source.runningCells.shift()!;
      const target = activeTargets[idx % activeTargets.length];
      target.runningCells.push(cellId);
      drainedCells.push({ cellId, targetNodeId: target.nodeId });
      idx += 1;
    }
    source.state = "OFFLINE";
    this.slotRing.removeNode(nodeId);
    return { drainedCells, finalState: source.state };
  }

  public listNodes(): BareMetalFleetNode[] {
    return Array.from(this.nodes.values());
  }
}


/**
 * M16 TASK-78: Real-Time `GET /state` & `fleet/capacity-v1.json` Autoscaler Evaluator
 * and Paced Drain-Token Shutdown Coordinator (`CELLD_SHUTDOWN_TOTAL_MS=40000`, `handoff=preserve`).
 */
export interface CelldInternalStateSample {
  nodeId: string;
  capacity_waiting: number;
  activation_waiting: number;
  restoring: number;
  owned_cells: number;
  resident_cells: number;
  pressured: boolean;
  memory_headroom: boolean;
  shed_cells: number;
  live_empty_isolates_sec?: number;
  retiring_isolates_sec?: number;
}

export class CelldStateAwareFleetAutoscaler {
  private activeDrainTokenHolder: string | null = null;
  public readonly shutdownTotalMs: number;
  public readonly drainTokenWaitMs: number;
  public readonly handoffNoProgressMs: number;

  constructor(shutdownTotalMs = 40_000) {
    this.shutdownTotalMs = shutdownTotalMs;
    this.drainTokenWaitMs = Math.floor((shutdownTotalMs * 3) / 4);
    this.handoffNoProgressMs = Math.floor((shutdownTotalMs * 5) / 8);
  }

  public evaluateFleetStateSamples(samples: CelldInternalStateSample[]): {
    decision: "ADD_NODE" | "SCALE_DOWN_SAFE" | "HOLD_STEADY";
    rollingUpgradeNextNodeAllowed: boolean;
    isolateMaintenanceWarning: boolean;
    reason: string;
  } {
    const anyRestoring = samples.some((s) => s.restoring > 0);
    const isolateMaintenanceWarning = samples.some(
      (s) => (s.live_empty_isolates_sec ?? 0) > 30 || (s.retiring_isolates_sec ?? 0) > 30
    );

    if (samples.some((s) => s.capacity_waiting > 0 || s.pressured)) {
      return {
        decision: "ADD_NODE",
        rollingUpgradeNextNodeAllowed: !anyRestoring,
        isolateMaintenanceWarning,
        reason: "Positive capacity_waiting or pressured node lease detected in /state",
      };
    }

    const allHaveHeadroomAndSmallBacklog =
      samples.length > 1 && samples.every((s) => s.memory_headroom && s.restoring <= 1);
    if (allHaveHeadroomAndSmallBacklog) {
      return {
        decision: "SCALE_DOWN_SAFE",
        rollingUpgradeNextNodeAllowed: !anyRestoring,
        isolateMaintenanceWarning,
        reason: "All survivor nodes report memory_headroom=true and low restoring backlog",
      };
    }

    return {
      decision: "HOLD_STEADY",
      rollingUpgradeNextNodeAllowed: !anyRestoring,
      isolateMaintenanceWarning,
      reason: "Fleet balanced within normal residency and memory watermarks",
    };
  }

  public initiateNodeShutdown(params: {
    nodeId: string;
    handoffMode?: "release_to_peers" | "preserve";
    systemdTimeoutStopSec: number;
  }): {
    claimedDrainToken: boolean;
    ownershipPreservedForSameNodeRestart: boolean;
    maxConcurrentReleases: number;
    drainTokenWaitMs: number;
    handoffNoProgressMs: number;
  } {
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
      handoffNoProgressMs: this.handoffNoProgressMs,
    };
  }

  public releaseDrainToken(nodeId: string): void {
    if (this.activeDrainTokenHolder === nodeId) {
      this.activeDrainTokenHolder = null;
    }
  }
}
