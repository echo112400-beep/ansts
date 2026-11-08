/**
 * @fileoverview M21 (TASK-101 ~ TASK-104): High-Availability `PartitionedSchedulerRouter`
 * Active-Active Mesh, `AnstsSchedulerCell` Shard Failover & SQLite Rehydration (`RTO < 3s`),
 * Drain Micro-Buffering, and Stale-Route Epoch Self-Healing.
 *
 * Implements:
 * 1. Stateless Active-Active Router Mesh synchronized via L1 Gossip + L2 S3 ETag CAS
 *    (`S3TopologyCasCoordinator` + `VirtualHashSlotRingManager`).
 * 2. Deterministic Slot-to-Shard Routing (`SHA256(routingKey) % 256` -> `idFromName(shardId)`)
 *    with micro-buffer queuing when a slot is in transient `DRAINING` / `SNAPSHOT_TRANSFERRED` state.
 * 3. Stale-Route `409 StaleSlotEpochError` Rejection & Automatic L1 Cache Self-Healing Retry.
 * 4. Shard Crash Failover (`failoverCrashedSchedulerShard`): Atomically reassigns all virtual
 *    slots owned by a crashed shard to a healthy standby shard, increments `slotEpoch`, and
 *    rehydrates the shard's 5D W-DRF shares, queues, leases, and DAG vouchers from SQLite (`RTO < 3s`).
 */

import * as crypto from "node:crypto";
import { StaleFenceError } from "../lease_coordinator";
import { PartitionedSchedulerRouter } from "./horizontal_scaler";
import { VirtualHashSlotRingManager } from "./horizontal_scaler";
import type { SlotLedgerSnapshot } from "./horizontal_scaler";

/** Brain node that owns a shard cell when the caller does not name one explicitly. */
const DEFAULT_BRAIN_NODE_ID = "celld-brain-01";

export interface RoutedScheduleRequest {
  requestId: string;
  tenantId: string;
  taskId: string;
  payload: Record<string, unknown>;
}

export interface ShardDispatchEnvelope {
  requestId: string;
  tenantId: string;
  taskId: string;
  slotIndex: number;
  slotEpoch: number;
  targetShardId: string;
  cellObjectId: string; // deterministic idFromName(targetShardId)
  hopMode: "ZERO_HOP_COLOCATED_ISOLATE" | "ONE_HOP_INTERNAL_MTLS_RPC";
  selfHealedFromStaleCache: boolean;
  bufferedDuringDrain: boolean;
}

export interface SchedulerShardCellInstance {
  shardId: string;
  hostBrainNodeId: string;
  healthy: boolean;
  authoritativeSlotEpochs: Map<number, number>;
  tenantWdrfShares: Map<string, number>;
  queuedTaskIds: Set<string>;
  dagEscrowVoucherIds: Set<string>;
  lastRehydrateRtoMs: number;
}

export class HaPartitionedSchedulerMeshCoordinator {
  public readonly slotRing: VirtualHashSlotRingManager;
  public readonly p2cRouter: PartitionedSchedulerRouter;
  public readonly shardCells = new Map<string, SchedulerShardCellInstance>();
  // Simulated L1 route cache per router node: routerNodeId -> Map<slotIndex, { ownerShardId, generation }>
  public readonly routerL1Caches = new Map<
    string,
    Map<number, { ownerShardId: string; generation: number }>
  >();
  // Micro-buffer for requests arriving while a slot is in transient DRAINING / SNAPSHOT_TRANSFERRED phase
  public readonly drainMicroBuffer = new Map<number, RoutedScheduleRequest[]>();
  // Persisted SQLite / S3 LTX snapshots per shard for RPO=0 crash rehydration
  private readonly persistedShardSnapshots = new Map<
    string,
    {
      tenantWdrfShares: Array<[string, number]>;
      queuedTaskIds: string[];
      dagEscrowVoucherIds: string[];
      savedAtMs: number;
    }
  >();

  constructor(initialShardConfigs: Array<{ shardId: string; hostBrainNodeId: string }>) {
    const shardIds = initialShardConfigs.map((c) => c.shardId);
    this.slotRing = new VirtualHashSlotRingManager(shardIds);
    this.p2cRouter = new PartitionedSchedulerRouter({
      initialShardCount: shardIds.length,
    });

    for (const cfg of initialShardConfigs) {
      this.shardCells.set(cfg.shardId, {
        shardId: cfg.shardId,
        hostBrainNodeId: cfg.hostBrainNodeId,
        healthy: true,
        authoritativeSlotEpochs: new Map(),
        tenantWdrfShares: new Map(),
        queuedTaskIds: new Set(),
        dagEscrowVoucherIds: new Set(),
        lastRehydrateRtoMs: 0,
      });
    }
    this.syncAuthoritativeEpochsToShards();
  }

  /**
   *Deterministic SHA-256 Virtual Slot computation (`0..255`).
   */
  public computeSha256VirtualSlot(routingKey: string): number {
    const digest = crypto.createHash("sha256").update(routingKey, "utf8").digest();
    return digest[0]; // 0..255
  }

  /**
   * Derives deterministic `celld` Durable Object ID (`env.ANSTS_SCHEDULER_CELL.idFromName(shardId)`).
   */
  public idFromName(shardId: string): string {
    const hex = crypto.createHash("sha256").update(`celld:scheduler:${shardId}`, "utf8").digest("hex");
    return `do_sched_${hex.slice(0, 24)}`;
  }

  /**
   * Initializes or refreshes a stateless router node's L1 slot cache from the authoritative L2 ring.
   */
  public syncRouterNodeL1Cache(routerNodeId: string): void {
    const cache = new Map<number, { ownerShardId: string; generation: number }>();
    for (const [slotIdx, entry] of this.slotRing.slots.entries()) {
      cache.set(slotIdx, {
        ownerShardId: entry.ownerShardId,
        generation: entry.generation,
      });
    }
    this.routerL1Caches.set(routerNodeId, cache);
  }

  /**
   * Injects a stale L1 cache entry on a router node (used to verify automatic 409 Stale-Route Self-Healing).
   */
  public simulateStaleL1CacheEntry(
    routerNodeId: string,
    slotIndex: number,
    staleShardId: string,
    staleGeneration: number,
  ): void {
    if (!this.routerL1Caches.has(routerNodeId)) {
      this.syncRouterNodeL1Cache(routerNodeId);
    }
    this.routerL1Caches.get(routerNodeId)!.set(slotIndex, {
      ownerShardId: staleShardId,
      generation: staleGeneration,
    });
  }

  /**
   * Routes a scheduling request from `routerBrainNodeId` to the owning `AnstsSchedulerCell` shard:
   * - If the slot is currently `DRAINING` or `SNAPSHOT_TRANSFERRED`, buffers the request in
   *   `drainMicroBuffer` so zero double-spending or split-brain occurs during scale-out.
   * - If the router node's L1 cache has a stale `slotEpoch`, the target shard rejects with
   *   `409 StaleFenceError`, triggering immediate L1 cache refresh from L2 and transparent retry.
   */
  public routeScheduleRequest(
    routerBrainNodeId: string,
    req: RoutedScheduleRequest,
  ): {
    status: "DISPATCHED" | "BUFFERED_DURING_DRAIN";
    envelope?: ShardDispatchEnvelope;
  } {
    if (!this.routerL1Caches.has(routerBrainNodeId)) {
      this.syncRouterNodeL1Cache(routerBrainNodeId);
    }

    const slotIndex = this.computeSha256VirtualSlot(req.tenantId);
    const authoritativeSlot = this.slotRing.slots.get(slotIndex)!;

    // 1. Check if slot is in transient 3-phase migration (DRAINING or SNAPSHOT_TRANSFERRED)
    if (authoritativeSlot.phase !== "STEADY") {
      const queue = this.drainMicroBuffer.get(slotIndex) ?? [];
      queue.push(req);
      this.drainMicroBuffer.set(slotIndex, queue);
      return { status: "BUFFERED_DURING_DRAIN" };
    }

    // 2. Look up L1 cache on this router node
    const l1Cache = this.routerL1Caches.get(routerBrainNodeId)!;
    const cachedRoute = l1Cache.get(slotIndex) ?? {
      ownerShardId: authoritativeSlot.ownerShardId,
      generation: authoritativeSlot.generation,
    };

    let selfHealedFromStaleCache = false;
    try {
      this.verifyShardEpochGate(cachedRoute.ownerShardId, slotIndex, cachedRoute.generation);
    } catch (err) {
      if (err instanceof StaleFenceError) {
        // Stale-Route Self-Healing: refresh L1 cache from authoritative L2 topology and retry
        this.syncRouterNodeL1Cache(routerBrainNodeId);
        selfHealedFromStaleCache = true;
      } else {
        throw err;
      }
    }

    const refreshedRoute = this.routerL1Caches.get(routerBrainNodeId)!.get(slotIndex)!;
    const targetShard = this.shardCells.get(refreshedRoute.ownerShardId);
    if (!targetShard || !targetShard.healthy) {
      throw new Error(`503 SchedulerShardUnavailable: ${refreshedRoute.ownerShardId}`);
    }

    // Apply state mutation to the target shard & persist to SQLite RPO=0 snapshot
    targetShard.queuedTaskIds.add(req.taskId);
    const prevShare = targetShard.tenantWdrfShares.get(req.tenantId) ?? 0;
    targetShard.tenantWdrfShares.set(req.tenantId, Number((prevShare + 0.05).toFixed(4)));
    this.persistShardSqlSnapshot(targetShard.shardId);

    const hopMode =
      targetShard.hostBrainNodeId === routerBrainNodeId
        ? "ZERO_HOP_COLOCATED_ISOLATE"
        : "ONE_HOP_INTERNAL_MTLS_RPC";

    return {
      status: "DISPATCHED",
      envelope: {
        requestId: req.requestId,
        tenantId: req.tenantId,
        taskId: req.taskId,
        slotIndex,
        slotEpoch: refreshedRoute.generation,
        targetShardId: targetShard.shardId,
        cellObjectId: this.idFromName(targetShard.shardId),
        hopMode,
        selfHealedFromStaleCache,
        bufferedDuringDrain: false,
      },
    };
  }

  /**
   * Executes online 3-phase slot migration (`DRAINING -> SNAPSHOT_TRANSFERRED -> CUTOVER`)
   * when scaling out to a newly added `AnstsSchedulerCell` shard, then automatically drains
   * and dispatches any requests buffered during the migration window.
   *
   * `targetHostBrainNodeIdOrEpoch` accepts the router node id; a plain number is tolerated because the
   * console migration endpoint passes the caller's expected next epoch, which the slot ledger derives itself.
   */
  public migrateSlotOnlineAndFlushBuffer(
    slotIndex: number,
    targetShardId: string,
    targetHostBrainNodeIdOrEpoch: string | number = DEFAULT_BRAIN_NODE_ID,
  ): {
    cutoverEntry: ReturnType<VirtualHashSlotRingManager["commitSlotCutover"]>;
    flushedEnvelopes: ShardDispatchEnvelope[];
    flushedRequests: RoutedScheduleRequest[];
    newEpoch: number;
  } {
    const targetHostBrainNodeId =
      typeof targetHostBrainNodeIdOrEpoch === "string"
        ? targetHostBrainNodeIdOrEpoch
        : DEFAULT_BRAIN_NODE_ID;

    if (!this.shardCells.has(targetShardId)) {
      this.shardCells.set(targetShardId, {
        shardId: targetShardId,
        hostBrainNodeId: targetHostBrainNodeId,
        healthy: true,
        authoritativeSlotEpochs: new Map(),
        tenantWdrfShares: new Map(),
        queuedTaskIds: new Set(),
        dagEscrowVoucherIds: new Set(),
        lastRehydrateRtoMs: 0,
      });
    }

    const currentEntry = this.slotRing.slots.get(slotIndex)!;
    const sourceShard = this.shardCells.get(currentEntry.ownerShardId)!;
    const targetShard = this.shardCells.get(targetShardId)!;

    // Phase 1: DRAINING
    this.slotRing.beginSlotDrain(slotIndex, targetShardId);

    // Phase 2: SNAPSHOT_TRANSFERRED
    const snapshot: SlotLedgerSnapshot = {
      slotIndex,
      fromShardId: sourceShard.shardId,
      toShardId: targetShardId,
      generation: currentEntry.generation + 1,
      tenantWdrfShares: Object.fromEntries(sourceShard.tenantWdrfShares.entries()),
      queuedTaskIds: Array.from(sourceShard.queuedTaskIds),
      dagEscrowVoucherIds: Array.from(sourceShard.dagEscrowVoucherIds),
    };
    this.slotRing.transferSlotSnapshot(snapshot);

    // Import transferred ledger state into target shard
    for (const [tId, share] of Object.entries(snapshot.tenantWdrfShares)) {
      targetShard.tenantWdrfShares.set(tId, share);
    }
    for (const qTask of snapshot.queuedTaskIds) {
      targetShard.queuedTaskIds.add(qTask);
    }
    for (const vId of snapshot.dagEscrowVoucherIds) {
      targetShard.dagEscrowVoucherIds.add(vId);
    }

    // Phase 3: Atomic CUTOVER
    const cutoverEntry = this.slotRing.commitSlotCutover(slotIndex);
    this.syncAuthoritativeEpochsToShards();
    for (const routerId of this.routerL1Caches.keys()) {
      this.syncRouterNodeL1Cache(routerId);
    }

    // Flush any requests buffered during DRAINING / SNAPSHOT_TRANSFERRED
    const buffered = this.drainMicroBuffer.get(slotIndex) ?? [];
    this.drainMicroBuffer.delete(slotIndex);
    const flushedEnvelopes: ShardDispatchEnvelope[] = [];

    for (const req of buffered) {
      const res = this.routeScheduleRequest(targetHostBrainNodeId, req);
      if (res.envelope) {
        res.envelope.bufferedDuringDrain = true;
        flushedEnvelopes.push(res.envelope);
      }
    }

    this.persistShardSqlSnapshot(targetShardId);
    return {
      cutoverEntry,
      flushedEnvelopes,
      flushedRequests: buffered,
      newEpoch: cutoverEntry.generation,
    };
  }

  /**
   * Handles sudden crash of a `SchedulerShard` node:
   * 1. Marks crashed shard unhealthy.
   * 2. Rehydrates the crashed shard's latest SQLite/S3 `RPO=0` snapshot into `standbyShardId` (`RTO < 3000ms`).
   * 3. Atomically cuts over all virtual slots owned by `crashedShardId` to `standbyShardId` and bumps `epoch`.
   */
  public failoverCrashedSchedulerShard(
    crashedShardId: string,
    standbyShardId: string,
    standbyHostBrainNodeId: string,
  ): {
    rehydratedTasksCount: number;
    rehydratedTenantsCount: number;
    migratedSlotsCount: number;
    rehydrateRtoMs: number;
    newGeneration: number;
  } {
    const startHr = process.hrtime.bigint();
    const crashedShard = this.shardCells.get(crashedShardId);
    if (crashedShard) {
      crashedShard.healthy = false;
    }

    if (!this.shardCells.has(standbyShardId)) {
      this.shardCells.set(standbyShardId, {
        shardId: standbyShardId,
        hostBrainNodeId: standbyHostBrainNodeId,
        healthy: true,
        authoritativeSlotEpochs: new Map(),
        tenantWdrfShares: new Map(),
        queuedTaskIds: new Set(),
        dagEscrowVoucherIds: new Set(),
        lastRehydrateRtoMs: 0,
      });
    }
    const standbyShard = this.shardCells.get(standbyShardId)!;

    // Rehydrate persisted SQLite state from crashed shard into standby shard
    const persisted = this.persistedShardSnapshots.get(crashedShardId);
    let rehydratedTasksCount = 0;
    let rehydratedTenantsCount = 0;
    if (persisted) {
      for (const [tenantId, share] of persisted.tenantWdrfShares) {
        standbyShard.tenantWdrfShares.set(tenantId, share);
        rehydratedTenantsCount += 1;
      }
      for (const taskId of persisted.queuedTaskIds) {
        standbyShard.queuedTaskIds.add(taskId);
        rehydratedTasksCount += 1;
      }
      for (const voucherId of persisted.dagEscrowVoucherIds) {
        standbyShard.dagEscrowVoucherIds.add(voucherId);
      }
    }

    // Reassign all virtual slots owned by crashedShardId to standbyShardId and bump generation
    let migratedSlotsCount = 0;
    this.slotRing.generation += 1;
    for (const [slotIdx, entry] of this.slotRing.slots.entries()) {
      if (entry.ownerShardId === crashedShardId) {
        entry.ownerShardId = standbyShardId;
        entry.phase = "STEADY";
        entry.state = entry.phase;
        entry.generation = this.slotRing.generation;
        entry.epoch = entry.generation;
        migratedSlotsCount += 1;
        void slotIdx;
      }
    }

    standbyShard.lastRehydrateRtoMs = Number(Math.max(0.01, Number(process.hrtime.bigint() - startHr) / 1_000_000).toFixed(3));
    this.syncAuthoritativeEpochsToShards();
    this.persistShardSqlSnapshot(standbyShardId);

    return {
      rehydratedTasksCount,
      rehydratedTenantsCount,
      migratedSlotsCount,
      rehydrateRtoMs: standbyShard.lastRehydrateRtoMs,
      newGeneration: this.slotRing.generation,
    };
  }

  public persistShardSqlSnapshot(shardId: string): void {
    const shard = this.shardCells.get(shardId);
    if (!shard) return;
    this.persistedShardSnapshots.set(shardId, {
      tenantWdrfShares: Array.from(shard.tenantWdrfShares.entries()),
      queuedTaskIds: Array.from(shard.queuedTaskIds),
      dagEscrowVoucherIds: Array.from(shard.dagEscrowVoucherIds),
      savedAtMs: Date.now(),
    });
  }

  private verifyShardEpochGate(targetShardId: string, slotIndex: number, requestEpoch: number): void {
    const authoritativeEntry = this.slotRing.slots.get(slotIndex)!;
    if (
      requestEpoch < authoritativeEntry.generation ||
      targetShardId !== authoritativeEntry.ownerShardId
    ) {
      throw new StaleFenceError(
        `409 StaleSlotEpochError: router sent (shard=${targetShardId}, epoch=${requestEpoch}) but slot ${slotIndex} is at (shard=${authoritativeEntry.ownerShardId}, epoch=${authoritativeEntry.generation})`,
      );
    }
  }

  private syncAuthoritativeEpochsToShards(): void {
    for (const [slotIdx, entry] of this.slotRing.slots.entries()) {
      const shard = this.shardCells.get(entry.ownerShardId);
      if (shard) {
        shard.authoritativeSlotEpochs.set(slotIdx, entry.generation);
      }
    }
  }
}
