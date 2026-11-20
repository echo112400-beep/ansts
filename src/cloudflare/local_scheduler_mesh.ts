import { createHash } from "node:crypto";
import { InMemoryCellSqlStorage, type SqlStorageLike } from "./env.js";

interface ShardConfig {
  shardId: string;
  hostBrainNodeId: string;
}

interface MeshShard extends ShardConfig {
  healthy: boolean;
  queuedTaskIds: Set<string>;
  lastRehydrateRtoMs: number;
}

interface MeshSlot {
  ownerShardId: string;
  epoch: number;
  state: "STEADY" | "DRAINING" | "SNAPSHOT_TRANSFERRED";
  targetShardId?: string;
}

interface RoutedScheduleRequest {
  requestId: string;
  tenantId: string;
  taskId: string;
  payload: Record<string, unknown>;
}

interface ShardDispatchEnvelope {
  requestId: string;
  tenantId: string;
  taskId: string;
  slotIndex: number;
  slotEpoch: number;
  targetShardId: string;
  cellObjectId: string;
  hopMode: "ZERO_HOP_COLOCATED_ISOLATE" | "ONE_HOP_INTERNAL_MTLS_RPC";
  selfHealedFromStaleCache: boolean;
  bufferedDuringDrain: boolean;
}

const SLOT_COUNT = 256;

class LocalSlotLedger {
  public readonly slots = new Map<number, MeshSlot>();
  public generation = 1;

  constructor(configs: readonly ShardConfig[]) {
    if (!configs.length || new Set(configs.map(config => config.shardId)).size !== configs.length) {
      throw new Error("INVALID_LOCAL_SHARD_ROSTER");
    }
    for (let slot = 0; slot < SLOT_COUNT; slot++) {
      this.slots.set(slot, { ownerShardId: configs[slot % configs.length].shardId, epoch: 1, state: "STEADY" });
    }
  }

  public getSlot(slotIndex: number): MeshSlot {
    const slot = this.slots.get(slotIndex);
    if (!slot) throw new RangeError("INVALID_LOCAL_SHARD_SLOT");
    return slot;
  }

  public beginSlotDrain(slotIndex: number, targetShardId: string): void {
    const slot = this.getSlot(slotIndex);
    if (slot.ownerShardId === targetShardId || (slot.targetShardId && slot.targetShardId !== targetShardId)) {
      throw new Error("INVALID_LOCAL_SLOT_MIGRATION");
    }
    slot.state = "DRAINING";
    slot.targetShardId = targetShardId;
  }
}

export class LocalSchedulerMesh {
  public readonly shardCells = new Map<string, MeshShard>();
  public readonly slotRing: LocalSlotLedger;
  public readonly drainMicroBuffer = new Map<number, RoutedScheduleRequest[]>();
  private readonly routeCache = new Map<string, Map<number, Pick<MeshSlot, "ownerShardId" | "epoch">>>();

  constructor(configs: readonly ShardConfig[], private readonly sql: SqlStorageLike = new InMemoryCellSqlStorage()) {
    this.slotRing = new LocalSlotLedger(configs);
    for (const config of configs) this.createShard(config.shardId, config.hostBrainNodeId);
    this.sql.exec("CREATE TABLE IF NOT EXISTS local_mesh_jobs (id TEXT PRIMARY KEY, slot_index INTEGER NOT NULL, payload_json TEXT NOT NULL)");
  }

  private createShard(shardId: string, hostBrainNodeId: string): MeshShard {
    if (!shardId || !hostBrainNodeId) throw new Error("INVALID_LOCAL_SHARD");
    const shard = { shardId, hostBrainNodeId, healthy: true, queuedTaskIds: new Set<string>(), lastRehydrateRtoMs: 0 };
    this.shardCells.set(shardId, shard);
    return shard;
  }

  public computeSha256VirtualSlot(routingKey: string): number {
    return createHash("sha256").update(routingKey).digest()[0];
  }

  public syncRouterNodeL1Cache(routerId: string): void {
    this.routeCache.set(routerId, new Map([...this.slotRing.slots].map(([index, { ownerShardId, epoch }]) => [
      index, { ownerShardId, epoch },
    ])));
  }

  private jobsForSlot(slotIndex: number): RoutedScheduleRequest[] {
    return this.sql.exec<{ payload_json: string }>("SELECT payload_json FROM local_mesh_jobs WHERE slot_index = ?", slotIndex)
      .toArray().map(row => JSON.parse(row.payload_json) as RoutedScheduleRequest);
  }

  private rebuildQueues(): void {
    for (const shard of this.shardCells.values()) shard.queuedTaskIds.clear();
    for (const [index, slot] of this.slotRing.slots) {
      const shard = this.shardCells.get(slot.ownerShardId);
      if (shard?.healthy) for (const job of this.jobsForSlot(index)) shard.queuedTaskIds.add(job.taskId);
    }
  }

  public routeScheduleRequest(routerId: string, request: RoutedScheduleRequest): {
    status: "DISPATCHED" | "BUFFERED_DURING_DRAIN";
    envelope?: ShardDispatchEnvelope;
  } {
    const slotIndex = this.computeSha256VirtualSlot(request.tenantId);
    const slot = this.slotRing.getSlot(slotIndex);
    const requestKey = JSON.stringify([request.tenantId, request.requestId]);
    if (slot.state !== "STEADY") {
      const pending = this.drainMicroBuffer.get(slotIndex) ?? [];
      const existing = pending.find(job => job.requestId === request.requestId && job.tenantId === request.tenantId);
      if (existing && JSON.stringify(existing) !== JSON.stringify(request)) throw new Error("LOCAL_MESH_REQUEST_CONFLICT");
      if (!existing) {
        if (pending.length >= 256) throw new Error("LOCAL_MESH_DRAIN_BUFFER_FULL");
        pending.push(structuredClone(request));
        this.drainMicroBuffer.set(slotIndex, pending);
      }
      return { status: "BUFFERED_DURING_DRAIN" };
    }
    const shard = this.shardCells.get(slot.ownerShardId);
    if (!shard?.healthy) throw new Error("LOCAL_SHARD_UNAVAILABLE");
    const cached = this.routeCache.get(routerId)?.get(slotIndex);
    const selfHealedFromStaleCache = Boolean(cached && (cached.epoch !== slot.epoch || cached.ownerShardId !== slot.ownerShardId));
    if (!cached || selfHealedFromStaleCache) this.syncRouterNodeL1Cache(routerId);
    const serialized = JSON.stringify(request);
    const existing = this.sql.exec<{ payload_json: string }>("SELECT payload_json FROM local_mesh_jobs WHERE id = ?", requestKey).one();
    if (existing && existing.payload_json !== serialized) throw new Error("LOCAL_MESH_REQUEST_CONFLICT");
    if (!existing) this.sql.exec("INSERT INTO local_mesh_jobs (id, slot_index, payload_json) VALUES (?, ?, ?)", requestKey, slotIndex, serialized);
    shard.queuedTaskIds.add(request.taskId);
    return { status: "DISPATCHED", envelope: {
      requestId: request.requestId, tenantId: request.tenantId, taskId: request.taskId,
      slotIndex, slotEpoch: slot.epoch, targetShardId: shard.shardId,
      cellObjectId: "do_sched_" + createHash("sha256").update(shard.shardId).digest("hex").slice(0, 24),
      hopMode: routerId === shard.hostBrainNodeId ? "ZERO_HOP_COLOCATED_ISOLATE" : "ONE_HOP_INTERNAL_MTLS_RPC",
      selfHealedFromStaleCache, bufferedDuringDrain: false,
    } };
  }

  public migrateSlotOnlineAndFlushBuffer(slotIndex: number, targetShardId: string, targetHost?: string) {
    const target = this.shardCells.get(targetShardId) ?? (targetHost ? this.createShard(targetShardId, targetHost) : undefined);
    if (!target?.healthy) throw new Error("LOCAL_MESH_TARGET_UNAVAILABLE");
    this.slotRing.beginSlotDrain(slotIndex, targetShardId);
    const slot = this.slotRing.getSlot(slotIndex);
    slot.state = "SNAPSHOT_TRANSFERRED";
    slot.ownerShardId = targetShardId;
    slot.epoch = ++this.slotRing.generation;
    slot.state = "STEADY";
    delete slot.targetShardId;
    this.rebuildQueues();
    const flushedRequests = this.drainMicroBuffer.get(slotIndex) ?? [];
    const flushedEnvelopes: ShardDispatchEnvelope[] = [];
    while (flushedRequests.length) {
      const request = flushedRequests[0];
      const { envelope } = this.routeScheduleRequest(target.hostBrainNodeId, request);
      if (!envelope) throw new Error("LOCAL_MESH_BUFFER_FLUSH_FAILED");
      flushedEnvelopes.push({ ...envelope, bufferedDuringDrain: true });
      flushedRequests.shift();
    }
    this.drainMicroBuffer.delete(slotIndex);
    return { cutoverEntry: { ...slot }, flushedEnvelopes, newEpoch: slot.epoch };
  }

  public failoverCrashedSchedulerShard(crashedShardId: string, standbyShardId: string, standbyHost: string) {
    const crashed = this.shardCells.get(crashedShardId);
    if (!crashed || crashedShardId === standbyShardId) throw new Error("INVALID_LOCAL_SHARD_FAILOVER");
    const started = performance.now();
    const affected = [...this.slotRing.slots].filter(([, slot]) => slot.ownerShardId === crashedShardId);
    if (affected.some(([index]) => this.drainMicroBuffer.get(index)?.length)) throw new Error("LOCAL_MESH_DRAIN_IN_PROGRESS");
    const restored = affected.flatMap(([index]) => this.jobsForSlot(index));
    const standby = this.shardCells.get(standbyShardId) ?? this.createShard(standbyShardId, standbyHost);
    if (!standby.healthy) throw new Error("LOCAL_MESH_TARGET_UNAVAILABLE");
    crashed.healthy = false;
    const newGeneration = ++this.slotRing.generation;
    for (const [, slot] of affected) {
      slot.ownerShardId = standbyShardId;
      slot.epoch = newGeneration;
      slot.state = "STEADY";
      delete slot.targetShardId;
    }
    this.rebuildQueues();
    standby.lastRehydrateRtoMs = performance.now() - started;
    return {
      rehydratedTasksCount: new Set(restored.map(job => job.taskId)).size,
      rehydratedTenantsCount: new Set(restored.map(job => job.tenantId)).size,
      rehydrateRtoMs: standby.lastRehydrateRtoMs, newGeneration,
    };
  }
}
