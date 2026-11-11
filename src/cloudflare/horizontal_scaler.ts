/**
 * @fileoverview M9 TASK-32 ~ TASK-34, M63-2 & M64-5: 64-Shard Consistent Hash Partitioned Scheduler Router,
 * Dynamic Queue Auto-Sharder & Brain-Body Container Pool Scaler with Custom State Factory, Multi-Shard Node Registration,
 * Multi-Shard Tick Scheduling & Zero-Division/NaN Guards.
 */
import * as crypto from "node:crypto";
import { AnstsSchedulerCell } from "./scheduler_cell.js";
import { DurableObjectStateLike, MockDurableObjectState } from "./env.js";
import { NodeHeartbeat, TaskSpec } from "../models.js";

export class PartitionedSchedulerRouter {
  public readonly numPartitions: number;
  private readonly partitions: Map<number, AnstsSchedulerCell> = new Map();

  constructor(
    numPartitions = 64,
    stateFactory?: (shardId: number) => DurableObjectStateLike
  ) {
    this.numPartitions = Math.max(1, Math.floor(numPartitions));
    for (let i = 0; i < this.numPartitions; i++) {
      const state = stateFactory ? stateFactory(i) : new MockDurableObjectState(`scheduler-shard-${i}`);
      this.partitions.set(i, new AnstsSchedulerCell(state));
    }
  }

  public getShardIndex(tenantId: string, taskId: string): number {
    const key = `${tenantId}:${taskId}`;
    const digest = crypto.createHash("sha256").update(key, "utf-8").digest();
    return digest.readUInt32BE(0) % this.numPartitions;
  }

  public getCellForTask(tenantId: string, taskId: string): { shardId: number; cell: AnstsSchedulerCell } {
    const shardId = this.getShardIndex(tenantId, taskId);
    return { shardId, cell: this.partitions.get(shardId)! };
  }

  public registerNodeAcrossShards(hb: NodeHeartbeat): void {
    for (const cell of this.partitions.values()) {
      cell.registerNode(hb);
    }
  }

  public async routeSubmitTask(
    spec: TaskSpec,
    options?: { mergeDuplicateInFlight?: boolean }
  ): Promise<{ shardId: number; submitted: TaskSpec }> {
    const { shardId, cell } = this.getCellForTask(spec.tenant_id, spec.task_id);
    const submitted = await cell.submitTask(spec, options);
    return { shardId, submitted };
  }

  public async scheduleTickAcrossShards(): Promise<{
    totalPlacements: Array<{ shardId: number; taskId: string; nodeId: string; fencingToken: number }>;
    totalRecoveredIds: string[];
  }> {
    const totalPlacements: Array<{ shardId: number; taskId: string; nodeId: string; fencingToken: number }> = [];
    const totalRecoveredIds: string[] = [];
    for (const [shardId, cell] of this.partitions.entries()) {
      const { placements, recoveredIds } = await cell.alarm();
      for (const [taskId, nodeId, fencingToken] of placements) {
        totalPlacements.push({ shardId, taskId, nodeId, fencingToken });
      }
      totalRecoveredIds.push(...recoveredIds);
    }
    return { totalPlacements, totalRecoveredIds };
  }
}

export class DynamicQueueAutoSharder {
  private currentShards: number;
  private readonly minShards: number;
  private readonly maxShards: number;

  constructor(initialShards = 16, minShards = 4, maxShards = 128) {
    this.minShards = Math.max(1, Math.floor(minShards));
    this.maxShards = Math.max(this.minShards, Math.floor(maxShards));
    this.currentShards = Math.max(this.minShards, Math.min(this.maxShards, Math.floor(initialShards)));
  }

  /**
   * [M63-2] DFX-Availability: Guards against `maxWriteRpsPerShard <= 0` or `NaN` `messagesPerSecond`
   * so `currentShards` never becomes `NaN` or `Infinity`.
   */
  public adjustShards(messagesPerSecond: number, maxWriteRpsPerShard = 450): {
    previousShards: number;
    newShards: number;
    rebalanced: boolean;
  } {
    const prev = this.currentShards;
    const safeMps = Number.isFinite(messagesPerSecond) && messagesPerSecond >= 0 ? messagesPerSecond : 0;
    const safeRpsPerShard =
      Number.isFinite(maxWriteRpsPerShard) && maxWriteRpsPerShard > 0 ? maxWriteRpsPerShard : 450;
    const required = Math.ceil(safeMps / safeRpsPerShard);
    const clamped = Math.max(this.minShards, Math.min(this.maxShards, required));
    this.currentShards = clamped;
    return {
      previousShards: prev,
      newShards: this.currentShards,
      rebalanced: prev !== this.currentShards,
    };
  }
}

export class BrainBodyContainerPoolScaler {
  public computeTargetSandboxContainers(params: {
    activeThinkCells: number;
    activeActCells: number;
    warmStandbyRatio?: number;
  }): {
    brainIsolateMemoryMb: number;
    requiredHeavyActContainers: number;
    warmBufferContainers: number;
    totalContainerPoolSize: number;
  } {
    const safeThink =
      Number.isFinite(params.activeThinkCells) && params.activeThinkCells >= 0
        ? Math.floor(params.activeThinkCells)
        : 0;
    const safeAct =
      Number.isFinite(params.activeActCells) && params.activeActCells >= 0
        ? Math.floor(params.activeActCells)
        : 0;
    const rawRatio = params.warmStandbyRatio ?? 0.25;
    const ratio = Number.isFinite(rawRatio) && rawRatio >= 0 ? Math.min(2.0, rawRatio) : 0.25;
    const brainIsolateMemoryMb = (safeThink + safeAct) * 8;
    const requiredHeavyActContainers = safeAct;
    const warmBufferContainers = Math.max(2, Math.ceil(safeThink * ratio));
    return {
      brainIsolateMemoryMb,
      requiredHeavyActContainers,
      warmBufferContainers,
      totalContainerPoolSize: requiredHeavyActContainers + warmBufferContainers,
    };
  }
}
