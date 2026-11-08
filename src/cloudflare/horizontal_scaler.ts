/**
 * @fileoverview M9 TASK-32 ~ TASK-34: 64-Shard Consistent Hash Partitioned Scheduler Router, Dynamic Queue Auto-Sharder & Brain-Body Container Pool Scaler.
 */
import * as crypto from "node:crypto";
import { AnstsSchedulerCell } from "./scheduler_cell.js";
import { MockDurableObjectState } from "./env.js";
import { TaskSpec } from "../models.js";

export class PartitionedSchedulerRouter {
  public readonly numPartitions: number;
  private readonly partitions: Map<number, AnstsSchedulerCell> = new Map();

  constructor(numPartitions = 64) {
    this.numPartitions = numPartitions;
    for (let i = 0; i < numPartitions; i++) {
      const state = new MockDurableObjectState(`scheduler-shard-${i}`);
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

  public async routeSubmitTask(spec: TaskSpec): Promise<{ shardId: number; submitted: TaskSpec }> {
    const { shardId, cell } = this.getCellForTask(spec.tenant_id, spec.task_id);
    const submitted = await cell.submitTask(spec);
    return { shardId, submitted };
  }
}

export class DynamicQueueAutoSharder {
  private currentShards: number;
  private readonly minShards: number;
  private readonly maxShards: number;

  constructor(initialShards = 16, minShards = 4, maxShards = 128) {
    this.currentShards = initialShards;
    this.minShards = minShards;
    this.maxShards = maxShards;
  }

  public adjustShards(messagesPerSecond: number, maxWriteRpsPerShard = 450): {
    previousShards: number;
    newShards: number;
    rebalanced: boolean;
  } {
    const prev = this.currentShards;
    const required = Math.ceil(messagesPerSecond / maxWriteRpsPerShard);
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
    const ratio = params.warmStandbyRatio ?? 0.25;
    const brainIsolateMemoryMb = (params.activeThinkCells + params.activeActCells) * 8;
    const requiredHeavyActContainers = params.activeActCells;
    const warmBufferContainers = Math.max(2, Math.ceil(params.activeThinkCells * ratio));
    return {
      brainIsolateMemoryMb,
      requiredHeavyActContainers,
      warmBufferContainers,
      totalContainerPoolSize: requiredHeavyActContainers + warmBufferContainers,
    };
  }
}
