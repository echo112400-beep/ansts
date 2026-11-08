/**
 * @fileoverview M9 TASK-32 ~ TASK-34: 64-Shard Consistent Hash Partitioned Scheduler Router, Dynamic Queue Auto-Sharder & Brain-Body Container Pool Scaler.
 */
import * as crypto from "node:crypto";
import { AnstsSchedulerCell } from "./scheduler_cell.js";
import { MockDurableObjectState } from "./env.js";
import { TaskSpec } from "../models.js";

/**
 * The virtual slot ring (consistent-hash ring + 256-slot ledger) is owned by the fleet manual scale
 * controller; it is re-exported here because the scheduler HA mesh imports the slot ledger API from
 * this module.
 */
export { VirtualHashSlotRingManager } from "../cluster/manual_scale_controller.js";
export type {
  SlotLedgerSnapshot,
  SlotMigrationPhase,
  VirtualSlotLedgerEntry,
} from "../cluster/manual_scale_controller.js";

/** Per-shard counters of the stateless router mesh, read by the console shard matrix. */
export interface SchedulerRouterShardStat {
  shardId: string;
  ewmaLatencyMs: number;
  inFlightRequests: number;
}

export class PartitionedSchedulerRouter {
  public readonly numPartitions: number;
  private readonly partitions: Map<number, AnstsSchedulerCell> = new Map();
  /** `scheduler-shard-<n> -> live router counters` for every shard the router owns. */
  public readonly shards: Map<string, SchedulerRouterShardStat> = new Map();

  constructor(numPartitionsOrConfig: number | { initialShardCount: number } = 64) {
    const numPartitions =
      typeof numPartitionsOrConfig === "number"
        ? numPartitionsOrConfig
        : numPartitionsOrConfig.initialShardCount;
    this.numPartitions = numPartitions;
    for (let i = 0; i < numPartitions; i++) {
      const shardId = `scheduler-shard-${String(i).padStart(2, "0")}`;
      const state = new MockDurableObjectState(shardId);
      this.partitions.set(i, new AnstsSchedulerCell(state));
      this.shards.set(shardId, {
        shardId,
        // No latency samples recorded yet; the console renders its cold-start fallback until observed.
        ewmaLatencyMs: 0,
        inFlightRequests: 0,
      });
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
