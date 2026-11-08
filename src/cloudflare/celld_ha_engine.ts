/**
 * @fileoverview M8 TASK-28 ~ TASK-31: RPO=0 LTX Durability Engine, DurableWorkflowContext, Sharded Queue Broker & Safe Epoch GC.
 */
import * as crypto from "node:crypto";
import { R2BucketLike, SqlStorageLike } from "./env.js";

export interface LtxSegmentMetadata {
  cellId: string;
  txId: number;
  fencingToken: number;
  sha256: string;
  r2Key: string;
  sizeBytes: number;
}

export class RpoZeroDurabilityEngine {
  private readonly bucket: R2BucketLike;
  private txCounter = 0;

  constructor(bucket: R2BucketLike) {
    this.bucket = bucket;
  }

  public async commitWithSyncLtxFlush(
    cellId: string,
    fencingToken: number,
    sqlMutation: () => void,
    serializedWalBytes: Uint8Array
  ): Promise<LtxSegmentMetadata> {
    sqlMutation();
    this.txCounter += 1;
    const txId = this.txCounter;
    const sha256 = crypto.createHash("sha256").update(serializedWalBytes).digest("hex");
    const r2Key = `ltx/${cellId}/tx-${String(txId).padStart(8, "0")}-f${fencingToken}.ltx`;

    const putResult = await this.bucket.put(r2Key, serializedWalBytes, {
      onlyIf: { etagDoesNotMatch: "*" },
      customMetadata: {
        cellId,
        txId: String(txId),
        fencingToken: String(fencingToken),
        sha256,
      },
    });
    if (!putResult) {
      throw new Error(`CAS conflict writing LTX segment '${r2Key}': split-brain fenced!`);
    }
    return {
      cellId,
      txId,
      fencingToken,
      sha256,
      r2Key,
      sizeBytes: serializedWalBytes.byteLength,
    };
  }
}

export class DurableWorkflowContext {
  private readonly sql: SqlStorageLike;
  private readonly workflowId: string;

  constructor(sql: SqlStorageLike, workflowId: string) {
    this.sql = sql;
    this.workflowId = workflowId;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS workflow_steps (step_key TEXT PRIMARY KEY, workflow_id TEXT, output_json TEXT, completed_at INTEGER)"
    );
  }

  public async stepDo<T>(stepName: string, fn: () => Promise<T> | T): Promise<{ result: T; replayedFromJournal: boolean }> {
    const stepKey = `${this.workflowId}:${stepName}`;
    const existing = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM workflow_steps WHERE step_key = ?", stepKey)
      .one();
    if (existing && existing.output_json !== undefined) {
      return {
        result: JSON.parse(String(existing.output_json)) as T,
        replayedFromJournal: true,
      };
    }
    const result = await fn();
    this.sql.exec(
      "INSERT OR REPLACE INTO workflow_steps (step_key, workflow_id, output_json, completed_at) VALUES (?, ?, ?, ?)",
      stepKey,
      this.workflowId,
      JSON.stringify(result),
      Date.now()
    );
    return { result, replayedFromJournal: false };
  }
}

export class ShardedQueueBrokerPool {
  private readonly shardCount: number;
  private readonly queuesByShard: Map<number, Array<{ id: string; payload: Record<string, unknown> }>> = new Map();

  constructor(shardCount = 16) {
    this.shardCount = shardCount;
    for (let i = 0; i < shardCount; i++) {
      this.queuesByShard.set(i, []);
    }
  }

  public computeShard(partitionKey: string): number {
    const hash = crypto.createHash("md5").update(partitionKey, "utf-8").digest();
    return hash.readUInt32BE(0) % this.shardCount;
  }

  public publish(partitionKey: string, id: string, payload: Record<string, unknown>): number {
    const shard = this.computeShard(partitionKey);
    this.queuesByShard.get(shard)!.push({ id, payload });
    return shard;
  }

  public consumeBatch(shard: number, maxBatch = 32): Array<{ id: string; payload: Record<string, unknown> }> {
    const q = this.queuesByShard.get(shard) ?? [];
    return q.splice(0, maxBatch);
  }
}

export class SafeEpochGarbageCollector {
  private readonly gracePeriodMs: number;

  constructor(gracePeriodMs = 3_600_000) {
    this.gracePeriodMs = gracePeriodMs;
  }

  public filterCollectibleSnapshots(
    snapshots: Array<{ key: string; createdAtMs: number; fencingToken: number; referencedByActiveCell: boolean }>,
    activeMinFencingToken: number,
    nowMs = Date.now()
  ): string[] {
    return snapshots
      .filter(
        (s) =>
          !s.referencedByActiveCell &&
          s.fencingToken < activeMinFencingToken &&
          nowMs - s.createdAtMs >= this.gracePeriodMs
      )
      .map((s) => s.key);
  }
}

export class FleetHaAndAutoscalerController {
  public evaluateFleetHealth(nodes: Array<{ nodeId: string; healthy: boolean; cpuUtilPct: number }>): {
    healthyNodes: number;
    unhealthyNodeIds: string[];
    scaleAction: "NONE" | "SCALE_OUT" | "SCALE_IN";
  } {
    const healthy = nodes.filter((n) => n.healthy);
    const unhealthyNodeIds = nodes.filter((n) => !n.healthy).map((n) => n.nodeId);
    const avgCpu =
      healthy.length > 0 ? healthy.reduce((a, b) => a + b.cpuUtilPct, 0) / healthy.length : 100;
    let scaleAction: "NONE" | "SCALE_OUT" | "SCALE_IN" = "NONE";
    if (avgCpu >= 75.0 || unhealthyNodeIds.length > 0) {
      scaleAction = "SCALE_OUT";
    } else if (avgCpu <= 20.0 && healthy.length > 2) {
      scaleAction = "SCALE_IN";
    }
    return { healthyNodes: healthy.length, unhealthyNodeIds, scaleAction };
  }
}
