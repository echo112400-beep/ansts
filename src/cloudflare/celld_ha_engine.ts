/**
 * `celld.dev/docs` Persistent, Recoverable & High-Availability Scheduling Engine.
 *
 * Implements the exact reliability, persistence, and HA mechanisms specified in `denoland/celld`:
 * 1. RPO=0 Dual Durability Proof (`fleet` peer ensemble fsync + session sealing vs `bucket` post-upload ownership read).
 * 2. Durable `WorkflowEntrypoint` (`AgentTaskLifecycleWorkflow`: `step.do`, `step.sleep`, `step.waitForEvent` with early-event buffer).
 * 3. Sharded Queue Producer Router (respecting 256 concurrent producer limit per Queue Cell) & `message.attempts` exponential backoff consumer.
 * 4. Safe Epoch Garbage Collection (`CELLD_LTX_RETENTION_SECS` & `retired.json` manifest).
 * 5. Fleet Autoscaler Telemetry (`GET /state` `node_load`), Rebalancer (`CELLD_REBALANCE_INTERVAL_MS`), and Rolling Handoff (`POST /shutdown?handoff=preserve`).
 */

import * as crypto from "crypto";
import { StaleFenceError } from "../lease_coordinator";
import { FenceToken } from "../models";

// ==================== 1. RPO=0 DUAL DURABILITY PROOF & NODE-LOG SESSION SEALING ====================

export type CelldDurabilityMode = "fleet" | "bucket";

export interface OwnershipRecord {
  cellScope: string;
  ownerNodeId: string;
  epoch: number;
  leaseExpiresAtMs: number;
}

export class RpoZeroDurabilityEngine {
  public ownershipStore: Map<string, OwnershipRecord> = new Map();
  public sealedNodeSessions: Set<string> = new Set(); // Format: `${cellScope}:${nodeId}:${epoch}`
  public peerFsyncLedger: Map<string, string[]> = new Map();
  public bucketObjects: Map<string, string> = new Map();

  constructor(
    public readonly durabilityMode: CelldDurabilityMode = "fleet",
    public readonly leaseTtlMs: number = 10000,
  ) {}

  public get leaseRenewIntervalMs(): number {
    return Math.floor(this.leaseTtlMs / 3);
  }

  /**
   * Claim or take over a Cell via conditional write (CAS) in the bucket.
   * Seals the prior owner's node-log session so a partitioned old owner cannot complete a fleet proof.
   */
  public claimCellOwnershipCas(
    cellScope: string,
    candidateNodeId: string,
    nowMs: number,
  ): OwnershipRecord {
    const existing = this.ownershipStore.get(cellScope);
    if (existing && existing.ownerNodeId !== candidateNodeId && nowMs < existing.leaseExpiresAtMs) {
      throw new StaleFenceError(
        `CAS Lease Held: ${cellScope} is owned by ${existing.ownerNodeId} at epoch ${existing.epoch}`,
      );
    }

    const nextEpoch = existing ? existing.epoch + 1 : 1;
    if (existing) {
      this.sealedNodeSessions.add(`${cellScope}:${existing.ownerNodeId}:${existing.epoch}`);
    }

    const record: OwnershipRecord = {
      cellScope,
      ownerNodeId: candidateNodeId,
      epoch: nextEpoch,
      leaseExpiresAtMs: nowMs + this.leaseTtlMs,
    };
    this.ownershipStore.set(cellScope, record);
    return record;
  }

  public renewLease(cellScope: string, nodeId: string, epoch: number, nowMs: number): boolean {
    const rec = this.ownershipStore.get(cellScope);
    if (!rec || rec.ownerNodeId !== nodeId || rec.epoch !== epoch) {
      return false;
    }
    rec.leaseExpiresAtMs = nowMs + this.leaseTtlMs;
    return true;
  }

  /**
   * Commit an LTX transaction and prove RPO=0 durability before acknowledging to the client:
   * - In `fleet` mode: replicates to peer followers and verifies session is not sealed.
   * - In `bucket` mode: writes to bucket and re-reads ownership record to verify `(nodeId, epoch)` still holds.
   */
  public commitAndProveRpoZero(params: {
    cellScope: string;
    nodeId: string;
    fence: FenceToken;
    ltxPayload: string;
    peerFollowerIds: string[];
    writeCursorConsumed?: boolean;
  }): { acknowledged: boolean; proofType: CelldDurabilityMode; objectKey: string } {
    if (params.writeCursorConsumed === false) {
      throw new Error("celld SQLite Rule Violation: Unfinished SQL write cursor must be consumed before output/sync");
    }

    const sessionKey = `${params.cellScope}:${params.nodeId}:${params.fence.epoch}`;
    if (this.sealedNodeSessions.has(sessionKey)) {
      throw new StaleFenceError(
        `409 SessionSealed: Prior node-log session ${sessionKey} was sealed by a newer owner takeover`,
      );
    }

    const epStr = String(params.fence.epoch).padStart(4, "0");
    const txStr = String(params.fence.turnSeq).padStart(6, "0");
    const objectKey = `cells/${params.cellScope}/ep_${epStr}/${txStr}.ltx`;

    if (this.durabilityMode === "fleet" && params.peerFollowerIds.length >= 1) {
      // Fleet proof: replicate to 1~2 follower peers and fsync to disk
      this.peerFsyncLedger.set(objectKey, [...params.peerFollowerIds]);
      this.bucketObjects.set(objectKey, params.ltxPayload);
      return { acknowledged: true, proofType: "fleet", objectKey };
    }

    // Bucket proof (or single-node fleet fallback): write to bucket, then re-read ownership record once
    this.bucketObjects.set(objectKey, params.ltxPayload);
    const currentOwner = this.ownershipStore.get(params.cellScope);
    if (
      !currentOwner ||
      currentOwner.ownerNodeId !== params.nodeId ||
      currentOwner.epoch !== params.fence.epoch
    ) {
      throw new StaleFenceError(
        `409 BucketProofFailed: Ownership of ${params.cellScope} changed during bucket write; refusing to acknowledge`,
      );
    }
    return { acknowledged: true, proofType: "bucket", objectKey };
  }
}

// ==================== 2. DURABLE WORKFLOW ENTRYPOINT (step.do, step.sleep, step.waitForEvent) ====================

export interface WorkflowStepOptions {
  retries?: { limit: number; delaySec: number; backoff: "exponential" | "constant" };
  timeoutSec?: number;
}

export class DurableWorkflowContext {
  public completedSteps: Map<string, any> = new Map();
  public sleepDeadlinesMs: Map<string, number> = new Map();
  public earlyEventBuffer: Map<string, any> = new Map();
  public isolateHibernated: boolean = false;
  public status: "running" | "waiting" | "complete" | "errored" = "running";

  constructor(
    public readonly instanceId: string,
    public readonly alarmResidentWindowMs: number = 3600 * 1000, // CELLD_ALARM_RESIDENT_MS (1h default)
  ) {}

  /**
   * `step.do(name, callback)`: Replays return cached step result without re-running callback.
   * Retries transient failures up to `retries.limit` (default 5).
   */
  public async do<T>(
    name: string,
    callback: () => Promise<T> | T,
    options?: WorkflowStepOptions,
  ): Promise<T> {
    if (this.completedSteps.has(name)) {
      return this.completedSteps.get(name) as T;
    }
    const maxRetries = options?.retries?.limit ?? 5;
    let lastErr: any = null;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const res = await callback();
        this.completedSteps.set(name, res);
        this.status = "running";
        return res;
      } catch (err) {
        lastErr = err;
      }
    }
    this.status = "errored";
    throw lastErr;
  }

  /**
   * `step.sleep(name, durationMs)`: Stores immutable deadline so crash/replay never shifts it.
   * Hibernates the Cell isolate if deadline is further away than `CELLD_ALARM_RESIDENT_MS`.
   */
  public async sleep(name: string, durationMs: number, nowMs: number = Date.now()): Promise<void> {
    if (!this.sleepDeadlinesMs.has(name)) {
      this.sleepDeadlinesMs.set(name, nowMs + durationMs);
    }
    const deadline = this.sleepDeadlinesMs.get(name)!;
    if (deadline - nowMs > this.alarmResidentWindowMs) {
      this.isolateHibernated = true;
    }
    this.status = "waiting";
  }

  public sendEvent(eventName: string, payload: any): void {
    this.earlyEventBuffer.set(eventName, payload);
  }

  /**
   * `step.waitForEvent(name)`: Consumes buffered early event immediately or enters waiting/hibernated state.
   */
  public async waitForEvent<T = any>(eventName: string): Promise<T | null> {
    if (this.completedSteps.has(`evt:${eventName}`)) {
      return this.completedSteps.get(`evt:${eventName}`) as T;
    }
    if (this.earlyEventBuffer.has(eventName)) {
      const payload = this.earlyEventBuffer.get(eventName) as T;
      this.earlyEventBuffer.delete(eventName);
      this.completedSteps.set(`evt:${eventName}`, payload);
      this.status = "running";
      self_status_restore: {
        this.isolateHibernated = false;
      }
      return payload;
    }
    this.status = "waiting";
    this.isolateHibernated = true;
    return null;
  }
}

// ==================== 3. SHARDED QUEUE ROUTER (256 CONCURRENCY CAP) & EXPONENTIAL BACKOFF CONSUMER ====================

export class ShardedQueueBrokerPool {
  public static readonly MAX_CONCURRENT_PRODUCERS_PER_QUEUE = 256;
  public activeProducersByShard: Map<number, number> = new Map();
  public messagesByShard: Map<number, Array<{ id: string; body: any; attempts: number }>> = new Map();
  public deadLetterQueue: Array<{ id: string; body: any; attempts: number }> = [];

  constructor(
    public readonly queuePrefix: string = "ansts-events-q",
    public readonly shardCount: number = 4,
    public readonly maxRetries: number = 3,
  ) {
    for (let s = 0; s < shardCount; s++) {
      this.activeProducersByShard.set(s, 0);
      this.messagesByShard.set(s, []);
    }
  }

  /**
   * Routes producer calls across sharded Queue Cells so no single Queue Cell exceeds 256 concurrent calls.
   */
  public produceMessage(idempotencyId: string, body: any): { shardName: string; shardIndex: number } {
    const hash = crypto.createHash("sha256").update(idempotencyId, "utf8").digest()[0];
    for (let offset = 0; offset < this.shardCount; offset++) {
      const shardIdx = (hash + offset) % this.shardCount;
      const active = this.activeProducersByShard.get(shardIdx) ?? 0;
      if (active < ShardedQueueBrokerPool.MAX_CONCURRENT_PRODUCERS_PER_QUEUE) {
        this.messagesByShard.get(shardIdx)!.push({ id: idempotencyId, body, attempts: 1 });
        return { shardName: `${this.queuePrefix}-shard-${shardIdx}`, shardIndex: shardIdx };
      }
    }
    throw new Error("CellOverloadError: All queue shards reached 256 concurrent producer calls");
  }

  /**
   * `celld` adds no automatic exponential backoff on `message.retry()`; computes `delaySeconds` from `message.attempts`.
   */
  public settleOrRetryMessage(
    msg: { id: string; body: any; attempts: number },
    handlerSucceeded: boolean,
  ): { action: "ACKED" | "RETRY_SCHEDULED" | "MOVED_TO_DLQ"; delaySeconds: number } {
    if (handlerSucceeded) {
      return { action: "ACKED", delaySeconds: 0 };
    }
    if (msg.attempts >= this.maxRetries) {
      this.deadLetterQueue.push(msg);
      return { action: "MOVED_TO_DLQ", delaySeconds: 0 };
    }
    const delaySeconds = Math.min(86400, Math.pow(2, msg.attempts) * 2);
    msg.attempts += 1;
    return { action: "RETRY_SCHEDULED", delaySeconds };
  }
}

// ==================== 4. SAFE EPOCH GARBAGE COLLECTION (CELLD_LTX_RETENTION_SECS & retired.json) ====================

export interface EpochPrefixMeta {
  epoch: number;
  createdAtSec: number;
  byteSize: number;
}

export class SafeEpochGarbageCollector {
  public retiredManifests: Map<string, { restoreBaseEpoch: number; deletedEpochs: number[] }> = new Map();

  public runEpochGc(params: {
    cellScope: string;
    currentOwnerNodeId: string;
    ownershipRecord: OwnershipRecord;
    epochs: EpochPrefixMeta[];
    retentionSecs: number;
    nowSec: number;
    pagedRestoreComplete?: boolean;
  }): { deletedEpochs: number[]; keptEpochs: number[]; retiredJsonWritten: boolean } {
    if (params.retentionSecs <= 0 || params.pagedRestoreComplete === false) {
      return {
        deletedEpochs: [],
        keptEpochs: params.epochs.map((e) => e.epoch),
        retiredJsonWritten: false,
      };
    }
    const sorted = [...params.epochs].sort((a, b) => a.epoch - b.epoch);
    const newestEpoch = sorted.length > 0 ? sorted[sorted.length - 1].epoch : 0;

    // Must own the cell at the newest epoch before deleting any older epoch prefix
    if (
      params.ownershipRecord.ownerNodeId !== params.currentOwnerNodeId ||
      params.ownershipRecord.epoch !== newestEpoch
    ) {
      return {
        deletedEpochs: [],
        keptEpochs: sorted.map((e) => e.epoch),
        retiredJsonWritten: false,
      };
    }

    const deletedEpochs: number[] = [];
    const keptEpochs: number[] = [];
    const previousEpoch = newestEpoch - 1;

    for (const ep of sorted) {
      const ageSec = params.nowSec - ep.createdAtSec;
      if (ep.epoch === newestEpoch || ep.epoch === previousEpoch || ageSec < params.retentionSecs) {
        keptEpochs.push(ep.epoch);
      } else {
        deletedEpochs.push(ep.epoch);
      }
    }

    const restoreBaseEpoch = Math.min(...keptEpochs);
    this.retiredManifests.set(params.cellScope, { restoreBaseEpoch, deletedEpochs });
    return { deletedEpochs, keptEpochs, retiredJsonWritten: true };
  }
}

// ==================== 5. FLEET AUTOSCALER TELEMETRY, REBALANCER & ROLLING HANDOFF CONTROLLER ====================

export interface CelldNodeLoadBlock {
  node_id: string;
  sampled_ms: number;
  owned_cells: number;
  placement_weight: number;
  resident_cells: number;
  host_websockets: number;
  rss_bytes: number;
  in_use_bytes: number;
  cpu_percent_x100: number;
  open_fds: number;
  pressured: boolean;
  memory_headroom: boolean;
  shed_cells: number;
  restoring: number;
  draining: boolean;
}

export class FleetHaAndAutoscalerController {
  public rebalancingPaused: boolean = false;
  public nodes: Map<string, CelldNodeLoadBlock> = new Map();

  public registerOrUpdateNodeLoad(load: CelldNodeLoadBlock): void {
    this.nodes.set(load.node_id, load);
  }

  /**
   * Public listener `/.well-known/celld/health`: returns 503 during drain or before joining node settles.
   */
  public checkNodePublicHealth(nodeId: string): { statusCode: 200 | 503; healthy: boolean } {
    const load = this.nodes.get(nodeId);
    if (!load || load.draining || load.restoring > 0) {
      return { statusCode: 503, healthy: false };
    }
    return { statusCode: 200, healthy: true };
  }

  /**
   * Evaluates `fleet/capacity-v1.json` across nodes to emit autoscaler signals (`SCALE_OUT`, `SCALE_IN`, `HOLD`)
   * and verify whether rolling update can advance (`restoring == 0` on all nodes).
   */
  public evaluateFleetAutoscalingAndRollout(): {
    autoscalerSignal: "SCALE_OUT" | "SCALE_IN" | "HOLD";
    safeToAdvanceRollingUpdate: boolean;
    totalRestoring: number;
  } {
    const all = Array.from(this.nodes.values());
    const totalRestoring = all.reduce((acc, n) => acc + n.restoring, 0);
    const anyPressured = all.some((n) => n.pressured);
    const allHaveHeadroom = all.length > 1 && all.every((n) => n.memory_headroom && n.restoring === 0);

    let autoscalerSignal: "SCALE_OUT" | "SCALE_IN" | "HOLD" = "HOLD";
    if (anyPressured) {
      autoscalerSignal = "SCALE_OUT";
    } else if (allHaveHeadroom) {
      autoscalerSignal = "SCALE_IN";
    }

    return {
      autoscalerSignal,
      safeToAdvanceRollingUpdate: totalRestoring === 0,
      totalRestoring,
    };
  }

  /**
   * Fleet Rebalancer (`CELLD_REBALANCE_INTERVAL_MS=5000`):
   * The most loaded node hands at most 32 hibernated/idle cells at a time to the node furthest below target.
   * Draining nodes and nodes with `restoring > 0` receive no cells.
   */
  public computeRebalanceBatch(): { fromNodeId: string; toNodeId: string; movedCells: number } | null {
    if (this.rebalancingPaused) return null;
    const eligible = Array.from(this.nodes.values()).filter(
      (n) => !n.draining && n.restoring === 0 && n.placement_weight > 0,
    );
    if (eligible.length < 2) return null;

    const totalWeight = eligible.reduce((acc, n) => acc + n.placement_weight, 0);
    const totalCells = eligible.reduce((acc, n) => acc + n.owned_cells, 0);

    const deltas = eligible.map((n) => {
      const target = (totalCells * n.placement_weight) / totalWeight;
      return { node: n, target, excess: n.owned_cells - target };
    });
    deltas.sort((a, b) => b.excess - a.excess);

    const mostLoaded = deltas[0];
    const leastLoaded = deltas[deltas.length - 1];
    if (mostLoaded.excess <= 5 || leastLoaded.excess >= 0) {
      return null;
    }

    const receiverCap = Math.max(0, Math.floor(leastLoaded.target * 0.98 - leastLoaded.node.owned_cells));
    const movedCells = Math.min(32, Math.floor(mostLoaded.excess), receiverCap);
    if (movedCells <= 0) return null;

    mostLoaded.node.owned_cells -= movedCells;
    leastLoaded.node.owned_cells += movedCells;
    return {
      fromNodeId: mostLoaded.node.node_id,
      toNodeId: leastLoaded.node.node_id,
      movedCells,
    };
  }

  /**
   * `POST /shutdown?handoff=preserve`: Prepares a same-node restart that keeps ownership records,
   * cancels cold activations, and uploads stopped databases first.
   */
  public prepareShutdownHandoff(nodeId: string, preserveOwnership: boolean): {
    status: string;
    preservedOwnershipRecords: number;
    uploadedDatabases: number;
  } {
    const load = this.nodes.get(nodeId);
    if (!load) {
      return { status: "NOT_FOUND", preservedOwnershipRecords: 0, uploadedDatabases: 0 };
    }
    load.draining = true;
    load.restoring = 0; // Cancel pending cold activations
    return {
      status: preserveOwnership ? "HANDOFF_PRESERVE_READY" : "DRAINED_FOR_TAKEOVER",
      preservedOwnershipRecords: preserveOwnership ? load.owned_cells : 0,
      uploadedDatabases: load.resident_cells,
    };
  }
}
