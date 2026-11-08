"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ShardedQueueBrokerPool = exports.SafeEpochGarbageCollector = exports.RpoZeroDurabilityEngine = exports.FleetReplicatedDurabilityEngine = exports.FleetHaAndAutoscalerController = exports.DurableWorkflowContext = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M8 TASK-28 ~ TASK-31: RPO=0 LTX Durability Engine, DurableWorkflowContext, Sharded Queue Broker & Safe Epoch GC.
 */









class RpoZeroDurabilityEngine {
  bucket;
  txCounter = 0;

  constructor(bucket) {
    this.bucket = bucket;
  }

  async commitWithSyncLtxFlush(
  cellId,
  fencingToken,
  sqlMutation,
  serializedWalBytes)
  {
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
        sha256
      }
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
      sizeBytes: serializedWalBytes.byteLength
    };
  }
}exports.RpoZeroDurabilityEngine = RpoZeroDurabilityEngine;

class DurableWorkflowContext {
  sql;
  workflowId;
  /** `true` while the owning Isolate is hibernated (`0.1C`); an inbound workflow event wakes it. */
  isolateHibernated = false;
  pendingEventsByType = new Map();
  eventWaitersByType = new Map();

  constructor(sql, workflowId) {
    this.sql = sql;
    this.workflowId = workflowId;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS workflow_steps (step_key TEXT PRIMARY KEY, workflow_id TEXT, output_json TEXT, completed_at INTEGER)"
    );
  }

  /**
   * Delivers an external event (`MCP` notification, CI/CD completion, ...) to this workflow. A waiter
   * already parked on the event type is resumed immediately, otherwise the event is buffered for the
   * next `waitForEvent` call.
   */
  sendEvent(eventType, payload) {
    const waiters = this.eventWaitersByType.get(eventType);
    const waiter = waiters?.shift();
    if (waiter) {
      waiter(payload);
      return;
    }
    const pending = this.pendingEventsByType.get(eventType) ?? [];
    pending.push(payload);
    this.pendingEventsByType.set(eventType, pending);
  }

  /** Resolves with the next event of `eventType`, replaying a buffered one first. */
  waitForEvent(eventType) {
    const pending = this.pendingEventsByType.get(eventType);
    const buffered = pending?.shift();
    if (buffered) {
      return Promise.resolve(buffered);
    }
    return new Promise((resolve) => {
      const waiters = this.eventWaitersByType.get(eventType) ?? [];
      waiters.push(resolve);
      this.eventWaitersByType.set(eventType, waiters);
    });
  }

  async stepDo(stepName, fn) {
    const stepKey = `${this.workflowId}:${stepName}`;
    const existing = this.sql.
    exec("SELECT * FROM workflow_steps WHERE step_key = ?", stepKey).
    one();
    if (existing && existing.output_json !== undefined) {
      return {
        result: JSON.parse(String(existing.output_json)),
        replayedFromJournal: true
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
}exports.DurableWorkflowContext = DurableWorkflowContext;

class ShardedQueueBrokerPool {
  shardCount;
  queuesByShard = new Map();

  constructor(shardCount = 16) {
    this.shardCount = shardCount;
    for (let i = 0; i < shardCount; i++) {
      this.queuesByShard.set(i, []);
    }
  }

  computeShard(partitionKey) {
    const hash = crypto.createHash("md5").update(partitionKey, "utf-8").digest();
    return hash.readUInt32BE(0) % this.shardCount;
  }

  publish(partitionKey, id, payload) {
    const shard = this.computeShard(partitionKey);
    this.queuesByShard.get(shard).push({ id, payload });
    return shard;
  }

  consumeBatch(shard, maxBatch = 32) {
    const q = this.queuesByShard.get(shard) ?? [];
    return q.splice(0, maxBatch);
  }
}exports.ShardedQueueBrokerPool = ShardedQueueBrokerPool;

class SafeEpochGarbageCollector {
  gracePeriodMs;

  constructor(gracePeriodMs = 3_600_000) {
    this.gracePeriodMs = gracePeriodMs;
  }

  filterCollectibleSnapshots(
  snapshots,
  activeMinFencingToken,
  nowMs = Date.now())
  {
    return snapshots.
    filter(
      (s) =>
      !s.referencedByActiveCell &&
      s.fencingToken < activeMinFencingToken &&
      nowMs - s.createdAtMs >= this.gracePeriodMs
    ).
    map((s) => s.key);
  }
}exports.SafeEpochGarbageCollector = SafeEpochGarbageCollector;

class FleetHaAndAutoscalerController {
  evaluateFleetHealth(nodes)



  {
    const healthy = nodes.filter((n) => n.healthy);
    const unhealthyNodeIds = nodes.filter((n) => !n.healthy).map((n) => n.nodeId);
    const avgCpu =
    healthy.length > 0 ? healthy.reduce((a, b) => a + b.cpuUtilPct, 0) / healthy.length : 100;
    let scaleAction = "NONE";
    if (avgCpu >= 75.0 || unhealthyNodeIds.length > 0) {
      scaleAction = "SCALE_OUT";
    } else if (avgCpu <= 20.0 && healthy.length > 2) {
      scaleAction = "SCALE_IN";
    }
    return { healthyNodes: healthy.length, unhealthyNodeIds, scaleAction };
  }
}


/**
 * M16 TASK-76: `CELLD_DURABILITY=fleet` Peer-Replicated Log Pipeline (`CELLD_LOG_PIPELINE=4`),
 * Adaptive Hedged Log Append (`CELLD_LOG_HEDGE_MS`), & Automatic Single-Node `bucket` Fallback.
 */exports.FleetHaAndAutoscalerController = FleetHaAndAutoscalerController;
class FleetReplicatedDurabilityEngine {
  mode;
  logPipelineLimit;
  slowestRecentAppendMs = 40;

  constructor(mode = "fleet", logPipelineLimit = 4) {
    this.mode = mode;
    this.logPipelineLimit = Math.max(1, Math.min(4, logPipelineLimit));
  }

  computeAdaptiveHedgeThresholdMs() {
    return Math.max(250, this.slowestRecentAppendMs * 4);
  }

  commitWriteWithFleetDurability(params)










  {
    const pipelineRoundsUsed = Math.min(this.logPipelineLimit, params.inFlightLogRounds ?? 1);
    if (this.mode === "bucket" || params.livePeerNodeCount < 1) {
      const effectiveDurabilityMode = this.mode === "bucket" ? "bucket" : "bucket_fallback";
      return {
        effectiveDurabilityMode,
        acknowledgedVia: "BUCKET_PUT",
        commitLatencyMs: params.bucketPutLatencyMs,
        hedgedCopyTriggered: false,
        pipelineRoundsUsed
      };
    }

    const hedgeThresholdMs = this.computeAdaptiveHedgeThresholdMs();
    const hedgedCopyTriggered = params.peerFsyncLatencyMs >= hedgeThresholdMs;
    const effectivePeerLatencyMs = hedgedCopyTriggered ?
    Math.min(params.peerFsyncLatencyMs, hedgeThresholdMs + 18) :
    params.peerFsyncLatencyMs;

    this.slowestRecentAppendMs = Math.max(10, Math.min(200, effectivePeerLatencyMs));

    if (effectivePeerLatencyMs <= params.bucketPutLatencyMs) {
      return {
        effectiveDurabilityMode: "fleet",
        acknowledgedVia: hedgedCopyTriggered ? "HEDGED_PEER_COPY" : "PEER_FSYNC",
        commitLatencyMs: effectivePeerLatencyMs,
        hedgedCopyTriggered,
        pipelineRoundsUsed
      };
    }
    return {
      effectiveDurabilityMode: "fleet",
      acknowledgedVia: "BUCKET_PUT",
      commitLatencyMs: params.bucketPutLatencyMs,
      hedgedCopyTriggered,
      pipelineRoundsUsed
    };
  }
}exports.FleetReplicatedDurabilityEngine = FleetReplicatedDurabilityEngine;