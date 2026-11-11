"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.FleetReplicatedDurabilityEngine = exports.FleetHaAndAutoscalerController = exports.SafeEpochGarbageCollector = exports.ShardedQueueBrokerPool = exports.DurableWorkflowContext = exports.RpoZeroDurabilityEngine = void 0;
/**
 * @fileoverview M8 TASK-28 ~ TASK-31, M55-4 & M67-2: RPO=0 LTX Durability Engine (with Atomic SQL Rollback on CAS Conflict),
 * DurableWorkflowContext (with Single-Flight Concurrent Deduplication & Step Invalidation),
 * Bounded Idempotent Sharded Queue Broker & Safe Epoch GC.
 */
const crypto = __importStar(require("node:crypto"));
class RpoZeroDurabilityEngine {
    bucket;
    sql;
    txCounter = 0;
    constructor(bucket, sql) {
        this.bucket = bucket;
        this.sql = sql;
    }
    /**
     * [M55-4] Executes `sqlMutation` inside a transactional boundary and flushes the LTX segment with `If-None-Match: *` CAS.
     * If the R2 CAS write fails due to a split-brain conflict, automatically rolls back the SQLite transaction (and invokes
     * optional `onRollback`) so local SQLite never diverges from R2 LTX segments.
     */
    async commitWithSyncLtxFlush(cellId, fencingToken, sqlMutation, serializedWalBytes, options) {
        const targetSql = options?.sql ?? this.sql;
        if (targetSql) {
            targetSql.exec("BEGIN TRANSACTION");
        }
        try {
            sqlMutation();
            this.txCounter += 1;
            const txId = options?.explicitTxId ?? this.txCounter;
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
                if (targetSql) {
                    targetSql.exec("ROLLBACK TRANSACTION");
                }
                options?.onRollback?.();
                throw new Error(`CAS conflict writing LTX segment '${r2Key}': split-brain fenced!`);
            }
            if (targetSql) {
                targetSql.exec("COMMIT TRANSACTION");
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
        catch (err) {
            if (targetSql && !err.message.includes("split-brain fenced")) {
                targetSql.exec("ROLLBACK TRANSACTION");
                options?.onRollback?.();
            }
            throw err;
        }
    }
}
exports.RpoZeroDurabilityEngine = RpoZeroDurabilityEngine;
class DurableWorkflowContext {
    sql;
    workflowId;
    inFlightSteps = new Map();
    constructor(sql, workflowId) {
        this.sql = sql;
        this.workflowId = workflowId;
        this.sql.exec("CREATE TABLE IF NOT EXISTS workflow_steps (step_key TEXT PRIMARY KEY, workflow_id TEXT, output_json TEXT, completed_at INTEGER)");
    }
    /**
     * [M55-4] Single-flight memoized workflow step execution:
     * - Replays completed step outputs from SQLite journal across restarts.
     * - Deduplicates concurrent in-flight callers for the same `stepName` so `fn()` never executes twice concurrently.
     */
    async stepDo(stepName, fn) {
        const stepKey = `${this.workflowId}:${stepName}`;
        const existing = this.sql
            .exec("SELECT * FROM workflow_steps WHERE step_key = ?", stepKey)
            .one();
        if (existing && existing.output_json !== undefined) {
            return {
                result: JSON.parse(String(existing.output_json)),
                replayedFromJournal: true,
            };
        }
        const existingInFlight = this.inFlightSteps.get(stepKey);
        if (existingInFlight) {
            const coalescedResult = await existingInFlight;
            return { result: coalescedResult, replayedFromJournal: true };
        }
        const promise = (async () => {
            const res = await fn();
            this.sql.exec("INSERT OR REPLACE INTO workflow_steps (step_key, workflow_id, output_json, completed_at) VALUES (?, ?, ?, ?)", stepKey, this.workflowId, JSON.stringify(res), Date.now());
            return res;
        })();
        this.inFlightSteps.set(stepKey, promise);
        try {
            const result = await promise;
            return { result, replayedFromJournal: false };
        }
        finally {
            this.inFlightSteps.delete(stepKey);
        }
    }
    invalidateStep(stepName) {
        const stepKey = `${this.workflowId}:${stepName}`;
        this.sql.exec("DELETE FROM workflow_steps WHERE step_key = ?", stepKey);
    }
}
exports.DurableWorkflowContext = DurableWorkflowContext;
class ShardedQueueBrokerPool {
    shardCount;
    maxDepthPerShard;
    queuesByShard = new Map();
    constructor(shardCount = 16, maxDepthPerShard = 4096) {
        const rawShards = Number(shardCount);
        const rawDepth = Number(maxDepthPerShard);
        this.shardCount = Number.isFinite(rawShards) && rawShards > 0 ? Math.max(1, Math.floor(rawShards)) : 16;
        this.maxDepthPerShard = Number.isFinite(rawDepth) && rawDepth > 0 ? Math.max(1, Math.floor(rawDepth)) : 4096;
        for (let i = 0; i < this.shardCount; i++) {
            this.queuesByShard.set(i, []);
        }
    }
    computeShard(partitionKey) {
        const hash = crypto.createHash("md5").update(partitionKey, "utf-8").digest();
        return hash.readUInt32BE(0) % this.shardCount;
    }
    /**
     * [M67-2] Idempotent bounded shard publish: deduplicates in-queue messages with the same `id`
     * and enforces `maxDepthPerShard` to prevent unbounded shard queue growth.
     */
    publish(partitionKey, id, payload) {
        const shard = this.computeShard(partitionKey);
        const queue = this.queuesByShard.get(shard);
        if (id && queue.some((item) => item.id === id)) {
            return shard;
        }
        if (queue.length >= this.maxDepthPerShard) {
            throw new Error(`Shard ${shard} queue capacity (${this.maxDepthPerShard}) exceeded`);
        }
        queue.push({ id, payload });
        return shard;
    }
    consumeBatch(shard, maxBatch = 32) {
        const safeBatch = Number.isFinite(maxBatch) && maxBatch > 0 ? Math.floor(maxBatch) : 32;
        const q = this.queuesByShard.get(shard) ?? [];
        return q.splice(0, safeBatch);
    }
}
exports.ShardedQueueBrokerPool = ShardedQueueBrokerPool;
class SafeEpochGarbageCollector {
    gracePeriodMs;
    constructor(gracePeriodMs = 3_600_000) {
        this.gracePeriodMs = gracePeriodMs;
    }
    filterCollectibleSnapshots(snapshots, activeMinFencingToken, nowMs = Date.now()) {
        return snapshots
            .filter((s) => !s.referencedByActiveCell &&
            s.fencingToken < activeMinFencingToken &&
            nowMs - s.createdAtMs >= this.gracePeriodMs)
            .map((s) => s.key);
    }
}
exports.SafeEpochGarbageCollector = SafeEpochGarbageCollector;
class FleetHaAndAutoscalerController {
    evaluateFleetHealth(nodes) {
        const healthy = nodes.filter((n) => n.healthy);
        const unhealthyNodeIds = nodes.filter((n) => !n.healthy).map((n) => n.nodeId);
        const avgCpu = healthy.length > 0 ? healthy.reduce((a, b) => a + b.cpuUtilPct, 0) / healthy.length : 100;
        let scaleAction = "NONE";
        if (avgCpu >= 75.0 || unhealthyNodeIds.length > 0) {
            scaleAction = "SCALE_OUT";
        }
        else if (avgCpu <= 20.0 && healthy.length > 2) {
            scaleAction = "SCALE_IN";
        }
        return { healthyNodes: healthy.length, unhealthyNodeIds, scaleAction };
    }
}
exports.FleetHaAndAutoscalerController = FleetHaAndAutoscalerController;
/**
 * M16 TASK-76: `CELLD_DURABILITY=fleet` Peer-Replicated Log Pipeline (`CELLD_LOG_PIPELINE=4`),
 * Adaptive Hedged Log Append (`CELLD_LOG_HEDGE_MS`), & Automatic Single-Node `bucket` Fallback.
 */
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
    commitWriteWithFleetDurability(params) {
        const pipelineRoundsUsed = Math.min(this.logPipelineLimit, params.inFlightLogRounds ?? 1);
        if (this.mode === "bucket" || params.livePeerNodeCount < 1) {
            const effectiveDurabilityMode = this.mode === "bucket" ? "bucket" : "bucket_fallback";
            return {
                effectiveDurabilityMode,
                acknowledgedVia: "BUCKET_PUT",
                commitLatencyMs: params.bucketPutLatencyMs,
                hedgedCopyTriggered: false,
                pipelineRoundsUsed,
            };
        }
        const hedgeThresholdMs = this.computeAdaptiveHedgeThresholdMs();
        const hedgedCopyTriggered = params.peerFsyncLatencyMs >= hedgeThresholdMs;
        const effectivePeerLatencyMs = hedgedCopyTriggered
            ? Math.min(params.peerFsyncLatencyMs, hedgeThresholdMs + 18)
            : params.peerFsyncLatencyMs;
        this.slowestRecentAppendMs = Math.max(10, Math.min(200, effectivePeerLatencyMs));
        if (effectivePeerLatencyMs <= params.bucketPutLatencyMs) {
            return {
                effectiveDurabilityMode: "fleet",
                acknowledgedVia: hedgedCopyTriggered ? "HEDGED_PEER_COPY" : "PEER_FSYNC",
                commitLatencyMs: effectivePeerLatencyMs,
                hedgedCopyTriggered,
                pipelineRoundsUsed,
            };
        }
        return {
            effectiveDurabilityMode: "fleet",
            acknowledgedVia: "BUCKET_PUT",
            commitLatencyMs: params.bucketPutLatencyMs,
            hedgedCopyTriggered,
            pipelineRoundsUsed,
        };
    }
}
exports.FleetReplicatedDurabilityEngine = FleetReplicatedDurabilityEngine;
