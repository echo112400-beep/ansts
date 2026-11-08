"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ShardedQueueBrokerPool = exports.SafeEpochGarbageCollector = exports.RpoZeroDurabilityEngine = exports.FleetHaAndAutoscalerController = exports.DurableWorkflowContext = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
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

  constructor(sql, workflowId) {
    this.sql = sql;
    this.workflowId = workflowId;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS workflow_steps (step_key TEXT PRIMARY KEY, workflow_id TEXT, output_json TEXT, completed_at INTEGER)");

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
    Date.now());

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
    nowMs - s.createdAtMs >= this.gracePeriodMs).

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
}exports.FleetHaAndAutoscalerController = FleetHaAndAutoscalerController;