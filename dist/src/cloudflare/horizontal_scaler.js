"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.PartitionedSchedulerRouter = exports.DynamicQueueAutoSharder = exports.BrainBodyContainerPoolScaler = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));
var _scheduler_cell = require("./scheduler_cell.js");
var _env = require("./env.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M9 TASK-32 ~ TASK-34 & M63-2: 64-Shard Consistent Hash Partitioned Scheduler Router,
 * Dynamic Queue Auto-Sharder & Brain-Body Container Pool Scaler with Zero-Division & NaN Guards.
 */class PartitionedSchedulerRouter {
  numPartitions;
  partitions = new Map();

  constructor(numPartitions = 64) {
    this.numPartitions = Math.max(1, Math.floor(numPartitions));
    for (let i = 0; i < this.numPartitions; i++) {
      const state = new _env.MockDurableObjectState(`scheduler-shard-${i}`);
      this.partitions.set(i, new _scheduler_cell.AnstsSchedulerCell(state));
    }
  }

  getShardIndex(tenantId, taskId) {
    const key = `${tenantId}:${taskId}`;
    const digest = crypto.createHash("sha256").update(key, "utf-8").digest();
    return digest.readUInt32BE(0) % this.numPartitions;
  }

  getCellForTask(tenantId, taskId) {
    const shardId = this.getShardIndex(tenantId, taskId);
    return { shardId, cell: this.partitions.get(shardId) };
  }

  async routeSubmitTask(spec) {
    const { shardId, cell } = this.getCellForTask(spec.tenant_id, spec.task_id);
    const submitted = await cell.submitTask(spec);
    return { shardId, submitted };
  }
}exports.PartitionedSchedulerRouter = PartitionedSchedulerRouter;

class DynamicQueueAutoSharder {
  currentShards;
  minShards;
  maxShards;

  constructor(initialShards = 16, minShards = 4, maxShards = 128) {
    this.minShards = Math.max(1, Math.floor(minShards));
    this.maxShards = Math.max(this.minShards, Math.floor(maxShards));
    this.currentShards = Math.max(this.minShards, Math.min(this.maxShards, Math.floor(initialShards)));
  }

  /**
   * [M63-2] DFX-Availability: Guards against `maxWriteRpsPerShard <= 0` or `NaN` `messagesPerSecond`
   * so `currentShards` never becomes `NaN` or `Infinity`.
   */
  adjustShards(messagesPerSecond, maxWriteRpsPerShard = 450)



  {
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
      rebalanced: prev !== this.currentShards
    };
  }
}exports.DynamicQueueAutoSharder = DynamicQueueAutoSharder;

class BrainBodyContainerPoolScaler {
  computeTargetSandboxContainers(params)








  {
    const safeThink =
    Number.isFinite(params.activeThinkCells) && params.activeThinkCells >= 0 ?
    Math.floor(params.activeThinkCells) :
    0;
    const safeAct =
    Number.isFinite(params.activeActCells) && params.activeActCells >= 0 ?
    Math.floor(params.activeActCells) :
    0;
    const rawRatio = params.warmStandbyRatio ?? 0.25;
    const ratio = Number.isFinite(rawRatio) && rawRatio >= 0 ? Math.min(2.0, rawRatio) : 0.25;
    const brainIsolateMemoryMb = (safeThink + safeAct) * 8;
    const requiredHeavyActContainers = safeAct;
    const warmBufferContainers = Math.max(2, Math.ceil(safeThink * ratio));
    return {
      brainIsolateMemoryMb,
      requiredHeavyActContainers,
      warmBufferContainers,
      totalContainerPoolSize: requiredHeavyActContainers + warmBufferContainers
    };
  }
}exports.BrainBodyContainerPoolScaler = BrainBodyContainerPoolScaler;