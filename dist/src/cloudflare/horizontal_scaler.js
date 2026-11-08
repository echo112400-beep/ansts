"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.PartitionedSchedulerRouter = exports.DynamicQueueAutoSharder = exports.BrainBodyContainerPoolScaler = void 0;Object.defineProperty(exports, "VirtualHashSlotRingManager", { enumerable: true, get: function () {return _manual_scale_controller.VirtualHashSlotRingManager;} });


var crypto = _interopRequireWildcard(require("node:crypto"));
var _scheduler_cell = require("./scheduler_cell.js");
var _env = require("./env.js");







var _manual_scale_controller = require("../cluster/manual_scale_controller.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M9 TASK-32 ~ TASK-34: 64-Shard Consistent Hash Partitioned Scheduler Router, Dynamic Queue Auto-Sharder & Brain-Body Container Pool Scaler.
 */ /**
 * The virtual slot ring (consistent-hash ring + 256-slot ledger) is owned by the fleet manual scale
 * controller; it is re-exported here because the scheduler HA mesh imports the slot ledger API from
 * this module.
 */
/** Per-shard counters of the stateless router mesh, read by the console shard matrix. */






class PartitionedSchedulerRouter {
  numPartitions;
  partitions = new Map();
  /** `scheduler-shard-<n> -> live router counters` for every shard the router owns. */
  shards = new Map();

  constructor(numPartitionsOrConfig = 64) {
    const numPartitions =
    typeof numPartitionsOrConfig === "number" ?
    numPartitionsOrConfig :
    numPartitionsOrConfig.initialShardCount;
    this.numPartitions = numPartitions;
    for (let i = 0; i < numPartitions; i++) {
      const shardId = `scheduler-shard-${String(i).padStart(2, "0")}`;
      const state = new _env.MockDurableObjectState(shardId);
      this.partitions.set(i, new _scheduler_cell.AnstsSchedulerCell(state));
      this.shards.set(shardId, {
        shardId,
        // No latency samples recorded yet; the console renders its cold-start fallback until observed.
        ewmaLatencyMs: 0,
        inFlightRequests: 0
      });
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
    this.currentShards = initialShards;
    this.minShards = minShards;
    this.maxShards = maxShards;
  }

  adjustShards(messagesPerSecond, maxWriteRpsPerShard = 450)



  {
    const prev = this.currentShards;
    const required = Math.ceil(messagesPerSecond / maxWriteRpsPerShard);
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
    const ratio = params.warmStandbyRatio ?? 0.25;
    const brainIsolateMemoryMb = (params.activeThinkCells + params.activeActCells) * 8;
    const requiredHeavyActContainers = params.activeActCells;
    const warmBufferContainers = Math.max(2, Math.ceil(params.activeThinkCells * ratio));
    return {
      brainIsolateMemoryMb,
      requiredHeavyActContainers,
      warmBufferContainers,
      totalContainerPoolSize: requiredHeavyActContainers + warmBufferContainers
    };
  }
}exports.BrainBodyContainerPoolScaler = BrainBodyContainerPoolScaler;