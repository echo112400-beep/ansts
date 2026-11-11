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
exports.BrainBodyContainerPoolScaler = exports.DynamicQueueAutoSharder = exports.PartitionedSchedulerRouter = void 0;
/**
 * @fileoverview M9 TASK-32 ~ TASK-34, M63-2 & M64-5: 64-Shard Consistent Hash Partitioned Scheduler Router,
 * Dynamic Queue Auto-Sharder & Brain-Body Container Pool Scaler with Custom State Factory, Multi-Shard Node Registration,
 * Multi-Shard Tick Scheduling & Zero-Division/NaN Guards.
 */
const crypto = __importStar(require("node:crypto"));
const scheduler_cell_js_1 = require("./scheduler_cell.js");
const env_js_1 = require("./env.js");
class PartitionedSchedulerRouter {
    numPartitions;
    partitions = new Map();
    constructor(numPartitions = 64, stateFactory) {
        this.numPartitions = Math.max(1, Math.floor(numPartitions));
        for (let i = 0; i < this.numPartitions; i++) {
            const state = stateFactory ? stateFactory(i) : new env_js_1.MockDurableObjectState(`scheduler-shard-${i}`);
            this.partitions.set(i, new scheduler_cell_js_1.AnstsSchedulerCell(state));
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
    registerNodeAcrossShards(hb) {
        for (const cell of this.partitions.values()) {
            cell.registerNode(hb);
        }
    }
    async routeSubmitTask(spec, options) {
        const { shardId, cell } = this.getCellForTask(spec.tenant_id, spec.task_id);
        const submitted = await cell.submitTask(spec, options);
        return { shardId, submitted };
    }
    async scheduleTickAcrossShards() {
        const totalPlacements = [];
        const totalRecoveredIds = [];
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
exports.PartitionedSchedulerRouter = PartitionedSchedulerRouter;
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
    adjustShards(messagesPerSecond, maxWriteRpsPerShard = 450) {
        const prev = this.currentShards;
        const safeMps = Number.isFinite(messagesPerSecond) && messagesPerSecond >= 0 ? messagesPerSecond : 0;
        const safeRpsPerShard = Number.isFinite(maxWriteRpsPerShard) && maxWriteRpsPerShard > 0 ? maxWriteRpsPerShard : 450;
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
exports.DynamicQueueAutoSharder = DynamicQueueAutoSharder;
class BrainBodyContainerPoolScaler {
    computeTargetSandboxContainers(params) {
        const safeThink = Number.isFinite(params.activeThinkCells) && params.activeThinkCells >= 0
            ? Math.floor(params.activeThinkCells)
            : 0;
        const safeAct = Number.isFinite(params.activeActCells) && params.activeActCells >= 0
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
exports.BrainBodyContainerPoolScaler = BrainBodyContainerPoolScaler;
