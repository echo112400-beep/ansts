"use strict";
/**
 * @fileoverview M24 (TASK-113 & TASK-114): Joint Compute-LLM Prefix-Cache Affinity Co-Scheduler
 * (`PrefixCacheAffinityRouter`).
 *
 * Implements:
 * 1. Rolling SHA-256 Prefix Fingerprint (`computePrefixFingerprint`) over system prompts and
 *    repository static context so parallel sub-agents and multi-turn sessions reuse warm GPU KV-Cache.
 * 2. Joint Compute-LLM Cost Function combining KV-Cache hit ratio, estimated TTFT reduction,
 *    FinOps cached-token billing discount (up to 80% off cached input tokens), and 5D W-DRF
 *    compute shard utilization to prevent hot-spotting a single inference backend.
 */
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
exports.PrefixCacheAffinityRouter = void 0;
const crypto = __importStar(require("node:crypto"));
class PrefixCacheAffinityRouter {
    backends = new Map();
    overloadSaturationThreshold = 0.85;
    cachedTokenDiscountRatio = 0.80;
    registerBackendNode(node) {
        this.backends.set(node.backendId, {
            ...node,
            cachedPrefixFingerprints: new Map(),
        });
    }
    computePrefixFingerprint(repoId, staticSystemContext) {
        return crypto
            .createHash("sha256")
            .update(`${repoId}::${staticSystemContext}`, "utf8")
            .digest("hex")
            .slice(0, 32);
    }
    /**
     * Jointly scores all healthy inference backends in the target model pool using:
     * - KV-Cache presence for `prefixFingerprint` (reduces TTFT by ~65% and applies 80% FinOps token discount)
     * - Current backend concurrency load + co-scheduled `computeShardUtilization` (0.0 .. 1.0)
     */
    routeWithJointPrefixAffinity(params) {
        const nowMs = params.nowMs ?? Date.now();
        const prefixFingerprint = this.computePrefixFingerprint(params.repoId, params.staticSystemContext);
        const candidates = Array.from(this.backends.values()).filter((b) => b.healthy && b.modelPool === params.preferredPool);
        if (candidates.length === 0) {
            throw new Error(`503 NoHealthyInferenceBackendInPool: ${params.preferredPool}`);
        }
        let bestNode = null;
        let bestScore = Number.POSITIVE_INFINITY;
        let bestKvHit = false;
        let fallbackDueToOverload = false;
        for (const node of candidates) {
            const loadRatio = node.inFlightRequests / Math.max(1, node.maxConcurrency);
            const hasCache = node.cachedPrefixFingerprints.has(prefixFingerprint);
            const isOverloaded = loadRatio >= this.overloadSaturationThreshold;
            if (hasCache && isOverloaded) {
                fallbackDueToOverload = true;
            }
            // Effective TTFT: KV-Cache hit cuts prefill latency by 65%
            const effectiveTtftMs = hasCache && !isOverloaded ? node.baseTtftMs * 0.35 : node.baseTtftMs;
            const computePenalty = (params.computeShardUtilization ?? 0) * 50;
            const loadPenalty = loadRatio * 200 + (isOverloaded ? 1000 : 0);
            const cacheBonus = hasCache && !isOverloaded ? -120 : 0;
            const score = effectiveTtftMs + loadPenalty + computePenalty + cacheBonus;
            if (score < bestScore) {
                bestScore = score;
                bestNode = node;
                bestKvHit = hasCache && !isOverloaded;
            }
        }
        const chosen = bestNode;
        chosen.cachedPrefixFingerprints.set(prefixFingerprint, {
            cachedTokens: params.prefixTokenCount,
            lastAccessedMs: nowMs,
        });
        const estimatedTtftMs = Number((bestKvHit ? chosen.baseTtftMs * 0.35 : chosen.baseTtftMs).toFixed(2));
        return {
            selectedBackendId: chosen.backendId,
            modelPool: chosen.modelPool,
            prefixFingerprint,
            kvCacheHit: bestKvHit,
            estimatedTtftMs,
            finopsDiscountRatio: bestKvHit ? this.cachedTokenDiscountRatio : 0,
            jointScore: Number(bestScore.toFixed(2)),
            fallbackDueToOverload,
        };
    }
}
exports.PrefixCacheAffinityRouter = PrefixCacheAffinityRouter;
