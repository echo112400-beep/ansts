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

import * as crypto from "node:crypto";

export interface LlmInferenceBackendNode {
  backendId: string;
  modelPool: "DeepSeek" | "GLM-5.3" | "Qwen-3.8";
  baseTtftMs: number;
  inFlightRequests: number;
  maxConcurrency: number;
  healthy: boolean;
  cachedPrefixFingerprints: Map<string, { cachedTokens: number; lastAccessedMs: number }>;
}

export interface JointAffinityRouteDecision {
  selectedBackendId: string;
  modelPool: "DeepSeek" | "GLM-5.3" | "Qwen-3.8";
  prefixFingerprint: string;
  kvCacheHit: boolean;
  estimatedTtftMs: number;
  finopsDiscountRatio: number; // e.g., 0.80 means 80% discount on cached prefix tokens
  jointScore: number;
  fallbackDueToOverload: boolean;
}

export class PrefixCacheAffinityRouter {
  public readonly backends = new Map<string, LlmInferenceBackendNode>();
  public readonly overloadSaturationThreshold = 0.85;
  public readonly cachedTokenDiscountRatio = 0.80;

  public registerBackendNode(node: Omit<LlmInferenceBackendNode, "cachedPrefixFingerprints">): void {
    this.backends.set(node.backendId, {
      ...node,
      cachedPrefixFingerprints: new Map(),
    });
  }

  public computePrefixFingerprint(repoId: string, staticSystemContext: string): string {
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
  public routeWithJointPrefixAffinity(params: {
    repoId: string;
    staticSystemContext: string;
    prefixTokenCount: number;
    preferredPool: "DeepSeek" | "GLM-5.3" | "Qwen-3.8";
    computeShardUtilization?: number;
    nowMs?: number;
  }): JointAffinityRouteDecision {
    const nowMs = params.nowMs ?? Date.now();
    const prefixFingerprint = this.computePrefixFingerprint(params.repoId, params.staticSystemContext);

    const candidates = Array.from(this.backends.values()).filter(
      (b) => b.healthy && b.modelPool === params.preferredPool,
    );
    if (candidates.length === 0) {
      throw new Error(`503 NoHealthyInferenceBackendInPool: ${params.preferredPool}`);
    }

    let bestNode: LlmInferenceBackendNode | null = null;
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

    const chosen = bestNode!;
    chosen.cachedPrefixFingerprints.set(prefixFingerprint, {
      cachedTokens: params.prefixTokenCount,
      lastAccessedMs: nowMs,
    });

    const estimatedTtftMs = Number(
      (bestKvHit ? chosen.baseTtftMs * 0.35 : chosen.baseTtftMs).toFixed(2),
    );

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
