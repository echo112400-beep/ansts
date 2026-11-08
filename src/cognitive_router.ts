/**
 * @fileoverview M3 TASK-08: Prefix-Cache Aware Cognitive Router with Automatic Degradation.
 */
import * as crypto from "node:crypto";
import { LlmEndpointMetrics, TaskPriority } from "./models.js";
import { SlidingWindowTokenBucket, ThreeStateCircuitBreaker } from "./router/breaker_limiter.js";

export class CognitiveRouter {
  private readonly endpoints: Map<string, LlmEndpointMetrics> = new Map();
  private readonly prefixRadixIndex: Map<string, string> = new Map();
  private readonly breakers: Map<string, ThreeStateCircuitBreaker> = new Map();
  private readonly limiters: Map<string, SlidingWindowTokenBucket> = new Map();
  private readonly ewmaAlpha: number;

  constructor(ewmaAlpha = 0.3) {
    this.ewmaAlpha = ewmaAlpha;
  }

  public registerEndpoint(ep: LlmEndpointMetrics, maxRpm = 1000, maxTpm = 2_000_000): void {
    this.endpoints.set(ep.endpoint_id, ep);
    this.breakers.set(ep.endpoint_id, new ThreeStateCircuitBreaker());
    this.limiters.set(ep.endpoint_id, new SlidingWindowTokenBucket(maxRpm, maxTpm));
  }

  public static computePrefixHash(systemPrompt: string, toolDefinitions: string, historyPrefix = ""): string {
    const raw = `${systemPrompt}||${toolDefinitions}||${historyPrefix.slice(0, 1024)}`;
    return crypto.createHash("sha256").update(raw, "utf-8").digest("hex").slice(0, 24);
  }

  public reportInvocationMetrics(
    endpointId: string,
    observedTtftMs: number,
    tokensUsed: number,
    isRateLimited = false
  ): void {
    const ep = this.endpoints.get(endpointId);
    if (!ep) return;
    const breaker = this.breakers.get(endpointId);
    ep.ttft_ewma_ms = this.ewmaAlpha * observedTtftMs + (1.0 - this.ewmaAlpha) * ep.ttft_ewma_ms;
    ep.tpm_used = Math.min(ep.tpm_limit, ep.tpm_used + tokensUsed);
    if (isRateLimited) {
      ep.rate_limit_hits_last_min += 1;
      breaker?.recordFailure();
      if (ep.rate_limit_hits_last_min >= 5) {
        ep.healthy = false;
      }
    } else {
      breaker?.recordSuccess();
    }
  }

  public selectEndpoint(
    prefixHash: string,
    estimatedTokens: number,
    priority: TaskPriority
  ): [LlmEndpointMetrics, boolean] {
    const cachedEpId = this.prefixRadixIndex.get(prefixHash);
    const candidates = Array.from(this.endpoints.values()).filter(
      (ep) =>
        ep.healthy &&
        (this.breakers.get(ep.endpoint_id)?.canPass() ?? true) &&
        ep.tpm_remaining >= estimatedTokens &&
        ep.active_requests < ep.max_concurrency
    );

    if (candidates.length === 0) {
      const fallback = Array.from(this.endpoints.values()).filter((ep) => ep.healthy);
      if (fallback.length === 0) {
        throw new Error("All LLM endpoints in CognitiveRouter are unhealthy");
      }
      fallback.sort((a, b) => a.active_requests - b.active_requests);
      return [fallback[0], false];
    }

    const scoreEndpoint = (ep: LlmEndpointMetrics): number => {
      const cacheHitBonus = ep.endpoint_id === cachedEpId ? 35.0 : 0.0;
      const ttftScore = Math.max(0.0, 40.0 * (1.0 - ep.ttft_ewma_ms / 2500.0));
      const quotaRatio = ep.tpm_remaining / Math.max(1, ep.tpm_limit);
      const quotaScore = 25.0 * quotaRatio;
      const penalty = ep.rate_limit_hits_last_min * 10.0;
      if (priority === TaskPriority.P3_BATCH && ep.endpoint_id === cachedEpId) {
        return cacheHitBonus * 1.5 + quotaScore - penalty;
      }
      return cacheHitBonus + ttftScore + quotaScore - penalty;
    };

    let bestEp = candidates[0];
    let bestScore = scoreEndpoint(bestEp);
    for (let i = 1; i < candidates.length; i++) {
      const s = scoreEndpoint(candidates[i]);
      if (s > bestScore) {
        bestScore = s;
        bestEp = candidates[i];
      }
    }

    const isCacheHit = bestEp.endpoint_id === cachedEpId;
    this.prefixRadixIndex.set(prefixHash, bestEp.endpoint_id);
    bestEp.active_requests += 1;
    bestEp.rpm_used += 1;
    return [bestEp, isCacheHit];
  }

  public releaseEndpoint(endpointId: string): void {
    const ep = this.endpoints.get(endpointId);
    if (ep) {
      ep.active_requests = Math.max(0, ep.active_requests - 1);
    }
  }
}
