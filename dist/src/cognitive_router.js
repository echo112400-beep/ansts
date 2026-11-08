"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CognitiveRouter = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));
var _models = require("./models.js");
var _breaker_limiter = require("./router/breaker_limiter.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M3 TASK-08: Prefix-Cache Aware Cognitive Router with Automatic Degradation.
 */class CognitiveRouter {
  endpoints = new Map();
  prefixRadixIndex = new Map();
  breakers = new Map();
  limiters = new Map();
  ewmaAlpha;

  constructor(ewmaAlpha = 0.3) {
    this.ewmaAlpha = ewmaAlpha;
  }

  registerEndpoint(ep, maxRpm = 1000, maxTpm = 2_000_000) {
    this.endpoints.set(ep.endpoint_id, ep);
    this.breakers.set(ep.endpoint_id, new _breaker_limiter.ThreeStateCircuitBreaker());
    this.limiters.set(ep.endpoint_id, new _breaker_limiter.SlidingWindowTokenBucket(maxRpm, maxTpm));
  }

  static computePrefixHash(systemPrompt, toolDefinitions, historyPrefix = "") {
    const raw = `${systemPrompt}||${toolDefinitions}||${historyPrefix.slice(0, 1024)}`;
    return crypto.createHash("sha256").update(raw, "utf-8").digest("hex").slice(0, 24);
  }

  reportInvocationMetrics(
  endpointId,
  observedTtftMs,
  tokensUsed,
  isRateLimited = false)
  {
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

  selectEndpoint(
  prefixHash,
  estimatedTokens,
  priority)
  {
    const cachedEpId = this.prefixRadixIndex.get(prefixHash);
    const candidates = Array.from(this.endpoints.values()).filter(
    (ep) =>
    ep.healthy && (
    this.breakers.get(ep.endpoint_id)?.canPass() ?? true) &&
    ep.tpm_remaining >= estimatedTokens &&
    ep.active_requests < ep.max_concurrency);


    if (candidates.length === 0) {
      const fallback = Array.from(this.endpoints.values()).filter((ep) => ep.healthy);
      if (fallback.length === 0) {
        throw new Error("All LLM endpoints in CognitiveRouter are unhealthy");
      }
      fallback.sort((a, b) => a.active_requests - b.active_requests);
      return [fallback[0], false];
    }

    const scoreEndpoint = (ep) => {
      const cacheHitBonus = ep.endpoint_id === cachedEpId ? 35.0 : 0.0;
      const ttftScore = Math.max(0.0, 40.0 * (1.0 - ep.ttft_ewma_ms / 2500.0));
      const quotaRatio = ep.tpm_remaining / Math.max(1, ep.tpm_limit);
      const quotaScore = 25.0 * quotaRatio;
      const penalty = ep.rate_limit_hits_last_min * 10.0;
      if (priority === _models.TaskPriority.P3_BATCH && ep.endpoint_id === cachedEpId) {
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

  releaseEndpoint(endpointId) {
    const ep = this.endpoints.get(endpointId);
    if (ep) {
      ep.active_requests = Math.max(0, ep.active_requests - 1);
    }
  }
}exports.CognitiveRouter = CognitiveRouter;