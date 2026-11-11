"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CognitiveRouter = void 0;




var crypto = _interopRequireWildcard(require("node:crypto"));
var _models = require("./models.js");
var _breaker_limiter = require("./router/breaker_limiter.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M3 TASK-08, M52-2, M64-3 & M69-1: Prefix-Cache Aware Cognitive Router with Sliding-Window Quota Decay,
 * Half-Open Circuit-Breaker Single-Probe Stampede Guard, Bounded Prefix-Radix Index FIFO Eviction,
 * Accurate Fallback Concurrency Accounting, Finite Metric Guards & SQLite Persistence.
 */class CognitiveRouter {endpoints = new Map();
  prefixRadixIndex = new Map();
  breakers = new Map();
  limiters = new Map();
  lastWindowResetMs = new Map();
  rateLimitDegradedSet = new Set();
  ewmaAlpha;
  maxPrefixCacheEntries;
  sql;

  constructor(ewmaAlpha = 0.3, sql, maxPrefixCacheEntries = 4096) {
    this.ewmaAlpha = Number.isFinite(ewmaAlpha) && ewmaAlpha > 0 && ewmaAlpha <= 1 ? ewmaAlpha : 0.3;
    this.maxPrefixCacheEntries =
    Number.isFinite(maxPrefixCacheEntries) && maxPrefixCacheEntries >= 1 ?
    Math.floor(maxPrefixCacheEntries) :
    4096;
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  enforcePrefixIndexBound() {
    while (this.prefixRadixIndex.size > this.maxPrefixCacheEntries) {
      const oldestKey = this.prefixRadixIndex.keys().next().value;
      if (oldestKey === undefined) break;
      this.prefixRadixIndex.delete(oldestKey);
    }
  }

  getPrefixIndexSize() {
    return this.prefixRadixIndex.size;
  }

  getEndpointBreaker(endpointId) {
    return this.breakers.get(endpointId);
  }

  /**
   * [M64-3] Binds SQLite storage (`persisted_router_state`) so endpoint health, EWMA TTFT, TPM/RPM usage,
   * and prefix-cache routing affinity (`prefixRadixIndex`) survive restarts.
   */
  bindSqlStorage(sql) {
    this.sql = sql;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS persisted_router_state (endpoint_id TEXT PRIMARY KEY, payload_json TEXT)");

    const rows = this.sql.
    exec("SELECT * FROM persisted_router_state").
    toArray();
    let restored = 0;
    for (const r of rows) {
      const epId = String(r.endpoint_id ?? "");
      const parsed = JSON.parse(String(r.payload_json ?? "{}"));
      if (!epId) continue;
      if (epId === "__prefix_radix_index__") {
        const indexMap =
        parsed.entries && typeof parsed.entries === "object" ?
        parsed.entries :
        {};
        for (const [k, v] of Object.entries(indexMap)) {
          if (typeof v === "string") {
            this.prefixRadixIndex.set(k, v);
          }
        }
        this.enforcePrefixIndexBound();
        continue;
      }
      const ep = new _models.LlmEndpointMetrics({
        endpoint_id: epId,
        provider: String(parsed.provider ?? "openai-compatible"),
        model_name: String(parsed.model_name ?? parsed.modelName ?? epId),
        ttft_ewma_ms: Number(parsed.ttft_ewma_ms ?? parsed.ttftEwmaMs ?? 250.0),
        tpm_limit: Number(parsed.tpm_limit ?? 1_000_000),
        tpm_used: Number(parsed.tpm_used ?? 0),
        rpm_limit: Number(parsed.rpm_limit ?? 2000),
        rpm_used: Number(parsed.rpm_used ?? 0),
        max_concurrency: Number(parsed.max_concurrency ?? 200),
        active_requests: Number(parsed.active_requests ?? 0),
        healthy: parsed.healthy !== undefined ? Boolean(parsed.healthy) : true
      });
      ep.rate_limit_hits_last_min = Number(parsed.rate_limit_hits_last_min ?? 0);
      this.endpoints.set(epId, ep);
      if (!this.breakers.has(epId)) {
        this.breakers.set(epId, new _breaker_limiter.ThreeStateCircuitBreaker());
      }
      if (!this.limiters.has(epId)) {
        this.limiters.set(epId, new _breaker_limiter.SlidingWindowTokenBucket(ep.rpm_limit, ep.tpm_limit));
      }
      this.lastWindowResetMs.set(epId, Number(parsed.lastWindowResetMs ?? Date.now()));
      if (parsed.rateLimitDegraded) {
        this.rateLimitDegradedSet.add(epId);
      }
      restored += 1;
    }
    return restored;
  }

  persistEndpointState(epId) {
    if (!this.sql) return;
    const ep = this.endpoints.get(epId);
    if (!ep) return;
    this.sql.exec(
    "INSERT OR REPLACE INTO persisted_router_state (endpoint_id, payload_json) VALUES (?, ?)",
    epId,
    JSON.stringify({
      endpoint_id: ep.endpoint_id,
      provider: ep.provider,
      model_name: ep.model_name,
      ttft_ewma_ms: ep.ttft_ewma_ms,
      tpm_limit: ep.tpm_limit,
      tpm_used: ep.tpm_used,
      rpm_limit: ep.rpm_limit,
      rpm_used: ep.rpm_used,
      max_concurrency: ep.max_concurrency,
      active_requests: ep.active_requests,
      rate_limit_hits_last_min: ep.rate_limit_hits_last_min,
      healthy: ep.healthy,
      lastWindowResetMs: this.lastWindowResetMs.get(epId) ?? Date.now(),
      rateLimitDegraded: this.rateLimitDegradedSet.has(epId)
    }));

  }

  persistPrefixRadixIndex() {
    if (!this.sql) return;
    const entries = {};
    for (const [k, v] of this.prefixRadixIndex.entries()) {
      entries[k] = v;
    }
    this.sql.exec(
    "INSERT OR REPLACE INTO persisted_router_state (endpoint_id, payload_json) VALUES (?, ?)",
    "__prefix_radix_index__",
    JSON.stringify({ entries }));

  }

  registerEndpoint(ep, maxRpm = 1000, maxTpm = 2_000_000, nowMs = Date.now()) {
    this.endpoints.set(ep.endpoint_id, ep);
    this.breakers.set(ep.endpoint_id, new _breaker_limiter.ThreeStateCircuitBreaker());
    this.limiters.set(ep.endpoint_id, new _breaker_limiter.SlidingWindowTokenBucket(maxRpm, maxTpm, nowMs));
    this.lastWindowResetMs.set(ep.endpoint_id, nowMs);
    this.persistEndpointState(ep.endpoint_id);
  }

  getEndpoint(endpointId) {
    return this.endpoints.get(endpointId);
  }

  listEndpoints() {
    return Array.from(this.endpoints.values());
  }

  static computePrefixHash(systemPrompt, toolDefinitions, historyPrefix = "") {
    const raw = `${systemPrompt}||${toolDefinitions}||${historyPrefix.slice(0, 1024)}`;
    return crypto.createHash("sha256").update(raw, "utf-8").digest("hex").slice(0, 24);
  }

  /**
   * [M52-2] Refreshes 60-second TPM/RPM/rate-limit windows and auto-recovers rate-limit-degraded
   * endpoints once their circuit breaker cooldown enters `HALF_OPEN` or `CLOSED`.
   */
  refreshEndpointWindowsAndHalfOpenProbes(nowMs = Date.now()) {
    const recoveredEndpointIds = [];
    for (const [epId, ep] of this.endpoints.entries()) {
      const lastReset = this.lastWindowResetMs.get(epId) ?? 0;
      let mutated = false;
      if (nowMs - lastReset >= 60_000) {
        ep.tpm_used = 0;
        ep.rpm_used = 0;
        ep.rate_limit_hits_last_min = 0;
        this.lastWindowResetMs.set(epId, nowMs);
        mutated = true;
      }
      const breaker = this.breakers.get(epId);
      if (!ep.healthy && this.rateLimitDegradedSet.has(epId) && breaker && breaker.canPass(nowMs)) {
        ep.healthy = true;
        ep.rate_limit_hits_last_min = 0;
        this.rateLimitDegradedSet.delete(epId);
        recoveredEndpointIds.push(epId);
        mutated = true;
      }
      if (mutated) {
        this.persistEndpointState(epId);
      }
    }
    return recoveredEndpointIds;
  }

  reportInvocationMetrics(
  endpointId,
  observedTtftMs,
  tokensUsed,
  isRateLimited = false,
  nowMs = Date.now())
  {
    const ep = this.endpoints.get(endpointId);
    if (!ep) return;
    const breaker = this.breakers.get(endpointId);
    if (Number.isFinite(observedTtftMs) && observedTtftMs >= 0) {
      ep.ttft_ewma_ms = this.ewmaAlpha * observedTtftMs + (1.0 - this.ewmaAlpha) * ep.ttft_ewma_ms;
    }
    const safeTokens = Number.isFinite(tokensUsed) && tokensUsed > 0 ? Math.floor(tokensUsed) : 0;
    ep.tpm_used = Math.min(ep.tpm_limit, ep.tpm_used + safeTokens);
    if (isRateLimited) {
      ep.rate_limit_hits_last_min += 1;
      breaker?.recordFailure(nowMs);
      if (ep.rate_limit_hits_last_min >= 5) {
        ep.healthy = false;
        this.rateLimitDegradedSet.add(endpointId);
      }
    } else {
      breaker?.recordSuccess();
      this.rateLimitDegradedSet.delete(endpointId);
    }
    this.persistEndpointState(endpointId);
  }

  /**
   * [M69-1] Acquires the Half-Open single-probe lock (`breaker.tryAcquireRequestSlot(nowMs)`) ONLY on the
   * winning endpoint so concurrent requests during `HALF_OPEN` recovery never stampede the recovering endpoint,
   * and enforces FIFO eviction on `prefixRadixIndex` (`maxPrefixCacheEntries`).
   */
  selectEndpoint(
  prefixHash,
  estimatedTokens,
  priority,
  nowMs = Date.now())
  {
    this.refreshEndpointWindowsAndHalfOpenProbes(nowMs);
    const cachedEpId = this.prefixRadixIndex.get(prefixHash);
    const safeTokens = Number.isFinite(estimatedTokens) && estimatedTokens >= 0 ? estimatedTokens : 0;
    const candidates = Array.from(this.endpoints.values()).filter(
    (ep) =>
    ep.healthy && (
    this.breakers.get(ep.endpoint_id)?.canPass(nowMs) ?? true) &&
    ep.tpm_remaining >= safeTokens &&
    ep.active_requests < ep.max_concurrency);


    if (candidates.length === 0) {
      const fallback = Array.from(this.endpoints.values()).filter(
      (ep) => ep.healthy && (this.breakers.get(ep.endpoint_id)?.canPass(nowMs) ?? true));

      const pool = fallback.length > 0 ? fallback : Array.from(this.endpoints.values()).filter((ep) => ep.healthy);
      if (pool.length === 0) {
        throw new Error("All LLM endpoints in CognitiveRouter are unhealthy");
      }
      pool.sort((a, b) => a.active_requests - b.active_requests);
      const chosen = pool[0];
      this.breakers.get(chosen.endpoint_id)?.tryAcquireRequestSlot(nowMs);
      chosen.active_requests += 1;
      chosen.rpm_used += 1;
      this.persistEndpointState(chosen.endpoint_id);
      return [chosen, false];
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

    // Lock the Half-Open single-probe slot on the selected endpoint
    this.breakers.get(bestEp.endpoint_id)?.tryAcquireRequestSlot(nowMs);

    const isCacheHit = bestEp.endpoint_id === cachedEpId;
    if (this.prefixRadixIndex.has(prefixHash)) {
      this.prefixRadixIndex.delete(prefixHash);
    }
    this.prefixRadixIndex.set(prefixHash, bestEp.endpoint_id);
    this.enforcePrefixIndexBound();
    bestEp.active_requests += 1;
    bestEp.rpm_used += 1;
    this.persistEndpointState(bestEp.endpoint_id);
    this.persistPrefixRadixIndex();
    return [bestEp, isCacheHit];
  }

  releaseEndpoint(endpointId) {
    const ep = this.endpoints.get(endpointId);
    if (ep) {
      ep.active_requests = Math.max(0, ep.active_requests - 1);
      this.breakers.get(endpointId)?.releaseHalfOpenProbe();
      this.persistEndpointState(endpointId);
    }
  }
}exports.CognitiveRouter = CognitiveRouter;