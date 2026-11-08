"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ThreeStateCircuitBreaker = exports.SlidingWindowTokenBucket = void 0; /**
 * @fileoverview M3 TASK-07: Dual-Token Bucket Rate Limiter (TPM/RPM) & 3-State Circuit Breaker.
 */


class SlidingWindowTokenBucket {
  maxRpm;
  maxTpm;
  remainingRequests;
  remainingTokens;
  lastRefillMs;

  constructor(maxRpm, maxTpm, nowMs = Date.now()) {
    this.maxRpm = maxRpm;
    this.maxTpm = maxTpm;
    this.remainingRequests = maxRpm;
    this.remainingTokens = maxTpm;
    this.lastRefillMs = nowMs;
  }

  tryConsume(tokens, nowMs = Date.now()) {
    this.refill(nowMs);
    if (this.remainingRequests >= 1 && this.remainingTokens >= tokens) {
      this.remainingRequests -= 1;
      this.remainingTokens -= tokens;
      return { allowed: true, waitMs: 0 };
    }
    const deficitTokens = Math.max(0, tokens - this.remainingTokens);
    const tokenWaitMs = deficitTokens / this.maxTpm * 60_000;
    const reqWaitMs = this.remainingRequests < 1 ? 1 / this.maxRpm * 60_000 : 0;
    return { allowed: false, waitMs: Math.ceil(Math.max(tokenWaitMs, reqWaitMs, 50)) };
  }

  refill(nowMs) {
    const elapsedMin = Math.max(0, (nowMs - this.lastRefillMs) / 60_000);
    if (elapsedMin > 0) {
      this.remainingRequests = Math.min(this.maxRpm, this.remainingRequests + elapsedMin * this.maxRpm);
      this.remainingTokens = Math.min(this.maxTpm, this.remainingTokens + elapsedMin * this.maxTpm);
      this.lastRefillMs = nowMs;
    }
  }
}exports.SlidingWindowTokenBucket = SlidingWindowTokenBucket;

class ThreeStateCircuitBreaker {
  state = "CLOSED";
  consecutiveFailures = 0;
  openedAtMs = 0;
  failureThreshold;
  openCooldownMs;

  constructor(failureThreshold = 3, openCooldownMs = 15_000) {
    this.failureThreshold = failureThreshold;
    this.openCooldownMs = openCooldownMs;
  }

  getState(nowMs = Date.now()) {
    if (this.state === "OPEN" && nowMs - this.openedAtMs >= this.openCooldownMs) {
      this.state = "HALF_OPEN";
    }
    return this.state;
  }

  canPass(nowMs = Date.now()) {
    const current = this.getState(nowMs);
    return current === "CLOSED" || current === "HALF_OPEN";
  }

  recordSuccess() {
    this.consecutiveFailures = 0;
    this.state = "CLOSED";
  }

  recordFailure(nowMs = Date.now()) {
    this.consecutiveFailures += 1;
    if (this.state === "HALF_OPEN" || this.consecutiveFailures >= this.failureThreshold) {
      this.state = "OPEN";
      this.openedAtMs = nowMs;
    }
  }
}exports.ThreeStateCircuitBreaker = ThreeStateCircuitBreaker;