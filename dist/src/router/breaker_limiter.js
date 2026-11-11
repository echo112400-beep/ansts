"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ThreeStateCircuitBreaker = exports.SlidingWindowTokenBucket = void 0;
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
        if (!Number.isFinite(tokens) || tokens < 0 || this.maxRpm <= 0 || this.maxTpm <= 0 || tokens > this.maxTpm) {
            return { allowed: false, waitMs: -1, unserviceable: true };
        }
        this.refill(nowMs);
        if (this.remainingRequests >= 1 && this.remainingTokens >= tokens) {
            this.remainingRequests -= 1;
            this.remainingTokens -= tokens;
            return { allowed: true, waitMs: 0, unserviceable: false };
        }
        const deficitTokens = Math.max(0, tokens - this.remainingTokens);
        const tokenWaitMs = (deficitTokens / this.maxTpm) * 60_000;
        const reqWaitMs = this.remainingRequests < 1 ? (1 / this.maxRpm) * 60_000 : 0;
        return {
            allowed: false,
            waitMs: Math.ceil(Math.max(tokenWaitMs, reqWaitMs, 50)),
            unserviceable: false,
        };
    }
    refill(nowMs) {
        const elapsedMin = Math.max(0, (nowMs - this.lastRefillMs) / 60_000);
        if (elapsedMin > 0) {
            this.remainingRequests = Math.min(this.maxRpm, this.remainingRequests + elapsedMin * this.maxRpm);
            this.remainingTokens = Math.min(this.maxTpm, this.remainingTokens + elapsedMin * this.maxTpm);
            this.lastRefillMs = nowMs;
        }
    }
}
exports.SlidingWindowTokenBucket = SlidingWindowTokenBucket;
class ThreeStateCircuitBreaker {
    state = "CLOSED";
    consecutiveFailures = 0;
    openedAtMs = 0;
    halfOpenProbeInFlight = false;
    halfOpenProbeStartedAtMs = 0;
    failureThreshold;
    openCooldownMs;
    probeTimeoutMs;
    constructor(failureThreshold = 3, openCooldownMs = 15_000, probeTimeoutMs = 10_000) {
        this.failureThreshold = failureThreshold;
        this.openCooldownMs = openCooldownMs;
        this.probeTimeoutMs = probeTimeoutMs;
    }
    getState(nowMs = Date.now()) {
        if (this.state === "OPEN" && nowMs - this.openedAtMs >= this.openCooldownMs) {
            this.state = "HALF_OPEN";
            this.halfOpenProbeInFlight = false;
            this.halfOpenProbeStartedAtMs = 0;
        }
        if (this.state === "HALF_OPEN" &&
            this.halfOpenProbeInFlight &&
            this.halfOpenProbeStartedAtMs > 0 &&
            nowMs - this.halfOpenProbeStartedAtMs >= this.probeTimeoutMs) {
            // Auto-reclaim stuck half-open probe slot if probe timed out without reporting
            this.halfOpenProbeInFlight = false;
            this.halfOpenProbeStartedAtMs = 0;
        }
        return this.state;
    }
    canPass(nowMs = Date.now()) {
        const current = this.getState(nowMs);
        if (current === "CLOSED")
            return true;
        if (current === "HALF_OPEN") {
            return !this.halfOpenProbeInFlight;
        }
        return false;
    }
    /**
     * [M54-2] Single-probe admission guard in `HALF_OPEN` state to prevent thundering-herd stampedes.
     * Only 1 probe request is admitted while `HALF_OPEN`; concurrent callers receive `false` until the probe resolves or times out.
     */
    tryAcquireRequestSlot(nowMs = Date.now()) {
        const current = this.getState(nowMs);
        if (current === "CLOSED") {
            return { admitted: true, state: "CLOSED", isHalfOpenProbe: false };
        }
        if (current === "HALF_OPEN") {
            if (this.halfOpenProbeInFlight) {
                return { admitted: false, state: "HALF_OPEN", isHalfOpenProbe: false };
            }
            this.halfOpenProbeInFlight = true;
            this.halfOpenProbeStartedAtMs = nowMs;
            return { admitted: true, state: "HALF_OPEN", isHalfOpenProbe: true };
        }
        return { admitted: false, state: "OPEN", isHalfOpenProbe: false };
    }
    releaseHalfOpenProbe() {
        if (this.state === "HALF_OPEN") {
            this.halfOpenProbeInFlight = false;
            this.halfOpenProbeStartedAtMs = 0;
        }
    }
    recordSuccess() {
        this.consecutiveFailures = 0;
        this.state = "CLOSED";
        this.halfOpenProbeInFlight = false;
        this.halfOpenProbeStartedAtMs = 0;
    }
    recordFailure(nowMs = Date.now()) {
        this.consecutiveFailures += 1;
        this.halfOpenProbeInFlight = false;
        this.halfOpenProbeStartedAtMs = 0;
        if (this.state === "HALF_OPEN" || this.consecutiveFailures >= this.failureThreshold) {
            if (this.state !== "OPEN" || nowMs - this.openedAtMs >= this.openCooldownMs) {
                this.openedAtMs = nowMs;
            }
            this.state = "OPEN";
        }
    }
}
exports.ThreeStateCircuitBreaker = ThreeStateCircuitBreaker;
