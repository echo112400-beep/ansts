/**
 * @fileoverview M3 TASK-07: Dual-Token Bucket Rate Limiter (TPM/RPM) & 3-State Circuit Breaker.
 */
export type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

export class SlidingWindowTokenBucket {
  private readonly maxRpm: number;
  private readonly maxTpm: number;
  private remainingRequests: number;
  private remainingTokens: number;
  private lastRefillMs: number;

  constructor(maxRpm: number, maxTpm: number, nowMs = Date.now()) {
    this.maxRpm = maxRpm;
    this.maxTpm = maxTpm;
    this.remainingRequests = maxRpm;
    this.remainingTokens = maxTpm;
    this.lastRefillMs = nowMs;
  }

  public tryConsume(tokens: number, nowMs = Date.now()): { allowed: boolean; waitMs: number } {
    this.refill(nowMs);
    if (this.remainingRequests >= 1 && this.remainingTokens >= tokens) {
      this.remainingRequests -= 1;
      this.remainingTokens -= tokens;
      return { allowed: true, waitMs: 0 };
    }
    const deficitTokens = Math.max(0, tokens - this.remainingTokens);
    const tokenWaitMs = (deficitTokens / this.maxTpm) * 60_000;
    const reqWaitMs = this.remainingRequests < 1 ? (1 / this.maxRpm) * 60_000 : 0;
    return { allowed: false, waitMs: Math.ceil(Math.max(tokenWaitMs, reqWaitMs, 50)) };
  }

  private refill(nowMs: number): void {
    const elapsedMin = Math.max(0, (nowMs - this.lastRefillMs) / 60_000);
    if (elapsedMin > 0) {
      this.remainingRequests = Math.min(this.maxRpm, this.remainingRequests + elapsedMin * this.maxRpm);
      this.remainingTokens = Math.min(this.maxTpm, this.remainingTokens + elapsedMin * this.maxTpm);
      this.lastRefillMs = nowMs;
    }
  }
}

export class ThreeStateCircuitBreaker {
  private state: CircuitState = "CLOSED";
  private consecutiveFailures = 0;
  private openedAtMs = 0;
  private readonly failureThreshold: number;
  private readonly openCooldownMs: number;

  constructor(failureThreshold = 3, openCooldownMs = 15_000) {
    this.failureThreshold = failureThreshold;
    this.openCooldownMs = openCooldownMs;
  }

  public getState(nowMs = Date.now()): CircuitState {
    if (this.state === "OPEN" && nowMs - this.openedAtMs >= this.openCooldownMs) {
      this.state = "HALF_OPEN";
    }
    return this.state;
  }

  public canPass(nowMs = Date.now()): boolean {
    const current = this.getState(nowMs);
    return current === "CLOSED" || current === "HALF_OPEN";
  }

  public recordSuccess(): void {
    this.consecutiveFailures = 0;
    this.state = "CLOSED";
  }

  public recordFailure(nowMs = Date.now()): void {
    this.consecutiveFailures += 1;
    if (this.state === "HALF_OPEN" || this.consecutiveFailures >= this.failureThreshold) {
      this.state = "OPEN";
      this.openedAtMs = nowMs;
    }
  }
}
