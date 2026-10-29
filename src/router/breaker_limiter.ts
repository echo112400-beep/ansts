/**
 * TASK-14: Sliding-Window Token Bucket Rate Limiter and 3-State Model Pool Circuit Breaker.
 */

export class SlidingWindowTokenBucket {
  public currentTokens: number;
  public lastRefillTs: number = 0.0;

  constructor(
    public readonly reservedTps: number,
    public readonly maxBurstTps: number,
  ) {
    this.currentTokens = maxBurstTps;
  }

  public consumeWithPacing(requestedTokens: number, now: number): number {
    const elapsed = Math.max(0.0, now - this.lastRefillTs);
    this.lastRefillTs = now;
    this.currentTokens = Math.min(this.maxBurstTps, this.currentTokens + elapsed * this.reservedTps);
    if (requestedTokens <= this.currentTokens) {
      this.currentTokens -= requestedTokens;
      return 0.0;
    }
    const deficit = requestedTokens - this.currentTokens;
    this.currentTokens = 0.0;
    return Number((deficit / Math.max(1, this.reservedTps)).toFixed(4));
  }
}

export class ThreeStateCircuitBreaker {
  public state: "CLOSED" | "OPEN" | "HALF_OPEN" = "CLOSED";
  public openedAtTs: number = 0.0;
  public recentOutcomes: boolean[] = [];

  constructor(
    public readonly poolName: string,
    public readonly errorThreshold: number = 0.25,
    public readonly ttftThresholdSec: number = 15.0,
    public readonly halfOpenCooldownSec: number = 10.0,
  ) {}

  public recordOutcome(success: boolean, ttftSec: number, now: number): string {
    if (this.state === "HALF_OPEN") {
      if (success && ttftSec <= this.ttftThresholdSec) {
        this.state = "CLOSED";
        this.recentOutcomes = [];
      } else {
        this.state = "OPEN";
        this.openedAtTs = now;
      }
      return this.state;
    }

    this.recentOutcomes.push(success && ttftSec <= this.ttftThresholdSec);
    if (this.recentOutcomes.length > 20) {
      this.recentOutcomes.shift();
    }

    const failures = this.recentOutcomes.filter((ok) => !ok).length;
    const errRate = failures / Math.max(1, this.recentOutcomes.length);
    if ((this.recentOutcomes.length >= 4 && errRate > this.errorThreshold) || ttftSec > this.ttftThresholdSec) {
      this.state = "OPEN";
      this.openedAtTs = now;
    }
    return this.state;
  }

  public allowRequest(now: number): boolean {
    if (this.state === "CLOSED") return true;
    if (this.state === "OPEN" && now - this.openedAtTs >= this.halfOpenCooldownSec) {
      this.state = "HALF_OPEN";
      return true;
    }
    return this.state === "HALF_OPEN";
  }
}
