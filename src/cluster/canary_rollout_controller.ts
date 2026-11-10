/**
 * @fileoverview M16 & M58-4: Progressive Canary Rollout Controller with Error-Rate Auto-Rollback
 * and Post-Rollback Lock Guard (preventing zombie promotion without explicit `resetCanary()`).
 */
export class CanaryRolloutController {
  private trafficWeightPct = 5;
  private state: "CANARY_ACTIVE" | "PROMOTED" | "ROLLED_BACK" = "CANARY_ACTIVE";

  public evaluateCanaryStep(params: {
    errorRatePct: number;
    p99LatencyMs: number;
    maxErrorRatePct?: number;
    maxP99LatencyMs?: number;
  }): { state: "CANARY_ACTIVE" | "PROMOTED" | "ROLLED_BACK"; trafficWeightPct: number } {
    if (
      !Number.isFinite(params.errorRatePct) ||
      !Number.isFinite(params.p99LatencyMs) ||
      params.errorRatePct > (params.maxErrorRatePct ?? 1.0) ||
      params.p99LatencyMs > (params.maxP99LatencyMs ?? 1500)
    ) {
      this.state = "ROLLED_BACK";
      this.trafficWeightPct = 0;
      return { state: this.state, trafficWeightPct: this.trafficWeightPct };
    }
    // [M58-4] Do not promote a ROLLED_BACK canary on subsequent healthy ticks unless resetCanary() was called
    if (this.state === "ROLLED_BACK") {
      this.trafficWeightPct = 0;
      return { state: this.state, trafficWeightPct: 0 };
    }
    this.trafficWeightPct = Math.min(100, this.trafficWeightPct * 2 + 15);
    if (this.trafficWeightPct >= 100) {
      this.state = "PROMOTED";
    }
    return { state: this.state, trafficWeightPct: this.trafficWeightPct };
  }

  public getStatus(): { state: "CANARY_ACTIVE" | "PROMOTED" | "ROLLED_BACK"; trafficWeightPct: number } {
    return { state: this.state, trafficWeightPct: this.trafficWeightPct };
  }

  public resetCanary(initialWeightPct = 5): { state: "CANARY_ACTIVE" | "PROMOTED" | "ROLLED_BACK"; trafficWeightPct: number } {
    const clamped = Number.isFinite(initialWeightPct)
      ? Math.max(1, Math.min(100, Math.round(initialWeightPct)))
      : 5;
    this.state = clamped >= 100 ? "PROMOTED" : "CANARY_ACTIVE";
    this.trafficWeightPct = clamped;
    return this.getStatus();
  }
}
