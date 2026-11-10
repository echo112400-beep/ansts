/**
 * @fileoverview Progressive Canary Rollout Controller with Error-Rate Auto-Rollback.
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
      params.errorRatePct > (params.maxErrorRatePct ?? 1.0) ||
      params.p99LatencyMs > (params.maxP99LatencyMs ?? 1500)
    ) {
      this.state = "ROLLED_BACK";
      this.trafficWeightPct = 0;
      return { state: this.state, trafficWeightPct: this.trafficWeightPct };
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
    this.state = "CANARY_ACTIVE";
    this.trafficWeightPct = initialWeightPct;
    return this.getStatus();
  }
}
