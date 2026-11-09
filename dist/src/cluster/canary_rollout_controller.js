"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CanaryRolloutController = void 0; /**
 * @fileoverview Progressive Canary Rollout Controller with Error-Rate Auto-Rollback.
 */
class CanaryRolloutController {
  trafficWeightPct = 5;
  state = "CANARY_ACTIVE";

  evaluateCanaryStep(params)




  {
    if (
    params.errorRatePct > (params.maxErrorRatePct ?? 1.0) ||
    params.p99LatencyMs > (params.maxP99LatencyMs ?? 1500))
    {
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
}exports.CanaryRolloutController = CanaryRolloutController;