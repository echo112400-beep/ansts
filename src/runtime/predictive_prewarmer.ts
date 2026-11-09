/**
 * @fileoverview M32 Predictive Time-Series EWMA Sandbox Pre-Warmer & MCP Hub Pool Scaler.
 */
export class PredictiveSandboxPrewarmer {
  private ewmaArrivalRatePerSec = 5.0;
  private readonly alpha: number;

  constructor(alpha = 0.4) {
    this.alpha = alpha;
  }

  public recordArrivalSampleAndPredict(observedArrivalsPerSec: number, avgSandboxHoldSec = 4.0): {
    predictedConcurrency: number;
    recommendedWarmPoolSize: number;
  } {
    this.ewmaArrivalRatePerSec =
      this.alpha * observedArrivalsPerSec + (1 - this.alpha) * this.ewmaArrivalRatePerSec;
    const predictedConcurrency = Math.ceil(this.ewmaArrivalRatePerSec * avgSandboxHoldSec);
    const recommendedWarmPoolSize = Math.max(4, Math.ceil(predictedConcurrency * 1.25));
    return { predictedConcurrency, recommendedWarmPoolSize };
  }
}
