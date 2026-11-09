"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.PredictiveSandboxPrewarmer = void 0; /**
 * @fileoverview M32 Predictive Time-Series EWMA Sandbox Pre-Warmer & MCP Hub Pool Scaler.
 */
class PredictiveSandboxPrewarmer {
  ewmaArrivalRatePerSec = 5.0;
  alpha;

  constructor(alpha = 0.4) {
    this.alpha = alpha;
  }

  recordArrivalSampleAndPredict(observedArrivalsPerSec, avgSandboxHoldSec = 4.0)


  {
    this.ewmaArrivalRatePerSec =
    this.alpha * observedArrivalsPerSec + (1 - this.alpha) * this.ewmaArrivalRatePerSec;
    const predictedConcurrency = Math.ceil(this.ewmaArrivalRatePerSec * avgSandboxHoldSec);
    const recommendedWarmPoolSize = Math.max(4, Math.ceil(predictedConcurrency * 1.25));
    return { predictedConcurrency, recommendedWarmPoolSize };
  }
}exports.PredictiveSandboxPrewarmer = PredictiveSandboxPrewarmer;