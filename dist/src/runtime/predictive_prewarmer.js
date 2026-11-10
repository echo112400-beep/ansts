"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.PredictiveSandboxPrewarmer = void 0; /**
 * @fileoverview M32 & M58-5: Predictive Time-Series EWMA Sandbox Pre-Warmer & MCP Hub Pool Scaler
 * with Non-Negative Finite Sample Sanitization to prevent EWMA NaN/Infinity Poisoning.
 */
class PredictiveSandboxPrewarmer {
  ewmaArrivalRatePerSec = 5.0;
  alpha;

  constructor(alpha = 0.4) {
    this.alpha = Number.isFinite(alpha) ? Math.max(0.05, Math.min(0.95, alpha)) : 0.4;
  }

  recordArrivalSampleAndPredict(observedArrivalsPerSec, avgSandboxHoldSec = 4.0)


  {
    const safeArrivals =
    Number.isFinite(observedArrivalsPerSec) && observedArrivalsPerSec >= 0 ?
    observedArrivalsPerSec :
    this.ewmaArrivalRatePerSec;
    const safeHoldSec =
    Number.isFinite(avgSandboxHoldSec) && avgSandboxHoldSec > 0 ? avgSandboxHoldSec : 4.0;
    this.ewmaArrivalRatePerSec =
    this.alpha * safeArrivals + (1 - this.alpha) * this.ewmaArrivalRatePerSec;
    const predictedConcurrency = Math.ceil(this.ewmaArrivalRatePerSec * safeHoldSec);
    const recommendedWarmPoolSize = Math.max(4, Math.ceil(predictedConcurrency * 1.25));
    return { predictedConcurrency, recommendedWarmPoolSize };
  }
}exports.PredictiveSandboxPrewarmer = PredictiveSandboxPrewarmer;