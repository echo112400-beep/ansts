/**
 * @fileoverview M32, M58-5 & M65-2: Predictive Time-Series EWMA Sandbox Pre-Warmer & MCP Hub Pool Scaler
 * with Non-Negative Finite Sample Sanitization & SQLite Persistence (`persisted_prewarmer_state`).
 */
import { SqlStorageLike } from "../cloudflare/env.js";

export class PredictiveSandboxPrewarmer {
  private ewmaArrivalRatePerSec = 5.0;
  private readonly alpha: number;
  private sql?: SqlStorageLike;

  constructor(alpha = 0.4, sql?: SqlStorageLike) {
    this.alpha = Number.isFinite(alpha) ? Math.max(0.05, Math.min(0.95, alpha)) : 0.4;
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M65-2] Binds SQLite storage (`persisted_prewarmer_state`) so learned EWMA arrival rate survives restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): boolean {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_prewarmer_state (id TEXT PRIMARY KEY, ewma_arrival_rate REAL)"
    );
    const row = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM persisted_prewarmer_state WHERE id = ?", "global")
      .one();
    if (!row || row.ewma_arrival_rate === undefined) return false;
    const rate = Number(row.ewma_arrival_rate);
    if (Number.isFinite(rate) && rate >= 0) {
      this.ewmaArrivalRatePerSec = rate;
      return true;
    }
    return false;
  }

  public getEwmaArrivalRatePerSec(): number {
    return this.ewmaArrivalRatePerSec;
  }

  public recordArrivalSampleAndPredict(observedArrivalsPerSec: number, avgSandboxHoldSec = 4.0): {
    predictedConcurrency: number;
    recommendedWarmPoolSize: number;
  } {
    const safeArrivals =
      Number.isFinite(observedArrivalsPerSec) && observedArrivalsPerSec >= 0
        ? observedArrivalsPerSec
        : this.ewmaArrivalRatePerSec;
    const safeHoldSec =
      Number.isFinite(avgSandboxHoldSec) && avgSandboxHoldSec > 0 ? avgSandboxHoldSec : 4.0;
    this.ewmaArrivalRatePerSec =
      this.alpha * safeArrivals + (1 - this.alpha) * this.ewmaArrivalRatePerSec;
    if (this.sql) {
      this.sql.exec(
        "INSERT OR REPLACE INTO persisted_prewarmer_state (id, ewma_arrival_rate) VALUES (?, ?)",
        "global",
        this.ewmaArrivalRatePerSec
      );
    }
    const predictedConcurrency = Math.ceil(this.ewmaArrivalRatePerSec * safeHoldSec);
    const recommendedWarmPoolSize = Math.max(4, Math.ceil(predictedConcurrency * 1.25));
    return { predictedConcurrency, recommendedWarmPoolSize };
  }
}
