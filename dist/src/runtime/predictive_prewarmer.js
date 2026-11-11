"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PredictiveSandboxPrewarmer = void 0;
class PredictiveSandboxPrewarmer {
    ewmaArrivalRatePerSec = 5.0;
    alpha;
    sql;
    constructor(alpha = 0.4, sql) {
        this.alpha = Number.isFinite(alpha) ? Math.max(0.05, Math.min(0.95, alpha)) : 0.4;
        if (sql) {
            this.bindSqlStorage(sql);
        }
    }
    /**
     * [M65-2] Binds SQLite storage (`persisted_prewarmer_state`) so learned EWMA arrival rate survives restarts.
     */
    bindSqlStorage(sql) {
        this.sql = sql;
        this.sql.exec("CREATE TABLE IF NOT EXISTS persisted_prewarmer_state (id TEXT PRIMARY KEY, ewma_arrival_rate REAL)");
        const row = this.sql
            .exec("SELECT * FROM persisted_prewarmer_state WHERE id = ?", "global")
            .one();
        if (!row || row.ewma_arrival_rate === undefined)
            return false;
        const rate = Number(row.ewma_arrival_rate);
        if (Number.isFinite(rate) && rate >= 0) {
            this.ewmaArrivalRatePerSec = rate;
            return true;
        }
        return false;
    }
    getEwmaArrivalRatePerSec() {
        return this.ewmaArrivalRatePerSec;
    }
    recordArrivalSampleAndPredict(observedArrivalsPerSec, avgSandboxHoldSec = 4.0) {
        const safeArrivals = Number.isFinite(observedArrivalsPerSec) && observedArrivalsPerSec >= 0
            ? observedArrivalsPerSec
            : this.ewmaArrivalRatePerSec;
        const safeHoldSec = Number.isFinite(avgSandboxHoldSec) && avgSandboxHoldSec > 0 ? avgSandboxHoldSec : 4.0;
        this.ewmaArrivalRatePerSec =
            this.alpha * safeArrivals + (1 - this.alpha) * this.ewmaArrivalRatePerSec;
        if (this.sql) {
            this.sql.exec("INSERT OR REPLACE INTO persisted_prewarmer_state (id, ewma_arrival_rate) VALUES (?, ?)", "global", this.ewmaArrivalRatePerSec);
        }
        const predictedConcurrency = Math.ceil(this.ewmaArrivalRatePerSec * safeHoldSec);
        const recommendedWarmPoolSize = Math.max(4, Math.ceil(predictedConcurrency * 1.25));
        return { predictedConcurrency, recommendedWarmPoolSize };
    }
}
exports.PredictiveSandboxPrewarmer = PredictiveSandboxPrewarmer;
