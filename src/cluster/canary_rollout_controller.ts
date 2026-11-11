/**
 * @fileoverview M16, M58-4 & M65-2: Progressive Canary Rollout Controller with Error-Rate Auto-Rollback,
 * Post-Rollback Lock Guard & SQLite Persistence (`persisted_canary_state`).
 */
import { SqlStorageLike } from "../cloudflare/env.js";

export class CanaryRolloutController {
  private trafficWeightPct = 5;
  private state: "CANARY_ACTIVE" | "PROMOTED" | "ROLLED_BACK" = "CANARY_ACTIVE";
  private sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M65-2] Binds SQLite storage (`persisted_canary_state`) so `ROLLED_BACK` post-rollback lock
   * and `trafficWeightPct` survive process restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): boolean {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_canary_state (id TEXT PRIMARY KEY, state TEXT, traffic_weight_pct INTEGER)"
    );
    const row = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM persisted_canary_state WHERE id = ?", "global")
      .one();
    if (!row) return false;
    const st = String(row.state ?? "CANARY_ACTIVE");
    if (st === "CANARY_ACTIVE" || st === "PROMOTED" || st === "ROLLED_BACK") {
      this.state = st;
    }
    if (row.traffic_weight_pct !== undefined && Number.isFinite(Number(row.traffic_weight_pct))) {
      this.trafficWeightPct = Number(row.traffic_weight_pct);
    }
    return true;
  }

  private persistState(): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO persisted_canary_state (id, state, traffic_weight_pct) VALUES (?, ?, ?)",
      "global",
      this.state,
      this.trafficWeightPct
    );
  }

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
      this.persistState();
      return { state: this.state, trafficWeightPct: this.trafficWeightPct };
    }
    // [M58-4] Do not promote a ROLLED_BACK canary on subsequent healthy ticks unless resetCanary() was called
    if (this.state === "ROLLED_BACK") {
      this.trafficWeightPct = 0;
      this.persistState();
      return { state: this.state, trafficWeightPct: 0 };
    }
    this.trafficWeightPct = Math.min(100, this.trafficWeightPct * 2 + 15);
    if (this.trafficWeightPct >= 100) {
      this.state = "PROMOTED";
    }
    this.persistState();
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
    this.persistState();
    return this.getStatus();
  }
}
