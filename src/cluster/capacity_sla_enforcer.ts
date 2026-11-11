/**
 * @fileoverview M17 & M62-4: Cluster Capacity SLA Enforcer & Headroom Reservation Guard
 * with Finite Metric Validation, Zero-Division Immunity, SQLite Headroom State Persistence & Dynamic Threshold Adjustment.
 */
import { SqlStorageLike } from "../cloudflare/env.js";

export interface CapacitySlaPolicy {
  maxP0QueueWaitP99Ms: number;
  minWarmPoolHitRatio: number;
  minFreeClusterCpuRatio: number;
  emergencyHeadroomCpuRatio: number;
}

export class CapacitySlaEnforcer {
  private policy: CapacitySlaPolicy;
  private emergencyHeadroomActive = false;
  private sql?: SqlStorageLike;

  constructor(
    policy?: Partial<CapacitySlaPolicy>,
    sql?: SqlStorageLike
  ) {
    this.policy = {
      maxP0QueueWaitP99Ms: policy?.maxP0QueueWaitP99Ms ?? 250,
      minWarmPoolHitRatio: policy?.minWarmPoolHitRatio ?? 0.85,
      minFreeClusterCpuRatio: policy?.minFreeClusterCpuRatio ?? 0.1,
      emergencyHeadroomCpuRatio: policy?.emergencyHeadroomCpuRatio ?? 0.15,
    };
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M62-4] DFX-Operability: Binds SQLite storage to persist emergency headroom reservation state.
   */
  public bindSqlStorage(sql: SqlStorageLike): boolean {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS cluster_capacity_sla_state (id TEXT PRIMARY KEY, headroom_active INTEGER, policy_json TEXT)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM cluster_capacity_sla_state WHERE id = ?", "global")
      .toArray();
    if (rows.length > 0) {
      this.emergencyHeadroomActive = Boolean(Number(rows[0].headroom_active ?? 0));
      if (rows[0].policy_json) {
        try {
          const parsed = JSON.parse(String(rows[0].policy_json)) as CapacitySlaPolicy;
          this.policy = { ...this.policy, ...parsed };
        } catch {
          // ignore malformed policy
        }
      }
      return true;
    }
    return false;
  }

  private persistState(): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO cluster_capacity_sla_state (id, headroom_active, policy_json) VALUES (?, ?, ?)",
      "global",
      this.emergencyHeadroomActive ? 1 : 0,
      JSON.stringify(this.policy)
    );
  }

  public isEmergencyHeadroomActive(): boolean {
    return this.emergencyHeadroomActive;
  }

  public evaluateSlaCompliance(params: {
    p0QueueWaitP99Ms: number;
    warmPoolHitRatio: number;
    freeClusterCpuRatio: number;
  }): { compliant: boolean; breachedMetrics: string[]; reserveEmergencyHeadroom: boolean } {
    const safeP0Wait = Number.isFinite(params.p0QueueWaitP99Ms) && params.p0QueueWaitP99Ms >= 0
      ? params.p0QueueWaitP99Ms
      : 0;
    const safeHitRatio = Number.isFinite(params.warmPoolHitRatio)
      ? Math.max(0, Math.min(1, params.warmPoolHitRatio))
      : 1.0;
    const safeCpuRatio = Number.isFinite(params.freeClusterCpuRatio)
      ? Math.max(0, Math.min(1, params.freeClusterCpuRatio))
      : 1.0;

    const breachedMetrics: string[] = [];
    if (safeP0Wait > this.policy.maxP0QueueWaitP99Ms) {
      breachedMetrics.push("p0QueueWaitP99Ms");
    }
    if (safeHitRatio < this.policy.minWarmPoolHitRatio) {
      breachedMetrics.push("warmPoolHitRatio");
    }
    if (safeCpuRatio < this.policy.minFreeClusterCpuRatio) {
      breachedMetrics.push("freeClusterCpuRatio");
    }

    const reserveEmergencyHeadroom = safeCpuRatio < this.policy.emergencyHeadroomCpuRatio;
    this.emergencyHeadroomActive = reserveEmergencyHeadroom;
    this.persistState();

    return {
      compliant: breachedMetrics.length === 0,
      breachedMetrics,
      reserveEmergencyHeadroom,
    };
  }

  public updatePolicy(newPolicy: Partial<CapacitySlaPolicy>): CapacitySlaPolicy {
    this.policy = { ...this.policy, ...newPolicy };
    this.persistState();
    return { ...this.policy };
  }
}
