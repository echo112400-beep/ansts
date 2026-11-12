/**
 * @fileoverview M2 TASK-05B, M58-5 & M65-2: Bayesian EWMA Task Resource Profiler for Think/Act Right-Sizing
 * with Finite Positive Sample Guard & SQLite Persistence (`persisted_profiler_history`).
 */
import { ResourceProfile, TaskSpec } from "./models.js";
import { SqlStorageLike } from "./cloudflare/env.js";

export class TaskResourceProfiler {
  private readonly historyByTenant: Map<string, { peakCpuEwma: number; peakMemMbEwma: number; samples: number }> =
    new Map();
  private readonly alpha: number;
  private sql?: SqlStorageLike;

  constructor(alpha = 0.35, sql?: SqlStorageLike) {
    this.alpha = Number.isFinite(alpha) ? Math.max(0.05, Math.min(0.95, alpha)) : 0.35;
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M65-2] Binds SQLite storage (`persisted_profiler_history`) so per-tenant EWMA CPU/Memory right-sizing
   * profiles survive restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): number {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_profiler_history (tenant_id TEXT PRIMARY KEY, payload_json TEXT)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM persisted_profiler_history")
      .toArray();
    let restored = 0;
    for (const r of rows) {
      const tenantId = String(r.tenant_id ?? "");
      const parsed = JSON.parse(String(r.payload_json ?? "{}")) as {
        peakCpuEwma?: number;
        peakMemMbEwma?: number;
        samples?: number;
      };
      if (
        tenantId &&
        Number.isFinite(parsed.peakCpuEwma) &&
        Number.isFinite(parsed.peakMemMbEwma) &&
        Number.isFinite(parsed.samples)
      ) {
        this.historyByTenant.set(tenantId, {
          peakCpuEwma: Number(parsed.peakCpuEwma),
          peakMemMbEwma: Number(parsed.peakMemMbEwma),
          samples: Number(parsed.samples),
        });
        restored += 1;
      }
    }
    return restored;
  }

  public recommendProfile(task: TaskSpec): ResourceProfile {
    const hist = this.historyByTenant.get(task.tenant_id);
    if (!hist || hist.samples < 3) {
      return { ...task.resource_profile };
    }
    return {
      think_cpu_cores: task.resource_profile.think_cpu_cores,
      act_cpu_cores: Math.max(task.resource_profile.think_cpu_cores, Number((hist.peakCpuEwma * 1.2).toFixed(2))),
      act_memory_mb: Math.max(512, Math.ceil(hist.peakMemMbEwma * 1.25)),
      disk_mb: task.resource_profile.disk_mb,
    };
  }

  public recordExecutionMetrics(task: TaskSpec, observedPeakCpu: number, observedPeakMemoryMb: number): void {
    if (
      !Number.isFinite(observedPeakCpu) ||
      observedPeakCpu <= 0 ||
      !Number.isFinite(observedPeakMemoryMb) ||
      observedPeakMemoryMb <= 0
    ) {
      return;
    }
    const existing = this.historyByTenant.get(task.tenant_id);
    let updated: { peakCpuEwma: number; peakMemMbEwma: number; samples: number };
    if (!existing) {
      updated = {
        peakCpuEwma: observedPeakCpu,
        peakMemMbEwma: observedPeakMemoryMb,
        samples: 1,
      };
    } else {
      updated = {
        peakCpuEwma: this.alpha * observedPeakCpu + (1 - this.alpha) * existing.peakCpuEwma,
        peakMemMbEwma: this.alpha * observedPeakMemoryMb + (1 - this.alpha) * existing.peakMemMbEwma,
        samples: existing.samples + 1,
      };
    }
    this.historyByTenant.set(task.tenant_id, updated);
    if (this.sql) {
      this.sql.exec(
        "INSERT OR REPLACE INTO persisted_profiler_history (tenant_id, payload_json) VALUES (?, ?)",
        task.tenant_id,
        JSON.stringify(updated)
      );
    }
  }

  public listTenantProfiles(): Array<{
    tenantId: string;
    peakCpuEwma: number;
    peakMemMbEwma: number;
    samples: number;
    recommendedActCores: number;
    recommendedActMemoryMb: number;
  }> {
    return Array.from(this.historyByTenant.entries()).map(([tenantId, hist]) => ({
      tenantId,
      peakCpuEwma: Number(hist.peakCpuEwma.toFixed(2)),
      peakMemMbEwma: Math.round(hist.peakMemMbEwma),
      samples: hist.samples,
      recommendedActCores: Math.max(0.25, Number((hist.peakCpuEwma * 1.2).toFixed(2))),
      recommendedActMemoryMb: Math.max(512, Math.ceil(hist.peakMemMbEwma * 1.25)),
    }));
  }
}
