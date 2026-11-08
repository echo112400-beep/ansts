/**
 * @fileoverview M2 TASK-05B: Bayesian EWMA Task Resource Profiler for Think/Act Right-Sizing.
 */
import { ResourceProfile, TaskSpec } from "./models.js";

export class TaskResourceProfiler {
  private readonly historyByTenant: Map<string, { peakCpuEwma: number; peakMemMbEwma: number; samples: number }> =
    new Map();
  private readonly alpha: number;

  constructor(alpha = 0.35) {
    this.alpha = alpha;
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
    const existing = this.historyByTenant.get(task.tenant_id);
    if (!existing) {
      this.historyByTenant.set(task.tenant_id, {
        peakCpuEwma: observedPeakCpu,
        peakMemMbEwma: observedPeakMemoryMb,
        samples: 1,
      });
      return;
    }
    existing.peakCpuEwma = this.alpha * observedPeakCpu + (1 - this.alpha) * existing.peakCpuEwma;
    existing.peakMemMbEwma = this.alpha * observedPeakMemoryMb + (1 - this.alpha) * existing.peakMemMbEwma;
    existing.samples += 1;
  }
}
