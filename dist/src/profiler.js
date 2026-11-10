"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TaskResourceProfiler = void 0; /**
 * @fileoverview M2 TASK-05B & M58-5: Bayesian EWMA Task Resource Profiler for Think/Act Right-Sizing
 * with Finite Positive Sample Guard to prevent EWMA NaN Poisoning.
 */


class TaskResourceProfiler {
  historyByTenant =
  new Map();
  alpha;

  constructor(alpha = 0.35) {
    this.alpha = Number.isFinite(alpha) ? Math.max(0.05, Math.min(0.95, alpha)) : 0.35;
  }

  recommendProfile(task) {
    const hist = this.historyByTenant.get(task.tenant_id);
    if (!hist || hist.samples < 3) {
      return { ...task.resource_profile };
    }
    return {
      think_cpu_cores: task.resource_profile.think_cpu_cores,
      act_cpu_cores: Math.max(task.resource_profile.think_cpu_cores, Number((hist.peakCpuEwma * 1.2).toFixed(2))),
      act_memory_mb: Math.max(512, Math.ceil(hist.peakMemMbEwma * 1.25)),
      disk_mb: task.resource_profile.disk_mb
    };
  }

  recordExecutionMetrics(task, observedPeakCpu, observedPeakMemoryMb) {
    if (
    !Number.isFinite(observedPeakCpu) ||
    observedPeakCpu <= 0 ||
    !Number.isFinite(observedPeakMemoryMb) ||
    observedPeakMemoryMb <= 0)
    {
      return;
    }
    const existing = this.historyByTenant.get(task.tenant_id);
    if (!existing) {
      this.historyByTenant.set(task.tenant_id, {
        peakCpuEwma: observedPeakCpu,
        peakMemMbEwma: observedPeakMemoryMb,
        samples: 1
      });
      return;
    }
    existing.peakCpuEwma = this.alpha * observedPeakCpu + (1 - this.alpha) * existing.peakCpuEwma;
    existing.peakMemMbEwma = this.alpha * observedPeakMemoryMb + (1 - this.alpha) * existing.peakMemMbEwma;
    existing.samples += 1;
  }
}exports.TaskResourceProfiler = TaskResourceProfiler;