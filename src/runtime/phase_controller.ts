/**
 * @fileoverview M4 TASK-13 & M62-1: Linux cgroup v2 Dynamic CPU/Memory Phase-Decoupled Controller
 * with Sub-Core Hard Floor (Preventing CFS Starvation), Non-Negative Resource Validation & Idempotent Cleanup.
 */
import { ResourceProfile } from "../models.js";

export interface CgroupAllocationState {
  taskId: string;
  phase: "THINK" | "ACT";
  cpuQuotaUsPer100ms: number;
  cpuWeight: number;
  memoryHighBytes: number;
  memoryMaxBytes: number;
}

export class CgroupPhaseController {
  private readonly allocations: Map<string, CgroupAllocationState> = new Map();
  public static readonly MIN_CPU_QUOTA_US = 5000; // 0.05 core floor (5000us per 100ms) to prevent cgroup freeze

  public applyThinkPhase(taskId: string, profile: ResourceProfile): CgroupAllocationState {
    const rawCpu = profile.think_cpu_cores;
    const safeCpu = Number.isFinite(rawCpu) && rawCpu > 0 ? rawCpu : 0.25;
    const quotaUs = Math.max(
      CgroupPhaseController.MIN_CPU_QUOTA_US,
      Math.round(safeCpu * 100_000)
    );

    const state: CgroupAllocationState = {
      taskId,
      phase: "THINK",
      cpuQuotaUsPer100ms: quotaUs,
      cpuWeight: 20,
      memoryHighBytes: 256 * 1024 * 1024,
      memoryMaxBytes: 512 * 1024 * 1024,
    };
    this.allocations.set(taskId, state);
    return state;
  }

  public applyActPhase(taskId: string, profile: ResourceProfile): CgroupAllocationState {
    const rawCpu = profile.act_cpu_cores;
    const safeCpu = Number.isFinite(rawCpu) && rawCpu > 0 ? rawCpu : 4.0;
    const rawMem = profile.act_memory_mb;
    const safeMem = Number.isFinite(rawMem) && rawMem >= 64 ? rawMem : 4096;
    const quotaUs = Math.max(
      CgroupPhaseController.MIN_CPU_QUOTA_US,
      Math.round(safeCpu * 100_000)
    );

    const state: CgroupAllocationState = {
      taskId,
      phase: "ACT",
      cpuQuotaUsPer100ms: quotaUs,
      cpuWeight: 200,
      memoryHighBytes: Math.round(safeMem * 0.85 * 1024 * 1024),
      memoryMaxBytes: Math.round(safeMem * 1024 * 1024),
    };
    this.allocations.set(taskId, state);
    return state;
  }

  public getAllocation(taskId: string): CgroupAllocationState | undefined {
    return this.allocations.get(taskId);
  }

  public removeTask(taskId: string): void {
    this.allocations.delete(taskId);
  }
}
