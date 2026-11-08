/**
 * @fileoverview M4 TASK-13: Linux cgroup v2 Dynamic CPU/Memory Phase-Decoupled Controller.
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

  public applyThinkPhase(taskId: string, profile: ResourceProfile): CgroupAllocationState {
    const state: CgroupAllocationState = {
      taskId,
      phase: "THINK",
      cpuQuotaUsPer100ms: Math.round(profile.think_cpu_cores * 100_000),
      cpuWeight: 20,
      memoryHighBytes: 256 * 1024 * 1024,
      memoryMaxBytes: 512 * 1024 * 1024,
    };
    this.allocations.set(taskId, state);
    return state;
  }

  public applyActPhase(taskId: string, profile: ResourceProfile): CgroupAllocationState {
    const state: CgroupAllocationState = {
      taskId,
      phase: "ACT",
      cpuQuotaUsPer100ms: Math.round(profile.act_cpu_cores * 100_000),
      cpuWeight: 200,
      memoryHighBytes: Math.round(profile.act_memory_mb * 0.85 * 1024 * 1024),
      memoryMaxBytes: Math.round(profile.act_memory_mb * 1024 * 1024),
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
