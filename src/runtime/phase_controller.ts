/**
 * @fileoverview M4 TASK-13: Linux cgroup v2 Dynamic CPU/Memory Phase-Decoupled Controller.
 */
import * as fs from "node:fs";
import * as path from "node:path";
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

  constructor(
    private readonly totalCpuCores = 32,
    private readonly actCpuCores = 4,
    private readonly cgroupRoot?: string,
  ) {}

  /** Writes the execution-phase cpu.max (cores * 100000 per 100ms) and memory.high files. */
  public enterExecutionPhase(task: { taskId: string; burstCpu?: number; execMemMb?: number }): {
    cgroupDirPath: string;
  } & CgroupAllocationState {
    const cores = task.burstCpu ?? this.actCpuCores;
    const state = this.applyActPhase(task.taskId, {
      think_cpu_cores: this.totalCpuCores,
      act_cpu_cores: cores,
      act_memory_mb: task.execMemMb ?? 2048,
      disk_mb: 10240,
    });
    const cgroupDirPath = path.join(this.cgroupRoot ?? path.join("/tmp", "ansts-cgroup"), task.taskId);
    fs.mkdirSync(cgroupDirPath, { recursive: true });
    fs.writeFileSync(path.join(cgroupDirPath, "cpu.max"), `${state.cpuQuotaUsPer100ms} 100000\n`, "utf8");
    fs.writeFileSync(path.join(cgroupDirPath, "memory.high"), `${state.memoryHighBytes}\n`, "utf8");
    return { ...state, cgroupDirPath };
  }

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
