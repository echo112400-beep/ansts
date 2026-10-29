/**
 * TASK-19: Cgroups v2 Bi-Modal Phase Controller (0.1C <-> 8.0C) and Host Burst Semaphore.
 */

import { AgentTask, SandboxPhase } from "../models";

export interface CgroupLimits {
  cpuCores: number;
  memoryHighMb: number;
  zramReclaimed: boolean;
  microQueuedMs: number;
}

export class CgroupPhaseController {
  public activeBurstTasks: Set<string> = new Set();
  public appliedLimits: Map<string, CgroupLimits> = new Map();

  constructor(
    public readonly hostPhysicalCores: number = 32.0,
    public readonly maxConcurrentBursts: number = 4,
  ) {}

  public enterCognitivePhase(task: AgentTask): CgroupLimits {
    this.activeBurstTasks.delete(task.taskId);
    task.phase = SandboxPhase.COGNITIVE;
    const lim: CgroupLimits = {
      cpuCores: task.cogCpu,
      memoryHighMb: task.cogMemMb,
      zramReclaimed: true,
      microQueuedMs: 0,
    };
    this.appliedLimits.set(task.taskId, lim);
    return lim;
  }

  public enterExecutionPhase(task: AgentTask): CgroupLimits {
    task.phase = SandboxPhase.EXECUTION;
    let lim: CgroupLimits;
    if (this.activeBurstTasks.size < this.maxConcurrentBursts) {
      this.activeBurstTasks.add(task.taskId);
      lim = {
        cpuCores: task.burstCpu,
        memoryHighMb: task.execMemMb,
        zramReclaimed: false,
        microQueuedMs: 0,
      };
    } else {
      lim = {
        cpuCores: task.execCpu,
        memoryHighMb: task.execMemMb,
        zramReclaimed: false,
        microQueuedMs: 220,
      };
    }
    this.appliedLimits.set(task.taskId, lim);
    return lim;
  }
}
