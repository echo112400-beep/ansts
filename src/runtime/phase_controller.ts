/**
 * @fileoverview TASK-19 & M27 (TASK-128): Cgroups v2 Bi-Modal Phase Controller (0.1C <-> 8.0C)
 * with Real Cgroup Filesystem (`cpu.max`, `memory.high`, `memory.reclaim`) Persistence.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { AgentTask, SandboxPhase } from "../models";

export interface CgroupLimits {
  cpuCores: number;
  memoryHighMb: number;
  zramReclaimed: boolean;
  microQueuedMs: number;
  cpuMaxFileContent?: string;
  cgroupDirPath?: string;
}

export class CgroupPhaseController {
  public activeBurstTasks: Set<string> = new Set();
  public appliedLimits: Map<string, CgroupLimits> = new Map();

  constructor(
    public readonly hostPhysicalCores: number = 32.0,
    public varMaxConcurrentBursts: number = 4,
    public readonly cgroupRootDir: string = "/tmp/ansts_cgroup_v2/phase.slice",
  ) {
    fs.mkdirSync(this.cgroupRootDir, { recursive: true });
  }

  public get maxConcurrentBursts(): number {
    return this.varMaxConcurrentBursts;
  }

  public set maxConcurrentBursts(val: number) {
    this.varMaxConcurrentBursts = val;
  }

  private writeCgroupControlFiles(taskId: string, cpuCores: number, memoryHighMb: number, reclaimZram: boolean): {
    cpuMaxFileContent: string;
    cgroupDirPath: string;
  } {
    const cgroupDirPath = path.join(this.cgroupRootDir, taskId);
    fs.mkdirSync(cgroupDirPath, { recursive: true });

    const periodUs = 100000;
    const quotaUs = Math.max(1000, Math.round(cpuCores * periodUs));
    const cpuMaxFileContent = `${quotaUs} ${periodUs}`;
    fs.writeFileSync(path.join(cgroupDirPath, "cpu.max"), `${cpuMaxFileContent}\n`, "utf8");
    fs.writeFileSync(path.join(cgroupDirPath, "memory.high"), `${memoryHighMb * 1024 * 1024}\n`, "utf8");
    if (reclaimZram) {
      fs.writeFileSync(path.join(cgroupDirPath, "memory.reclaim"), "64M\n", "utf8");
    }
    return { cpuMaxFileContent, cgroupDirPath };
  }

  public enterCognitivePhase(task: AgentTask): CgroupLimits {
    this.activeBurstTasks.delete(task.taskId);
    task.phase = SandboxPhase.COGNITIVE;
    const disk = this.writeCgroupControlFiles(task.taskId, task.cogCpu, task.cogMemMb, true);
    const lim: CgroupLimits = {
      cpuCores: task.cogCpu,
      memoryHighMb: task.cogMemMb,
      zramReclaimed: true,
      microQueuedMs: 0,
      cpuMaxFileContent: disk.cpuMaxFileContent,
      cgroupDirPath: disk.cgroupDirPath,
    };
    this.appliedLimits.set(task.taskId, lim);
    return lim;
  }

  public enterExecutionPhase(task: AgentTask): CgroupLimits {
    task.phase = SandboxPhase.EXECUTION;
    let lim: CgroupLimits;
    if (this.activeBurstTasks.size < this.varMaxConcurrentBursts) {
      this.activeBurstTasks.add(task.taskId);
      const disk = this.writeCgroupControlFiles(task.taskId, task.burstCpu, task.execMemMb, false);
      lim = {
        cpuCores: task.burstCpu,
        memoryHighMb: task.execMemMb,
        zramReclaimed: false,
        microQueuedMs: 0,
        cpuMaxFileContent: disk.cpuMaxFileContent,
        cgroupDirPath: disk.cgroupDirPath,
      };
    } else {
      const disk = this.writeCgroupControlFiles(task.taskId, task.execCpu, task.execMemMb, false);
      lim = {
        cpuCores: task.execCpu,
        memoryHighMb: task.execMemMb,
        zramReclaimed: false,
        microQueuedMs: 220,
        cpuMaxFileContent: disk.cpuMaxFileContent,
        cgroupDirPath: disk.cgroupDirPath,
      };
    }
    this.appliedLimits.set(task.taskId, lim);
    return lim;
  }
}
