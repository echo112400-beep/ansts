/**
 * @fileoverview M4 TASK-14, M48-4 & M58-4: Linux PSI (Pressure Stall Information) Memory Guard,
 * Real `/proc/pressure/memory` + `/proc/meminfo` Kernel Probe, Non-Terminal Victim Filtering & Priority-Scoped Evictor.
 */
import * as fs from "node:fs";
import * as os from "node:os";
import { TaskPriority, TaskSpec, TaskState } from "../models.js";

const TERMINAL_STATES = new Set<TaskState>([
  TaskState.SUCCEEDED,
  TaskState.FAILED,
  TaskState.DEAD_LETTER,
  TaskState.CANCELLED,
]);

export interface PsiSample {
  someAvg10Pct: number;
  fullAvg10Pct: number;
  freeSwapMb: number;
}

export interface PsiDefenseAction {
  level: "NORMAL" | "THROTTLE_BATCH" | "FREEZE_AND_CHECKPOINT";
  evictedTaskIds: string[];
}

export class LinuxPsiMemoryGuard {
  private readonly softPsiThresholdPct: number;
  private readonly criticalPsiThresholdPct: number;

  constructor(softPsiThresholdPct = 40.0, criticalPsiThresholdPct = 70.0) {
    this.softPsiThresholdPct = softPsiThresholdPct;
    this.criticalPsiThresholdPct = criticalPsiThresholdPct;
  }

  public probeLiveKernelPsiSample(): PsiSample & { kernelPsiFilePresent: boolean; freeMemMb: number } {
    let someAvg10Pct = 1.2;
    let fullAvg10Pct = 0.2;
    let freeSwapMb = 2048;
    const kernelPsiFilePresent = fs.existsSync("/proc/pressure/memory");

    try {
      if (kernelPsiFilePresent) {
        const content = fs.readFileSync("/proc/pressure/memory", "utf-8");
        for (const line of content.split("\n")) {
          const mSome = line.match(/^some\s+avg10=([0-9.]+)/);
          if (mSome) someAvg10Pct = parseFloat(mSome[1]);
          const mFull = line.match(/^full\s+avg10=([0-9.]+)/);
          if (mFull) fullAvg10Pct = parseFloat(mFull[1]);
        }
      }
      if (fs.existsSync("/proc/meminfo")) {
        const meminfo = fs.readFileSync("/proc/meminfo", "utf-8");
        const mSwap = meminfo.match(/^SwapFree:\s+(\d+)\s+kB/m);
        if (mSwap) {
          freeSwapMb = Math.max(256, Math.round(Number(mSwap[1]) / 1024));
        }
      }
    } catch {
      // fallback on non-Linux hosts
    }

    const freeMemMb = Math.round(os.freemem() / (1024 * 1024));
    return {
      someAvg10Pct,
      fullAvg10Pct,
      freeSwapMb,
      kernelPsiFilePresent,
      freeMemMb,
    };
  }

  public evaluateAndDefend(sample: PsiSample, runningTasks: TaskSpec[]): PsiDefenseAction {
    // [M58-4] Exclude tasks already in terminal states so eviction only targets live active tasks
    const liveTasks = runningTasks.filter((t) => !TERMINAL_STATES.has(t.state));
    if (sample.fullAvg10Pct >= this.criticalPsiThresholdPct || sample.freeSwapMb < 128) {
      const evictableCandidates = liveTasks
        .filter((t) => t.priority >= TaskPriority.P1_HIGH)
        .sort((a, b) => {
          if (b.priority !== a.priority) return b.priority - a.priority;
          return b.resource_profile.act_memory_mb - a.resource_profile.act_memory_mb;
        });
      const victims =
        evictableCandidates.length > 0
          ? evictableCandidates
              .slice(0, Math.max(1, Math.ceil(evictableCandidates.length * 0.25)))
              .map((t) => t.task_id)
          : [];
      return { level: "FREEZE_AND_CHECKPOINT", evictedTaskIds: victims };
    }
    if (sample.someAvg10Pct >= this.softPsiThresholdPct) {
      const p3Victims = liveTasks
        .filter((t) => t.priority === TaskPriority.P3_BATCH)
        .map((t) => t.task_id);
      return { level: "THROTTLE_BATCH", evictedTaskIds: p3Victims };
    }
    return { level: "NORMAL", evictedTaskIds: [] };
  }
}
