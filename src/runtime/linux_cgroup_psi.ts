/**
 * @fileoverview M11 TASK-38, M58-2 & M62-1: Real Linux cgroup v2 (`cpu.max`, `memory.high`, `cgroup.freeze`) & `/proc/pressure/memory` Driver
 * with TaskId Path-Traversal Sanitization, Finite Resource Validation, CFS Min-Floor & Idempotent `removeTaskCgroup` Directory Cleanup.
 */
import * as fs from "node:fs";
import * as path from "node:path";

export class LinuxCgroupV2AndPsiDriver {
  private readonly cgroupRootDir: string;
  public static readonly MIN_CPU_QUOTA_US = 5000;

  constructor(cgroupRootDir: string) {
    this.cgroupRootDir = cgroupRootDir;
    fs.mkdirSync(this.cgroupRootDir, { recursive: true });
  }

  public ensureTaskCgroup(taskId: string): string {
    const safeId = taskId.trim().replace(/[^a-zA-Z0-9._-]/g, "_");
    if (!safeId || safeId === "." || safeId === "..") {
      throw new Error(`Invalid taskId '${taskId}' for cgroup v2 directory`);
    }
    const dir = path.join(this.cgroupRootDir, `ansts-${safeId}`);
    fs.mkdirSync(dir, { recursive: true });
    return dir;
  }

  /**
   * [M62-1] DFX-Maintainability: Removes task cgroup directory on task completion or failure
   * preventing thousands of dangling empty cgroups from exhausting host inode and kernel cgroup ID limits.
   */
  public removeTaskCgroup(taskId: string): boolean {
    const safeId = taskId.trim().replace(/[^a-zA-Z0-9._-]/g, "_");
    if (!safeId || safeId === "." || safeId === "..") return false;
    const dir = path.join(this.cgroupRootDir, `ansts-${safeId}`);
    if (fs.existsSync(dir)) {
      try {
        fs.rmSync(dir, { recursive: true, force: true });
        return true;
      } catch {
        return false;
      }
    }
    return false;
  }

  public writePhaseLimits(
    taskId: string,
    phase: "THINK" | "ACT",
    cpuCores: number,
    memoryHighMb: number,
    memoryMaxMb: number
  ): { cpuMaxContent: string; memoryHighBytes: string; memoryMaxBytes: string } {
    if (
      !Number.isFinite(cpuCores) ||
      cpuCores <= 0 ||
      !Number.isFinite(memoryHighMb) ||
      memoryHighMb <= 0 ||
      !Number.isFinite(memoryMaxMb) ||
      memoryMaxMb < memoryHighMb
    ) {
      throw new Error(
        `Invalid cgroup v2 phase limits: cpu=${cpuCores}, memHighMb=${memoryHighMb}, memMaxMb=${memoryMaxMb}`
      );
    }
    const dir = this.ensureTaskCgroup(taskId);
    const quotaUs = Math.max(
      LinuxCgroupV2AndPsiDriver.MIN_CPU_QUOTA_US,
      Math.round(cpuCores * 100_000)
    );
    const cpuMaxContent = `${quotaUs} 100000`;
    const memoryHighBytes = String(Math.round(memoryHighMb * 1024 * 1024));
    const memoryMaxBytes = String(Math.round(memoryMaxMb * 1024 * 1024));

    fs.writeFileSync(path.join(dir, "cpu.max"), cpuMaxContent, "utf-8");
    fs.writeFileSync(path.join(dir, "memory.high"), memoryHighBytes, "utf-8");
    fs.writeFileSync(path.join(dir, "memory.max"), memoryMaxBytes, "utf-8");
    fs.writeFileSync(path.join(dir, "ansts.phase"), phase, "utf-8");

    return { cpuMaxContent, memoryHighBytes, memoryMaxBytes };
  }

  public setFreezeState(taskId: string, freeze: boolean): string {
    const dir = this.ensureTaskCgroup(taskId);
    const val = freeze ? "1" : "0";
    fs.writeFileSync(path.join(dir, "cgroup.freeze"), val, "utf-8");
    return val;
  }

  public static parseProcPressureMemory(content: string): { someAvg10: number; fullAvg10: number } {
    let someAvg10 = 0;
    let fullAvg10 = 0;
    for (const line of content.split("\n")) {
      const mSome = line.match(/^some\s+avg10=([0-9.]+)/);
      if (mSome) {
        const parsed = parseFloat(mSome[1]);
        if (Number.isFinite(parsed) && parsed >= 0) someAvg10 = parsed;
      }
      const mFull = line.match(/^full\s+avg10=([0-9.]+)/);
      if (mFull) {
        const parsed = parseFloat(mFull[1]);
        if (Number.isFinite(parsed) && parsed >= 0) fullAvg10 = parsed;
      }
    }
    return { someAvg10, fullAvg10 };
  }
}
