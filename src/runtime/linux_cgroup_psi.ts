/**
 * @fileoverview M11 TASK-38: Real Linux cgroup v2 (`cpu.max`, `memory.high`, `cgroup.freeze`) & `/proc/pressure/memory` Driver.
 */
import * as fs from "node:fs";
import * as path from "node:path";

export class LinuxCgroupV2AndPsiDriver {
  private readonly cgroupRootDir: string;

  constructor(cgroupRootDir: string) {
    this.cgroupRootDir = cgroupRootDir;
    fs.mkdirSync(this.cgroupRootDir, { recursive: true });
  }

  public ensureTaskCgroup(taskId: string): string {
    const dir = path.join(this.cgroupRootDir, `ansts-${taskId}`);
    fs.mkdirSync(dir, { recursive: true });
    return dir;
  }

  public writePhaseLimits(
    taskId: string,
    phase: "THINK" | "ACT",
    cpuCores: number,
    memoryHighMb: number,
    memoryMaxMb: number
  ): { cpuMaxContent: string; memoryHighBytes: string; memoryMaxBytes: string } {
    const dir = this.ensureTaskCgroup(taskId);
    const quotaUs = Math.round(cpuCores * 100_000);
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
      if (mSome) someAvg10 = parseFloat(mSome[1]);
      const mFull = line.match(/^full\s+avg10=([0-9.]+)/);
      if (mFull) fullAvg10 = parseFloat(mFull[1]);
    }
    return { someAvg10, fullAvg10 };
  }
}
