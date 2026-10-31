/**
 * TASK-53: Real Linux Cgroups v2 (`/sys/fs/cgroup`) & PSI (`/proc/pressure/*`) Driver.
 *
 * Operates against real Linux CgroupFS / procfs trees or a configurable root directory
 * (allowing full filesystem verification in unprivileged CI sandboxes).
 * - Manages `/ansts/<sandboxId>/cpu.max`, `memory.high`, `memory.max`, `pids.max`, and `cgroup.freeze`
 * - Parses Linux kernel Pressure Stall Information (`some avg10=... avg60=... avg300=... total=...`)
 *   from `/proc/pressure/cpu` and `/proc/pressure/memory`
 */

import * as fs from "node:fs";
import * as path from "node:path";

export interface PsiMetricLine {
  avg10: number;
  avg60: number;
  avg300: number;
  totalUs: number;
}

export interface PsiPressureSnapshot {
  cpuSome: PsiMetricLine;
  memorySome: PsiMetricLine;
  memoryFull: PsiMetricLine;
  emergencyHibernateRequired: boolean;
}

export class LinuxCgroupV2AndPsiDriver {
  public readonly cgroupRoot: string;
  public readonly procPressureRoot: string;
  public readonly periodUs: number = 100_000; // 100ms standard CFS quota period

  constructor(options?: { cgroupRoot?: string; procPressureRoot?: string }) {
    this.cgroupRoot = options?.cgroupRoot ?? "/sys/fs/cgroup";
    this.procPressureRoot = options?.procPressureRoot ?? "/proc/pressure";
  }

  /**
   * Initializes `/ansts/<sandboxId>` Cgroups v2 hierarchy and writes baseline limits.
   */
  public initSandboxCgroup(params: {
    sandboxId: string;
    cogCpuCores?: number; // default 0.1C -> "10000 100000"
    cogMemHighMb?: number; // default 256MB
    memMaxMb?: number; // default 4096MB
    pidsMax?: number; // default 256
  }): string {
    const dir = this.getSandboxDir(params.sandboxId);
    fs.mkdirSync(dir, { recursive: true });

    const cogCores = params.cogCpuCores ?? 0.1;
    const quotaUs = Math.max(1000, Math.round(cogCores * this.periodUs));
    const memHighBytes = Math.round((params.cogMemHighMb ?? 256) * 1024 * 1024);
    const memMaxBytes = Math.round((params.memMaxMb ?? 4096) * 1024 * 1024);
    const pidsMax = params.pidsMax ?? 256;

    fs.writeFileSync(path.join(dir, "cpu.max"), `${quotaUs} ${this.periodUs}\n`, "utf8");
    fs.writeFileSync(path.join(dir, "memory.high"), `${memHighBytes}\n`, "utf8");
    fs.writeFileSync(path.join(dir, "memory.max"), `${memMaxBytes}\n`, "utf8");
    fs.writeFileSync(path.join(dir, "pids.max"), `${pidsMax}\n`, "utf8");
    fs.writeFileSync(path.join(dir, "cgroup.freeze"), "0\n", "utf8");
    return dir;
  }

  /**
   * Bi-modal phase transition (`0.1C` Cognitive Wait <-> `8.0C` Tool Burst).
   */
  public applyPhaseLimits(params: {
    sandboxId: string;
    phase: "COGNITIVE" | "TOOL_BURST";
    cogCpuCores?: number;
    burstCpuCores?: number;
    cogMemHighMb?: number;
    burstMemHighMb?: number;
  }): { cpuMax: string; memoryHighBytes: number; durationMs: number } {
    const start = performance.now();
    const dir = this.getSandboxDir(params.sandboxId);
    fs.mkdirSync(dir, { recursive: true });

    const cores =
      params.phase === "TOOL_BURST" ? (params.burstCpuCores ?? 8.0) : (params.cogCpuCores ?? 0.1);
    const memMb =
      params.phase === "TOOL_BURST" ? (params.burstMemHighMb ?? 4096) : (params.cogMemHighMb ?? 256);

    const quotaUs = Math.max(1000, Math.round(cores * this.periodUs));
    const cpuMax = `${quotaUs} ${this.periodUs}`;
    const memoryHighBytes = Math.round(memMb * 1024 * 1024);

    fs.writeFileSync(path.join(dir, "cpu.max"), `${cpuMax}\n`, "utf8");
    fs.writeFileSync(path.join(dir, "memory.high"), `${memoryHighBytes}\n`, "utf8");

    return {
      cpuMax,
      memoryHighBytes,
      durationMs: Number((performance.now() - start).toFixed(3)),
    };
  }

  /**
   * Freezes or thaws the sandbox cgroup (`cgroup.freeze` = `1` or `0`), used by the Sandbox Abuse Breaker.
   */
  public setCgroupFreeze(sandboxId: string, freeze: boolean): void {
    const dir = this.getSandboxDir(sandboxId);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, "cgroup.freeze"), freeze ? "1\n" : "0\n", "utf8");
  }

  public readSandboxCgroupState(sandboxId: string): {
    cpuMax: string;
    memoryHigh: number;
    pidsMax: number;
    frozen: boolean;
  } {
    const dir = this.getSandboxDir(sandboxId);
    const cpuMax = fs.readFileSync(path.join(dir, "cpu.max"), "utf8").trim();
    const memoryHigh = Number(fs.readFileSync(path.join(dir, "memory.high"), "utf8").trim());
    const pidsMax = Number(fs.readFileSync(path.join(dir, "pids.max"), "utf8").trim());
    const frozen = fs.readFileSync(path.join(dir, "cgroup.freeze"), "utf8").trim() === "1";
    return { cpuMax, memoryHigh, pidsMax, frozen };
  }

  /**
   * Reads `/proc/pressure/cpu` and `/proc/pressure/memory` in standard Linux PSI format:
   *   `some avg10=42.10 avg60=15.00 avg300=5.00 total=981234`
   *   `full avg10=18.20 avg60=6.10 avg300=1.20 total=412345`
   */
  public sampleHostPsiPressure(thresholds?: {
    memorySomeAvg10Max?: number; // default 40.0 (%)
    memoryFullAvg10Max?: number; // default 15.0 (%)
  }): PsiPressureSnapshot {
    const cpuText = this.readFileOrDefault(
      path.join(this.procPressureRoot, "cpu"),
      "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
    );
    const memText = this.readFileOrDefault(
      path.join(this.procPressureRoot, "memory"),
      "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n",
    );

    const cpuSome = this.parsePsiLine(cpuText, "some");
    const memorySome = this.parsePsiLine(memText, "some");
    const memoryFull = this.parsePsiLine(memText, "full");

    const someLimit = thresholds?.memorySomeAvg10Max ?? 40.0;
    const fullLimit = thresholds?.memoryFullAvg10Max ?? 15.0;
    const emergencyHibernateRequired =
      memorySome.avg10 >= someLimit || memoryFull.avg10 >= fullLimit;

    return {
      cpuSome,
      memorySome,
      memoryFull,
      emergencyHibernateRequired,
    };
  }

  private parsePsiLine(content: string, prefix: "some" | "full"): PsiMetricLine {
    const line = content
      .split("\n")
      .map((l) => l.trim())
      .find((l) => l.startsWith(`${prefix} `));
    if (!line) {
      return { avg10: 0, avg60: 0, avg300: 0, totalUs: 0 };
    }
    const kv: Record<string, number> = {};
    for (const token of line.split(/\s+/).slice(1)) {
      const [k, v] = token.split("=");
      if (k && v !== undefined) {
        kv[k] = Number(v);
      }
    }
    return {
      avg10: kv.avg10 ?? 0,
      avg60: kv.avg60 ?? 0,
      avg300: kv.avg300 ?? 0,
      totalUs: kv.total ?? 0,
    };
  }

  private readFileOrDefault(filePath: string, fallback: string): string {
    try {
      return fs.readFileSync(filePath, "utf8");
    } catch {
      return fallback;
    }
  }

  private getSandboxDir(sandboxId: string): string {
    return path.join(this.cgroupRoot, "ansts", sandboxId);
  }
}
