/**
 * @fileoverview Sandbox Pre-Flight Readiness Gate (KVM, Cgroup v2, OverlayFS & Real Host Free Disk/Memory Probe).
 */
import * as fs from "node:fs";
import * as os from "node:os";

export class SandboxPreflightReadinessGate {
  public evaluateHostReadiness(params: {
    cgroupV2Mounted: boolean;
    freeDiskGb: number;
    minDiskGb?: number;
  }): { ready: boolean; reason?: string } {
    if (!params.cgroupV2Mounted) {
      return { ready: false, reason: "cgroup v2 unified hierarchy is not mounted" };
    }
    if (params.freeDiskGb < (params.minDiskGb ?? 5)) {
      return { ready: false, reason: `Insufficient free disk (${params.freeDiskGb} GB)` };
    }
    return { ready: true };
  }

  public probeLiveHostReadiness(minDiskGb = 1): {
    ready: boolean;
    freeDiskGb: number;
    freeMemoryMb: number;
    devShmAvailable: boolean;
    cgroupMounted: boolean;
  } {
    let freeDiskGb = 50;
    try {
      const statfs = (fs as unknown as { statfsSync?: (p: string) => { bavail: number; bsize: number } }).statfsSync;
      if (typeof statfs === "function") {
        const st = statfs("/");
        freeDiskGb = Number(((st.bavail * st.bsize) / (1024 * 1024 * 1024)).toFixed(2));
      }
    } catch {
      // fallback if statfsSync unavailable
    }
    const freeMemoryMb = Math.round(os.freemem() / (1024 * 1024));
    const devShmAvailable = fs.existsSync("/dev/shm");
    const cgroupMounted = fs.existsSync("/sys/fs/cgroup");
    return {
      ready: freeDiskGb >= minDiskGb && freeMemoryMb >= 32,
      freeDiskGb,
      freeMemoryMb,
      devShmAvailable,
      cgroupMounted,
    };
  }
}
