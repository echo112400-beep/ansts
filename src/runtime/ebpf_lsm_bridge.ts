/**
 * @fileoverview eBPF LSM Kernel Security Policy Bridge (Syscall, Path Access & Linux Seccomp/Cgroup Probe).
 */
import * as fs from "node:fs";
import * as path from "node:path";

export class EbpfLsmSecurityBridge {
  private readonly deniedPaths: string[] = [
    "/etc/shadow",
    "/etc/gshadow",
    "/proc/kcore",
    "/var/run/docker.sock",
    "/run/containerd/containerd.sock",
  ];

  public checkFileOpenHook(filePath: string): {
    allowed: boolean;
    errno: number;
    normalizedPath: string;
  } {
    const normalizedPath = path.posix.normalize(filePath);
    if (this.deniedPaths.some((p) => normalizedPath === p || normalizedPath.startsWith(`${p}/`))) {
      return { allowed: false, errno: 13, normalizedPath };
    }
    return { allowed: true, errno: 0, normalizedPath };
  }

  public inspectHostKernelSecurityPosture(): {
    seccompMode: number;
    cgroupV2UnifiedMounted: boolean;
    noNewPrivs: number;
  } {
    let seccompMode = 0;
    let noNewPrivs = 0;
    try {
      if (fs.existsSync("/proc/self/status")) {
        const statusText = fs.readFileSync("/proc/self/status", "utf-8");
        const mSec = statusText.match(/^Seccomp:\s*(\d+)/m);
        if (mSec) seccompMode = Number(mSec[1]);
        const mPriv = statusText.match(/^NoNewPrivs:\s*(\d+)/m);
        if (mPriv) noNewPrivs = Number(mPriv[1]);
      }
    } catch {
      // fallback on non-Linux hosts
    }
    const cgroupV2UnifiedMounted = fs.existsSync("/sys/fs/cgroup/cgroup.controllers");
    return { seccompMode, cgroupV2UnifiedMounted, noNewPrivs };
  }
}
