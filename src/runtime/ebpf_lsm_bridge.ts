/**
 * @fileoverview M22, M58-2 & M67-4: eBPF LSM Kernel Security Policy Bridge
 * (Syscall, Null-Byte Injection Rejection, `/proc/<pid>/*` Escape Protection, Root-Anchored Path Normalization & Linux Seccomp/Cgroup Probe).
 */
import * as fs from "node:fs";
import * as path from "node:path";

export class EbpfLsmSecurityBridge {
  private readonly deniedPaths: string[] = [
    "/etc/shadow",
    "/etc/gshadow",
    "/etc/sudoers",
    "/proc/kcore",
    "/proc/self/environ",
    "/proc/self/mem",
    "/proc/1/environ",
    "/var/run/docker.sock",
    "/run/containerd/containerd.sock",
    "/var/run/secrets/kubernetes.io",
  ];

  public checkFileOpenHook(filePath: string): {
    allowed: boolean;
    errno: number;
    normalizedPath: string;
  } {
    if (typeof filePath !== "string" || filePath.includes("\0")) {
      return { allowed: false, errno: 13, normalizedPath: String(filePath ?? "").replace(/\0/g, "") };
    }
    const raw = filePath.trim().replace(/\\/g, "/");
    // [M58-2] Anchor relative or traversal paths (e.g. "../../etc/shadow" or "etc/shadow") against "/"
    // so relative path tricks cannot bypass root-anchored LSM deny rules.
    const normalizedPath = path.posix.resolve("/", raw);
    // [M67-4] Block sensitive /proc/<pid|self|thread-self>/(environ|mem|maps|root|cwd|fd|exe) across ALL numeric PIDs
    if (/^\/proc\/(?:\d+|self|thread-self)\/(?:environ|mem|maps|root|cwd|fd|exe)(?:\/|$)/.test(normalizedPath)) {
      return { allowed: false, errno: 13, normalizedPath };
    }
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

export { EbpfSocketNetworkTrackerAndBreaker } from "./ebpf_network_tracker.js";
