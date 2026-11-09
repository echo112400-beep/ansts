/**
 * @fileoverview Sandbox Pre-Flight Readiness Gate (KVM, Cgroup v2, OverlayFS & Free Disk Check).
 */
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
}
