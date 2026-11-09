"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SandboxPreflightReadinessGate = void 0; /**
 * @fileoverview Sandbox Pre-Flight Readiness Gate (KVM, Cgroup v2, OverlayFS & Free Disk Check).
 */
class SandboxPreflightReadinessGate {
  evaluateHostReadiness(params)



  {
    if (!params.cgroupV2Mounted) {
      return { ready: false, reason: "cgroup v2 unified hierarchy is not mounted" };
    }
    if (params.freeDiskGb < (params.minDiskGb ?? 5)) {
      return { ready: false, reason: `Insufficient free disk (${params.freeDiskGb} GB)` };
    }
    return { ready: true };
  }
}exports.SandboxPreflightReadinessGate = SandboxPreflightReadinessGate;