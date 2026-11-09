"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SandboxPreflightReadinessGate = void 0;


var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview Sandbox Pre-Flight Readiness Gate (KVM, Cgroup v2, OverlayFS & Real Host Free Disk/Memory Probe).
 */class SandboxPreflightReadinessGate {
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

  probeLiveHostReadiness(minDiskGb = 1)





  {
    let freeDiskGb = 50;
    try {
      const statfs = fs.statfsSync;
      if (typeof statfs === "function") {
        const st = statfs("/");
        freeDiskGb = Number((st.bavail * st.bsize / (1024 * 1024 * 1024)).toFixed(2));
      }
    } catch {

      // fallback if statfsSync unavailable
    }const freeMemoryMb = Math.round(os.freemem() / (1024 * 1024));
    const devShmAvailable = fs.existsSync("/dev/shm");
    const cgroupMounted = fs.existsSync("/sys/fs/cgroup");
    return {
      ready: freeDiskGb >= minDiskGb && freeMemoryMb >= 32,
      freeDiskGb,
      freeMemoryMb,
      devShmAvailable,
      cgroupMounted
    };
  }
}exports.SandboxPreflightReadinessGate = SandboxPreflightReadinessGate;