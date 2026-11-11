"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.SandboxPreflightReadinessGate = void 0;
/**
 * @fileoverview Sandbox Pre-Flight Readiness Gate (KVM, Cgroup v2, OverlayFS & Real Host Free Disk/Memory Probe).
 */
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
class SandboxPreflightReadinessGate {
    evaluateHostReadiness(params) {
        if (!params.cgroupV2Mounted) {
            return { ready: false, reason: "cgroup v2 unified hierarchy is not mounted" };
        }
        if (params.freeDiskGb < (params.minDiskGb ?? 5)) {
            return { ready: false, reason: `Insufficient free disk (${params.freeDiskGb} GB)` };
        }
        return { ready: true };
    }
    probeLiveHostReadiness(minDiskGb = 1) {
        let freeDiskGb = 50;
        try {
            const statfs = fs.statfsSync;
            if (typeof statfs === "function") {
                const st = statfs("/");
                freeDiskGb = Number(((st.bavail * st.bsize) / (1024 * 1024 * 1024)).toFixed(2));
            }
        }
        catch {
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
exports.SandboxPreflightReadinessGate = SandboxPreflightReadinessGate;
