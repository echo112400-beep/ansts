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
exports.EbpfLsmSecurityBridge = void 0;
/**
 * @fileoverview M22, M58-2 & M67-4: eBPF LSM Kernel Security Policy Bridge
 * (Syscall, Null-Byte Injection Rejection, `/proc/<pid>/*` Escape Protection, Root-Anchored Path Normalization & Linux Seccomp/Cgroup Probe).
 */
const fs = __importStar(require("node:fs"));
const path = __importStar(require("node:path"));
class EbpfLsmSecurityBridge {
    deniedPaths = [
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
    checkFileOpenHook(filePath) {
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
    inspectHostKernelSecurityPosture() {
        let seccompMode = 0;
        let noNewPrivs = 0;
        try {
            if (fs.existsSync("/proc/self/status")) {
                const statusText = fs.readFileSync("/proc/self/status", "utf-8");
                const mSec = statusText.match(/^Seccomp:\s*(\d+)/m);
                if (mSec)
                    seccompMode = Number(mSec[1]);
                const mPriv = statusText.match(/^NoNewPrivs:\s*(\d+)/m);
                if (mPriv)
                    noNewPrivs = Number(mPriv[1]);
            }
        }
        catch {
            // fallback on non-Linux hosts
        }
        const cgroupV2UnifiedMounted = fs.existsSync("/sys/fs/cgroup/cgroup.controllers");
        return { seccompMode, cgroupV2UnifiedMounted, noNewPrivs };
    }
}
exports.EbpfLsmSecurityBridge = EbpfLsmSecurityBridge;
