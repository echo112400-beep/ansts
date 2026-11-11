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
exports.LinuxPsiMemoryGuard = void 0;
/**
 * @fileoverview M4 TASK-14, M48-4, M58-4 & M62-3: Linux PSI (Pressure Stall Information) Memory Guard,
 * Real `/proc/pressure/memory` + `/proc/meminfo` Kernel Probe, Non-Terminal Victim Filtering,
 * Non-Negative / Finite PSI Sanitization & High-Priority P0/P1 Eviction Immunity.
 */
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const models_js_1 = require("../models.js");
const TERMINAL_STATES = new Set([
    models_js_1.TaskState.SUCCEEDED,
    models_js_1.TaskState.FAILED,
    models_js_1.TaskState.DEAD_LETTER,
    models_js_1.TaskState.CANCELLED,
]);
class LinuxPsiMemoryGuard {
    softPsiThresholdPct;
    criticalPsiThresholdPct;
    constructor(softPsiThresholdPct = 40.0, criticalPsiThresholdPct = 70.0) {
        this.softPsiThresholdPct = softPsiThresholdPct;
        this.criticalPsiThresholdPct = criticalPsiThresholdPct;
    }
    probeLiveKernelPsiSample() {
        let someAvg10Pct = 1.2;
        let fullAvg10Pct = 0.2;
        let freeSwapMb = 2048;
        const kernelPsiFilePresent = fs.existsSync("/proc/pressure/memory");
        try {
            if (kernelPsiFilePresent) {
                const content = fs.readFileSync("/proc/pressure/memory", "utf-8");
                for (const line of content.split("\n")) {
                    const mSome = line.match(/^some\s+avg10=([0-9.]+)/);
                    if (mSome) {
                        const parsed = parseFloat(mSome[1]);
                        if (Number.isFinite(parsed) && parsed >= 0)
                            someAvg10Pct = parsed;
                    }
                    const mFull = line.match(/^full\s+avg10=([0-9.]+)/);
                    if (mFull) {
                        const parsed = parseFloat(mFull[1]);
                        if (Number.isFinite(parsed) && parsed >= 0)
                            fullAvg10Pct = parsed;
                    }
                }
            }
            if (fs.existsSync("/proc/meminfo")) {
                const meminfo = fs.readFileSync("/proc/meminfo", "utf-8");
                const mSwap = meminfo.match(/^SwapFree:\s+(\d+)\s+kB/m);
                if (mSwap) {
                    const rawKb = Number(mSwap[1]);
                    if (Number.isFinite(rawKb) && rawKb >= 0) {
                        freeSwapMb = Math.max(256, Math.round(rawKb / 1024));
                    }
                }
            }
        }
        catch {
            // fallback on non-Linux hosts
        }
        const freeMemMb = Math.round(os.freemem() / (1024 * 1024));
        return {
            someAvg10Pct,
            fullAvg10Pct,
            freeSwapMb,
            kernelPsiFilePresent,
            freeMemMb,
        };
    }
    /**
     * [M58-4 & M62-3] DFX-Safety: Evaluates PSI pressure and applies defensive actions:
     * - Sanitizes negative or NaN metrics in incoming `sample` to prevent mis-triggered emergency freezes.
     * - Restricts critical freeze-and-checkpoint eviction to `P2_STANDARD` and `P3_BATCH` tasks only,
     *   providing immunity to `P0_EMERGENCY` and `P1_HIGH` tasks under memory pressure so control-plane
     *   remediation and user-critical jobs are never abruptly sacrificed.
     */
    evaluateAndDefend(sample, runningTasks) {
        const safeSome = Number.isFinite(sample.someAvg10Pct) && sample.someAvg10Pct >= 0 ? sample.someAvg10Pct : 0;
        const safeFull = Number.isFinite(sample.fullAvg10Pct) && sample.fullAvg10Pct >= 0 ? sample.fullAvg10Pct : 0;
        const safeSwap = Number.isFinite(sample.freeSwapMb) && sample.freeSwapMb >= 0 ? sample.freeSwapMb : 1024;
        const liveTasks = runningTasks.filter((t) => !TERMINAL_STATES.has(t.state));
        if (safeFull >= this.criticalPsiThresholdPct || safeSwap < 128) {
            const evictableCandidates = liveTasks
                .filter((t) => t.priority >= models_js_1.TaskPriority.P2_STANDARD) // P0 & P1 have emergency immunity
                .sort((a, b) => {
                if (b.priority !== a.priority)
                    return b.priority - a.priority;
                return b.resource_profile.act_memory_mb - a.resource_profile.act_memory_mb;
            });
            const victims = evictableCandidates.length > 0
                ? evictableCandidates
                    .slice(0, Math.max(1, Math.ceil(evictableCandidates.length * 0.25)))
                    .map((t) => t.task_id)
                : [];
            return { level: "FREEZE_AND_CHECKPOINT", evictedTaskIds: victims };
        }
        if (safeSome >= this.softPsiThresholdPct) {
            const p3Victims = liveTasks
                .filter((t) => t.priority === models_js_1.TaskPriority.P3_BATCH)
                .map((t) => t.task_id);
            return { level: "THROTTLE_BATCH", evictedTaskIds: p3Victims };
        }
        return { level: "NORMAL", evictedTaskIds: [] };
    }
}
exports.LinuxPsiMemoryGuard = LinuxPsiMemoryGuard;
