"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LinuxPsiMemoryGuard = void 0;



var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var _models = require("../models.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M4 TASK-14 & M48-4: Linux PSI (Pressure Stall Information) Memory Guard,
 * Real `/proc/pressure/memory` + `/proc/meminfo` Kernel Probe & Graceful Evictor.
 */









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
          if (mSome) someAvg10Pct = parseFloat(mSome[1]);
          const mFull = line.match(/^full\s+avg10=([0-9.]+)/);
          if (mFull) fullAvg10Pct = parseFloat(mFull[1]);
        }
      }
      if (fs.existsSync("/proc/meminfo")) {
        const meminfo = fs.readFileSync("/proc/meminfo", "utf-8");
        const mSwap = meminfo.match(/^SwapFree:\s+(\d+)\s+kB/m);
        if (mSwap) {
          freeSwapMb = Math.max(256, Math.round(Number(mSwap[1]) / 1024));
        }
      }
    } catch {

      // fallback on non-Linux hosts
    }
    const freeMemMb = Math.round(os.freemem() / (1024 * 1024));
    return {
      someAvg10Pct,
      fullAvg10Pct,
      freeSwapMb,
      kernelPsiFilePresent,
      freeMemMb
    };
  }

  evaluateAndDefend(sample, runningTasks) {
    if (sample.fullAvg10Pct >= this.criticalPsiThresholdPct || sample.freeSwapMb < 128) {
      const candidates = [...runningTasks].sort((a, b) => {
        if (b.priority !== a.priority) return b.priority - a.priority;
        return b.resource_profile.act_memory_mb - a.resource_profile.act_memory_mb;
      });
      const victims = candidates.
      filter((t) => t.priority >= _models.TaskPriority.P1_HIGH).
      slice(0, Math.max(1, Math.ceil(candidates.length * 0.25))).
      map((t) => t.task_id);
      return { level: "FREEZE_AND_CHECKPOINT", evictedTaskIds: victims };
    }
    if (sample.someAvg10Pct >= this.softPsiThresholdPct) {
      const p3Victims = runningTasks.
      filter((t) => t.priority === _models.TaskPriority.P3_BATCH).
      map((t) => t.task_id);
      return { level: "THROTTLE_BATCH", evictedTaskIds: p3Victims };
    }
    return { level: "NORMAL", evictedTaskIds: [] };
  }
}exports.LinuxPsiMemoryGuard = LinuxPsiMemoryGuard;