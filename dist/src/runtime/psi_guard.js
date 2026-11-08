"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LinuxPsiMemoryGuard = void 0;


var _models = require("../models.js"); /**
 * @fileoverview M4 TASK-14: Linux PSI (Pressure Stall Information) Memory Guard & Graceful Evictor.
 */










class LinuxPsiMemoryGuard {
  softPsiThresholdPct;
  criticalPsiThresholdPct;

  constructor(softPsiThresholdPct = 40.0, criticalPsiThresholdPct = 70.0) {
    this.softPsiThresholdPct = softPsiThresholdPct;
    this.criticalPsiThresholdPct = criticalPsiThresholdPct;
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