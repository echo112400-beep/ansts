"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CgroupPhaseController = void 0; /**
 * @fileoverview M4 TASK-13 & M62-1: Linux cgroup v2 Dynamic CPU/Memory Phase-Decoupled Controller
 * with Sub-Core Hard Floor (Preventing CFS Starvation), Non-Negative Resource Validation & Idempotent Cleanup.
 */











class CgroupPhaseController {
  allocations = new Map();
  static MIN_CPU_QUOTA_US = 5000; // 0.05 core floor (5000us per 100ms) to prevent cgroup freeze

  applyThinkPhase(taskId, profile) {
    const rawCpu = profile.think_cpu_cores;
    const safeCpu = Number.isFinite(rawCpu) && rawCpu > 0 ? rawCpu : 0.25;
    const quotaUs = Math.max(
    CgroupPhaseController.MIN_CPU_QUOTA_US,
    Math.round(safeCpu * 100_000));


    const state = {
      taskId,
      phase: "THINK",
      cpuQuotaUsPer100ms: quotaUs,
      cpuWeight: 20,
      memoryHighBytes: 256 * 1024 * 1024,
      memoryMaxBytes: 512 * 1024 * 1024
    };
    this.allocations.set(taskId, state);
    return state;
  }

  applyActPhase(taskId, profile) {
    const rawCpu = profile.act_cpu_cores;
    const safeCpu = Number.isFinite(rawCpu) && rawCpu > 0 ? rawCpu : 4.0;
    const rawMem = profile.act_memory_mb;
    const safeMem = Number.isFinite(rawMem) && rawMem >= 64 ? rawMem : 4096;
    const quotaUs = Math.max(
    CgroupPhaseController.MIN_CPU_QUOTA_US,
    Math.round(safeCpu * 100_000));


    const state = {
      taskId,
      phase: "ACT",
      cpuQuotaUsPer100ms: quotaUs,
      cpuWeight: 200,
      memoryHighBytes: Math.round(safeMem * 0.85 * 1024 * 1024),
      memoryMaxBytes: Math.round(safeMem * 1024 * 1024)
    };
    this.allocations.set(taskId, state);
    return state;
  }

  getAllocation(taskId) {
    return this.allocations.get(taskId);
  }

  removeTask(taskId) {
    this.allocations.delete(taskId);
  }
}exports.CgroupPhaseController = CgroupPhaseController;