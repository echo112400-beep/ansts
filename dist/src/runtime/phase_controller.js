"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CgroupPhaseController = void 0; /**
 * @fileoverview M4 TASK-13: Linux cgroup v2 Dynamic CPU/Memory Phase-Decoupled Controller.
 */











class CgroupPhaseController {
  allocations = new Map();

  applyThinkPhase(taskId, profile) {
    const state = {
      taskId,
      phase: "THINK",
      cpuQuotaUsPer100ms: Math.round(profile.think_cpu_cores * 100_000),
      cpuWeight: 20,
      memoryHighBytes: 256 * 1024 * 1024,
      memoryMaxBytes: 512 * 1024 * 1024
    };
    this.allocations.set(taskId, state);
    return state;
  }

  applyActPhase(taskId, profile) {
    const state = {
      taskId,
      phase: "ACT",
      cpuQuotaUsPer100ms: Math.round(profile.act_cpu_cores * 100_000),
      cpuWeight: 200,
      memoryHighBytes: Math.round(profile.act_memory_mb * 0.85 * 1024 * 1024),
      memoryMaxBytes: Math.round(profile.act_memory_mb * 1024 * 1024)
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