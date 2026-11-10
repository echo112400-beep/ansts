"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LinuxCgroupV2AndPsiDriver = void 0;



var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M11 TASK-38 & M58-2: Real Linux cgroup v2 (`cpu.max`, `memory.high`, `cgroup.freeze`) & `/proc/pressure/memory` Driver
 * with TaskId Path-Traversal Sanitization & Finite Resource Validation.
 */class LinuxCgroupV2AndPsiDriver {cgroupRootDir;

  constructor(cgroupRootDir) {
    this.cgroupRootDir = cgroupRootDir;
    fs.mkdirSync(this.cgroupRootDir, { recursive: true });
  }

  ensureTaskCgroup(taskId) {
    const safeId = taskId.trim().replace(/[^a-zA-Z0-9._-]/g, "_");
    if (!safeId || safeId === "." || safeId === "..") {
      throw new Error(`Invalid taskId '${taskId}' for cgroup v2 directory`);
    }
    const dir = path.join(this.cgroupRootDir, `ansts-${safeId}`);
    fs.mkdirSync(dir, { recursive: true });
    return dir;
  }

  writePhaseLimits(
  taskId,
  phase,
  cpuCores,
  memoryHighMb,
  memoryMaxMb)
  {
    if (!Number.isFinite(cpuCores) || cpuCores <= 0 || !Number.isFinite(memoryHighMb) || memoryHighMb <= 0 || !Number.isFinite(memoryMaxMb) || memoryMaxMb < memoryHighMb) {
      throw new Error(`Invalid cgroup v2 phase limits: cpu=${cpuCores}, memHighMb=${memoryHighMb}, memMaxMb=${memoryMaxMb}`);
    }
    const dir = this.ensureTaskCgroup(taskId);
    const quotaUs = Math.round(cpuCores * 100_000);
    const cpuMaxContent = `${quotaUs} 100000`;
    const memoryHighBytes = String(Math.round(memoryHighMb * 1024 * 1024));
    const memoryMaxBytes = String(Math.round(memoryMaxMb * 1024 * 1024));

    fs.writeFileSync(path.join(dir, "cpu.max"), cpuMaxContent, "utf-8");
    fs.writeFileSync(path.join(dir, "memory.high"), memoryHighBytes, "utf-8");
    fs.writeFileSync(path.join(dir, "memory.max"), memoryMaxBytes, "utf-8");
    fs.writeFileSync(path.join(dir, "ansts.phase"), phase, "utf-8");

    return { cpuMaxContent, memoryHighBytes, memoryMaxBytes };
  }

  setFreezeState(taskId, freeze) {
    const dir = this.ensureTaskCgroup(taskId);
    const val = freeze ? "1" : "0";
    fs.writeFileSync(path.join(dir, "cgroup.freeze"), val, "utf-8");
    return val;
  }

  static parseProcPressureMemory(content) {
    let someAvg10 = 0;
    let fullAvg10 = 0;
    for (const line of content.split("\n")) {
      const mSome = line.match(/^some\s+avg10=([0-9.]+)/);
      if (mSome) someAvg10 = parseFloat(mSome[1]);
      const mFull = line.match(/^full\s+avg10=([0-9.]+)/);
      if (mFull) fullAvg10 = parseFloat(mFull[1]);
    }
    return { someAvg10, fullAvg10 };
  }
}exports.LinuxCgroupV2AndPsiDriver = LinuxCgroupV2AndPsiDriver;