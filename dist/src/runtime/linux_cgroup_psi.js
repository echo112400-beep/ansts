"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LinuxCgroupV2AndPsiDriver = void 0;


var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M11 TASK-38: Real Linux cgroup v2 (`cpu.max`, `memory.high`, `cgroup.freeze`) & `/proc/pressure/memory` Driver.
 */class LinuxCgroupV2AndPsiDriver {
  cgroupRootDir;

  constructor(cgroupRootDir) {
    this.cgroupRootDir = cgroupRootDir;
    fs.mkdirSync(this.cgroupRootDir, { recursive: true });
  }

  ensureTaskCgroup(taskId) {
    const dir = path.join(this.cgroupRootDir, `ansts-${taskId}`);
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