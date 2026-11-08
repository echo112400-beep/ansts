"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.WarmSandboxPoolManager = void 0; /**
 * @fileoverview M4 TASK-12: Pre-warmed Firecracker / Kata MicroVM Snapshot Pool Manager.
 */









class WarmSandboxPoolManager {
  nodeId;
  targetWarmCount;
  warmPool = [];
  activeVms = new Map();
  vmSeq = 0;

  constructor(nodeId, targetWarmCount = 8) {
    this.nodeId = nodeId;
    this.targetWarmCount = targetWarmCount;
    this.replenishPool();
  }

  replenishPool(templateHash = "ubuntu24-py312-node22-v1") {
    let created = 0;
    while (this.warmPool.length < this.targetWarmCount) {
      this.vmSeq += 1;
      this.warmPool.push({
        vmId: `fc-vm-${this.nodeId}-${this.vmSeq}`,
        nodeId: this.nodeId,
        templateHash,
        state: "WARM_STANDBY",
        bootLatencyMs: 65
      });
      created += 1;
    }
    return created;
  }

  acquireSandbox(taskId, templateHash = "ubuntu24-py312-node22-v1") {
    const idx = this.warmPool.findIndex((vm) => vm.templateHash === templateHash);
    let vm;
    if (idx >= 0) {
      vm = this.warmPool.splice(idx, 1)[0];
      vm.state = "ALLOCATED";
      vm.allocatedTaskId = taskId;
    } else {
      this.vmSeq += 1;
      vm = {
        vmId: `fc-vm-${this.nodeId}-${this.vmSeq}`,
        nodeId: this.nodeId,
        templateHash,
        state: "ALLOCATED",
        allocatedTaskId: taskId,
        bootLatencyMs: 145
      };
    }
    this.activeVms.set(taskId, vm);
    this.replenishPool(templateHash);
    return vm;
  }

  releaseSandbox(taskId) {
    const vm = this.activeVms.get(taskId);
    if (!vm) return false;
    vm.state = "DESTROYED";
    this.activeVms.delete(taskId);
    this.replenishPool(vm.templateHash);
    return true;
  }

  stats() {
    return { warmCount: this.warmPool.length, activeCount: this.activeVms.size };
  }
}exports.WarmSandboxPoolManager = WarmSandboxPoolManager;