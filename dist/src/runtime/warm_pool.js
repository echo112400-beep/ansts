"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.WarmSandboxPoolManager = exports.LayerFsNumberedSandboxMultiplexer = void 0; /**
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
}


/**
 * M17 TASK-87: LayerFS Numbered Self-Closing Sandbox Multiplexer
 * (Each Sandbox mounts up to 16 concurrent Workspaces, starts a new Sandbox only when existing ones hold 16,
 * and auto-closes after 120 seconds of inactivity).
 */exports.WarmSandboxPoolManager = WarmSandboxPoolManager;







class LayerFsNumberedSandboxMultiplexer {
  static MAX_WORKSPACES_PER_SANDBOX = 16;
  static IDLE_CLOSE_TIMEOUT_MS = 120_000;
  sandboxes = new Map();
  nextSandboxNumber = 1;

  mountWorkspaceIntoSandbox(workspaceId, nowMs = Date.now())



  {
    for (const sb of this.sandboxes.values()) {
      if (
      !sb.closed &&
      sb.activeWorkspaceIds.size < LayerFsNumberedSandboxMultiplexer.MAX_WORKSPACES_PER_SANDBOX)
      {
        sb.activeWorkspaceIds.add(workspaceId);
        sb.lastActiveAtMs = nowMs;
        return {
          sandboxNumber: sb.sandboxNumber,
          activeWorkspacesOnSandbox: sb.activeWorkspaceIds.size,
          spawnedNewSandbox: false
        };
      }
    }

    const sandboxNumber = this.nextSandboxNumber++;
    const created = {
      sandboxNumber,
      activeWorkspaceIds: new Set([workspaceId]),
      lastActiveAtMs: nowMs,
      closed: false
    };
    this.sandboxes.set(sandboxNumber, created);
    return {
      sandboxNumber,
      activeWorkspacesOnSandbox: 1,
      spawnedNewSandbox: true
    };
  }

  unmountWorkspace(sandboxNumber, workspaceId, nowMs = Date.now()) {
    const sb = this.sandboxes.get(sandboxNumber);
    if (!sb) return;
    sb.activeWorkspaceIds.delete(workspaceId);
    sb.lastActiveAtMs = nowMs;
  }

  sweepIdleSandboxes(nowMs = Date.now()) {
    const closedNumbers = [];
    for (const sb of this.sandboxes.values()) {
      if (
      !sb.closed &&
      sb.activeWorkspaceIds.size === 0 &&
      nowMs - sb.lastActiveAtMs >= LayerFsNumberedSandboxMultiplexer.IDLE_CLOSE_TIMEOUT_MS)
      {
        sb.closed = true;
        closedNumbers.push(sb.sandboxNumber);
      }
    }
    return closedNumbers;
  }
}exports.LayerFsNumberedSandboxMultiplexer = LayerFsNumberedSandboxMultiplexer;