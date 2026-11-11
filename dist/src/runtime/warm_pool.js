"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.WarmSandboxPoolManager = exports.LayerFsNumberedSandboxMultiplexer = void 0; /**
 * @fileoverview M4 TASK-12, M53-3 & M66-4: Pre-warmed Firecracker / Kata MicroVM Snapshot Pool Manager
 * with Idempotent Task Sandbox Re-Allocation, Target Count Clamping & Closed-Sandbox Pruning in Multiplexer.
 */









class WarmSandboxPoolManager {
  nodeId;
  targetWarmCount;
  warmPool = [];
  activeVms = new Map();
  vmSeq = 0;

  constructor(nodeId, targetWarmCount = 8) {
    this.nodeId = nodeId;
    const rawTarget = Number(targetWarmCount);
    this.targetWarmCount =
    Number.isFinite(rawTarget) && rawTarget >= 0 ?
    Math.min(256, Math.floor(rawTarget)) :
    8;
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

  /**
   * [M53-3] Idempotent sandbox allocation:
   * - If `taskId` already holds an ALLOCATED MicroVM with the same `templateHash`, return it idempotently without leaking a VM.
   * - If `taskId` holds an ALLOCATED MicroVM with a different `templateHash`, destroy the previous VM before allocating the new one.
   */
  acquireSandbox(taskId, templateHash = "ubuntu24-py312-node22-v1") {
    const existing = this.activeVms.get(taskId);
    if (existing && existing.state === "ALLOCATED") {
      if (existing.templateHash === templateHash) {
        return existing;
      }
      existing.state = "DESTROYED";
      this.activeVms.delete(taskId);
    }

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

  getActiveVm(taskId) {
    return this.activeVms.get(taskId);
  }

  releaseSandbox(taskId) {
    const vm = this.activeVms.get(taskId);
    if (!vm) return false;
    vm.state = "DESTROYED";
    this.activeVms.delete(taskId);
    this.replenishPool(vm.templateHash);
    return true;
  }

  /**
   * [M53-3] Reclaim orphaned MicroVMs whose owning task is no longer active in the scheduler/lease table.
   */
  reclaimOrphanedSandboxes(activeTaskIds) {
    const reclaimedVmIds = [];
    for (const [taskId, vm] of Array.from(this.activeVms.entries())) {
      if (!activeTaskIds.has(taskId)) {
        vm.state = "DESTROYED";
        this.activeVms.delete(taskId);
        reclaimedVmIds.push(vm.vmId);
        this.replenishPool(vm.templateHash);
      }
    }
    return reclaimedVmIds;
  }

  stats() {
    return { warmCount: this.warmPool.length, activeCount: this.activeVms.size };
  }
}

/**
 * M17 TASK-87, M53-3 & M66-4: LayerFS Numbered Self-Closing Sandbox Multiplexer
 * (Each Sandbox mounts up to 16 concurrent Workspaces, prevents duplicate multi-sandbox mounts for the same workspaceId,
 * starts a new Sandbox only when existing ones hold 16, auto-closes after 120 seconds of inactivity,
 * and prunes closed entries from `sandboxes` so linear lookup never degrades).
 */exports.WarmSandboxPoolManager = WarmSandboxPoolManager;







class LayerFsNumberedSandboxMultiplexer {
  static MAX_WORKSPACES_PER_SANDBOX = 16;
  static IDLE_CLOSE_TIMEOUT_MS = 120_000;
  sandboxes = new Map();
  nextSandboxNumber = 1;

  mountWorkspaceIntoSandbox(workspaceId, nowMs = Date.now())



  {
    // [M53-3] First check if workspaceId is already mounted in any open sandbox (idempotent re-mount guard)
    for (const sb of this.sandboxes.values()) {
      if (!sb.closed && sb.activeWorkspaceIds.has(workspaceId)) {
        sb.lastActiveAtMs = nowMs;
        return {
          sandboxNumber: sb.sandboxNumber,
          activeWorkspacesOnSandbox: sb.activeWorkspaceIds.size,
          spawnedNewSandbox: false
        };
      }
    }

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

  /**
   * [M66-4] Closes idle sandboxes and physically deletes closed entries from `this.sandboxes`
   * so long-running nodes do not accumulate dead sandbox descriptors.
   */
  sweepIdleSandboxes(nowMs = Date.now()) {
    const closedNumbers = [];
    for (const [num, sb] of Array.from(this.sandboxes.entries())) {
      if (sb.closed) {
        this.sandboxes.delete(num);
        continue;
      }
      if (
      sb.activeWorkspaceIds.size === 0 &&
      nowMs - sb.lastActiveAtMs >= LayerFsNumberedSandboxMultiplexer.IDLE_CLOSE_TIMEOUT_MS)
      {
        sb.closed = true;
        closedNumbers.push(sb.sandboxNumber);
        this.sandboxes.delete(num);
      }
    }
    return closedNumbers;
  }

  getActiveSandboxCount() {
    return this.sandboxes.size;
  }
}exports.LayerFsNumberedSandboxMultiplexer = LayerFsNumberedSandboxMultiplexer;