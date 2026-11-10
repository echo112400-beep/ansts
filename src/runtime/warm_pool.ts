/**
 * @fileoverview M4 TASK-12 & M53-3: Pre-warmed Firecracker / Kata MicroVM Snapshot Pool Manager
 * with Idempotent Task Sandbox Re-Allocation & Orphan VM Reclamation.
 */
export interface MicroVmInstance {
  vmId: string;
  nodeId: string;
  templateHash: string;
  state: "WARM_STANDBY" | "ALLOCATED" | "DESTROYED";
  allocatedTaskId?: string;
  bootLatencyMs: number;
}

export class WarmSandboxPoolManager {
  private readonly nodeId: string;
  private readonly targetWarmCount: number;
  private readonly warmPool: MicroVmInstance[] = [];
  private readonly activeVms: Map<string, MicroVmInstance> = new Map();
  private vmSeq = 0;

  constructor(nodeId: string, targetWarmCount = 8) {
    this.nodeId = nodeId;
    this.targetWarmCount = targetWarmCount;
    this.replenishPool();
  }

  public replenishPool(templateHash = "ubuntu24-py312-node22-v1"): number {
    let created = 0;
    while (this.warmPool.length < this.targetWarmCount) {
      this.vmSeq += 1;
      this.warmPool.push({
        vmId: `fc-vm-${this.nodeId}-${this.vmSeq}`,
        nodeId: this.nodeId,
        templateHash,
        state: "WARM_STANDBY",
        bootLatencyMs: 65,
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
  public acquireSandbox(taskId: string, templateHash = "ubuntu24-py312-node22-v1"): MicroVmInstance {
    const existing = this.activeVms.get(taskId);
    if (existing && existing.state === "ALLOCATED") {
      if (existing.templateHash === templateHash) {
        return existing;
      }
      existing.state = "DESTROYED";
      this.activeVms.delete(taskId);
    }

    const idx = this.warmPool.findIndex((vm) => vm.templateHash === templateHash);
    let vm: MicroVmInstance;
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
        bootLatencyMs: 145,
      };
    }
    this.activeVms.set(taskId, vm);
    this.replenishPool(templateHash);
    return vm;
  }

  public getActiveVm(taskId: string): MicroVmInstance | undefined {
    return this.activeVms.get(taskId);
  }

  public releaseSandbox(taskId: string): boolean {
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
  public reclaimOrphanedSandboxes(activeTaskIds: Set<string>): string[] {
    const reclaimedVmIds: string[] = [];
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

  public stats(): { warmCount: number; activeCount: number } {
    return { warmCount: this.warmPool.length, activeCount: this.activeVms.size };
  }
}

/**
 * M17 TASK-87 & M53-3: LayerFS Numbered Self-Closing Sandbox Multiplexer
 * (Each Sandbox mounts up to 16 concurrent Workspaces, prevents duplicate multi-sandbox mounts for the same workspaceId,
 * starts a new Sandbox only when existing ones hold 16, and auto-closes after 120 seconds of inactivity).
 */
export interface NumberedLayerFsSandbox {
  sandboxNumber: number;
  activeWorkspaceIds: Set<string>;
  lastActiveAtMs: number;
  closed: boolean;
}

export class LayerFsNumberedSandboxMultiplexer {
  public static readonly MAX_WORKSPACES_PER_SANDBOX = 16;
  public static readonly IDLE_CLOSE_TIMEOUT_MS = 120_000;
  private readonly sandboxes: Map<number, NumberedLayerFsSandbox> = new Map();
  private nextSandboxNumber = 1;

  public mountWorkspaceIntoSandbox(workspaceId: string, nowMs = Date.now()): {
    sandboxNumber: number;
    activeWorkspacesOnSandbox: number;
    spawnedNewSandbox: boolean;
  } {
    // [M53-3] First check if workspaceId is already mounted in any open sandbox (idempotent re-mount guard)
    for (const sb of this.sandboxes.values()) {
      if (!sb.closed && sb.activeWorkspaceIds.has(workspaceId)) {
        sb.lastActiveAtMs = nowMs;
        return {
          sandboxNumber: sb.sandboxNumber,
          activeWorkspacesOnSandbox: sb.activeWorkspaceIds.size,
          spawnedNewSandbox: false,
        };
      }
    }

    for (const sb of this.sandboxes.values()) {
      if (
        !sb.closed &&
        sb.activeWorkspaceIds.size < LayerFsNumberedSandboxMultiplexer.MAX_WORKSPACES_PER_SANDBOX
      ) {
        sb.activeWorkspaceIds.add(workspaceId);
        sb.lastActiveAtMs = nowMs;
        return {
          sandboxNumber: sb.sandboxNumber,
          activeWorkspacesOnSandbox: sb.activeWorkspaceIds.size,
          spawnedNewSandbox: false,
        };
      }
    }

    const sandboxNumber = this.nextSandboxNumber++;
    const created: NumberedLayerFsSandbox = {
      sandboxNumber,
      activeWorkspaceIds: new Set([workspaceId]),
      lastActiveAtMs: nowMs,
      closed: false,
    };
    this.sandboxes.set(sandboxNumber, created);
    return {
      sandboxNumber,
      activeWorkspacesOnSandbox: 1,
      spawnedNewSandbox: true,
    };
  }

  public unmountWorkspace(sandboxNumber: number, workspaceId: string, nowMs = Date.now()): void {
    const sb = this.sandboxes.get(sandboxNumber);
    if (!sb) return;
    sb.activeWorkspaceIds.delete(workspaceId);
    sb.lastActiveAtMs = nowMs;
  }

  public sweepIdleSandboxes(nowMs = Date.now()): number[] {
    const closedNumbers: number[] = [];
    for (const sb of this.sandboxes.values()) {
      if (
        !sb.closed &&
        sb.activeWorkspaceIds.size === 0 &&
        nowMs - sb.lastActiveAtMs >= LayerFsNumberedSandboxMultiplexer.IDLE_CLOSE_TIMEOUT_MS
      ) {
        sb.closed = true;
        closedNumbers.push(sb.sandboxNumber);
      }
    }
    return closedNumbers;
  }
}
