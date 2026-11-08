/**
 * @fileoverview M4 TASK-12: Pre-warmed Firecracker / Kata MicroVM Snapshot Pool Manager.
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

  public acquireSandbox(taskId: string, templateHash = "ubuntu24-py312-node22-v1"): MicroVmInstance {
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

  public releaseSandbox(taskId: string): boolean {
    const vm = this.activeVms.get(taskId);
    if (!vm) return false;
    vm.state = "DESTROYED";
    this.activeVms.delete(taskId);
    this.replenishPool(vm.templateHash);
    return true;
  }

  public stats(): { warmCount: number; activeCount: number } {
    return { warmCount: this.warmPool.length, activeCount: this.activeVms.size };
  }
}
