/**
 * @fileoverview M4 TASK-12: Pre-warmed Firecracker / Kata MicroVM Snapshot Pool Manager.
 */
import * as fs from "node:fs";
import * as path from "node:path";
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
  private readonly scratchRoot?: string;
  private readonly warmPool: MicroVmInstance[] = [];
  private readonly activeVms: Map<string, MicroVmInstance> = new Map();
  private vmSeq = 0;

  constructor(nodeIdOrPoolSize: string | number = "local", targetWarmCountOrDir: number | string = 8) {
    if (typeof nodeIdOrPoolSize === "number") {
      this.nodeId = "warm-pool";
      this.targetWarmCount = nodeIdOrPoolSize;
      this.scratchRoot = typeof targetWarmCountOrDir === "string" ? targetWarmCountOrDir : undefined;
    } else {
      this.nodeId = nodeIdOrPoolSize;
      this.targetWarmCount = typeof targetWarmCountOrDir === "number" ? targetWarmCountOrDir : 8;
    }
    this.replenishPool();
  }

  public acquireWarmSandbox(poolId: string, runtime: string): { scratchFilePath: string; poolId: string; runtime: string } {
    const dir = this.scratchRoot ?? path.join("/tmp", `ansts-warm-${this.nodeId}`);
    fs.mkdirSync(dir, { recursive: true });
    const scratchFilePath = path.join(dir, `${poolId}.img`);
    fs.writeFileSync(scratchFilePath, Buffer.alloc(4096));
    return { scratchFilePath, poolId, runtime };
  }

  public teardownAndZeroBlocks(sandbox: { scratchFilePath?: string }): void {
    if (!sandbox.scratchFilePath || !fs.existsSync(sandbox.scratchFilePath)) return;
    const size = fs.statSync(sandbox.scratchFilePath).size;
    fs.writeFileSync(sandbox.scratchFilePath, Buffer.alloc(size));
    fs.unlinkSync(sandbox.scratchFilePath);
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


/**
 * M17 TASK-87: LayerFS Numbered Self-Closing Sandbox Multiplexer
 * (Each Sandbox mounts up to 16 concurrent Workspaces, starts a new Sandbox only when existing ones hold 16,
 * and auto-closes after 120 seconds of inactivity).
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
