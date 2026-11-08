/**
 * @fileoverview M2 TASK-05: Deadlock-Free Parent-Child Sub-Agent Quota Escrow Coordinator.
 */
import { TaskSpec, TaskState } from "../models.js";

export interface QuotaEscrowRecord {
  parentTaskId: string;
  tenantId: string;
  escrowedSlots: number;
  usedSlots: number;
  childTaskIds: Set<string>;
}

export class DagEscrowCoordinator {
  private readonly escrows: Map<string, QuotaEscrowRecord> = new Map();
  private readonly dependencyGraph: Map<string, Set<string>> = new Map();

  public reserveParentEscrow(parent: TaskSpec, maxTenantQuota: number, currentTenantUsed: number): QuotaEscrowRecord {
    const requestedEscrow = Math.max(0, parent.expected_SubAgent_count);
    if (currentTenantUsed + 1 + requestedEscrow > maxTenantQuota) {
      throw new Error(
        `Insufficient tenant quota to escrow ${requestedEscrow} SubAgent slots for parent ${parent.task_id}`
      );
    }
    const record: QuotaEscrowRecord = {
      parentTaskId: parent.task_id,
      tenantId: parent.tenant_id,
      escrowedSlots: requestedEscrow,
      usedSlots: 0,
      childTaskIds: new Set(),
    };
    this.escrows.set(parent.task_id, record);
    return record;
  }

  public registerChildSubAgent(parent: TaskSpec, child: TaskSpec): { usedEscrowSlot: boolean } {
    this.assertNoCycle(child.task_id, new Set([parent.task_id, ...child.dependencies]));
    this.dependencyGraph.set(child.task_id, new Set([parent.task_id, ...child.dependencies]));

    parent.state = TaskState.WAITING_SUB_AGENT;
    const escrow = this.escrows.get(parent.task_id);
    if (escrow && escrow.usedSlots < escrow.escrowedSlots) {
      escrow.usedSlots += 1;
      escrow.childTaskIds.add(child.task_id);
      return { usedEscrowSlot: true };
    }
    if (escrow) {
      escrow.childTaskIds.add(child.task_id);
    }
    return { usedEscrowSlot: false };
  }

  public completeChildSubAgent(parent: TaskSpec, childTaskId: string): boolean {
    const escrow = this.escrows.get(parent.task_id);
    if (!escrow) {
      parent.state = TaskState.RUNNING_THINK;
      return true;
    }
    escrow.childTaskIds.delete(childTaskId);
    if (escrow.usedSlots > 0) {
      escrow.usedSlots -= 1;
    }
    if (escrow.childTaskIds.size === 0) {
      parent.state = TaskState.RUNNING_THINK;
      return true;
    }
    return false;
  }

  private assertNoCycle(sourceTaskId: string, targets: Set<string>): void {
    const visited = new Set<string>();
    const stack = [...targets];
    while (stack.length > 0) {
      const curr = stack.pop()!;
      if (curr === sourceTaskId) {
        throw new Error(`DAG cycle detected involving task '${sourceTaskId}'`);
      }
      if (visited.has(curr)) continue;
      visited.add(curr);
      const nextDeps = this.dependencyGraph.get(curr);
      if (nextDeps) {
        for (const dep of nextDeps) {
          stack.push(dep);
        }
      }
    }
  }
}
