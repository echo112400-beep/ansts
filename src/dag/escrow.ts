/**
 * @fileoverview M2 TASK-05 & M52-3: Deadlock-Free Parent-Child Sub-Agent Quota Escrow Coordinator
 * with Child-Failure Unblocking & Parent Escrow Slot Reclamation.
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

  /**
   * [M52-3] Handles child sub-agent failure/cancellation so the parent never hangs forever in `WAITING_SUB_AGENT`,
   * and reclaims the escrowed slot immediately.
   */
  public failOrCancelChildSubAgent(
    parent: TaskSpec,
    childTaskId: string,
    abortParentOnChildFailure = true
  ): { parentUnblocked: boolean; parentState: TaskState; reclaimedEscrowSlots: number } {
    const escrow = this.escrows.get(parent.task_id);
    let reclaimed = 0;
    if (escrow) {
      if (escrow.childTaskIds.delete(childTaskId) && escrow.usedSlots > 0) {
        escrow.usedSlots -= 1;
        reclaimed += 1;
      }
    }
    this.dependencyGraph.delete(childTaskId);

    if (abortParentOnChildFailure) {
      parent.state = TaskState.DEAD_LETTER;
      parent.dead_letter_reason = `CHILD_SUB_AGENT_FAILED:${childTaskId}`;
      reclaimed += this.releaseParentEscrow(parent.task_id);
      return { parentUnblocked: true, parentState: parent.state, reclaimedEscrowSlots: reclaimed };
    }

    if (!escrow || escrow.childTaskIds.size === 0) {
      parent.state = TaskState.RUNNING_THINK;
      return { parentUnblocked: true, parentState: parent.state, reclaimedEscrowSlots: reclaimed };
    }
    return { parentUnblocked: false, parentState: parent.state, reclaimedEscrowSlots: reclaimed };
  }

  /**
   * [M52-3] Releases a parent task's escrow reservation upon completion or dead-letter termination.
   */
  public releaseParentEscrow(parentTaskId: string): number {
    const escrow = this.escrows.get(parentTaskId);
    if (!escrow) return 0;
    const freed = escrow.escrowedSlots;
    this.escrows.delete(parentTaskId);
    return freed;
  }

  public getTenantEscrowedSlots(tenantId: string): number {
    let total = 0;
    for (const rec of this.escrows.values()) {
      if (rec.tenantId === tenantId) {
        total += rec.escrowedSlots;
      }
    }
    return total;
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
