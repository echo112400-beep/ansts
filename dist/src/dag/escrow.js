"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.DagEscrowCoordinator = void 0;


var _models = require("../models.js"); /**
 * @fileoverview M2 TASK-05: Deadlock-Free Parent-Child Sub-Agent Quota Escrow Coordinator.
 */







class DagEscrowCoordinator {
  escrows = new Map();
  dependencyGraph = new Map();

  reserveParentEscrow(parent, maxTenantQuota, currentTenantUsed) {
    const requestedEscrow = Math.max(0, parent.expected_SubAgent_count);
    if (currentTenantUsed + 1 + requestedEscrow > maxTenantQuota) {
      throw new Error(
        `Insufficient tenant quota to escrow ${requestedEscrow} SubAgent slots for parent ${parent.task_id}`
      );
    }
    const record = {
      parentTaskId: parent.task_id,
      tenantId: parent.tenant_id,
      escrowedSlots: requestedEscrow,
      usedSlots: 0,
      childTaskIds: new Set()
    };
    this.escrows.set(parent.task_id, record);
    return record;
  }

  registerChildSubAgent(parent, child) {
    this.assertNoCycle(child.task_id, new Set([parent.task_id, ...child.dependencies]));
    this.dependencyGraph.set(child.task_id, new Set([parent.task_id, ...child.dependencies]));

    parent.state = _models.TaskState.WAITING_SUB_AGENT;
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

  completeChildSubAgent(parent, childTaskId) {
    const escrow = this.escrows.get(parent.task_id);
    if (!escrow) {
      parent.state = _models.TaskState.RUNNING_THINK;
      return true;
    }
    escrow.childTaskIds.delete(childTaskId);
    if (escrow.usedSlots > 0) {
      escrow.usedSlots -= 1;
    }
    if (escrow.childTaskIds.size === 0) {
      parent.state = _models.TaskState.RUNNING_THINK;
      return true;
    }
    return false;
  }

  assertNoCycle(sourceTaskId, targets) {
    const visited = new Set();
    const stack = [...targets];
    while (stack.length > 0) {
      const curr = stack.pop();
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
}exports.DagEscrowCoordinator = DagEscrowCoordinator;