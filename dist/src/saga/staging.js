"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.StagingCommitAndApprovalGate = void 0;


var _models = require("../models.js"); /**
 * @fileoverview M3 TASK-10: Staging Hold-and-Commit Buffer & Human-in-the-Loop Approval Gate.
 */


















class StagingCommitAndApprovalGate {
  stagedByTask = new Map();
  tickets = new Map();

  stageEffect(effect) {
    const full = { ...effect, committed: false, aborted: false };
    let list = this.stagedByTask.get(effect.taskId);
    if (!list) {
      list = [];
      this.stagedByTask.set(effect.taskId, list);
    }
    list.push(full);
    return full;
  }

  commitStagedEffects(
  taskId,
  executor)
  {
    const list = this.stagedByTask.get(taskId) ?? [];
    const results = [];
    for (const item of list) {
      if (!item.committed && !item.aborted) {
        const output = executor(item.toolName, item.args);
        item.committed = true;
        results.push({ effectId: item.effectId, output });
      }
    }
    return results;
  }

  abortStagedEffects(taskId) {
    const list = this.stagedByTask.get(taskId) ?? [];
    let count = 0;
    for (const item of list) {
      if (!item.committed && !item.aborted) {
        item.aborted = true;
        count += 1;
      }
    }
    return count;
  }

  requestHumanApproval(task, toolName, args) {
    task.state = _models.TaskState.SUSPENDED_APPROVAL;
    const ticket = {
      ticketId: `approval-${task.task_id}-${Date.now()}`,
      taskId: task.task_id,
      toolName,
      args,
      status: "PENDING"
    };
    this.tickets.set(ticket.ticketId, ticket);
    return ticket;
  }

  resolveApproval(ticketId, approved, decidedBy, task) {
    const ticket = this.tickets.get(ticketId);
    if (!ticket) {
      throw new Error(`Approval ticket '${ticketId}' not found`);
    }
    ticket.status = approved ? "APPROVED" : "REJECTED";
    ticket.decidedBy = decidedBy;
    task.state = approved ? _models.TaskState.RUNNING_ACT : _models.TaskState.DEAD_LETTER;
    return ticket;
  }
}exports.StagingCommitAndApprovalGate = StagingCommitAndApprovalGate;