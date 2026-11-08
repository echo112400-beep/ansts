"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.StagingCommitAndApprovalGate = void 0;


var _models = require("../models.js"); /**
 * @fileoverview M3 TASK-10: Staging Hold-and-Commit Buffer & Human-in-the-Loop Approval Gate.
 */









/** A high-risk tool write held in the HITL staging buffer before it touches production. */



























class StagingCommitAndApprovalGate {
  stagedByTask = new Map();
  tickets = new Map();
  /** Staged high-risk writes per task, waiting for a human decision. */
  stagedItems = new Map();
  /** Append-only ledger of writes that were approved and flushed to production. */
  committedProductionLedger = [];

  /**
   * Stages a high-risk production write. When `requireHumanApproval` is set, the task is parked in
   * `SUSPENDED_APPROVAL` until `approveAndResume` runs.
   */
  stageWrite(
  task,
  fence,
  toolName,
  payload,
  requireHumanApproval = true)
  {
    const item = {
      taskId: task.taskId,
      toolName,
      payload,
      fence,
      requireHumanApproval,
      approved: false,
      stagedAtMs: Date.now()
    };
    const list = this.stagedItems.get(task.taskId);
    if (list) {
      list.push(item);
    } else {
      this.stagedItems.set(task.taskId, [item]);
    }
    if (requireHumanApproval) {
      task.state = _models.TaskState.SUSPENDED_APPROVAL;
    }
    return item;
  }

  /** Marks every staged write of `task` as human-approved and resumes the task. */
  approveAndResume(task) {
    const list = this.stagedItems.get(task.taskId) ?? [];
    let approvedCount = 0;
    for (const item of list) {
      if (!item.approved) {
        item.approved = true;
        approvedCount += 1;
      }
    }
    task.state = _models.TaskState.RUNNING_ACT;
    return approvedCount;
  }

  /** Moves all approved writes of `taskId` into the committed production ledger. */
  atomicFlush(taskId) {
    const list = this.stagedItems.get(taskId) ?? [];
    const approved = list.filter((item) => item.approved);
    for (const item of approved) {
      this.committedProductionLedger.push({
        taskId: item.taskId,
        toolName: item.toolName,
        payload: item.payload,
        fence: item.fence,
        committedAtMs: Date.now()
      });
    }
    const remaining = list.filter((item) => !item.approved);
    if (remaining.length === 0) {
      this.stagedItems.delete(taskId);
    } else {
      this.stagedItems.set(taskId, remaining);
    }
    return approved.length;
  }

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