"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.StagingCommitAndApprovalGate = void 0;



var _models = require("../models.js"); /**
 * @fileoverview M3 TASK-10 & M54-3: Staging Hold-and-Commit Buffer & Human-in-the-Loop Approval Gate
 * with Auto-Abort on Rejection & Approval TTL Expiration Sweep to prevent permanent SUSPENDED_APPROVAL deadlocks.
 */


















class StagingCommitAndApprovalGate {
  stagedByTask = new Map();
  tickets = new Map();
  ticketSeq = 0;

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

  getStagedEffects(taskId) {
    return [...(this.stagedByTask.get(taskId) ?? [])];
  }

  requestHumanApproval(
  task,
  toolName,
  args,
  nowMs = Date.now())
  {
    task.state = _models.TaskState.SUSPENDED_APPROVAL;
    this.ticketSeq += 1;
    const ticket = {
      ticketId: `approval-${task.task_id}-${nowMs}-${this.ticketSeq}`,
      taskId: task.task_id,
      toolName,
      args,
      status: "PENDING",
      createdAtMs: nowMs
    };
    this.tickets.set(ticket.ticketId, ticket);
    return ticket;
  }

  /**
   * [M54-3] Resolves an approval ticket. If rejected (`approved = false`), automatically aborts
   * all uncommitted staged side-effects for `task.task_id` so staged effects never leak.
   */
  resolveApproval(ticketId, approved, decidedBy, task) {
    const ticket = this.tickets.get(ticketId);
    if (!ticket) {
      throw new Error(`Approval ticket '${ticketId}' not found`);
    }
    ticket.status = approved ? "APPROVED" : "REJECTED";
    ticket.decidedBy = decidedBy;
    if (approved) {
      task.state = _models.TaskState.RUNNING_ACT;
    } else {
      task.state = _models.TaskState.DEAD_LETTER;
      this.abortStagedEffects(ticket.taskId);
    }
    return ticket;
  }

  /**
   * [M54-3] Sweeps expired `PENDING` human approval tickets exceeding `approvalTtlMs` (default 3,600,000ms),
   * aborts their uncommitted staged side-effects, and transitions suspended tasks to `DEAD_LETTER`
   * so abandoned approvals never deadlock tasks in `SUSPENDED_APPROVAL` forever.
   */
  expireStaleApprovalTickets(
  tasksById,
  nowMs = Date.now(),
  approvalTtlMs = 3_600_000)
  {
    const expiredTicketIds = [];
    for (const ticket of this.tickets.values()) {
      if (ticket.status === "PENDING" && nowMs - ticket.createdAtMs >= approvalTtlMs) {
        ticket.status = "EXPIRED";
        ticket.decidedBy = "system:ttl-watchdog";
        this.abortStagedEffects(ticket.taskId);
        const task = tasksById.get(ticket.taskId);
        if (task && task.state === _models.TaskState.SUSPENDED_APPROVAL) {
          task.state = _models.TaskState.DEAD_LETTER;
        }
        expiredTicketIds.push(ticket.ticketId);
      }
    }
    return expiredTicketIds;
  }
}exports.StagingCommitAndApprovalGate = StagingCommitAndApprovalGate;