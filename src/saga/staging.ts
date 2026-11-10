/**
 * @fileoverview M3 TASK-10 & M54-3: Staging Hold-and-Commit Buffer & Human-in-the-Loop Approval Gate
 * with Auto-Abort on Rejection & Approval TTL Expiration Sweep to prevent permanent SUSPENDED_APPROVAL deadlocks.
 */
import { TaskSpec, TaskState } from "../models.js";

export interface StagedSideEffect {
  effectId: string;
  taskId: string;
  turnIndex: number;
  toolName: string;
  args: Record<string, unknown>;
  committed: boolean;
  aborted: boolean;
}

export interface ApprovalTicket {
  ticketId: string;
  taskId: string;
  toolName: string;
  args: Record<string, unknown>;
  status: "PENDING" | "APPROVED" | "REJECTED" | "EXPIRED";
  createdAtMs: number;
  decidedBy?: string;
}

export class StagingCommitAndApprovalGate {
  private readonly stagedByTask: Map<string, StagedSideEffect[]> = new Map();
  private readonly tickets: Map<string, ApprovalTicket> = new Map();
  private ticketSeq = 0;

  public stageEffect(effect: Omit<StagedSideEffect, "committed" | "aborted">): StagedSideEffect {
    const full: StagedSideEffect = { ...effect, committed: false, aborted: false };
    let list = this.stagedByTask.get(effect.taskId);
    if (!list) {
      list = [];
      this.stagedByTask.set(effect.taskId, list);
    }
    list.push(full);
    return full;
  }

  public commitStagedEffects(
    taskId: string,
    executor: (toolName: string, args: Record<string, unknown>) => Record<string, unknown>
  ): Array<{ effectId: string; output: Record<string, unknown> }> {
    const list = this.stagedByTask.get(taskId) ?? [];
    const results: Array<{ effectId: string; output: Record<string, unknown> }> = [];
    for (const item of list) {
      if (!item.committed && !item.aborted) {
        const output = executor(item.toolName, item.args);
        item.committed = true;
        results.push({ effectId: item.effectId, output });
      }
    }
    return results;
  }

  public abortStagedEffects(taskId: string): number {
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

  public getStagedEffects(taskId: string): StagedSideEffect[] {
    return [...(this.stagedByTask.get(taskId) ?? [])];
  }

  public requestHumanApproval(
    task: TaskSpec,
    toolName: string,
    args: Record<string, unknown>,
    nowMs = Date.now()
  ): ApprovalTicket {
    task.state = TaskState.SUSPENDED_APPROVAL;
    this.ticketSeq += 1;
    const ticket: ApprovalTicket = {
      ticketId: `approval-${task.task_id}-${nowMs}-${this.ticketSeq}`,
      taskId: task.task_id,
      toolName,
      args,
      status: "PENDING",
      createdAtMs: nowMs,
    };
    this.tickets.set(ticket.ticketId, ticket);
    return ticket;
  }

  /**
   * [M54-3] Resolves an approval ticket. If rejected (`approved = false`), automatically aborts
   * all uncommitted staged side-effects for `task.task_id` so staged effects never leak.
   */
  public resolveApproval(ticketId: string, approved: boolean, decidedBy: string, task: TaskSpec): ApprovalTicket {
    const ticket = this.tickets.get(ticketId);
    if (!ticket) {
      throw new Error(`Approval ticket '${ticketId}' not found`);
    }
    ticket.status = approved ? "APPROVED" : "REJECTED";
    ticket.decidedBy = decidedBy;
    if (approved) {
      task.state = TaskState.RUNNING_ACT;
    } else {
      task.state = TaskState.DEAD_LETTER;
      this.abortStagedEffects(ticket.taskId);
    }
    return ticket;
  }

  /**
   * [M54-3] Sweeps expired `PENDING` human approval tickets exceeding `approvalTtlMs` (default 3,600,000ms),
   * aborts their uncommitted staged side-effects, and transitions suspended tasks to `DEAD_LETTER`
   * so abandoned approvals never deadlock tasks in `SUSPENDED_APPROVAL` forever.
   */
  public expireStaleApprovalTickets(
    tasksById: Map<string, TaskSpec>,
    nowMs = Date.now(),
    approvalTtlMs = 3_600_000
  ): string[] {
    const expiredTicketIds: string[] = [];
    for (const ticket of this.tickets.values()) {
      if (ticket.status === "PENDING" && nowMs - ticket.createdAtMs >= approvalTtlMs) {
        ticket.status = "EXPIRED";
        ticket.decidedBy = "system:ttl-watchdog";
        this.abortStagedEffects(ticket.taskId);
        const task = tasksById.get(ticket.taskId);
        if (task && task.state === TaskState.SUSPENDED_APPROVAL) {
          task.state = TaskState.DEAD_LETTER;
        }
        expiredTicketIds.push(ticket.ticketId);
      }
    }
    return expiredTicketIds;
  }
}
