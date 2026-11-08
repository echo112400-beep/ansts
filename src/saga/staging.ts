/**
 * @fileoverview M3 TASK-10: Staging Hold-and-Commit Buffer & Human-in-the-Loop Approval Gate.
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
  status: "PENDING" | "APPROVED" | "REJECTED";
  decidedBy?: string;
}

export class StagingCommitAndApprovalGate {
  private readonly stagedByTask: Map<string, StagedSideEffect[]> = new Map();
  private readonly tickets: Map<string, ApprovalTicket> = new Map();

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

  public requestHumanApproval(task: TaskSpec, toolName: string, args: Record<string, unknown>): ApprovalTicket {
    task.state = TaskState.SUSPENDED_APPROVAL;
    const ticket: ApprovalTicket = {
      ticketId: `approval-${task.task_id}-${Date.now()}`,
      taskId: task.task_id,
      toolName,
      args,
      status: "PENDING",
    };
    this.tickets.set(ticket.ticketId, ticket);
    return ticket;
  }

  public resolveApproval(ticketId: string, approved: boolean, decidedBy: string, task: TaskSpec): ApprovalTicket {
    const ticket = this.tickets.get(ticketId);
    if (!ticket) {
      throw new Error(`Approval ticket '${ticketId}' not found`);
    }
    ticket.status = approved ? "APPROVED" : "REJECTED";
    ticket.decidedBy = decidedBy;
    task.state = approved ? TaskState.RUNNING_ACT : TaskState.DEAD_LETTER;
    return ticket;
  }
}
