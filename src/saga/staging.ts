/**
 * @fileoverview M3 TASK-10: Staging Hold-and-Commit Buffer & Human-in-the-Loop Approval Gate.
 */
import { AgentTask, FenceToken, TaskSpec, TaskState } from "../models.js";

export interface StagedSideEffect {
  effectId: string;
  taskId: string;
  turnIndex: number;
  toolName: string;
  args: Record<string, unknown>;
  committed: boolean;
  aborted: boolean;
}

/** A high-risk tool write held in the HITL staging buffer before it touches production. */
export interface StagedWriteItem {
  taskId: string;
  toolName: string;
  payload: string;
  fence: FenceToken;
  requireHumanApproval: boolean;
  approved: boolean;
  stagedAtMs: number;
}

export interface CommittedLedgerEntry {
  taskId: string;
  toolName: string;
  payload: string;
  fence: FenceToken;
  committedAtMs: number;
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
  /** Staged high-risk writes per task, waiting for a human decision. */
  public readonly stagedItems: Map<string, StagedWriteItem[]> = new Map();
  /** Append-only ledger of writes that were approved and flushed to production. */
  public readonly committedProductionLedger: CommittedLedgerEntry[] = [];

  /**
   * Stages a high-risk production write. When `requireHumanApproval` is set, the task is parked in
   * `SUSPENDED_APPROVAL` until `approveAndResume` runs.
   */
  public stageWrite(
    task: AgentTask,
    fence: FenceToken,
    toolName: string,
    payload: string,
    requireHumanApproval = true,
  ): StagedWriteItem {
    const item: StagedWriteItem = {
      taskId: task.taskId,
      toolName,
      payload,
      fence,
      requireHumanApproval,
      approved: false,
      stagedAtMs: Date.now(),
    };
    const list = this.stagedItems.get(task.taskId);
    if (list) {
      list.push(item);
    } else {
      this.stagedItems.set(task.taskId, [item]);
    }
    if (requireHumanApproval) {
      task.state = TaskState.SUSPENDED_APPROVAL;
    }
    return item;
  }

  /** Marks every staged write of `task` as human-approved and resumes the task. */
  public approveAndResume(task: AgentTask): number {
    const list = this.stagedItems.get(task.taskId) ?? [];
    let approvedCount = 0;
    for (const item of list) {
      if (!item.approved) {
        item.approved = true;
        approvedCount += 1;
      }
    }
    task.state = TaskState.RUNNING_ACT;
    return approvedCount;
  }

  /** Moves all approved writes of `taskId` into the committed production ledger. */
  public atomicFlush(taskId: string): number {
    const list = this.stagedItems.get(taskId) ?? [];
    const approved = list.filter((item) => item.approved);
    for (const item of approved) {
      this.committedProductionLedger.push({
        taskId: item.taskId,
        toolName: item.toolName,
        payload: item.payload,
        fence: item.fence,
        committedAtMs: Date.now(),
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
