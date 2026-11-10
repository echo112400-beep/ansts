/**
 * @fileoverview M3 TASK-10, M54-3 & M61-2: Staging Hold-and-Commit Buffer & Human-in-the-Loop Approval Gate
 * with Idempotent Effect Staging, Coalesced Pending Approval Tickets, Terminal-State Guard, TTL Sweep & SQLite Persistence.
 */
import { TaskSpec, TaskState } from "../models.js";
import { SqlStorageLike } from "../cloudflare/env.js";
import { SagaToolGateway } from "../saga_gateway.js";

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
  private sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M61-2] Binds SQLite storage so staged side-effects and human approval tickets survive restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): { ticketsRestored: number; effectsRestored: number } {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_approval_tickets (ticket_id TEXT PRIMARY KEY, payload_json TEXT)"
    );
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_staged_effects (effect_id TEXT PRIMARY KEY, task_id TEXT, payload_json TEXT)"
    );
    let ticketsRestored = 0;
    for (const r of this.sql.exec<Record<string, unknown>>("SELECT * FROM persisted_approval_tickets").toArray()) {
      const tk = JSON.parse(String(r.payload_json ?? "{}")) as ApprovalTicket;
      if (tk && tk.ticketId) {
        this.tickets.set(tk.ticketId, tk);
        ticketsRestored += 1;
      }
    }
    let effectsRestored = 0;
    for (const r of this.sql.exec<Record<string, unknown>>("SELECT * FROM persisted_staged_effects").toArray()) {
      const eff = JSON.parse(String(r.payload_json ?? "{}")) as StagedSideEffect;
      if (eff && eff.effectId && eff.taskId) {
        const list = this.stagedByTask.get(eff.taskId) ?? [];
        const existingIdx = list.findIndex((e) => e.effectId === eff.effectId);
        if (existingIdx >= 0) {
          list[existingIdx] = eff;
        } else {
          list.push(eff);
        }
        this.stagedByTask.set(eff.taskId, list);
        effectsRestored += 1;
      }
    }
    return { ticketsRestored, effectsRestored };
  }

  private persistEffect(eff: StagedSideEffect): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO persisted_staged_effects (effect_id, task_id, payload_json) VALUES (?, ?, ?)",
      eff.effectId,
      eff.taskId,
      JSON.stringify(eff)
    );
  }

  private persistTicket(tk: ApprovalTicket): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO persisted_approval_tickets (ticket_id, payload_json) VALUES (?, ?)",
      tk.ticketId,
      JSON.stringify(tk)
    );
  }

  /**
   * [M61-2] Idempotent effect staging: if `effect.effectId` (or identical `(taskId, turnIndex, toolName, canonicalArgs)`)
   * is already staged for `effect.taskId`, returns the existing `StagedSideEffect` instead of duplicating it.
   */
  public stageEffect(effect: Omit<StagedSideEffect, "committed" | "aborted">): StagedSideEffect {
    let list = this.stagedByTask.get(effect.taskId);
    if (!list) {
      list = [];
      this.stagedByTask.set(effect.taskId, list);
    }
    const canonicalNewArgs = JSON.stringify(SagaToolGateway.canonicalizeValue(effect.args));
    const existing = list.find(
      (item) =>
        item.effectId === effect.effectId ||
        (!item.aborted &&
          item.turnIndex === effect.turnIndex &&
          item.toolName === effect.toolName &&
          JSON.stringify(SagaToolGateway.canonicalizeValue(item.args)) === canonicalNewArgs)
    );
    if (existing) {
      return existing;
    }

    const full: StagedSideEffect = { ...effect, committed: false, aborted: false };
    list.push(full);
    this.persistEffect(full);
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
        this.persistEffect(item);
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
        this.persistEffect(item);
        count += 1;
      }
    }
    return count;
  }

  public getStagedEffects(taskId: string): StagedSideEffect[] {
    return [...(this.stagedByTask.get(taskId) ?? [])];
  }

  /**
   * [M61-2] Coalesces duplicate `PENDING` approval requests for the same `(task.task_id, toolName, canonicalArgs)`
   * so retried turns never spawn duplicate human approval tickets.
   */
  public requestHumanApproval(
    task: TaskSpec,
    toolName: string,
    args: Record<string, unknown>,
    nowMs = Date.now()
  ): ApprovalTicket {
    task.state = TaskState.SUSPENDED_APPROVAL;
    const canonicalReqArgs = JSON.stringify(SagaToolGateway.canonicalizeValue(args));
    for (const existing of this.tickets.values()) {
      if (
        existing.taskId === task.task_id &&
        existing.toolName === toolName &&
        existing.status === "PENDING" &&
        JSON.stringify(SagaToolGateway.canonicalizeValue(existing.args)) === canonicalReqArgs
      ) {
        return existing;
      }
    }

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
    this.persistTicket(ticket);
    return ticket;
  }

  /**
   * [M54-3 & M61-2] Resolves an approval ticket with idempotent repeat-decision handling and terminal-state guard.
   */
  public resolveApproval(ticketId: string, approved: boolean, decidedBy: string, task: TaskSpec): ApprovalTicket {
    const ticket = this.tickets.get(ticketId);
    if (!ticket) {
      throw new Error(`Approval ticket '${ticketId}' not found`);
    }
    const desiredStatus = approved ? "APPROVED" : "REJECTED";
    if (ticket.status !== "PENDING") {
      if (ticket.status === desiredStatus) {
        return ticket;
      }
      throw new Error(
        `Approval ticket '${ticketId}' is already in terminal status '${ticket.status}' and cannot transition to '${desiredStatus}'`
      );
    }

    ticket.status = desiredStatus;
    ticket.decidedBy = decidedBy;
    if (approved) {
      task.state = TaskState.RUNNING_ACT;
    } else {
      task.state = TaskState.DEAD_LETTER;
      this.abortStagedEffects(ticket.taskId);
    }
    this.persistTicket(ticket);
    return ticket;
  }

  /**
   * [M54-3] Sweeps expired `PENDING` human approval tickets exceeding `approvalTtlMs` (default 3,600,000ms),
   * aborts their uncommitted staged side-effects, and transitions suspended tasks to `DEAD_LETTER`.
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
        this.persistTicket(ticket);
        expiredTicketIds.push(ticket.ticketId);
      }
    }
    return expiredTicketIds;
  }
}
