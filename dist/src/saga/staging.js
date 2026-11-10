"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.StagingCommitAndApprovalGate = void 0;



var _models = require("../models.js");

var _saga_gateway = require("../saga_gateway.js"); /**
 * @fileoverview M3 TASK-10, M54-3 & M61-2: Staging Hold-and-Commit Buffer & Human-in-the-Loop Approval Gate
 * with Idempotent Effect Staging, Coalesced Pending Approval Tickets, Terminal-State Guard, TTL Sweep & SQLite Persistence.
 */


















class StagingCommitAndApprovalGate {
  stagedByTask = new Map();
  tickets = new Map();
  ticketSeq = 0;
  sql;

  constructor(sql) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M61-2] Binds SQLite storage so staged side-effects and human approval tickets survive restarts.
   */
  bindSqlStorage(sql) {
    this.sql = sql;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS persisted_approval_tickets (ticket_id TEXT PRIMARY KEY, payload_json TEXT)");

    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS persisted_staged_effects (effect_id TEXT PRIMARY KEY, task_id TEXT, payload_json TEXT)");

    let ticketsRestored = 0;
    for (const r of this.sql.exec("SELECT * FROM persisted_approval_tickets").toArray()) {
      const tk = JSON.parse(String(r.payload_json ?? "{}"));
      if (tk && tk.ticketId) {
        this.tickets.set(tk.ticketId, tk);
        ticketsRestored += 1;
      }
    }
    let effectsRestored = 0;
    for (const r of this.sql.exec("SELECT * FROM persisted_staged_effects").toArray()) {
      const eff = JSON.parse(String(r.payload_json ?? "{}"));
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

  persistEffect(eff) {
    if (!this.sql) return;
    this.sql.exec(
    "INSERT OR REPLACE INTO persisted_staged_effects (effect_id, task_id, payload_json) VALUES (?, ?, ?)",
    eff.effectId,
    eff.taskId,
    JSON.stringify(eff));

  }

  persistTicket(tk) {
    if (!this.sql) return;
    this.sql.exec(
    "INSERT OR REPLACE INTO persisted_approval_tickets (ticket_id, payload_json) VALUES (?, ?)",
    tk.ticketId,
    JSON.stringify(tk));

  }

  /**
   * [M61-2] Idempotent effect staging: if `effect.effectId` (or identical `(taskId, turnIndex, toolName, canonicalArgs)`)
   * is already staged for `effect.taskId`, returns the existing `StagedSideEffect` instead of duplicating it.
   */
  stageEffect(effect) {
    let list = this.stagedByTask.get(effect.taskId);
    if (!list) {
      list = [];
      this.stagedByTask.set(effect.taskId, list);
    }
    const canonicalNewArgs = JSON.stringify(_saga_gateway.SagaToolGateway.canonicalizeValue(effect.args));
    const existing = list.find(
    (item) =>
    item.effectId === effect.effectId ||
    !item.aborted &&
    item.turnIndex === effect.turnIndex &&
    item.toolName === effect.toolName &&
    JSON.stringify(_saga_gateway.SagaToolGateway.canonicalizeValue(item.args)) === canonicalNewArgs);

    if (existing) {
      return existing;
    }

    const full = { ...effect, committed: false, aborted: false };
    list.push(full);
    this.persistEffect(full);
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
        this.persistEffect(item);
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
        this.persistEffect(item);
        count += 1;
      }
    }
    return count;
  }

  getStagedEffects(taskId) {
    return [...(this.stagedByTask.get(taskId) ?? [])];
  }

  /**
   * [M61-2] Coalesces duplicate `PENDING` approval requests for the same `(task.task_id, toolName, canonicalArgs)`
   * so retried turns never spawn duplicate human approval tickets.
   */
  requestHumanApproval(
  task,
  toolName,
  args,
  nowMs = Date.now())
  {
    task.state = _models.TaskState.SUSPENDED_APPROVAL;
    const canonicalReqArgs = JSON.stringify(_saga_gateway.SagaToolGateway.canonicalizeValue(args));
    for (const existing of this.tickets.values()) {
      if (
      existing.taskId === task.task_id &&
      existing.toolName === toolName &&
      existing.status === "PENDING" &&
      JSON.stringify(_saga_gateway.SagaToolGateway.canonicalizeValue(existing.args)) === canonicalReqArgs)
      {
        return existing;
      }
    }

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
    this.persistTicket(ticket);
    return ticket;
  }

  /**
   * [M54-3 & M61-2] Resolves an approval ticket with idempotent repeat-decision handling and terminal-state guard.
   */
  resolveApproval(ticketId, approved, decidedBy, task) {
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
      `Approval ticket '${ticketId}' is already in terminal status '${ticket.status}' and cannot transition to '${desiredStatus}'`);

    }

    ticket.status = desiredStatus;
    ticket.decidedBy = decidedBy;
    if (approved) {
      task.state = _models.TaskState.RUNNING_ACT;
    } else {
      task.state = _models.TaskState.DEAD_LETTER;
      this.abortStagedEffects(ticket.taskId);
    }
    this.persistTicket(ticket);
    return ticket;
  }

  /**
   * [M54-3] Sweeps expired `PENDING` human approval tickets exceeding `approvalTtlMs` (default 3,600,000ms),
   * aborts their uncommitted staged side-effects, and transitions suspended tasks to `DEAD_LETTER`.
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
        this.persistTicket(ticket);
        expiredTicketIds.push(ticket.ticketId);
      }
    }
    return expiredTicketIds;
  }
}exports.StagingCommitAndApprovalGate = StagingCommitAndApprovalGate;