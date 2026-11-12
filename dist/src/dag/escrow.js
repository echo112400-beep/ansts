"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.DagEscrowCoordinator = void 0;




var _models = require("../models.js"); /**
 * @fileoverview M2 TASK-05, M52-3, M61-2, M64-3 & M69-3: Deadlock-Free Parent-Child Sub-Agent Quota Escrow Coordinator
 * with Idempotent Parent Reservation, Idempotent Child Registration, Terminal-Parent Resurrection Guard,
 * Child-Failure Unblocking, Dependency Graph Cycle Persistence & SQLite Rehydration.
 */







const TERMINAL_PARENT_STATES = new Set([
_models.TaskState.COMPLETED,
_models.TaskState.SUCCEEDED,
_models.TaskState.FAILED,
_models.TaskState.DEAD_LETTER,
_models.TaskState.CANCELLED]);


class DagEscrowCoordinator {
  escrows = new Map();
  dependencyGraph = new Map();
  sql;

  constructor(sql) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M61-2 & M64-3] Binds SQLite storage (`persisted_escrows`) to persist and rehydrate parent-child escrow records
   * AND `dependencyGraph` edges so DAG cycle detection remains intact across process restarts.
   */
  bindSqlStorage(sql) {
    this.sql = sql;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS persisted_escrows (parent_task_id TEXT PRIMARY KEY, payload_json TEXT)");

    const rows = this.sql.
    exec("SELECT * FROM persisted_escrows").
    toArray();
    let restored = 0;
    for (const r of rows) {
      const parentTaskId = String(r.parent_task_id ?? "");
      const parsed = JSON.parse(String(r.payload_json ?? "{}"));
      if (!parentTaskId || parsed.escrowedSlots === undefined && parsed.escrowReserved === undefined) continue;
      const childIds = Array.isArray(parsed.childTaskIds) ? parsed.childTaskIds.map(String) : [];
      const rec = {
        parentTaskId,
        tenantId: String(parsed.tenantId ?? "default"),
        escrowedSlots: Number(parsed.escrowedSlots ?? parsed.escrowReserved ?? 0),
        usedSlots: Number(parsed.usedSlots ?? 0),
        childTaskIds: new Set(childIds),
        replanRevision: Number(parsed.replanRevision ?? 0)
      };
      this.escrows.set(parentTaskId, rec);
      const childDepsMap =
      parsed.childDependencies && typeof parsed.childDependencies === "object" ?
      parsed.childDependencies :
      {};
      for (const cId of childIds) {
        const extraDeps = Array.isArray(childDepsMap[cId]) ? childDepsMap[cId].map(String) : [];
        this.dependencyGraph.set(cId, new Set([parentTaskId, ...extraDeps]));
      }
      restored += 1;
    }
    return restored;
  }

  persistEscrow(rec) {
    if (!this.sql) return;
    const childDependencies = {};
    for (const cId of rec.childTaskIds) {
      const deps = this.dependencyGraph.get(cId);
      if (deps) {
        childDependencies[cId] = Array.from(deps);
      }
    }
    this.sql.exec(
    "INSERT OR REPLACE INTO persisted_escrows (parent_task_id, payload_json) VALUES (?, ?)",
    rec.parentTaskId,
    JSON.stringify({
      parentTaskId: rec.parentTaskId,
      tenantId: rec.tenantId,
      escrowedSlots: rec.escrowedSlots,
      usedSlots: rec.usedSlots,
      childTaskIds: Array.from(rec.childTaskIds),
      replanRevision: rec.replanRevision ?? 0,
      childDependencies
    }));

  }

  /**
   * [M61-2] Idempotent parent escrow reservation: if `parent.task_id` already holds an escrow record,
   * returns the existing record (updating `escrowedSlots` only if higher) without double-counting quota or wiping `usedSlots`.
   */
  reserveParentEscrow(parent, maxTenantQuota, currentTenantUsed) {
    const requestedEscrow = Math.max(0, parent.expected_SubAgent_count);
    const existing = this.escrows.get(parent.task_id);
    if (existing) {
      if (requestedEscrow > existing.escrowedSlots) {
        const delta = requestedEscrow - existing.escrowedSlots;
        if (currentTenantUsed + delta > maxTenantQuota) {
          throw new Error(
          `Insufficient tenant quota to expand SubAgent escrow by ${delta} slots for parent ${parent.task_id}`);

        }
        existing.escrowedSlots = requestedEscrow;
        this.persistEscrow(existing);
      }
      return existing;
    }

    if (currentTenantUsed + 1 + requestedEscrow > maxTenantQuota) {
      throw new Error(
      `Insufficient tenant quota to escrow ${requestedEscrow} SubAgent slots for parent ${parent.task_id}`);

    }
    const record = {
      parentTaskId: parent.task_id,
      tenantId: parent.tenant_id,
      escrowedSlots: requestedEscrow,
      usedSlots: 0,
      childTaskIds: new Set()
    };
    this.escrows.set(parent.task_id, record);
    this.persistEscrow(record);
    return record;
  }

  /**
   * [M61-2 & M69-3] Idempotent child registration with terminal-parent guard:
   * - Rejects child registration if `parent` is already in a terminal state (`COMPLETED`, `DEAD_LETTER`, `FAILED`, `CANCELLED`).
   * - If `child.task_id` is already registered under `parent.task_id`, returns `{ usedEscrowSlot: true, deduplicated: true }`
   *   without consuming a second escrow slot.
   */
  registerChildSubAgent(parent, child) {
    if (TERMINAL_PARENT_STATES.has(parent.state)) {
      throw new Error(
      `Cannot register child sub-agent '${child.task_id}' under parent '${parent.task_id}' in terminal state '${parent.state}'`);

    }
    this.assertNoCycle(child.task_id, new Set([parent.task_id, ...child.dependencies]));
    this.dependencyGraph.set(child.task_id, new Set([parent.task_id, ...child.dependencies]));

    parent.state = _models.TaskState.WAITING_SUB_AGENT;
    const escrow = this.escrows.get(parent.task_id);
    if (escrow && escrow.childTaskIds.has(child.task_id)) {
      this.persistEscrow(escrow);
      return { usedEscrowSlot: true, deduplicated: true };
    }
    if (escrow && escrow.usedSlots < escrow.escrowedSlots) {
      escrow.usedSlots += 1;
      escrow.childTaskIds.add(child.task_id);
      this.persistEscrow(escrow);
      return { usedEscrowSlot: true, deduplicated: false };
    }
    if (escrow) {
      escrow.childTaskIds.add(child.task_id);
      this.persistEscrow(escrow);
    }
    return { usedEscrowSlot: false, deduplicated: false };
  }

  /**
   * [M69-3] Completes a child sub-agent without resurrecting `parent.state` if `parent` has already
   * transitioned to a terminal state (such as `DEAD_LETTER` after an earlier sibling failure).
   */
  completeChildSubAgent(parent, childTaskId) {
    const escrow = this.escrows.get(parent.task_id);
    if (!escrow) {
      if (!TERMINAL_PARENT_STATES.has(parent.state)) {
        parent.state = _models.TaskState.RUNNING_THINK;
      }
      return true;
    }
    if (escrow.childTaskIds.delete(childTaskId) && escrow.usedSlots > 0) {
      escrow.usedSlots -= 1;
    }
    this.dependencyGraph.delete(childTaskId);
    this.persistEscrow(escrow);
    if (escrow.childTaskIds.size === 0) {
      if (!TERMINAL_PARENT_STATES.has(parent.state)) {
        parent.state = _models.TaskState.RUNNING_THINK;
      }
      return true;
    }
    return false;
  }

  /**
   * [M52-3] Handles child sub-agent failure/cancellation so the parent never hangs forever in `WAITING_SUB_AGENT`,
   * and reclaims the escrowed slot immediately.
   */
  failOrCancelChildSubAgent(
  parent,
  childTaskId,
  abortParentOnChildFailure = true)
  {
    const escrow = this.escrows.get(parent.task_id);
    let reclaimed = 0;
    if (escrow) {
      if (escrow.childTaskIds.delete(childTaskId) && escrow.usedSlots > 0) {
        escrow.usedSlots -= 1;
        reclaimed += 1;
      }
      this.persistEscrow(escrow);
    }
    this.dependencyGraph.delete(childTaskId);

    if (abortParentOnChildFailure) {
      parent.state = _models.TaskState.DEAD_LETTER;
      parent.dead_letter_reason = `CHILD_SUB_AGENT_FAILED:${childTaskId}`;
      reclaimed += this.releaseParentEscrow(parent.task_id);
      return { parentUnblocked: true, parentState: parent.state, reclaimedEscrowSlots: reclaimed };
    }

    if (!escrow || escrow.childTaskIds.size === 0) {
      if (!TERMINAL_PARENT_STATES.has(parent.state)) {
        parent.state = _models.TaskState.RUNNING_THINK;
      }
      return { parentUnblocked: true, parentState: parent.state, reclaimedEscrowSlots: reclaimed };
    }
    return { parentUnblocked: false, parentState: parent.state, reclaimedEscrowSlots: reclaimed };
  }

  /**
   * [M52-3] Releases a parent task's escrow reservation upon completion or dead-letter termination.
   */
  releaseParentEscrow(parentTaskId) {
    const escrow = this.escrows.get(parentTaskId);
    if (!escrow) return 0;
    const freed = escrow.escrowedSlots;
    for (const cId of escrow.childTaskIds) {
      this.dependencyGraph.delete(cId);
    }
    this.escrows.delete(parentTaskId);
    if (this.sql) {
      this.sql.exec("DELETE FROM persisted_escrows WHERE parent_task_id = ?", parentTaskId);
    }
    return freed;
  }


  /**
   * [M71-1 / F1] Adaptive DAG Subgraph Re-planner:
   * Atomically prunes obsolete/failed child sub-agents, reclaims their escrowed quota slots,
   * validates cycle-freedom for newly injected dynamic fan-out or fallback children, expands escrow
   * within tenant quota if needed, and persists the incremented `replanRevision` to SQLite.
   */
  replanSubgraph(params)














  {
    const { parent } = params;
    const canReviveDeadLetterParent =
    parent.state === _models.TaskState.DEAD_LETTER &&
    params.fallbackForFailedChildId !== undefined &&
    parent.dead_letter_reason === `CHILD_SUB_AGENT_FAILED:${params.fallbackForFailedChildId}`;

    if (TERMINAL_PARENT_STATES.has(parent.state) && !canReviveDeadLetterParent) {
      throw new Error(
      `Cannot replan subgraph for parent '${parent.task_id}' in terminal state '${parent.state}'`);

    }

    let escrow = this.escrows.get(parent.task_id);
    if (!escrow) {
      escrow = {
        parentTaskId: parent.task_id,
        tenantId: parent.tenant_id,
        escrowedSlots: Math.max(0, parent.expected_SubAgent_count),
        usedSlots: 0,
        childTaskIds: new Set(),
        replanRevision: 0
      };
      this.escrows.set(parent.task_id, escrow);
    }

    const pruneSet = new Set([
    ...(params.pruneChildIds ?? []),
    ...(params.fallbackForFailedChildId ? [params.fallbackForFailedChildId] : [])]);

    const prunedChildIds = [];
    let reclaimedSlots = 0;

    for (const cId of pruneSet) {
      if (escrow.childTaskIds.delete(cId)) {
        prunedChildIds.push(cId);
        if (escrow.usedSlots > 0) {
          escrow.usedSlots -= 1;
          reclaimedSlots += 1;
        }
      }
      this.dependencyGraph.delete(cId);
    }

    const injectList = params.injectChildren ?? [];
    const newUniqueChildren = injectList.filter((c) => !escrow.childTaskIds.has(c.task_id));

    for (const child of injectList) {
      const cleanedDeps = child.dependencies.filter((d) => !pruneSet.has(d));
      this.assertNoCycle(child.task_id, new Set([parent.task_id, ...cleanedDeps]));
    }

    let expandedEscrowSlots = 0;
    const neededSlots = escrow.usedSlots + newUniqueChildren.length;
    if (neededSlots > escrow.escrowedSlots) {
      const delta = neededSlots - escrow.escrowedSlots;
      if (
      params.maxTenantQuota !== undefined &&
      params.currentTenantUsed !== undefined &&
      params.currentTenantUsed + delta > params.maxTenantQuota)
      {
        throw new Error(
        `Insufficient tenant quota to expand SubAgent escrow by ${delta} slots during adaptive replanning for ${parent.task_id}`);

      }
      escrow.escrowedSlots += delta;
      expandedEscrowSlots = delta;
    }

    const injectedChildIds = [];
    for (const child of injectList) {
      const cleanedDeps = child.dependencies.filter((d) => !pruneSet.has(d));
      this.dependencyGraph.set(child.task_id, new Set([parent.task_id, ...cleanedDeps]));
      if (!escrow.childTaskIds.has(child.task_id)) {
        escrow.childTaskIds.add(child.task_id);
        escrow.usedSlots += 1;
      }
      injectedChildIds.push(child.task_id);
    }

    escrow.replanRevision = (escrow.replanRevision ?? 0) + 1;
    if (canReviveDeadLetterParent) {
      parent.dead_letter_reason = "";
    }
    parent.state =
    escrow.childTaskIds.size > 0 ? _models.TaskState.WAITING_SUB_AGENT : _models.TaskState.RUNNING_THINK;
    this.persistEscrow(escrow);

    return {
      replanRevision: escrow.replanRevision,
      prunedChildIds,
      injectedChildIds,
      reclaimedSlots,
      expandedEscrowSlots,
      activeChildCount: escrow.childTaskIds.size,
      parentState: parent.state
    };
  }

  inspectDagEscrowSnapshot()











  {
    const escrows = Array.from(this.escrows.values()).map((r) => ({
      parentTaskId: r.parentTaskId,
      tenantId: r.tenantId,
      escrowedSlots: r.escrowedSlots,
      usedSlots: r.usedSlots,
      replanRevision: r.replanRevision ?? 0,
      childTaskIds: Array.from(r.childTaskIds)
    }));
    const edges = Array.from(this.dependencyGraph.entries()).map(([childTaskId, deps]) => ({
      childTaskId,
      dependsOn: Array.from(deps)
    }));
    return {
      totalEscrowsCount: escrows.length,
      totalEdgesCount: edges.length,
      escrows,
      edges
    };
  }

  getEscrowRecord(parentTaskId) {
    return this.escrows.get(parentTaskId);
  }

  getTenantEscrowedSlots(tenantId) {
    let total = 0;
    for (const rec of this.escrows.values()) {
      if (rec.tenantId === tenantId) {
        total += rec.escrowedSlots;
      }
    }
    return total;
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