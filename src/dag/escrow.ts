/**
 * @fileoverview M2 TASK-05, M52-3, M61-2, M64-3 & M69-3: Deadlock-Free Parent-Child Sub-Agent Quota Escrow Coordinator
 * with Idempotent Parent Reservation, Idempotent Child Registration, Terminal-Parent Resurrection Guard,
 * Child-Failure Unblocking, Dependency Graph Cycle Persistence & SQLite Rehydration.
 */
import { TaskSpec, TaskState } from "../models.js";
import { SqlStorageLike } from "../cloudflare/env.js";

export interface QuotaEscrowRecord {
  parentTaskId: string;
  tenantId: string;
  escrowedSlots: number;
  usedSlots: number;
  childTaskIds: Set<string>;
}

const TERMINAL_PARENT_STATES = new Set<TaskState>([
  TaskState.COMPLETED,
  TaskState.SUCCEEDED,
  TaskState.FAILED,
  TaskState.DEAD_LETTER,
  TaskState.CANCELLED,
]);

export class DagEscrowCoordinator {
  private readonly escrows: Map<string, QuotaEscrowRecord> = new Map();
  private readonly dependencyGraph: Map<string, Set<string>> = new Map();
  private sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M61-2 & M64-3] Binds SQLite storage (`persisted_escrows`) to persist and rehydrate parent-child escrow records
   * AND `dependencyGraph` edges so DAG cycle detection remains intact across process restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): number {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_escrows (parent_task_id TEXT PRIMARY KEY, payload_json TEXT)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM persisted_escrows")
      .toArray();
    let restored = 0;
    for (const r of rows) {
      const parentTaskId = String(r.parent_task_id ?? "");
      const parsed = JSON.parse(String(r.payload_json ?? "{}")) as Record<string, unknown>;
      if (!parentTaskId || (parsed.escrowedSlots === undefined && parsed.escrowReserved === undefined)) continue;
      const childIds = Array.isArray(parsed.childTaskIds) ? parsed.childTaskIds.map(String) : [];
      const rec: QuotaEscrowRecord = {
        parentTaskId,
        tenantId: String(parsed.tenantId ?? "default"),
        escrowedSlots: Number(parsed.escrowedSlots ?? parsed.escrowReserved ?? 0),
        usedSlots: Number(parsed.usedSlots ?? 0),
        childTaskIds: new Set(childIds),
      };
      this.escrows.set(parentTaskId, rec);
      const childDepsMap =
        parsed.childDependencies && typeof parsed.childDependencies === "object"
          ? (parsed.childDependencies as Record<string, unknown>)
          : {};
      for (const cId of childIds) {
        const extraDeps = Array.isArray(childDepsMap[cId]) ? (childDepsMap[cId] as unknown[]).map(String) : [];
        this.dependencyGraph.set(cId, new Set([parentTaskId, ...extraDeps]));
      }
      restored += 1;
    }
    return restored;
  }

  private persistEscrow(rec: QuotaEscrowRecord): void {
    if (!this.sql) return;
    const childDependencies: Record<string, string[]> = {};
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
        childDependencies,
      })
    );
  }

  /**
   * [M61-2] Idempotent parent escrow reservation: if `parent.task_id` already holds an escrow record,
   * returns the existing record (updating `escrowedSlots` only if higher) without double-counting quota or wiping `usedSlots`.
   */
  public reserveParentEscrow(parent: TaskSpec, maxTenantQuota: number, currentTenantUsed: number): QuotaEscrowRecord {
    const requestedEscrow = Math.max(0, parent.expected_SubAgent_count);
    const existing = this.escrows.get(parent.task_id);
    if (existing) {
      if (requestedEscrow > existing.escrowedSlots) {
        const delta = requestedEscrow - existing.escrowedSlots;
        if (currentTenantUsed + delta > maxTenantQuota) {
          throw new Error(
            `Insufficient tenant quota to expand SubAgent escrow by ${delta} slots for parent ${parent.task_id}`
          );
        }
        existing.escrowedSlots = requestedEscrow;
        this.persistEscrow(existing);
      }
      return existing;
    }

    if (currentTenantUsed + 1 + requestedEscrow > maxTenantQuota) {
      throw new Error(
        `Insufficient tenant quota to escrow ${requestedEscrow} SubAgent slots for parent ${parent.task_id}`
      );
    }
    const record: QuotaEscrowRecord = {
      parentTaskId: parent.task_id,
      tenantId: parent.tenant_id,
      escrowedSlots: requestedEscrow,
      usedSlots: 0,
      childTaskIds: new Set(),
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
  public registerChildSubAgent(parent: TaskSpec, child: TaskSpec): { usedEscrowSlot: boolean; deduplicated?: boolean } {
    if (TERMINAL_PARENT_STATES.has(parent.state)) {
      throw new Error(
        `Cannot register child sub-agent '${child.task_id}' under parent '${parent.task_id}' in terminal state '${parent.state}'`
      );
    }
    this.assertNoCycle(child.task_id, new Set([parent.task_id, ...child.dependencies]));
    this.dependencyGraph.set(child.task_id, new Set([parent.task_id, ...child.dependencies]));

    parent.state = TaskState.WAITING_SUB_AGENT;
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
  public completeChildSubAgent(parent: TaskSpec, childTaskId: string): boolean {
    const escrow = this.escrows.get(parent.task_id);
    if (!escrow) {
      if (!TERMINAL_PARENT_STATES.has(parent.state)) {
        parent.state = TaskState.RUNNING_THINK;
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
        parent.state = TaskState.RUNNING_THINK;
      }
      return true;
    }
    return false;
  }

  /**
   * [M52-3] Handles child sub-agent failure/cancellation so the parent never hangs forever in `WAITING_SUB_AGENT`,
   * and reclaims the escrowed slot immediately.
   */
  public failOrCancelChildSubAgent(
    parent: TaskSpec,
    childTaskId: string,
    abortParentOnChildFailure = true
  ): { parentUnblocked: boolean; parentState: TaskState; reclaimedEscrowSlots: number } {
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
      parent.state = TaskState.DEAD_LETTER;
      parent.dead_letter_reason = `CHILD_SUB_AGENT_FAILED:${childTaskId}`;
      reclaimed += this.releaseParentEscrow(parent.task_id);
      return { parentUnblocked: true, parentState: parent.state, reclaimedEscrowSlots: reclaimed };
    }

    if (!escrow || escrow.childTaskIds.size === 0) {
      if (!TERMINAL_PARENT_STATES.has(parent.state)) {
        parent.state = TaskState.RUNNING_THINK;
      }
      return { parentUnblocked: true, parentState: parent.state, reclaimedEscrowSlots: reclaimed };
    }
    return { parentUnblocked: false, parentState: parent.state, reclaimedEscrowSlots: reclaimed };
  }

  /**
   * [M52-3] Releases a parent task's escrow reservation upon completion or dead-letter termination.
   */
  public releaseParentEscrow(parentTaskId: string): number {
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

  public getTenantEscrowedSlots(tenantId: string): number {
    let total = 0;
    for (const rec of this.escrows.values()) {
      if (rec.tenantId === tenantId) {
        total += rec.escrowedSlots;
      }
    }
    return total;
  }

  private assertNoCycle(sourceTaskId: string, targets: Set<string>): void {
    const visited = new Set<string>();
    const stack = [...targets];
    while (stack.length > 0) {
      const curr = stack.pop()!;
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
}
