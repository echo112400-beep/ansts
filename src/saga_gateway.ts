/**
 * TASK-15: 4-Tier Transactional Tool Governance, Staging Buffer & LIFO Saga Compensation.
 */

import * as crypto from "crypto";
import { LeaseCoordinator } from "./lease_coordinator";
import { FenceToken, ToolEffectClass } from "./models";

export interface SagaLogEntry {
  taskId: string;
  fence: FenceToken;
  toolName: string;
  effectClass: ToolEffectClass;
  compensationTool?: string;
  payload: string;
  idempotencyKey: string;
  committed: boolean;
}

export class SagaToolGateway {
  public readCache: Map<string, string> = new Map();
  public sagaJournal: Map<string, SagaLogEntry[]> = new Map();
  public stagingBuffer: Map<string, SagaLogEntry[]> = new Map();
  public flushedProductionWrites: SagaLogEntry[] = [];

  constructor(public readonly leaseCoordinator: LeaseCoordinator) {}

  public executeTool(params: {
    fence: FenceToken;
    toolCallId: string;
    toolName: string;
    effectClass: ToolEffectClass;
    payload: string;
    compensationTool?: string;
  }): { status: string; staged: boolean; idempotencyKey: string; result: string } {
    this.leaseCoordinator.verifyAndAdvanceFence(params.fence);

    const idemRaw = `${params.fence.taskId}:${params.fence.epoch}:${params.fence.turnSeq}:${params.toolCallId}`;
    const idemKey = crypto.createHash("sha256").update(idemRaw, "utf8").digest("hex").slice(0, 20);

    const entry: SagaLogEntry = {
      taskId: params.fence.taskId,
      fence: params.fence,
      toolName: params.toolName,
      effectClass: params.effectClass,
      compensationTool: params.compensationTool,
      payload: params.payload,
      idempotencyKey: idemKey,
      committed: false,
    };

    if (params.effectClass === ToolEffectClass.PURE_READ) {
      let cached = this.readCache.get(idemKey);
      if (!cached) {
        cached = `READ_OK:${params.toolName}:${params.payload}`;
        this.readCache.set(idemKey, cached);
      }
      return { status: "OK", staged: false, idempotencyKey: idemKey, result: cached };
    }

    if (params.effectClass === ToolEffectClass.IDEMPOTENT_WRITE) {
      entry.committed = true;
      const list = this.sagaJournal.get(params.fence.taskId) ?? [];
      list.push(entry);
      this.sagaJournal.set(params.fence.taskId, list);
      return { status: "OK", staged: false, idempotencyKey: idemKey, result: "IDEMPOTENT_COMMITTED" };
    }

    if (params.effectClass === ToolEffectClass.SAGA_COMPENSATABLE) {
      entry.committed = true;
      const list = this.sagaJournal.get(params.fence.taskId) ?? [];
      list.push(entry);
      this.sagaJournal.set(params.fence.taskId, list);
      return { status: "OK", staged: false, idempotencyKey: idemKey, result: "SAGA_LOGGED" };
    }

    const stagedList = this.stagingBuffer.get(params.fence.taskId) ?? [];
    stagedList.push(entry);
    this.stagingBuffer.set(params.fence.taskId, stagedList);
    return { status: "STAGED", staged: true, idempotencyKey: idemKey, result: "HELD_IN_STAGING_BUFFER" };
  }

  public rollbackAndCompensate(fence: FenceToken, targetTurnSeq: number): string[] {
    this.leaseCoordinator.verifyAndAdvanceFence(fence);
    const taskId = fence.taskId;

    if (this.stagingBuffer.has(taskId)) {
      const keptStaged = (this.stagingBuffer.get(taskId) ?? []).filter(
        (e) => e.fence.turnSeq <= targetTurnSeq,
      );
      this.stagingBuffer.set(taskId, keptStaged);
    }

    const entries = this.sagaJournal.get(taskId) ?? [];
    const toCompensate = entries.filter(
      (e) =>
        e.fence.turnSeq > targetTurnSeq &&
        e.effectClass === ToolEffectClass.SAGA_COMPENSATABLE &&
        Boolean(e.compensationTool),
    );
    const compensated: string[] = [];
    for (let i = toCompensate.length - 1; i >= 0; i--) {
      const entry = toCompensate[i];
      compensated.push(`${entry.compensationTool}(${entry.payload})`);
    }
    this.sagaJournal.set(
      taskId,
      entries.filter((e) => e.fence.turnSeq <= targetTurnSeq),
    );
    return compensated;
  }

  public flushStagedCommits(fence: FenceToken, humanApproved: boolean = true): number {
    this.leaseCoordinator.verifyAndAdvanceFence(fence);
    if (!humanApproved) return 0;
    const staged = this.stagingBuffer.get(fence.taskId) ?? [];
    this.stagingBuffer.delete(fence.taskId);
    for (const entry of staged) {
      entry.committed = true;
      this.flushedProductionWrites.push(entry);
    }
    return staged.length;
  }
}
