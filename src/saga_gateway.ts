/**
 * TASK-15, TASK-16, TASK-17 Unified: 4-Tier Transactional Tool Gateway
 * Composing SagaCompensatorEngine and StagingCommitAndApprovalGate.
 */

import * as crypto from "crypto";
import { LeaseCoordinator } from "./lease_coordinator";
import { AgentTask, FenceToken, ToolEffectClass } from "./models";
import { SagaCompensatorEngine } from "./saga/compensator";
import { StagingCommitAndApprovalGate } from "./saga/staging";
import { stableTaskOperationId, TaskOperationProvider, TaskOperationRequest, TaskOperations } from "./persistence/task-operations";

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
  public readonly compensator: SagaCompensatorEngine;
  public readonly approvalGate = new StagingCommitAndApprovalGate();

  constructor(public readonly leaseCoordinator: LeaseCoordinator, public readonly taskOperations?: TaskOperations) {
    this.compensator = new SagaCompensatorEngine(3, taskOperations);
  }

  /** Durable path used by TaskCell integrations. It returns only after TaskOperations confirms intent. */
  public executeDurableTool<T>(params: Omit<TaskOperationRequest, "operationId"> & {
    toolCallId: string;
    provider: TaskOperationProvider<T>;
  }): ReturnType<TaskOperations["executeOperation"]> {
    if (!this.taskOperations) throw new Error("TASK_OPERATIONS_NOT_CONFIGURED");
    const { toolCallId, provider, ...request } = params;
    const operationId = stableTaskOperationId(request.fence.taskId, toolCallId);
    return this.taskOperations.executeOperation({ ...request, operationId }, provider);
  }

  public compensatePersisted(
    fence: FenceToken,
    targetTurnSeq: number,
    executor: Parameters<SagaCompensatorEngine["compensatePersisted"]>[3],
  ): ReturnType<SagaCompensatorEngine["compensatePersisted"]> {
    if (!this.taskOperations) throw new Error("TASK_OPERATIONS_NOT_CONFIGURED");
    return this.compensator.compensatePersisted(fence.taskId, targetTurnSeq, fence, executor);
  }

  /** Legacy in-process simulator retained for existing non-TaskCell callers. */
  public executeTool(params: {
    fence: FenceToken;
    toolCallId: string;
    toolName: string;
    effectClass: ToolEffectClass;
    payload: string;
    compensationTool?: string;
    task?: AgentTask;
    requireHumanApproval?: boolean;
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
      if (params.compensationTool) {
        this.compensator.registerForwardAction(
          params.fence.taskId,
          params.fence,
          params.toolName,
          params.compensationTool,
          params.payload,
        );
      }
      return { status: "OK", staged: false, idempotencyKey: idemKey, result: "SAGA_LOGGED" };
    }

    const stagedList = this.stagingBuffer.get(params.fence.taskId) ?? [];
    stagedList.push(entry);
    this.stagingBuffer.set(params.fence.taskId, stagedList);
    if (params.task) {
      this.approvalGate.stageWrite(
        params.task,
        params.fence,
        params.toolName,
        params.payload,
        params.requireHumanApproval ?? true,
      );
    }
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

    const compensated = this.compensator.compensateLifo(taskId, targetTurnSeq);
    const entries = this.sagaJournal.get(taskId) ?? [];
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
