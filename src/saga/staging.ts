/**
 * TASK-17: STAGED_COMMIT Host Staging Buffer and Human Approval Gate with Sandbox Hibernation.
 */

import { AgentTask, FenceToken, SandboxPhase, TaskState } from "../models";

export interface StagedWriteItem {
  taskId: string;
  fence: FenceToken;
  toolName: string;
  payload: string;
  requireHumanApproval: boolean;
  approved: boolean;
}

export class StagingCommitAndApprovalGate {
  public stagedItems: Map<string, StagedWriteItem[]> = new Map();
  public committedProductionLedger: StagedWriteItem[] = [];

  public stageWrite(
    task: AgentTask,
    fence: FenceToken,
    toolName: string,
    payload: string,
    requireHumanApproval: boolean = true,
  ): StagedWriteItem {
    const item: StagedWriteItem = {
      taskId: task.taskId,
      fence,
      toolName,
      payload,
      requireHumanApproval,
      approved: !requireHumanApproval,
    };
    const list = this.stagedItems.get(task.taskId) ?? [];
    list.push(item);
    this.stagedItems.set(task.taskId, list);

    if (requireHumanApproval) {
      task.state = TaskState.HIBERNATED;
      task.phase = SandboxPhase.IDLE;
      task.activeSessions = 0;
    }
    return item;
  }

  public approveAndResume(task: AgentTask): number {
    const list = this.stagedItems.get(task.taskId) ?? [];
    for (const it of list) {
      it.approved = true;
    }
    task.state = TaskState.RUNNING;
    task.phase = SandboxPhase.COGNITIVE;
    return list.length;
  }

  public atomicFlush(taskId: string): number {
    const list = this.stagedItems.get(taskId) ?? [];
    if (list.some((it) => it.requireHumanApproval && !it.approved)) {
      return 0;
    }
    this.stagedItems.delete(taskId);
    this.committedProductionLedger.push(...list);
    return list.length;
  }
}
