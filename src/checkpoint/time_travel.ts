/**
 * @fileoverview M5 TASK-21: Deterministic Time-Travel Debugger & Turn Forking Engine.
 */
import { TaskSpec, TaskState } from "../models.js";
import { WalDeltaEntry } from "./wal_engine.js";

export interface ForkedTaskBranch {
  forkedTask: TaskSpec;
  restoredTurnIndex: number;
  overriddenPrompt: string;
  inheritedFiles: string[];
}

export class DeterministicTimeTravelReplayer {
  public forkTaskAtTurn(
    originalTask: TaskSpec,
    walEntries: WalDeltaEntry[],
    targetTurnIndex: number,
    newTaskId: string,
    overriddenPrompt?: string
  ): ForkedTaskBranch {
    const validEntries = walEntries.filter((e) => e.turnIndex <= targetTurnIndex);
    if (validEntries.length === 0) {
      throw new Error(`Cannot fork task '${originalTask.task_id}' at turn ${targetTurnIndex}: no WAL found`);
    }
    const inheritedFiles = new Set<string>();
    for (const e of validEntries) {
      for (const f of e.changedFiles) {
        inheritedFiles.add(f);
      }
    }
    const forkedTask = new TaskSpec({
      task_id: newTaskId,
      tenant_id: originalTask.tenant_id,
      priority: originalTask.priority,
      prompt: overriddenPrompt ?? originalTask.prompt,
      max_turns: originalTask.max_turns,
      timeout_seconds: originalTask.timeout_seconds,
      resource_profile: { ...originalTask.resource_profile },
      dependencies: [...originalTask.dependencies],
      parent_task_id: originalTask.task_id,
    });
    forkedTask.current_turn = targetTurnIndex;
    forkedTask.state = TaskState.QUEUED;

    return {
      forkedTask,
      restoredTurnIndex: targetTurnIndex,
      overriddenPrompt: forkedTask.prompt,
      inheritedFiles: [...inheritedFiles],
    };
  }
}
