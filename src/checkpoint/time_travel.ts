/**
 * @fileoverview M5 TASK-21: Deterministic Time-Travel Debugger & Turn Forking Engine.
 */
import { TaskSpec, TaskState } from "../models.js";
import { TurnWalFrame } from "../models.js";
import { WalDeltaEntry } from "./wal_engine.js";

export interface TurnReplayResult {
  taskId: string;
  replayedUpToTurn: number;
  replayedTurnsCount: number;
  /** Time-travel replay re-executes no LLM calls, so nothing is billed. */
  tokensBilledDuringReplay: 0;
  restoredWorkspaceHash: string;
  replayedFrameIds: string[];
}

export interface ForkedTaskBranch {
  forkedTask: TaskSpec;
  restoredTurnIndex: number;
  overriddenPrompt: string;
  inheritedFiles: string[];
}

import { LayerFsWorkspaceDriver } from "../runtime/layerfs_workspace_driver.js";

export class DeterministicTimeTravelReplayer {
  /**
   * Zero-token deterministic replay: folds the persisted `TurnWalFrame`s up to `targetTurnSeq` in strict
   * turn order and reports the restored workspace Merkle hash (no work is re-executed).
   */
  public replayToTurn(
    taskId: string,
    frames: TurnWalFrame[],
    targetTurnSeq: number
  ): TurnReplayResult {
    const replayed = frames
      .filter((f) => f.taskId === taskId && f.fence.turnSeq <= targetTurnSeq)
      .sort((a, b) => a.fence.turnSeq - b.fence.turnSeq);
    const last = replayed.length > 0 ? replayed[replayed.length - 1] : null;
    return {
      taskId,
      replayedUpToTurn: targetTurnSeq,
      replayedTurnsCount: replayed.length,
      tokensBilledDuringReplay: 0,
      restoredWorkspaceHash: last ? last.workspaceMerkleHash : "",
      replayedFrameIds: replayed.map((f) => f.frameId),
    };
  }
  /**
   * M17 TASK-86: Zero-Copy Recursive Commit Fork (`O(1)` metadata fork for MCTS/RL & Time-Travel Debugging).
   */
  public forkLayerFsCommitZeroCopy(
    driver: LayerFsWorkspaceDriver,
    sourceCommitId: string,
    newBranchName: string
  ): { branchName: string; headCommitId: string; copiedFilesCount: 0 } {
    return driver.forkBranch(sourceCommitId, newBranchName);
  }
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
