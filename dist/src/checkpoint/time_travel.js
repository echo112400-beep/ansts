"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.DeterministicTimeTravelReplayer = void 0;



var _models = require("../models.js"); /**
 * @fileoverview M5 TASK-21 & M55-4: Deterministic Time-Travel Debugger & Turn Forking Engine
 * with WAL Frame CRC32 Integrity Verification & Highest-Epoch Turn Deduplication.
 */








class DeterministicTimeTravelReplayer {
  /**
   * M17 TASK-86: Zero-Copy Recursive Commit Fork (`O(1)` metadata fork for MCTS/RL & Time-Travel Debugging).
   */
  forkLayerFsCommitZeroCopy(
  driver,
  sourceCommitId,
  newBranchName)
  {
    return driver.forkBranch(sourceCommitId, newBranchName);
  }

  /**
   * [M55-4] Forks a task at `targetTurnIndex` after verifying every WAL frame's CRC32 integrity
   * and deduplicating multi-epoch turn entries by selecting the highest `(fencingToken, walSeq)` per turn.
   */
  forkTaskAtTurn(
  originalTask,
  walEntries,
  targetTurnIndex,
  newTaskId,
  overriddenPrompt)
  {
    const candidateEntries = walEntries.filter((e) => e.turnIndex <= targetTurnIndex);
    if (candidateEntries.length === 0) {
      throw new Error(`Cannot fork task '${originalTask.task_id}' at turn ${targetTurnIndex}: no WAL found`);
    }

    // [M55-4] Deduplicate by turnIndex choosing highest (fencingToken, walSeq) and verify CRC32 integrity
    const canonicalByTurn = new Map();
    let maxFencingToken = originalTask.lease_fencing_token;
    for (const entry of candidateEntries) {
      if (entry.frame && typeof entry.frame.verifyIntegrity === "function" && !entry.frame.verifyIntegrity()) {
        throw new Error(
        `Cannot fork task '${originalTask.task_id}' at turn ${targetTurnIndex}: corrupted WAL frame '${entry.frame.frameId}' (CRC32 mismatch at turn ${entry.turnIndex})`);

      }
      const existing = canonicalByTurn.get(entry.turnIndex);
      if (
      !existing ||
      entry.fencingToken > existing.fencingToken ||
      entry.fencingToken === existing.fencingToken && entry.walSeq > existing.walSeq)
      {
        canonicalByTurn.set(entry.turnIndex, entry);
      }
      if (entry.fencingToken > maxFencingToken) {
        maxFencingToken = entry.fencingToken;
      }
    }

    const orderedTurns = Array.from(canonicalByTurn.values()).sort((a, b) => a.turnIndex - b.turnIndex);
    const inheritedFiles = new Set();
    for (const e of orderedTurns) {
      for (const f of e.changedFiles) {
        inheritedFiles.add(f);
      }
    }

    const forkedTask = new _models.TaskSpec({
      task_id: newTaskId,
      tenant_id: originalTask.tenant_id,
      priority: originalTask.priority,
      prompt: overriddenPrompt ?? originalTask.prompt,
      max_turns: originalTask.max_turns,
      timeout_seconds: originalTask.timeout_seconds,
      resource_profile: { ...originalTask.resource_profile },
      dependencies: [...originalTask.dependencies],
      parent_task_id: originalTask.task_id
    });
    forkedTask.current_turn = targetTurnIndex;
    forkedTask.state = _models.TaskState.QUEUED;
    forkedTask.lease_fencing_token = maxFencingToken;

    return {
      forkedTask,
      restoredTurnIndex: targetTurnIndex,
      overriddenPrompt: forkedTask.prompt,
      inheritedFiles: [...inheritedFiles],
      inheritedFencingToken: maxFencingToken
    };
  }
}exports.DeterministicTimeTravelReplayer = DeterministicTimeTravelReplayer;