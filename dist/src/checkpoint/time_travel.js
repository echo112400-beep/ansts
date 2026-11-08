"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.DeterministicTimeTravelReplayer = void 0;


var _models = require("../models.js"); /**
 * @fileoverview M5 TASK-21: Deterministic Time-Travel Debugger & Turn Forking Engine.
 */







class DeterministicTimeTravelReplayer {
  forkTaskAtTurn(
  originalTask,
  walEntries,
  targetTurnIndex,
  newTaskId,
  overriddenPrompt)
  {
    const validEntries = walEntries.filter((e) => e.turnIndex <= targetTurnIndex);
    if (validEntries.length === 0) {
      throw new Error(`Cannot fork task '${originalTask.task_id}' at turn ${targetTurnIndex}: no WAL found`);
    }
    const inheritedFiles = new Set();
    for (const e of validEntries) {
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

    return {
      forkedTask,
      restoredTurnIndex: targetTurnIndex,
      overriddenPrompt: forkedTask.prompt,
      inheritedFiles: [...inheritedFiles]
    };
  }
}exports.DeterministicTimeTravelReplayer = DeterministicTimeTravelReplayer;