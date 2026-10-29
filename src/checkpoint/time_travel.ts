/**
 * TASK-26: Zero-Token Deterministic Time-Travel Replay Engine.
 */

import { TurnWalFrame } from "../models";

export interface TimeTravelReplayResult {
  taskId: string;
  replayedUpToTurn: number;
  tokensBilledDuringReplay: number;
  restoredWorkspaceHash: string;
  restoredContextMessages: string[];
}

export class DeterministicTimeTravelReplayer {
  public replayToTurn(
    taskId: string,
    walFrames: TurnWalFrame[],
    targetTurnSeq: number,
  ): TimeTravelReplayResult {
    const eligible = walFrames.filter((f) => f.fence.turnSeq <= targetTurnSeq);
    if (eligible.length === 0) {
      throw new Error(`No TurnWAL frames found for ${taskId} <= turn ${targetTurnSeq}`);
    }
    const msgs: string[] = [];
    for (const f of eligible) {
      msgs.push(...f.contextMessages);
    }
    const last = eligible[eligible.length - 1];
    return {
      taskId,
      replayedUpToTurn: last.fence.turnSeq,
      tokensBilledDuringReplay: 0,
      restoredWorkspaceHash: last.workspaceMerkleHash,
      restoredContextMessages: msgs,
    };
  }
}
