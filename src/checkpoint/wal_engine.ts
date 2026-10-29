/**
 * TASK-24: Binary/JSON TurnWAL Engine with Conditional Write (If-None-Match Epoch Guard).
 */

import { StaleFenceError } from "../lease_coordinator";
import { FenceToken, TurnWalFrame } from "../models";

export class TurnWalEngine {
  private objectStore: Map<string, string> = new Map();
  private highestEpochByTask: Map<string, number> = new Map();

  public objectKey(fence: FenceToken): string {
    const ep = String(fence.epoch).padStart(2, "0");
    const tr = String(fence.turnSeq).padStart(4, "0");
    return `checkpoints/${fence.taskId}/ep_${ep}/turn_${tr}.wal`;
  }

  public writeWalFrameIfNoneMatch(frame: TurnWalFrame): string {
    const highestEp = this.highestEpochByTask.get(frame.taskId) ?? 0;
    if (frame.fence.epoch < highestEp) {
      throw new StaleFenceError(
        `409 StaleFenceRejected on storage write: epoch ${frame.fence.epoch} < ${highestEp}`,
      );
    }
    this.highestEpochByTask.set(frame.taskId, frame.fence.epoch);
    const key = this.objectKey(frame.fence);
    if (this.objectStore.has(key)) {
      throw new StaleFenceError(`412 PreconditionFailed (If-None-Match): ${key} already exists`);
    }
    this.objectStore.set(key, JSON.stringify(frame));
    return key;
  }

  public listTaskFrames(taskId: string): TurnWalFrame[] {
    const prefix = `checkpoints/${taskId}/`;
    const keys = Array.from(this.objectStore.keys())
      .filter((k) => k.startsWith(prefix))
      .sort();
    return keys.map((k) => {
      const raw = JSON.parse(this.objectStore.get(k)!);
      return {
        ...raw,
        fence: new FenceToken(raw.fence.epoch, raw.fence.turnSeq, raw.taskId),
      } as TurnWalFrame;
    });
  }
}
