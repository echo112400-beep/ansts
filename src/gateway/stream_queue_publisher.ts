/**
 * @fileoverview TASK-85 & TASK-86: Queue-Backed Agent Stream Publisher & Incremental TurnWAL Catch-Up Coordinator.
 *
 * Bridges `MultiQueueManager` (`QUEUED -> CLAIMED -> RUNNING -> TOOL_EXEC -> COMPLETED`),
 * `TurnWalEngine` (conditional write RPO=0 persistence), and `SseEventStreamGateway` (credit-window
 * backpressure + `Last-Event-ID` / `turn_seq` zero-loss incremental catch-up).
 */

import { TurnWalEngine } from "../checkpoint/wal_engine";
import { FenceToken, TaskState, TurnWalFrame } from "../models";
import { SseEventFrame, SseEventStreamGateway } from "./sse_stream";

export interface StreamPublishEventParams {
  clientId: string;
  taskId: string;
  eventType:
    | "TASK_QUEUED"
    | "TASK_CLAIMED"
    | "REASONING_CHUNK"
    | "TOOL_EXEC_PULSE"
    | "FINAL_ANSWER"
    | "TASK_COMPLETED";
  state: TaskState;
  fence: FenceToken;
  payload: Record<string, unknown>;
}

export interface IncrementalCatchupResult {
  taskId: string;
  clientId: string;
  lastReceivedTurnSeq: number;
  currentEpoch: number;
  currentTurnSeq: number;
  replayedWalFrames: TurnWalFrame[];
  dispatchedSseFrames: SseEventFrame[];
  liveStreamResumed: boolean;
}

export class AgentStreamQueuePublisher {
  constructor(
    public readonly sseGateway: SseEventStreamGateway,
    public readonly walEngine: TurnWalEngine,
  ) {}

  /**
   * Atomically persists a TurnWAL frame (with If-None-Match Epoch guard) and dispatches
   * the corresponding SSE stream frame subject to the client's credit window.
   */
  public publishTaskStreamEvent(params: StreamPublishEventParams): {
    walObjectKey: string;
    sseFrames: SseEventFrame[];
  } {
    const serializedPayload = JSON.stringify({
      eventType: params.eventType,
      ...params.payload,
    });

    const walFrame: TurnWalFrame = TurnWalFrame.fromPartial({
      taskId: params.taskId,
      fence: new FenceToken(params.fence.epoch, params.fence.turnSeq, params.taskId),
      state: params.state,
      diffSnapshot: serializedPayload,
      timestampMs: Date.now(),
    });

    // 1. Persist to TurnWAL with Epoch fencing before emitting to stream
    const walObjectKey = this.walEngine.writeWalFrameIfNoneMatch(walFrame);

    // 2. Push through credit-controlled SSE Gateway
    const sseFrames = this.sseGateway.publishEvent(
      params.clientId,
      params.eventType,
      params.taskId,
      serializedPayload,
    );

    return { walObjectKey, sseFrames };
  }

  /**
   * Handles client reconnection after a Stream Tunnel or NodeDaemon interruption:
   * - Slices all persisted TurnWAL frames with `turnSeq > lastReceivedTurnSeq`
   * - Re-emits missed frames in strict monotonic order to the reconnected SSE client
   * - Seamlessly transitions the subscription back into live streaming mode
   */
  public resumeAndCatchUpFromWal(params: {
    clientId: string;
    taskId: string;
    lastReceivedTurnSeq: number;
    replenishCredits?: number;
  }): IncrementalCatchupResult {
    if (params.replenishCredits && params.replenishCredits > 0) {
      this.sseGateway.ackCredits(params.clientId, params.replenishCredits);
    }

    const allWalFrames = this.walEngine.listTaskFrames(params.taskId);
    const missedWalFrames = allWalFrames.filter(
      (f) => f.fence.turnSeq > params.lastReceivedTurnSeq,
    );

    const dispatchedSseFrames: SseEventFrame[] = [];
    for (const frame of missedWalFrames) {
      let parsedEventType = "WAL_REPLAY_FRAME";
      try {
        const parsed = JSON.parse(frame.diffSnapshot);
        if (parsed && typeof parsed.eventType === "string") {
          parsedEventType = parsed.eventType;
        }
      } catch {
        // Keep default event type if diffSnapshot is raw text
      }

      const emitted = this.sseGateway.publishEvent(
        params.clientId,
        parsedEventType,
        params.taskId,
        frame.diffSnapshot,
      );
      dispatchedSseFrames.push(...emitted);
    }

    const latest = allWalFrames.length > 0 ? allWalFrames[allWalFrames.length - 1] : null;
    return {
      taskId: params.taskId,
      clientId: params.clientId,
      lastReceivedTurnSeq: params.lastReceivedTurnSeq,
      currentEpoch: latest ? latest.fence.epoch : 1,
      currentTurnSeq: latest ? latest.fence.turnSeq : params.lastReceivedTurnSeq,
      replayedWalFrames: missedWalFrames,
      dispatchedSseFrames,
      liveStreamResumed: true,
    };
  }
}
