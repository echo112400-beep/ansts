/**
 * @fileoverview M1 TASK-03, M54-4, M60-3 & M66-2: Server-Sent Events (SSE) & gRPC Stream Fan-out Multiplexer
 * with Evicted `Last-Event-ID` Fallback Replay, Completed Task Stream GC (Across History + Orphan Subscriptions),
 * Idempotent `envelope.id` Deduplication & CRLF Frame-Injection Sanitization.
 */
import { PassThrough } from "node:stream";

export interface SseEventEnvelope {
  id: string;
  event: string;
  taskId: string;
  turnIndex: number;
  timestampMs: number;
  data: Record<string, unknown>;
}

export class ClientStreamSubscription {
  public readonly subscriptionId: string;
  public readonly taskId: string;
  public readonly stream: PassThrough;
  public closed = false;
  public replayGapDetected = false;

  constructor(subscriptionId: string, taskId: string) {
    this.subscriptionId = subscriptionId;
    this.taskId = taskId;
    this.stream = new PassThrough();
  }

  /**
   * [M66-2] Strips `\r` and `\n` characters from `id` and `event` headers before framing
   * to prevent SSE header/frame injection attacks.
   */
  public writeSse(envelope: SseEventEnvelope): void {
    if (this.closed) return;
    const safeId = String(envelope.id ?? "").replace(/[\r\n]+/g, "");
    const safeEvent = String(envelope.event ?? "message").replace(/[\r\n]+/g, "");
    const frame =
      `id: ${safeId}\n` +
      `event: ${safeEvent}\n` +
      `data: ${JSON.stringify(envelope)}\n\n`;
    this.stream.write(frame);
  }

  public close(): void {
    if (!this.closed) {
      this.closed = true;
      this.stream.end();
    }
  }
}

export interface SseEventFrame extends SseEventEnvelope {
  clientId: string;
}

export interface ClientCreditState {
  clientId: string;
  credits: number;
  droppedForNoCredit: number;
}

export class SseEventStreamGateway {
  private readonly subscriptionsByTask: Map<string, Map<string, ClientStreamSubscription>> = new Map();
  private readonly historyByTask: Map<string, SseEventEnvelope[]> = new Map();
  private readonly seenEventIdsByTask: Map<string, Set<string>> = new Map();
  private readonly clientCredits: Map<string, ClientCreditState> = new Map();
  private readonly maxHistoryPerTask: number;

  constructor(maxHistoryPerTask = 100) {
    const rawMax = Number(maxHistoryPerTask);
    this.maxHistoryPerTask =
      Number.isFinite(rawMax) && rawMax > 0 ? Math.max(1, Math.floor(rawMax)) : 100;
  }

  public subscribe(taskId: string, subscriptionId: string, replayFromEventId?: string): ClientStreamSubscription {
    const sub = new ClientStreamSubscription(subscriptionId, taskId);
    let taskSubs = this.subscriptionsByTask.get(taskId);
    if (!taskSubs) {
      taskSubs = new Map();
      this.subscriptionsByTask.set(taskId, taskSubs);
    }
    taskSubs.set(subscriptionId, sub);

    const history = this.historyByTask.get(taskId) ?? [];
    if (!replayFromEventId) {
      for (const evt of history) {
        sub.writeSse(evt);
      }
      return sub;
    }

    const matchIdx = history.findIndex((evt) => evt.id === replayFromEventId);
    if (matchIdx >= 0) {
      for (let i = matchIdx + 1; i < history.length; i++) {
        sub.writeSse(history[i]);
      }
    } else if (history.length > 0) {
      // [M54-4] If replayFromEventId was already evicted from the ring buffer, fall back to replaying
      // the retained history window rather than silently dropping all buffered events!
      sub.replayGapDetected = true;
      for (const evt of history) {
        sub.writeSse(evt);
      }
    }
    return sub;
  }

  /**
   * M16 TASK-81: Seamless WebSocket/SSE Reconnect Handler for Code `1012 Service Restart`
   * during `celld` cell rebalancing or `CELLD_DEPLOY_MAX_AGE_S` deployment rollouts.
   */
  public handleWebSocket1012Reconnect(
    taskId: string,
    oldSubscriptionId: string,
    newSubscriptionId: string,
    lastReceivedEventId?: string
  ): { subscription: ClientStreamSubscription; closeCodeHandled: 1012 } {
    this.unsubscribe(taskId, oldSubscriptionId);
    const subscription = this.subscribe(taskId, newSubscriptionId, lastReceivedEventId);
    return { subscription, closeCodeHandled: 1012 };
  }

  public unsubscribe(taskId: string, subscriptionId: string): void {
    const taskSubs = this.subscriptionsByTask.get(taskId);
    const sub = taskSubs?.get(subscriptionId);
    if (sub) {
      sub.close();
      taskSubs?.delete(subscriptionId);
      if (taskSubs && taskSubs.size === 0) {
        this.subscriptionsByTask.delete(taskId);
      }
    }
  }

  /**
   * [M54-4 & M66-2] Garbage-collects SSE history, deduplication sets, and open subscriptions across
   * the union of `historyByTask`, `subscriptionsByTask`, and `seenEventIdsByTask` so tasks that
   * terminated with zero published events never leak open `PassThrough` streams.
   */
  public gcCompletedTaskStreams(activeTaskIds: Set<string>): number {
    let cleanedTasks = 0;
    const candidateTaskIds = new Set<string>([
      ...this.historyByTask.keys(),
      ...this.subscriptionsByTask.keys(),
      ...this.seenEventIdsByTask.keys(),
    ]);
    for (const taskId of candidateTaskIds) {
      if (!activeTaskIds.has(taskId)) {
        const subs = this.subscriptionsByTask.get(taskId);
        if (subs) {
          for (const s of subs.values()) s.close();
          this.subscriptionsByTask.delete(taskId);
        }
        this.historyByTask.delete(taskId);
        this.seenEventIdsByTask.delete(taskId);
        cleanedTasks += 1;
      }
    }
    return cleanedTasks;
  }

  /**
   * [M60-3] Idempotent event publication: suppresses duplicate `envelope.id` publications for the same
   * `taskId` so retried turn emitters never duplicate history entries or double-stream frames to clients.
   */
  public publish(envelope: SseEventEnvelope): number {
    let seenSet = this.seenEventIdsByTask.get(envelope.taskId);
    if (!seenSet) {
      seenSet = new Set();
      this.seenEventIdsByTask.set(envelope.taskId, seenSet);
    }
    if (envelope.id && seenSet.has(envelope.id)) {
      return 0;
    }
    if (envelope.id) {
      seenSet.add(envelope.id);
    }

    let history = this.historyByTask.get(envelope.taskId);
    if (!history) {
      history = [];
      this.historyByTask.set(envelope.taskId, history);
    }
    history.push(envelope);
    if (history.length > this.maxHistoryPerTask) {
      const evicted = history.shift();
      if (evicted?.id && seenSet.size > this.maxHistoryPerTask * 4) {
        seenSet.delete(evicted.id);
      }
    }
    const taskSubs = this.subscriptionsByTask.get(envelope.taskId);
    if (!taskSubs) return 0;
    let delivered = 0;
    for (const sub of taskSubs.values()) {
      if (!sub.closed) {
        sub.writeSse(envelope);
        delivered += 1;
      }
    }
    return delivered;
  }

  public getTaskHistory(taskId: string): SseEventEnvelope[] {
    return [...(this.historyByTask.get(taskId) ?? [])];
  }

  /**
   * [TASK-86] Registers a stream client with an initial credit window.
   * A client with no credits receives no frames: that is the backpressure signal, not a dropped client.
   */
  public registerClient(clientId: string, creditWindow = 64): ClientCreditState {
    const state: ClientCreditState = {
      clientId,
      credits: Math.max(0, Math.floor(creditWindow)),
      droppedForNoCredit: 0,
    };
    this.clientCredits.set(clientId, state);
    return state;
  }

  /** Replenishes a client's credit window (ack/consumed frames) and returns the new balance. */
  public ackCredits(clientId: string, replenish: number): number {
    const state = this.clientCredits.get(clientId);
    if (!state) return 0;
    state.credits += Math.max(0, Math.floor(replenish));
    return state.credits;
  }

  public getClientCreditState(clientId: string): ClientCreditState | undefined {
    return this.clientCredits.get(clientId);
  }

  /**
   * [TASK-86] Emits one event to every registered client inside its credit window and records it in
   * the per-task history so a reconnect can replay what was missed. Frames with no credit are counted
   * as dropped rather than silently discarded.
   */
  public publishEvent(
    clientId: string,
    eventType: string,
    taskId: string,
    payload: string,
    turnIndex = 0,
  ): SseEventFrame[] {
    const state = this.clientCredits.get(clientId);
    let data: Record<string, unknown>;
    try {
      data = JSON.parse(payload) as Record<string, unknown>;
    } catch {
      data = { raw: payload };
    }
    const timestampMs = Date.now();
    const envelope: SseEventEnvelope = {
      id: `${taskId}:${eventType}:${turnIndex}:${timestampMs}`,
      event: eventType,
      taskId,
      turnIndex,
      timestampMs,
      data,
    };
    this.publish(envelope);

    if (!state) return [];
    if (state.credits <= 0) {
      state.droppedForNoCredit += 1;
      return [];
    }
    state.credits -= 1;
    return [{ ...envelope, clientId }];
  }

  public listRecentEvents(limit = 15): SseEventEnvelope[] {
    const all: SseEventEnvelope[] = [];
    for (const list of this.historyByTask.values()) {
      for (const evt of list) {
        all.push(evt);
      }
    }
    all.sort((a, b) => b.timestampMs - a.timestampMs);
    return all.slice(0, Math.max(1, limit));
  }

  public getActiveSubscriptionCount(taskId?: string): number {
    if (taskId !== undefined) {
      return this.subscriptionsByTask.get(taskId)?.size ?? 0;
    }
    let total = 0;
    for (const m of this.subscriptionsByTask.values()) {
      total += m.size;
    }
    return total;
  }
}
