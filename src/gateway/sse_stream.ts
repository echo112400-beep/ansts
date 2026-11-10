/**
 * @fileoverview M1 TASK-03, M54-4 & M60-3: Server-Sent Events (SSE) & gRPC Stream Fan-out Multiplexer
 * with Evicted `Last-Event-ID` Fallback Replay, Completed Task Stream GC & Idempotent `envelope.id` Deduplication.
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

  public writeSse(envelope: SseEventEnvelope): void {
    if (this.closed) return;
    const frame =
      `id: ${envelope.id}\n` +
      `event: ${envelope.event}\n` +
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

export class SseEventStreamGateway {
  private readonly subscriptionsByTask: Map<string, Map<string, ClientStreamSubscription>> = new Map();
  private readonly historyByTask: Map<string, SseEventEnvelope[]> = new Map();
  private readonly seenEventIdsByTask: Map<string, Set<string>> = new Map();
  private readonly maxHistoryPerTask: number;

  constructor(maxHistoryPerTask = 100) {
    this.maxHistoryPerTask = maxHistoryPerTask;
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
   * [M54-4] Garbage-collects SSE history and closed subscriptions for terminated/completed tasks
   * to prevent unbounded memory growth across long-running gateways.
   */
  public gcCompletedTaskStreams(activeTaskIds: Set<string>): number {
    let cleanedTasks = 0;
    for (const taskId of Array.from(this.historyByTask.keys())) {
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
}
