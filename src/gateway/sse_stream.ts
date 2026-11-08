/**
 * @fileoverview M1 TASK-03: Server-Sent Events (SSE) & gRPC Stream Fan-out Multiplexer.
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

/**
 * One dispatched SSE frame plus the credit-window bookkeeping the stream publisher reports back to the client.
 */
export interface SseEventFrame {
  id: string;
  event: string;
  taskId: string;
  clientId: string;
  /** Monotonic gateway-assigned stream cursor, replayed through the `Last-Event-ID` header. */
  eventSeq: number;
  timestampMs: number;
  payload: string;
}

export class ClientStreamSubscription {
  public readonly subscriptionId: string;
  public readonly taskId: string;
  public readonly stream: PassThrough;
  public closed = false;

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
  /** Per-client credit window (`clientId -> credits remaining`) for backpressure-controlled fan-out. */
  private readonly creditsByClient: Map<string, number> = new Map();
  private readonly maxHistoryPerTask: number;
  private eventSeq = 0;

  constructor(maxHistoryPerTask = 100) {
    this.maxHistoryPerTask = maxHistoryPerTask;
  }

  /**
   * Registers a client with an initial credit window. A client whose credits are exhausted stops
   * receiving `publishEvent` frames until `ackCredits` replenishes the window.
   */
  public registerClient(clientId: string, initialCredits = 100): void {
    this.creditsByClient.set(clientId, initialCredits);
  }

  /** Replenishes a client's credit window and returns the new remaining balance. */
  public ackCredits(clientId: string, credits: number): number {
    const next = (this.creditsByClient.get(clientId) ?? 0) + credits;
    this.creditsByClient.set(clientId, next);
    return next;
  }

  public getRemainingCredits(clientId: string): number {
    return this.creditsByClient.get(clientId) ?? 0;
  }

  /**
   * Publishes a serialized task stream event to the task's subscribers subject to the client's credit
   * window. Returns the dispatched frames; an empty array means the frame was withheld (no credits left).
   */
  public publishEvent(
    clientId: string,
    eventType: string,
    taskId: string,
    payload: string,
  ): SseEventFrame[] {
    const remaining = this.creditsByClient.get(clientId);
    if (remaining !== undefined && remaining <= 0) {
      return [];
    }

    this.eventSeq += 1;
    const frame: SseEventFrame = {
      id: `${taskId}-${this.eventSeq}`,
      event: eventType,
      taskId,
      clientId,
      eventSeq: this.eventSeq,
      timestampMs: Date.now(),
      payload,
    };

    let data: Record<string, unknown>;
    try {
      const parsed = JSON.parse(payload) as unknown;
      data = parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : { value: parsed };
    } catch {
      data = { value: payload };
    }

    this.publish({
      id: frame.id,
      event: eventType,
      taskId,
      turnIndex: frame.eventSeq,
      timestampMs: frame.timestampMs,
      data,
    });

    if (remaining !== undefined) {
      this.creditsByClient.set(clientId, remaining - 1);
    }
    return [frame];
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
    let shouldReplay = !replayFromEventId;
    for (const evt of history) {
      if (shouldReplay) {
        sub.writeSse(evt);
      } else if (evt.id === replayFromEventId) {
        shouldReplay = true;
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
    }
  }

  public publish(envelope: SseEventEnvelope): number {
    let history = this.historyByTask.get(envelope.taskId);
    if (!history) {
      history = [];
      this.historyByTask.set(envelope.taskId, history);
    }
    history.push(envelope);
    if (history.length > this.maxHistoryPerTask) {
      history.shift();
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
}
