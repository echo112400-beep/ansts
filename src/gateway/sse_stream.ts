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
