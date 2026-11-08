"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SseEventStreamGateway = exports.ClientStreamSubscription = void 0;


var _nodeStream = require("node:stream"); /**
 * @fileoverview M1 TASK-03: Server-Sent Events (SSE) & gRPC Stream Fan-out Multiplexer.
 */








/**
 * One dispatched SSE frame plus the credit-window bookkeeping the stream publisher reports back to the client.
 */











class ClientStreamSubscription {
  subscriptionId;
  taskId;
  stream;
  closed = false;

  constructor(subscriptionId, taskId) {
    this.subscriptionId = subscriptionId;
    this.taskId = taskId;
    this.stream = new _nodeStream.PassThrough();
  }

  writeSse(envelope) {
    if (this.closed) return;
    const frame =
    `id: ${envelope.id}\n` +
    `event: ${envelope.event}\n` +
    `data: ${JSON.stringify(envelope)}\n\n`;
    this.stream.write(frame);
  }

  close() {
    if (!this.closed) {
      this.closed = true;
      this.stream.end();
    }
  }
}exports.ClientStreamSubscription = ClientStreamSubscription;

class SseEventStreamGateway {
  subscriptionsByTask = new Map();
  historyByTask = new Map();
  /** Per-client credit window (`clientId -> credits remaining`) for backpressure-controlled fan-out. */
  creditsByClient = new Map();
  maxHistoryPerTask;
  eventSeq = 0;

  constructor(maxHistoryPerTask = 100) {
    this.maxHistoryPerTask = maxHistoryPerTask;
  }

  /**
   * Registers a client with an initial credit window. A client whose credits are exhausted stops
   * receiving `publishEvent` frames until `ackCredits` replenishes the window.
   */
  registerClient(clientId, initialCredits = 100) {
    this.creditsByClient.set(clientId, initialCredits);
  }

  /** Replenishes a client's credit window and returns the new remaining balance. */
  ackCredits(clientId, credits) {
    const next = (this.creditsByClient.get(clientId) ?? 0) + credits;
    this.creditsByClient.set(clientId, next);
    return next;
  }

  getRemainingCredits(clientId) {
    return this.creditsByClient.get(clientId) ?? 0;
  }

  /**
   * Publishes a serialized task stream event to the task's subscribers subject to the client's credit
   * window. Returns the dispatched frames; an empty array means the frame was withheld (no credits left).
   */
  publishEvent(
  clientId,
  eventType,
  taskId,
  payload)
  {
    const remaining = this.creditsByClient.get(clientId);
    if (remaining !== undefined && remaining <= 0) {
      return [];
    }

    this.eventSeq += 1;
    const frame = {
      id: `${taskId}-${this.eventSeq}`,
      event: eventType,
      taskId,
      clientId,
      eventSeq: this.eventSeq,
      timestampMs: Date.now(),
      payload
    };

    let data;
    try {
      const parsed = JSON.parse(payload);
      data = parsed && typeof parsed === "object" ? parsed : { value: parsed };
    } catch {
      data = { value: payload };
    }

    this.publish({
      id: frame.id,
      event: eventType,
      taskId,
      turnIndex: frame.eventSeq,
      timestampMs: frame.timestampMs,
      data
    });

    if (remaining !== undefined) {
      this.creditsByClient.set(clientId, remaining - 1);
    }
    return [frame];
  }

  subscribe(taskId, subscriptionId, replayFromEventId) {
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
  handleWebSocket1012Reconnect(
  taskId,
  oldSubscriptionId,
  newSubscriptionId,
  lastReceivedEventId)
  {
    this.unsubscribe(taskId, oldSubscriptionId);
    const subscription = this.subscribe(taskId, newSubscriptionId, lastReceivedEventId);
    return { subscription, closeCodeHandled: 1012 };
  }

  unsubscribe(taskId, subscriptionId) {
    const taskSubs = this.subscriptionsByTask.get(taskId);
    const sub = taskSubs?.get(subscriptionId);
    if (sub) {
      sub.close();
      taskSubs?.delete(subscriptionId);
    }
  }

  publish(envelope) {
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
}exports.SseEventStreamGateway = SseEventStreamGateway;