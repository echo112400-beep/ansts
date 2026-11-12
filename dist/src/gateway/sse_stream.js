"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SseEventStreamGateway = exports.ClientStreamSubscription = void 0;




var _nodeStream = require("node:stream"); /**
 * @fileoverview M1 TASK-03, M54-4, M60-3 & M66-2: Server-Sent Events (SSE) & gRPC Stream Fan-out Multiplexer
 * with Evicted `Last-Event-ID` Fallback Replay, Completed Task Stream GC (Across History + Orphan Subscriptions),
 * Idempotent `envelope.id` Deduplication & CRLF Frame-Injection Sanitization.
 */






class ClientStreamSubscription {
  subscriptionId;
  taskId;
  stream;
  closed = false;
  replayGapDetected = false;

  constructor(subscriptionId, taskId) {
    this.subscriptionId = subscriptionId;
    this.taskId = taskId;
    this.stream = new _nodeStream.PassThrough();
  }

  /**
   * [M66-2] Strips `\r` and `\n` characters from `id` and `event` headers before framing
   * to prevent SSE header/frame injection attacks.
   */
  writeSse(envelope) {
    if (this.closed) return;
    const safeId = String(envelope.id ?? "").replace(/[\r\n]+/g, "");
    const safeEvent = String(envelope.event ?? "message").replace(/[\r\n]+/g, "");
    const frame =
    `id: ${safeId}\n` +
    `event: ${safeEvent}\n` +
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
  seenEventIdsByTask = new Map();
  maxHistoryPerTask;

  constructor(maxHistoryPerTask = 100) {
    const rawMax = Number(maxHistoryPerTask);
    this.maxHistoryPerTask =
    Number.isFinite(rawMax) && rawMax > 0 ? Math.max(1, Math.floor(rawMax)) : 100;
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
  gcCompletedTaskStreams(activeTaskIds) {
    let cleanedTasks = 0;
    const candidateTaskIds = new Set([
    ...this.historyByTask.keys(),
    ...this.subscriptionsByTask.keys(),
    ...this.seenEventIdsByTask.keys()]);

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
  publish(envelope) {
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

  getTaskHistory(taskId) {
    return [...(this.historyByTask.get(taskId) ?? [])];
  }

  listRecentEvents(limit = 15) {
    const all = [];
    for (const list of this.historyByTask.values()) {
      for (const evt of list) {
        all.push(evt);
      }
    }
    all.sort((a, b) => b.timestampMs - a.timestampMs);
    return all.slice(0, Math.max(1, limit));
  }

  getActiveSubscriptionCount(taskId) {
    if (taskId !== undefined) {
      return this.subscriptionsByTask.get(taskId)?.size ?? 0;
    }
    let total = 0;
    for (const m of this.subscriptionsByTask.values()) {
      total += m.size;
    }
    return total;
  }
}exports.SseEventStreamGateway = SseEventStreamGateway;