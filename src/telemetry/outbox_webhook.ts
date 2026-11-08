/**
 * @fileoverview M6 TASK-23: Transactional Outbox & HMAC-SHA256 Signed Webhook Dispatcher.
 */
import * as crypto from "node:crypto";

export interface OutboxWebhookMessage {
  messageId: string;
  taskId: string;
  targetUrl: string;
  payload: Record<string, unknown>;
  attempts: number;
  delivered: boolean;
  deadLettered: boolean;
  signatureHeader?: string;
}

export class TransactionalOutboxDispatcher {
  private readonly outbox: Map<string, OutboxWebhookMessage> = new Map();
  private readonly maxAttempts: number;
  private readonly signingSecret: string;

  constructor(signingSecret = "ansts-webhook-hmac-secret", maxAttempts = 4) {
    this.signingSecret = signingSecret;
    this.maxAttempts = maxAttempts;
  }

  public enqueueOutbox(messageId: string, taskId: string, targetUrl: string, payload: Record<string, unknown>): OutboxWebhookMessage {
    const msg: OutboxWebhookMessage = {
      messageId,
      taskId,
      targetUrl,
      payload,
      attempts: 0,
      delivered: false,
      deadLettered: false,
    };
    this.outbox.set(messageId, msg);
    return msg;
  }

  public signPayload(payloadJson: string, timestampSec = Math.floor(Date.now() / 1000)): string {
    const body = `${timestampSec}.${payloadJson}`;
    const hmac = crypto.createHmac("sha256", this.signingSecret).update(body, "utf-8").digest("hex");
    return `t=${timestampSec},v1=${hmac}`;
  }

  public processPending(sender: (msg: OutboxWebhookMessage) => boolean): {
    deliveredCount: number;
    deadLetterCount: number;
  } {
    let deliveredCount = 0;
    let deadLetterCount = 0;
    for (const msg of this.outbox.values()) {
      if (msg.delivered || msg.deadLettered) continue;
      msg.attempts += 1;
      msg.signatureHeader = this.signPayload(JSON.stringify(msg.payload));
      const ok = sender(msg);
      if (ok) {
        msg.delivered = true;
        deliveredCount += 1;
      } else if (msg.attempts >= this.maxAttempts) {
        msg.deadLettered = true;
        deadLetterCount += 1;
      }
    }
    return { deliveredCount, deadLetterCount };
  }
}
