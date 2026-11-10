/**
 * @fileoverview M6 TASK-23 & M54-4: Transactional Outbox & HMAC-SHA256 Signed Webhook Dispatcher
 * with Per-Message Exception Isolation, Replay-Window Signature Verification & Completed Message Compaction.
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
  lastError?: string;
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

  public enqueueOutbox(
    messageId: string,
    taskId: string,
    targetUrl: string,
    payload: Record<string, unknown>
  ): OutboxWebhookMessage {
    const existing = this.outbox.get(messageId);
    if (existing) {
      return existing;
    }
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

  /**
   * [M54-4] Verifies HMAC-SHA256 webhook signature and enforces replay timestamp tolerance (`maxSkewSec`).
   */
  public verifySignature(
    payloadJson: string,
    signatureHeader: string,
    nowSec = Math.floor(Date.now() / 1000),
    maxSkewSec = 300
  ): boolean {
    const parts = signatureHeader.split(",");
    const tPart = parts.find((p) => p.startsWith("t="));
    const v1Part = parts.find((p) => p.startsWith("v1="));
    if (!tPart || !v1Part) return false;
    const ts = Number(tPart.slice(2));
    if (!Number.isFinite(ts) || Math.abs(nowSec - ts) > maxSkewSec) {
      return false;
    }
    const expected = this.signPayload(payloadJson, ts);
    return expected === signatureHeader;
  }

  /**
   * [M54-4] Isolates exceptions thrown by `sender(msg)` per message so one poisoned/failing webhook endpoint
   * never aborts the dispatch loop or starves subsequent messages in the outbox.
   */
  public processPending(sender: (msg: OutboxWebhookMessage) => boolean): {
    deliveredCount: number;
    deadLetterCount: number;
    failedAttemptCount: number;
  } {
    let deliveredCount = 0;
    let deadLetterCount = 0;
    let failedAttemptCount = 0;
    for (const msg of this.outbox.values()) {
      if (msg.delivered || msg.deadLettered) continue;
      msg.attempts += 1;
      msg.signatureHeader = this.signPayload(JSON.stringify(msg.payload));
      let ok = false;
      try {
        ok = sender(msg);
      } catch (err) {
        ok = false;
        msg.lastError = (err as Error).message;
      }
      if (ok) {
        msg.delivered = true;
        msg.lastError = undefined;
        deliveredCount += 1;
      } else {
        failedAttemptCount += 1;
        if (msg.attempts >= this.maxAttempts) {
          msg.deadLettered = true;
          deadLetterCount += 1;
        }
      }
    }
    return { deliveredCount, deadLetterCount, failedAttemptCount };
  }

  public purgeCompletedMessages(): number {
    let purged = 0;
    for (const [id, msg] of Array.from(this.outbox.entries())) {
      if (msg.delivered || msg.deadLettered) {
        this.outbox.delete(id);
        purged += 1;
      }
    }
    return purged;
  }
}
