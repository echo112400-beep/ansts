/**
 * TASK-28: Transactional Outbox & HMAC-SHA256 Signed Webhook Dispatcher with DLQ Replay.
 */

import * as crypto from "crypto";

export interface OutboxRecord {
  eventId: string;
  tenantId: string;
  payloadJson: string;
  signatureSha256: string;
  delivered: boolean;
  attempts: number;
}

export class TransactionalOutboxDispatcher {
  private secret: Buffer;
  public outbox: OutboxRecord[] = [];
  public deadLetterQueue: OutboxRecord[] = [];

  constructor(
    webhookSecret: string = "whsec_ansts_v2_prod",
    public readonly maxRetries: number = 5,
  ) {
    this.secret = Buffer.from(webhookSecret, "utf8");
  }

  public signPayload(payloadJson: string): string {
    const digest = crypto.createHmac("sha256", this.secret).update(payloadJson, "utf8").digest("hex");
    return `sha256=${digest}`;
  }

  public stageInTransaction(eventEnvelope: Record<string, any>): OutboxRecord {
    const payloadJson = JSON.stringify(eventEnvelope);
    const rec: OutboxRecord = {
      eventId: eventEnvelope.id,
      tenantId: eventEnvelope.data.tenant_id,
      payloadJson,
      signatureSha256: this.signPayload(payloadJson),
      delivered: false,
      attempts: 0,
    };
    this.outbox.push(rec);
    return rec;
  }

  public dispatchPending(transportFn: (rec: OutboxRecord) => number): { deliveredCount: number; dlqCount: number } {
    let deliveredCount = 0;
    let dlqCount = 0;
    const remaining: OutboxRecord[] = [];

    for (const rec of this.outbox) {
      if (rec.delivered) {
        remaining.push(rec);
        continue;
      }
      while (rec.attempts < this.maxRetries) {
        rec.attempts += 1;
        const status = transportFn(rec);
        if (status >= 200 && status < 300) {
          rec.delivered = true;
          deliveredCount += 1;
          break;
        }
      }
      if (rec.delivered) {
        remaining.push(rec);
      } else {
        this.deadLetterQueue.push(rec);
        dlqCount += 1;
      }
    }
    this.outbox = remaining;
    return { deliveredCount, dlqCount };
  }
}
