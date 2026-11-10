/**
 * @fileoverview M6 TASK-23, M54-4 & M60-1: Transactional Outbox & HMAC-SHA256 Signed Webhook Dispatcher
 * with SQLite Durability, Per-Message Exception Isolation, Replay-Window Signature Verification & Compaction.
 */
import * as crypto from "node:crypto";
import { SqlStorageLike } from "../cloudflare/env.js";

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
  private sql?: SqlStorageLike;

  constructor(signingSecret = "ansts-webhook-hmac-secret", maxAttempts = 4, sql?: SqlStorageLike) {
    this.signingSecret = signingSecret;
    this.maxAttempts = maxAttempts;
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M60-1] Binds SQLite storage so queued webhook messages survive process crashes before delivery.
   */
  public bindSqlStorage(sql: SqlStorageLike): number {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS transactional_outbox_messages (message_id TEXT PRIMARY KEY, task_id TEXT, target_url TEXT, payload_json TEXT, attempts INTEGER, delivered INTEGER, dead_lettered INTEGER, last_error TEXT, signature_header TEXT)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM transactional_outbox_messages")
      .toArray();
    let restored = 0;
    for (const r of rows) {
      const msg: OutboxWebhookMessage = {
        messageId: String(r.message_id),
        taskId: String(r.task_id),
        targetUrl: String(r.target_url),
        payload: JSON.parse(String(r.payload_json ?? "{}")) as Record<string, unknown>,
        attempts: Number(r.attempts ?? 0),
        delivered: Boolean(Number(r.delivered ?? 0)),
        deadLettered: Boolean(Number(r.dead_lettered ?? 0)),
        lastError: r.last_error ? String(r.last_error) : undefined,
        signatureHeader: r.signature_header ? String(r.signature_header) : undefined,
      };
      this.outbox.set(msg.messageId, msg);
      restored += 1;
    }
    return restored;
  }

  private persistOutboxMessage(msg: OutboxWebhookMessage): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO transactional_outbox_messages (message_id, task_id, target_url, payload_json, attempts, delivered, dead_lettered, last_error, signature_header) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      msg.messageId,
      msg.taskId,
      msg.targetUrl,
      JSON.stringify(msg.payload),
      msg.attempts,
      msg.delivered ? 1 : 0,
      msg.deadLettered ? 1 : 0,
      msg.lastError ?? "",
      msg.signatureHeader ?? ""
    );
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
    this.persistOutboxMessage(msg);
    return msg;
  }

  public listMessages(): OutboxWebhookMessage[] {
    return Array.from(this.outbox.values());
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
   * [M54-4 & M60-1] Isolates exceptions thrown by `sender(msg)` per message and persists delivery state to SQLite.
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
      this.persistOutboxMessage(msg);
    }
    return { deliveredCount, deadLetterCount, failedAttemptCount };
  }

  public purgeCompletedMessages(): number {
    let purged = 0;
    for (const [id, msg] of Array.from(this.outbox.entries())) {
      if (msg.delivered || msg.deadLettered) {
        this.outbox.delete(id);
        if (this.sql) {
          this.sql.exec("DELETE FROM transactional_outbox_messages WHERE message_id = ?", id);
        }
        purged += 1;
      }
    }
    return purged;
  }
}
