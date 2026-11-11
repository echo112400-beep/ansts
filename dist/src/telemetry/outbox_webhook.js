"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TransactionalOutboxDispatcher = void 0;




var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M6 TASK-23, M54-4, M60-1 & M66-1: Transactional Outbox & HMAC-SHA256 Signed Webhook Dispatcher
 * with SQLite Durability, Per-Message Exception Isolation, Timing-Safe Replay-Window Signature Verification,
 * Constructor Overload/MaxAttempts Sanitization, Dead-Letter Requeue & Compaction.
 */










class TransactionalOutboxDispatcher {
  outbox = new Map();
  maxAttempts;
  signingSecret;
  sql;

  constructor(
  signingSecret = "ansts-webhook-hmac-secret",
  maxAttemptsOrSql = 4,
  sql)
  {
    this.signingSecret = signingSecret || "ansts-webhook-hmac-secret";
    if (
    typeof maxAttemptsOrSql === "object" &&
    maxAttemptsOrSql !== null &&
    "exec" in maxAttemptsOrSql)
    {
      this.maxAttempts = 4;
      this.bindSqlStorage(maxAttemptsOrSql);
    } else {
      const numericMax = Number(maxAttemptsOrSql);
      this.maxAttempts =
      Number.isFinite(numericMax) && numericMax > 0 ?
      Math.max(1, Math.floor(numericMax)) :
      4;
      if (sql) {
        this.bindSqlStorage(sql);
      }
    }
  }

  /**
   * [M60-1] Binds SQLite storage so queued webhook messages survive process crashes before delivery.
   */
  bindSqlStorage(sql) {
    this.sql = sql;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS transactional_outbox_messages (message_id TEXT PRIMARY KEY, task_id TEXT, target_url TEXT, payload_json TEXT, attempts INTEGER, delivered INTEGER, dead_lettered INTEGER, last_error TEXT, signature_header TEXT)");

    const rows = this.sql.
    exec("SELECT * FROM transactional_outbox_messages").
    toArray();
    let restored = 0;
    for (const r of rows) {
      const msg = {
        messageId: String(r.message_id),
        taskId: String(r.task_id),
        targetUrl: String(r.target_url),
        payload: JSON.parse(String(r.payload_json ?? "{}")),
        attempts: Number(r.attempts ?? 0),
        delivered: Boolean(Number(r.delivered ?? 0)),
        deadLettered: Boolean(Number(r.dead_lettered ?? 0)),
        lastError: r.last_error ? String(r.last_error) : undefined,
        signatureHeader: r.signature_header ? String(r.signature_header) : undefined
      };
      this.outbox.set(msg.messageId, msg);
      restored += 1;
    }
    return restored;
  }

  persistOutboxMessage(msg) {
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
    msg.signatureHeader ?? "");

  }

  enqueueOutbox(
  messageId,
  taskId,
  targetUrl,
  payload,
  options)
  {
    const existing = this.outbox.get(messageId);
    if (existing) {
      if (existing.deadLettered && options?.resetIfDeadLettered) {
        existing.deadLettered = false;
        existing.attempts = 0;
        existing.lastError = undefined;
        existing.targetUrl = targetUrl;
        existing.payload = payload;
        this.persistOutboxMessage(existing);
      }
      return existing;
    }
    const msg = {
      messageId,
      taskId,
      targetUrl,
      payload,
      attempts: 0,
      delivered: false,
      deadLettered: false
    };
    this.outbox.set(messageId, msg);
    this.persistOutboxMessage(msg);
    return msg;
  }

  /**
   * [M66-1] Explicitly resets a dead-lettered webhook message so operators can re-drive failed deliveries
   * after fixing downstream webhook endpoints.
   */
  retryDeadLetteredMessage(
  messageId,
  newTargetUrl)
  {
    const msg = this.outbox.get(messageId);
    if (!msg || msg.delivered) return null;
    msg.deadLettered = false;
    msg.attempts = 0;
    msg.lastError = undefined;
    if (newTargetUrl) {
      msg.targetUrl = newTargetUrl;
    }
    this.persistOutboxMessage(msg);
    return msg;
  }

  listMessages() {
    return Array.from(this.outbox.values());
  }

  signPayload(payloadJson, timestampSec = Math.floor(Date.now() / 1000)) {
    const body = `${timestampSec}.${payloadJson}`;
    const hmac = crypto.createHmac("sha256", this.signingSecret).update(body, "utf-8").digest("hex");
    return `t=${timestampSec},v1=${hmac}`;
  }

  /**
   * [M54-4 & M66-1] Verifies HMAC-SHA256 webhook signature using constant-time `crypto.timingSafeEqual`
   * and enforces replay timestamp tolerance (`maxSkewSec`).
   */
  verifySignature(
  payloadJson,
  signatureHeader,
  nowSec = Math.floor(Date.now() / 1000),
  maxSkewSec = 300)
  {
    if (!signatureHeader || typeof signatureHeader !== "string") return false;
    const parts = signatureHeader.split(",").map((p) => p.trim());
    const tPart = parts.find((p) => p.startsWith("t="));
    const v1Part = parts.find((p) => p.startsWith("v1="));
    if (!tPart || !v1Part) return false;
    const ts = Number(tPart.slice(2));
    const tolerance = Number.isFinite(maxSkewSec) && maxSkewSec >= 0 ? maxSkewSec : 300;
    if (!Number.isFinite(ts) || Math.abs(nowSec - ts) > tolerance) {
      return false;
    }
    const providedHex = v1Part.slice(3);
    if (!/^[0-9a-fA-F]{64}$/.test(providedHex)) {
      return false;
    }
    const body = `${ts}.${payloadJson}`;
    const expectedHex = crypto.
    createHmac("sha256", this.signingSecret).
    update(body, "utf-8").
    digest("hex");
    const expectedBuf = Buffer.from(expectedHex, "hex");
    const providedBuf = Buffer.from(providedHex, "hex");
    if (expectedBuf.length !== providedBuf.length) {
      return false;
    }
    return crypto.timingSafeEqual(expectedBuf, providedBuf);
  }

  /**
   * [M54-4 & M60-1] Isolates exceptions thrown by `sender(msg)` per message and persists delivery state to SQLite.
   */
  processPending(sender)



  {
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
        msg.lastError = err.message;
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

  purgeCompletedMessages() {
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
}exports.TransactionalOutboxDispatcher = TransactionalOutboxDispatcher;