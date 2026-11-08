"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TransactionalOutboxDispatcher = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M6 TASK-23: Transactional Outbox & HMAC-SHA256 Signed Webhook Dispatcher.
 */










class TransactionalOutboxDispatcher {
  outbox = new Map();
  maxAttempts;
  signingSecret;

  constructor(signingSecret = "ansts-webhook-hmac-secret", maxAttempts = 4) {
    this.signingSecret = signingSecret;
    this.maxAttempts = maxAttempts;
  }

  enqueueOutbox(messageId, taskId, targetUrl, payload) {
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
    return msg;
  }

  signPayload(payloadJson, timestampSec = Math.floor(Date.now() / 1000)) {
    const body = `${timestampSec}.${payloadJson}`;
    const hmac = crypto.createHmac("sha256", this.signingSecret).update(body, "utf-8").digest("hex");
    return `t=${timestampSec},v1=${hmac}`;
  }

  processPending(sender)


  {
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
}exports.TransactionalOutboxDispatcher = TransactionalOutboxDispatcher;