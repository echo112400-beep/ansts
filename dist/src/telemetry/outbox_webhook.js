"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TransactionalOutboxDispatcher = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
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