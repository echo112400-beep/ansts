/** @fileoverview Short-lived control-plane execution grants bound to one attempt and input digest. */
import { createHmac, randomUUID } from "node:crypto";
import { equalBodyToken } from "./body_auth.js";
import { ExecutionAttempt, sameOwner } from "./execution_attempt.js";

export interface ExecutionGrant {
  identity: Record<string, unknown>;
  nonce: string;
  issued_at: number;
  expires_at: number;
}
export function issueExecutionGrant(secret: string, execution: ExecutionAttempt, now = Date.now()): string {
  if (!/^[a-f0-9]{64}$/.test(secret)) throw new Error("EXECUTION_SIGNING_KEY_REQUIRED");
  const identity = { tenant_id: execution.tenant_id, task_id: execution.task_id, attempt_id: execution.attempt_id, node_id: execution.node_id, fencing_token: execution.fencing_token, input_digest: execution.input_digest };
  const payload = Buffer.from(JSON.stringify({ identity, nonce: randomUUID(), issued_at: now, expires_at: now + 30000 })).toString("base64url");
  return `${payload}.${createHmac("sha256", secret).update(`execution-v1:${payload}`).digest("hex")}`;
}
export function verifyExecutionGrant(secret: string, token: string, execution: ExecutionAttempt, now = Date.now()): ExecutionGrant | null {
  if (!/^[a-f0-9]{64}$/.test(secret) || token.length > 8192) return null;
  const [payload, signature, extra] = token.split(".");
  if (!payload || !signature || extra || !equalBodyToken(signature, createHmac("sha256", secret).update(`execution-v1:${payload}`).digest("hex"))) return null;
  try {
    const grant = JSON.parse(Buffer.from(payload, "base64url").toString("utf8")) as ExecutionGrant;
    if (typeof grant.nonce !== "string" || !Number.isFinite(grant.issued_at) || !Number.isFinite(grant.expires_at) || grant.issued_at > now || grant.expires_at <= now || grant.expires_at - grant.issued_at !== 30000 || !sameOwner(execution, grant.identity)) return null;
    return grant;
  } catch { return null; }
}
