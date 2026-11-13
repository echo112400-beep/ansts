/** @fileoverview Versioned execution attempts and conservative migration of legacy admission records. */
import { createHash } from "node:crypto";

export type AttemptPhase = "ADMITTED" | "RUNNING" | "RESULT_PENDING" | "COMPLETED" | "UNCERTAIN";
export interface ExecutionAttempt {
  schema_version: 1;
  tenant_id: string;
  task_id: string;
  attempt_id: string;
  node_id: string;
  fencing_token: number;
  input_digest: string;
  version: number;
  phase: AttemptPhase;
  updated_at: number;
  result?: Record<string, unknown>;
  result_digest?: string;
  reason?: string;
  reconciliation_digest?: string;
  audit?: Array<{ version: number; actor: string; action: string; evidence: string; reason: string; timestamp: number; previous_hash: string; hash: string }>;
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonical((value as Record<string, unknown>)[key])}`).join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}
export function executionDigest(value: unknown): string {
  return createHash("sha256").update(canonical(value)).digest("hex");
}
export function newAttempt(body: Record<string, unknown>): ExecutionAttempt | null {
  for (const key of ["tenant_id", "task_id", "attempt_id", "node_id", "input_digest"]) {
    if (typeof body[key] !== "string" || !String(body[key]).trim() || String(body[key]).length > 512) return null;
  }
  if (!Number.isSafeInteger(body.fencing_token) || Number(body.fencing_token) < 1) return null;
  return {
    schema_version: 1, tenant_id: String(body.tenant_id), task_id: String(body.task_id),
    attempt_id: String(body.attempt_id), node_id: String(body.node_id), fencing_token: Number(body.fencing_token),
    input_digest: String(body.input_digest), version: 1, phase: "ADMITTED", updated_at: Date.now(),
  };
}
export function migrateAttempt(saved: Record<string, unknown>): ExecutionAttempt {
  if (saved.schema_version === 1) return saved as unknown as ExecutionAttempt;
  // Legacy records cannot prove tenant, input identity or execution progress.
  return {
    schema_version: 1, tenant_id: String(saved.tenant_id ?? ""), task_id: String(saved.task_id ?? ""),
    attempt_id: "legacy", node_id: String(saved.node_id ?? ""), fencing_token: Number(saved.fencing_token ?? 0),
    input_digest: "", version: 1, phase: "UNCERTAIN", reason: "LEGACY_RECORD_REQUIRES_RECONCILIATION", updated_at: Date.now(),
    ...(saved.result ? { result: saved.result as Record<string, unknown>, result_digest: executionDigest(saved.result) } : {}),
  };
}
export function sameExecution(saved: ExecutionAttempt, body: Record<string, unknown>): boolean {
  return ["tenant_id", "task_id", "input_digest"].every(key => saved[key as keyof ExecutionAttempt] === body[key]);
}
export function sameOwner(saved: ExecutionAttempt, body: Record<string, unknown>): boolean {
  return sameExecution(saved, body) && saved.attempt_id === body.attempt_id && saved.node_id === body.node_id && saved.fencing_token === body.fencing_token;
}
