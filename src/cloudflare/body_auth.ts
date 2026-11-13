/** @fileoverview Host-scoped service credentials; no default signing key. */
import { createHmac, timingSafeEqual } from "node:crypto";

export function bodyServiceToken(secret: string, host: string): string {
  return createHmac("sha256", secret).update(`ansts-body-v1:${host}`).digest("hex");
}
export function equalBodyToken(actual: string, expected: string): boolean {
  const a = Buffer.from(actual);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}
export function authenticateBody(request: Request, secret?: string): string | null {
  if (!secret || !/^[a-f0-9]{64}$/.test(secret)) return null;
  const host = request.headers.get("x-ansts-body-id") ?? "";
  if (!/^[a-zA-Z0-9_-]{1,128}$/.test(host)) return null;
  return equalBodyToken(request.headers.get("authorization") ?? "", `Body ${bodyServiceToken(secret, host)}`) ? host : null;
}
export function authenticateBodyOperator(request: Request, secret?: string): boolean {
  return !!secret && /^[a-f0-9]{64}$/.test(secret) && equalBodyToken(
    request.headers.get("authorization") ?? "", `Operator ${bodyServiceToken(secret, "operator:reconcile")}`
  );
}
