/** @fileoverview Host-scoped service credentials; no default signing key. */
import { createHmac, timingSafeEqual } from "node:crypto";
export interface BodyAuthConfiguration { ANSTS_BODY_AUTH_SECRET?: string; ANSTS_BODY_AUTH_PREVIOUS_SECRET?: string; ANSTS_BODY_AUTH_PREVIOUS_UNTIL?: string; }
export function serviceKeys(config?: string | BodyAuthConfiguration): string[] {
  const current = typeof config === "string" ? config : config?.ANSTS_BODY_AUTH_SECRET;
  if (!current || !/^[a-f0-9]{64}$/.test(current)) return [];
  const keys = [current];
  if (config && typeof config !== "string") {
    const until = Number(config.ANSTS_BODY_AUTH_PREVIOUS_UNTIL), now = Math.floor(Date.now() / 1000);
    if (config.ANSTS_BODY_AUTH_PREVIOUS_SECRET && /^[a-f0-9]{64}$/.test(config.ANSTS_BODY_AUTH_PREVIOUS_SECRET) && Number.isSafeInteger(until) && until > now && until <= now + 900) keys.push(config.ANSTS_BODY_AUTH_PREVIOUS_SECRET);
  }
  return keys;
}

export function bodyServiceToken(secret: string, host: string): string {
  return createHmac("sha256", secret).update(`ansts-body-v1:${host}`).digest("hex");
}
export function equalBodyToken(actual: string, expected: string): boolean {
  const a = Buffer.from(actual);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}
export function authenticateBody(request: Request, secret?: string | BodyAuthConfiguration): string | null {
  const host = request.headers.get("x-ansts-body-id") ?? "";
  if (!/^[a-zA-Z0-9_-]{1,128}$/.test(host)) return null;
  return serviceKeys(secret).some(key => equalBodyToken(request.headers.get("authorization") ?? "", `Body ${bodyServiceToken(key, host)}`)) ? host : null;
}
export function authenticateBodyOperator(request: Request, secret?: string | BodyAuthConfiguration): boolean {
  return serviceKeys(secret).some(key => equalBodyToken(request.headers.get("authorization") ?? "", `Operator ${bodyServiceToken(key, "operator:reconcile")}`));
}
export function authenticateGatewayService(request: Request, secret?: string | BodyAuthConfiguration): boolean {
  return serviceKeys(secret).some(key => equalBodyToken(request.headers.get("authorization") ?? "", `Gateway ${bodyServiceToken(key, "gateway:control")}`));
}
export function authenticateBodyControl(request: Request, secret?: string | BodyAuthConfiguration): boolean {
  return serviceKeys(secret).some(key => equalBodyToken(request.headers.get("authorization") ?? "", `BodyControl ${bodyServiceToken(key, "body:control")}`));
}
