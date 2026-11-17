/** @fileoverview Explicit user identities and tenant-scoped Gateway authorization. */
import { ConsoleJwtClaims, ConsoleRbacAuthGuard } from "../dashboard/console_rbac_guard.js";

export function gatewayPrincipal(request: Request, secret?: string, previousSecret?: string, previousUntil?: number): (ConsoleJwtClaims & { readOnly: boolean }) | null {
  if (!secret || secret.trim().length < 32) return null;
  try {
    const token = (request.headers.get("authorization") ?? "").slice(7);
    const header = JSON.parse(Buffer.from(token.split(".")[0], "base64url").toString("utf8"));
    if (header.alg !== "HS256") return null;
    let verified = new ConsoleRbacAuthGuard(secret).verifyAuthorizationHeader(request.headers.get("authorization") ?? undefined);
    let previous = false;
    if (!verified.authenticated && request.method === "GET" && previousSecret && previousSecret.trim().length >= 32 && Number.isSafeInteger(previousUntil) && Math.floor(Date.now() / 1000) < previousUntil! && previousUntil! <= Math.floor(Date.now() / 1000) + 900) {
      verified = new ConsoleRbacAuthGuard(previousSecret).verifyAuthorizationHeader(request.headers.get("authorization") ?? undefined);
      previous = true;
    }
    const claims = verified.claims;
    const now = Math.floor(Date.now() / 1000);
    if (!verified.authenticated || !claims || !["VIEWER", "OPERATOR", "SRE_ADMIN"].includes(claims.role) || !claims.sub.trim() || !Number.isSafeInteger(claims.exp) || !Number.isSafeInteger(claims.iat) || claims.iat > now || claims.exp <= now || claims.exp <= claims.iat || typeof claims.scopeTenant !== "string" || !claims.scopeTenant.trim()) return null;
    if (previous && claims.iat > previousUntil! - 900) return null;
    return { ...claims, readOnly: previous || claims.role === "VIEWER" };
  } catch { return null; }
}
export function tenantAllowed(claims: ConsoleJwtClaims, tenantId: unknown): boolean {
  return typeof tenantId === "string" && (claims.scopeTenant === "all" || claims.scopeTenant === tenantId);
}
export function gatewayAuthorizationError(request: Request, claims: ConsoleJwtClaims, tenantId?: unknown): Response | null {
  if (request.method !== "GET" && claims.role === "VIEWER") return Response.json({ error: "RBAC_FORBIDDEN" }, { status: 403 });
  if (tenantId !== undefined && !tenantAllowed(claims, tenantId)) return Response.json({ error: "TENANT_FORBIDDEN" }, { status: 403 });
  return null;
}
