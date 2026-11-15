/** Explicit issuer/JWKS trust. OIDC configuration disables the development HMAC fallback. */
import { ConsoleJwtClaims } from "../dashboard/console_rbac_guard.js";
export interface OidcConfiguration { ANSTS_OIDC_ISSUER?: string; ANSTS_OIDC_AUDIENCE?: string; ANSTS_OIDC_JWKS_URL?: string; }
export async function oidcPrincipal(request: Request, config: OidcConfiguration): Promise<(ConsoleJwtClaims & { readOnly: boolean }) | null> {
  try {
    const issuer = new URL(config.ANSTS_OIDC_ISSUER ?? ""), jwksUrl = new URL(config.ANSTS_OIDC_JWKS_URL ?? "");
    if (issuer.protocol !== "https:" || jwksUrl.protocol !== "https:" || issuer.username || issuer.password || jwksUrl.username || jwksUrl.password || !config.ANSTS_OIDC_AUDIENCE) return null;
    const authorization = request.headers.get("authorization") ?? "";
    if (!authorization.startsWith("Bearer ") || authorization.length > 16384) return null;
    const parts = authorization.slice(7).split(".");
    if (parts.length !== 3 || parts.some(part => !/^[A-Za-z0-9_-]+$/.test(part))) return null;
    const decode = (part: string) => JSON.parse(Buffer.from(part, "base64url").toString("utf8"));
    const header = decode(parts[0]);
    if (header.alg !== "RS256" || typeof header.kid !== "string" || header.kid.length > 256 || header.crit || header.jku || header.jwk) return null;
    const response = await fetch(jwksUrl, { signal: AbortSignal.timeout(3000), redirect: "error" });
    if (!response.ok || !response.body) return null;
    const reader = response.body.getReader(), chunks: Uint8Array[] = []; let length = 0;
    try { for (;;) { const { done, value } = await reader.read(); if (done) break; length += value.length; if (length > 65536) { await reader.cancel(); return null; } chunks.push(value); } } finally { reader.releaseLock(); }
    const document = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    if (!Array.isArray(document.keys) || document.keys.length > 32) return null;
    const keys = document.keys.filter((key: JsonWebKey & { kid?: string }) => key.kid === header.kid && key.kty === "RSA" && (!key.alg || key.alg === "RS256") && (!key.use || key.use === "sig") && (!key.key_ops || key.key_ops.includes("verify")));
    if (keys.length !== 1) return null;
    const key = await crypto.subtle.importKey("jwk", keys[0], { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" }, false, ["verify"]);
    if ((key.algorithm as RsaKeyAlgorithm).modulusLength < 2048) return null;
    if (!await crypto.subtle.verify("RSASSA-PKCS1-v1_5", key, Buffer.from(parts[2], "base64url"), new TextEncoder().encode(parts[0] + "." + parts[1]))) return null;
    const claims = decode(parts[1]), now = Math.floor(Date.now() / 1000);
    const audience = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
    if (claims.iss !== config.ANSTS_OIDC_ISSUER || !audience.includes(config.ANSTS_OIDC_AUDIENCE) || typeof claims.sub !== "string" || !claims.sub.trim() || claims.sub.length > 256 || !Number.isSafeInteger(claims.exp) || claims.exp <= now || !Number.isSafeInteger(claims.iat) || claims.iat > now + 30 || claims.exp <= claims.iat || (claims.nbf !== undefined && (!Number.isSafeInteger(claims.nbf) || claims.nbf > now + 30)) || !["VIEWER", "OPERATOR", "SRE_ADMIN"].includes(claims.role) || typeof claims.scopeTenant !== "string" || !claims.scopeTenant.trim() || claims.scopeTenant.length > 256) return null;
    return { sub: claims.sub, role: claims.role, scopeTenant: claims.scopeTenant, scopeRegion: typeof claims.scopeRegion === "string" ? claims.scopeRegion : "all", iat: claims.iat, exp: claims.exp, readOnly: claims.role === "VIEWER" };
  } catch { return null; }
}
