/** GitHub OAuth authorization code + PKCE; application permissions come only from a server allowlist. */
import { createHash, createHmac, randomBytes } from "node:crypto";
import { rememberGithubProfile } from "./github_profile.js";
import { equalBodyToken } from "./body_auth.js";
import { ConsoleRbacAuthGuard } from "../dashboard/console_rbac_guard.js";
export interface GithubConfiguration {
  ANSTS_GITHUB_CLIENT_ID?: string;
  ANSTS_GITHUB_CLIENT_SECRET?: string;
  ANSTS_GITHUB_IDENTITIES_JSON?: string;
  ANSTS_PUBLIC_ORIGIN?: string;
  ANSTS_USER_AUTH_SECRET?: string;
  ANSTS_RUNTIME_PROFILE?: string;
}
export function githubDevelopmentLoopback(env: GithubConfiguration): boolean {
  try { const url = new URL(env.ANSTS_PUBLIC_ORIGIN!); return env.ANSTS_RUNTIME_PROFILE === "development" && url.protocol === "http:" && ["127.0.0.1", "[::1]"].includes(url.hostname); } catch { return false; }
}
export function githubConfigured(env: GithubConfiguration): boolean {
  try { return !!env.ANSTS_GITHUB_CLIENT_ID && !!env.ANSTS_GITHUB_CLIENT_SECRET && !!env.ANSTS_GITHUB_IDENTITIES_JSON && (env.ANSTS_USER_AUTH_SECRET?.length ?? 0) >= 32 && new URL(env.ANSTS_PUBLIC_ORIGIN!).origin === env.ANSTS_PUBLIC_ORIGIN && (env.ANSTS_PUBLIC_ORIGIN!.startsWith("https://") || githubDevelopmentLoopback(env)); } catch { return false; }
}
export function applicationCookie(request: Request, name: string): string | undefined { return request.headers.get("cookie")?.split(";").map(part => part.trim()).find(part => part.startsWith(name + "="))?.slice(name.length + 1); }
export function applicationSession(request: Request, env: GithubConfiguration): string | undefined { return applicationCookie(request, githubDevelopmentLoopback(env) ? "ansts_dev_session" : "__Host-ansts_session"); }
const cookie = (name: string, value: string, age: number, sameSite = "Strict", secure = true) => `${name}=${value}; Path=/; HttpOnly${secure ? "; Secure" : ""}; SameSite=${sameSite}; Max-Age=${age}`;
async function boundedJson(response: Response): Promise<Record<string, unknown>> {
  if (!response.ok || !response.body) throw new Error("IDENTITY_PROVIDER_UNAVAILABLE");
  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let total = 0;
  try { for (;;) { const part = await reader.read(); if (part.done) break; total += part.value.length; if (total > 65536) { await reader.cancel(); throw new Error("IDENTITY_RESPONSE_TOO_LARGE"); } chunks.push(part.value); } } finally { reader.releaseLock(); }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}
export async function githubAuthRequest(request: Request, env: GithubConfiguration): Promise<Response | null> {
  const url = new URL(request.url), route = url.pathname;
  if (route === "/v1/auth/providers" && request.method === "GET") return Response.json({ github: githubConfigured(env) }, { headers: { "cache-control": "no-store" } });
  if (!["/v1/auth/github", "/auth/callback/github", "/v1/auth/logout"].includes(route)) return null;
  if (!githubConfigured(env)) return Response.json({ error: "GITHUB_LOGIN_UNCONFIGURED" }, { status: 503 });
  const secure = !githubDevelopmentLoopback(env), sessionName = secure ? "__Host-ansts_session" : "ansts_dev_session", oauthName = secure ? "__Host-ansts_oauth" : "ansts_dev_oauth";
  if (route === "/v1/auth/logout") {
    if (request.method !== "POST" || request.headers.get("origin") !== env.ANSTS_PUBLIC_ORIGIN) return Response.json({ error: "CSRF_REJECTED" }, { status: 403 });
    return Response.json({ loggedOut: true }, { headers: { "set-cookie": cookie(sessionName, "", 0, "Strict", secure), "cache-control": "no-store" } });
  }
  if (request.method !== "GET") return Response.json({ error: "METHOD_NOT_ALLOWED" }, { status: 405 });
  const callback = env.ANSTS_PUBLIC_ORIGIN + "/auth/callback/github";
  const sign = (value: string) => createHmac("sha256", env.ANSTS_USER_AUTH_SECRET!).update("github-oauth:" + value).digest("hex");
  if (route === "/v1/auth/github") {
    const state = randomBytes(24).toString("hex"), verifier = randomBytes(32).toString("base64url");
    const payload = `${state}.${Math.floor(Date.now() / 1000)}.${verifier}`;
    const destination = new URL("https://github.com/login/oauth/authorize");
    const parameters = new URLSearchParams({ client_id: env.ANSTS_GITHUB_CLIENT_ID!, redirect_uri: callback, state, code_challenge: createHash("sha256").update(verifier).digest("base64url"), code_challenge_method: "S256" });
    if (!env.ANSTS_GITHUB_CLIENT_ID!.startsWith("Iv")) parameters.set("scope", "read:user");
    destination.search = parameters.toString();
    return new Response(null, { status: 302, headers: { location: destination.toString(), "set-cookie": cookie(oauthName, payload + "." + sign(payload), 300, "Lax", secure), "cache-control": "no-store" } });
  }
  const saved = applicationCookie(request, oauthName) ?? "", parts = saved.split(".");
  const now = Math.floor(Date.now() / 1000), code = url.searchParams.get("code");
  if (parts.length !== 4 || !/^[a-f0-9]{48}$/.test(parts[0]) || !/^[A-Za-z0-9_-]{43}$/.test(parts[2]) || !equalBodyToken(parts[3], sign(parts.slice(0, 3).join("."))) || !equalBodyToken(parts[0], url.searchParams.get("state") ?? "") || !Number.isSafeInteger(Number(parts[1])) || Number(parts[1]) > now || now - Number(parts[1]) > 300 || !code || code.length > 512) return Response.json({ error: "OAUTH_STATE_REJECTED" }, { status: 403 });
  try {
    const token = await boundedJson(await fetch("https://github.com/login/oauth/access_token", { method: "POST", headers: { accept: "application/json", "content-type": "application/x-www-form-urlencoded" }, body: new URLSearchParams({ client_id: env.ANSTS_GITHUB_CLIENT_ID!, client_secret: env.ANSTS_GITHUB_CLIENT_SECRET!, code, redirect_uri: callback, code_verifier: parts[2] }), signal: AbortSignal.timeout(10000), redirect: "error" }));
    if (typeof token.access_token !== "string" || token.token_type !== "bearer") throw new Error("OAUTH_TOKEN_REJECTED");
    const profile = await boundedJson(await fetch("https://api.github.com/user", { headers: { authorization: `Bearer ${token.access_token}`, accept: "application/vnd.github+json", "user-agent": "ANSTS", "x-github-api-version": "2022-11-28" }, signal: AbortSignal.timeout(10000), redirect: "error" }));
    if (!Number.isSafeInteger(profile.id) || Number(profile.id) < 1) throw new Error("OAUTH_IDENTITY_REJECTED");
    const permissions = JSON.parse(env.ANSTS_GITHUB_IDENTITIES_JSON!)[String(profile.id)];
    if (!permissions || !["VIEWER", "OPERATOR", "SRE_ADMIN"].includes(permissions.role) || typeof permissions.scopeTenant !== "string" || !permissions.scopeTenant.trim()) return Response.json({ error: "GITHUB_USER_NOT_AUTHORIZED" }, { status: 403 });
    const identityProfile = rememberGithubProfile(profile);
    const jwt = new ConsoleRbacAuthGuard(env.ANSTS_USER_AUTH_SECRET!).issueToken({ sub: `github:${profile.id}`, role: permissions.role, scopeTenant: permissions.scopeTenant, scopeRegion: "all", ttlSec: 900, profile: identityProfile });
    const headers = new Headers({ location: env.ANSTS_PUBLIC_ORIGIN + "/", "cache-control": "no-store", "referrer-policy": "no-referrer" });
    headers.append("set-cookie", cookie(oauthName, "", 0, "Lax", secure)); headers.append("set-cookie", cookie(sessionName, jwt, 900, "Strict", secure));
    return new Response(null, { status: 302, headers });
  } catch { return Response.json({ error: "GITHUB_LOGIN_FAILED" }, { status: 502 }); }
}
