import { it } from "node:test";
import * as assert from "node:assert/strict";
import { githubAuthRequest, githubConfigured } from "../src/cloudflare/github_identity.js";
import { gatewayPrincipal } from "../src/cloudflare/gateway_authorization.js";
it("does not send OAuth App scopes when authorizing a GitHub App client", async () => {
  const config = { ANSTS_GITHUB_CLIENT_ID: "Iv1.diagnostic", ANSTS_GITHUB_CLIENT_SECRET: "private-client-secret", ANSTS_GITHUB_IDENTITIES_JSON: "{}", ANSTS_PUBLIC_ORIGIN: "https://console.example", ANSTS_USER_AUTH_SECRET: "a".repeat(64) };
  const response = await githubAuthRequest(new Request("https://gateway/v1/auth/github"), config);
  assert.equal(response?.status, 302);
  const url = new URL(response!.headers.get("location")!);
  assert.equal(url.searchParams.has("scope"), false);
  assert.equal(url.searchParams.get("redirect_uri"), "https://console.example/auth/callback/github");
  assert.equal(url.searchParams.get("code_challenge_method"), "S256");
});
it("binds GitHub login to state and PKCE and maps immutable GitHub IDs to explicit application permissions", async () => {
  const config = { ANSTS_GITHUB_CLIENT_ID: "client", ANSTS_GITHUB_CLIENT_SECRET: "private-client-secret", ANSTS_GITHUB_IDENTITIES_JSON: JSON.stringify({ "123": { role: "OPERATOR", scopeTenant: "tenant-a" } }), ANSTS_PUBLIC_ORIGIN: "https://console.example", ANSTS_USER_AUTH_SECRET: "a".repeat(64) };
  assert.equal(githubConfigured({ ...config, ANSTS_PUBLIC_ORIGIN: "http://127.0.0.1:8088" }), false);
  assert.equal(githubConfigured({ ...config, ANSTS_PUBLIC_ORIGIN: "http://127.0.0.1:8088", ANSTS_RUNTIME_PROFILE: "development" }), true);
  assert.equal(githubConfigured({ ...config, ANSTS_PUBLIC_ORIGIN: "http://public.example", ANSTS_RUNTIME_PROFILE: "development" }), false);
  const begin = await githubAuthRequest(new Request("https://gateway/v1/auth/github"), config);
  assert.equal(begin?.status, 302);
  const redirect = new URL(begin!.headers.get("location")!), stateCookie = begin!.headers.get("set-cookie")!.split(";")[0];
  assert.equal(redirect.origin, "https://github.com");
  assert.equal(redirect.searchParams.get("redirect_uri"), "https://console.example/auth/callback/github");
  assert.equal(redirect.searchParams.get("code_challenge_method"), "S256");
  assert.ok(!redirect.toString().includes(config.ANSTS_GITHUB_CLIENT_SECRET));
  const original = globalThis.fetch; let calls = 0;
  globalThis.fetch = async (input, init) => {
    calls++;
    if (String(input).includes("access_token")) {
      const body = new URLSearchParams(String(init?.body)); assert.equal(body.get("code_verifier")?.length, 43);
      assert.equal(body.get("redirect_uri"), "https://console.example/auth/callback/github");
      return Response.json({ access_token: "github-provider-token", token_type: "bearer" });
    }
    return Response.json({ id: 123, login: "renameable-login" });
  };
  try {
    assert.equal((await githubAuthRequest(new Request("https://gateway/auth/callback/github?code=x&state=wrong", { headers: { cookie: stateCookie } }), config))?.status, 403);
    assert.equal(calls, 0);
    const callback = await githubAuthRequest(new Request(`https://gateway/auth/callback/github?code=x&state=${redirect.searchParams.get("state")}`, { headers: { cookie: stateCookie } }), config);
    assert.equal(callback?.status, 302); assert.equal(callback?.headers.get("location"), "https://console.example/");
    const cookies = callback!.headers.getSetCookie();
    const session = cookies.find(value => value.startsWith("__Host-ansts_session="))!;
    assert.match(session, /HttpOnly; Secure; SameSite=Strict/);
    assert.ok(!session.includes("github-provider-token"));
    const jwt = session.split(";")[0].slice("__Host-ansts_session=".length);
    assert.equal(gatewayPrincipal(new Request("https://gateway", { headers: { authorization: `Bearer ${jwt}` } }), config.ANSTS_USER_AUTH_SECRET)?.sub, "github:123");
    assert.equal(gatewayPrincipal(new Request("https://gateway", { headers: { authorization: `Bearer ${jwt}` } }), config.ANSTS_USER_AUTH_SECRET)?.profile?.login, "renameable-login");
    assert.equal((await githubAuthRequest(new Request("https://gateway/v1/auth/logout", { method: "POST", headers: { origin: "https://attacker" } }), config))?.status, 403);
    assert.equal((await githubAuthRequest(new Request("https://gateway/v1/auth/logout", { method: "POST", headers: { origin: config.ANSTS_PUBLIC_ORIGIN } }), config))?.status, 200);
  } finally { globalThis.fetch = original; }
});
