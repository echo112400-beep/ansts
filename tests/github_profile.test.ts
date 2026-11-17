import { it } from "node:test";
import * as assert from "node:assert/strict";
import gatewayWorker from "../src/cloudflare/gateway_worker.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import {
  githubProfileForSubject,
  rememberGithubProfile,
} from "../src/cloudflare/github_profile.js";
it("loads only the authenticated GitHub identity, caches public presentation fields, and preserves application permissions", async () => {
  const secret = "github-profile-test-secret-at-least-32-chars";
  const jwt = new ConsoleRbacAuthGuard(secret).issueToken({
    sub: "github:44556677",
    role: "VIEWER",
    scopeTenant: "tenant-profile",
    ttlSec: 900,
  });
  const env = { ANSTS_USER_AUTH_SECRET: secret } as never;
  const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async (input, init) => {
    calls++;
    assert.equal(String(input), "https://api.github.com/user/44556677");
    assert.equal(new Headers(init?.headers).has("authorization"), false);
    return Response.json({
      id: 44556677,
      login: "profile-user",
      name: "Profile Name",
      bio: "Public biography",
      avatar_url: "https://avatars.githubusercontent.com/u/44556677?v=4",
      html_url: "https://attacker.example",
      email: "private@example.com",
      role: "SRE_ADMIN",
      created_at: "2020-01-01T00:00:00Z",
    });
  };
  const request = (route: string, auth = true) =>
    gatewayWorker.fetch(
      new Request("https://console.example" + route, {
        headers: auth ? { authorization: `Bearer ${jwt}` } : {},
      }),
      env,
    );
  try {
    assert.equal((await request("/v1/auth/profile", false)).status, 401);
    assert.equal(calls, 0);
    const response = await request("/v1/auth/profile?userId=123");
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("cache-control"), "no-store");
    const { profile } = await response.json();
    assert.equal(profile.name, "Profile Name");
    assert.equal(profile.url, "https://github.com/profile-user");
    assert.equal(profile.email, undefined);
    assert.equal(profile.role, undefined);
    assert.deepEqual(
      (await (await request("/v1/auth/profile")).json()).profile,
      profile,
    );
    assert.equal(calls, 1);
    const identity = await (await request("/v1/auth/me")).json();
    assert.equal(identity.user.role, "VIEWER");
    assert.equal(identity.user.scopeTenant, "tenant-profile");
  } finally {
    globalThis.fetch = original;
  }
});
it("rejects provider identity mismatches and oversized responses; credential users make no GitHub request", async () => {
  const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    return Response.json({ id: 99887766, login: "wrong-user" });
  };
  try {
    assert.equal(await githubProfileForSubject("development-operator"), null);
    assert.equal(await githubProfileForSubject("github:../../etc"), null);
    assert.equal(calls, 0);
    await assert.rejects(
      githubProfileForSubject("github:99887765"),
      /ID_MISMATCH/,
    );
    globalThis.fetch = async () =>
      Response.json({
        id: 99887765,
        login: "large-user",
        bio: "x".repeat(65536),
      });
    await assert.rejects(
      githubProfileForSubject("github:99887765"),
      /TOO_LARGE/,
    );
    const sanitized = rememberGithubProfile({
      id: 99887764,
      login: "safe-user",
      avatar_url: "https://attacker.example/avatar",
      name: "Safe\u0000 Name",
    });
    assert.equal(sanitized.avatarUrl, null);
    assert.equal(sanitized.name, "Safe Name");
  } finally {
    globalThis.fetch = original;
  }
});

it("serves the signed login snapshot without provider calls and bounds UTF-8 fields to fit a cookie", async () => {
  const original = globalThis.fetch;
  globalThis.fetch = async () => {
    throw new Error("provider rate limited");
  };
  const secret = "github-profile-snapshot-secret-at-least-32-chars";
  const profile = rememberGithubProfile({
    id: 44556679,
    login: "snapshot-user",
    name: "姓".repeat(200),
    bio: "简".repeat(600),
    company: "公".repeat(300),
    location: "地".repeat(300),
    avatar_url: "https://avatars.githubusercontent.com/u/44556679?v=4",
  });
  const jwt = new ConsoleRbacAuthGuard(secret).issueToken({
    sub: "github:44556679",
    role: "VIEWER",
    scopeTenant: "租".repeat(128),
    ttlSec: 900,
    profile,
  });
  assert.ok(Buffer.byteLength(jwt) < 3800);
  try {
    const response = await gatewayWorker.fetch(
      new Request("https://console.example/v1/auth/profile", {
        headers: { authorization: `Bearer ${jwt}` },
      }),
      { ANSTS_USER_AUTH_SECRET: secret } as never,
    );
    assert.equal(response.status, 200);
    assert.equal((await response.json()).profile.login, "snapshot-user");
  } finally {
    globalThis.fetch = original;
  }
});
