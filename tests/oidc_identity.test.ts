import { it } from "node:test";
import * as assert from "node:assert/strict";
import { oidcPrincipal } from "../src/cloudflare/oidc_identity.js";
it("verifies issuer-pinned RSA identities and rejects wrong audience, expiry, forged roles and key confusion", async () => {
  const keys = await crypto.subtle.generateKey({ name: "RSASSA-PKCS1-v1_5", modulusLength: 2048, publicExponent: new Uint8Array([1, 0, 1]), hash: "SHA-256" }, true, ["sign", "verify"]);
  const jwk = { ...await crypto.subtle.exportKey("jwk", keys.publicKey), kid: "key-1", alg: "RS256" };
  const config = { ANSTS_OIDC_ISSUER: "https://identity.example", ANSTS_OIDC_AUDIENCE: "ansts", ANSTS_OIDC_JWKS_URL: "https://identity.example/jwks" };
  const now = Math.floor(Date.now() / 1000), original = globalThis.fetch;
  globalThis.fetch = async () => Response.json({ keys: [jwk] });
  const encode = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const request = async (patch = {}, headerPatch = {}) => {
    const header = encode({ alg: "RS256", kid: "key-1", ...headerPatch });
    const payload = encode({ sub: "operator-1", role: "OPERATOR", scopeTenant: "tenant", iss: config.ANSTS_OIDC_ISSUER, aud: "ansts", iat: now, exp: now + 300, ...patch });
    const signature = await crypto.subtle.sign("RSASSA-PKCS1-v1_5", keys.privateKey, new TextEncoder().encode(header + "." + payload));
    return new Request("https://gateway", { headers: { authorization: `Bearer ${header}.${payload}.${Buffer.from(signature).toString("base64url")}` } });
  };
  try {
    assert.equal((await oidcPrincipal(await request(), config))?.sub, "operator-1");
    for (const patch of [{ aud: "other" }, { iss: "https://other" }, { exp: now - 1 }, { role: "ROOT" }, { nbf: now + 600 }]) assert.equal(await oidcPrincipal(await request(patch), config), null);
    assert.equal(await oidcPrincipal(await request({}, { alg: "HS256" }), config), null);
    assert.equal(await oidcPrincipal(await request({}, { jku: "https://attacker" }), config), null);
    globalThis.fetch = async () => Response.json({ keys: [] });
    assert.equal(await oidcPrincipal(await request(), config), null);
  } finally { globalThis.fetch = original; }
});
