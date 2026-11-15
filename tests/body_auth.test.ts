/** @fileoverview Body service authentication and host scope enforced before cell access. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import worker from "../src/cloudflare/worker.js";
import { authenticateBody, authenticateGatewayService, bodyServiceToken } from "../src/cloudflare/body_auth.js";
import { CloudflareCelldEnv } from "../src/cloudflare/env.js";

it("fails closed on absent credentials and prevents host impersonation", () => {
  const secret = "a".repeat(64);
  const headers = { "x-ansts-body-id": "host-a", authorization: `Body ${bodyServiceToken(secret, "host-a")}` };
  assert.equal(authenticateBody(new Request("https://cell", { headers }), secret), "host-a");
  assert.equal(authenticateBody(new Request("https://cell", { headers })), null);
  assert.equal(authenticateBody(new Request("https://cell", { headers: { ...headers, "x-ansts-body-id": "host-b" } }), secret), null);
});
it("accepts bounded old service credentials during rollover and rejects them after expiry", () => {
  const current = "c".repeat(64), old = "d".repeat(64), now = Math.floor(Date.now() / 1000);
  const config = { ANSTS_BODY_AUTH_SECRET: current, ANSTS_BODY_AUTH_PREVIOUS_SECRET: old, ANSTS_BODY_AUTH_PREVIOUS_UNTIL: String(now + 600) };
  const request = new Request("https://cell", { headers: { authorization: `Gateway ${bodyServiceToken(old, "gateway:control")}` } });
  assert.equal(authenticateGatewayService(request, config), true);
  assert.equal(authenticateGatewayService(request, { ...config, ANSTS_BODY_AUTH_PREVIOUS_UNTIL: String(now - 1) }), false);
  assert.equal(authenticateGatewayService(request, { ...config, ANSTS_BODY_AUTH_PREVIOUS_UNTIL: String(now + 3600) }), false);
});
it("rejects forged host payloads before reading a Durable Object", async () => {
  const secret = "b".repeat(64);
  let calls = 0;
  const env = {
    ANSTS_BODY_AUTH_SECRET: secret,
    ANSTS_BODY_CELL: {
      idFromName: (name: string) => ({ toString: () => name }),
      get: () => { calls++; throw new Error("must not reach storage"); },
    },
  } as unknown as CloudflareCelldEnv;
  const headers = { "x-ansts-body-id": "host-a", authorization: `Body ${bodyServiceToken(secret, "host-a")}` };
  assert.equal((await worker.fetch(new Request("https://cell/v1/body/state"), env)).status, 401);
  assert.equal((await worker.fetch(new Request("https://cell/v1/body/state?hostId=host-b", { headers }), env)).status, 403);
  assert.equal((await worker.fetch(new Request("https://cell/v1/body/acquire?taskId=task", { method: "POST", headers, body: JSON.stringify({ node_id: "host-b", task_id: "task" }) }), env)).status, 403);
  assert.equal(calls, 0);
  assert.equal((await worker.fetch(new Request("https://cell/v1/scheduler/complete", { method: "POST", body: "{}" }), env)).status, 401);
  assert.equal((await worker.fetch(new Request("https://cell/v1/scheduler/register-node", { method: "POST", headers, body: JSON.stringify({ node_id: "host-b" }) }), env)).status, 403);
});
