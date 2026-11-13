/** @fileoverview Expiring attempt grants, input binding, tamper rejection and durable replay protection. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import { issueExecutionGrant, verifyExecutionGrant } from "../src/cloudflare/execution_token.js";
import { newAttempt } from "../src/cloudflare/execution_attempt.js";
import { AnstsBodyCell } from "../src/cloudflare/body_cell.js";
import { MockDurableObjectState, CloudflareCelldEnv } from "../src/cloudflare/env.js";
import { bodyServiceToken } from "../src/cloudflare/body_auth.js";
import worker from "../src/cloudflare/worker.js";

it("binds grants to identity and expiry and rejects modified signatures", () => {
  const secret = "a".repeat(64);
  const execution = newAttempt({ tenant_id: "tenant", task_id: "task", attempt_id: "a1", node_id: "host", fencing_token: 1, input_digest: "digest" })!;
  const token = issueExecutionGrant(secret, execution, 100000);
  assert.ok(verifyExecutionGrant(secret, token, execution, 100001));
  assert.equal(verifyExecutionGrant(secret, token, execution, 130000), null);
  assert.equal(verifyExecutionGrant(secret, token, execution, 99999), null);
  assert.equal(verifyExecutionGrant(secret, token, { ...execution, input_digest: "changed" }, 100001), null);
  assert.equal(verifyExecutionGrant(secret, token.slice(0, -1) + (token.endsWith("a") ? "b" : "a"), execution, 100001), null);
});
it("issues grants on start and consumes each nonce once across cell reconstruction", async () => {
  const secret = "b".repeat(64);
  const storage = new MockDurableObjectState("execution:task");
  let cell = new AnstsBodyCell(storage);
  const owner = { tenant_id: "tenant", task_id: "task", attempt_id: "a1", node_id: "host", fencing_token: 1, input_digest: "digest" };
  const env = { ANSTS_BODY_AUTH_SECRET: secret, ANSTS_BODY_CELL: { idFromName: (name: string) => ({ toString: () => name }), get: () => cell } } as unknown as CloudflareCelldEnv;
  const request = (route: string, body: unknown) => worker.fetch(new Request(`https://cell/v1/body/${route}?taskId=task`, { method: "POST", headers: { authorization: `Body ${bodyServiceToken(secret, "host")}`, "x-ansts-body-id": "host" }, body: JSON.stringify(body) }), env);
  await request("acquire", owner);
  const started = await (await request("start", { ...owner, expected_version: 1 })).json();
  const body = { ...owner, execution_token: started.execution_token };
  assert.equal((await request("verify-execution-token", body)).status, 200);
  cell = new AnstsBodyCell(storage);
  assert.equal((await request("verify-execution-token", body)).status, 409);
  assert.equal((await request("consume-grant", { ...owner, nonce: "forged" })).status, 403);
});
