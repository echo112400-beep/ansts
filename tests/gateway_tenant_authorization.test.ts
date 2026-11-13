/** @fileoverview User identity, role and tenant scope on every public Gateway data path. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import gatewayWorker from "../src/cloudflare/gateway_worker.js";
import { AnstsGatewayCell } from "../src/cloudflare/gateway_cell.js";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell.js";
import { MockDurableObjectState } from "../src/cloudflare/env.js";
import { ConsoleRbacAuthGuard, ConsoleRole } from "../src/dashboard/console_rbac_guard.js";
import { gatewayPrincipal } from "../src/cloudflare/gateway_authorization.js";

it("allows only bounded pre-rotation reads with the previous key", () => {
  const oldSecret = "old-user-signing-key-at-least-32-chars";
  const newSecret = "new-user-signing-key-at-least-32-chars";
  const now = Math.floor(Date.now() / 1000);
  const token = new ConsoleRbacAuthGuard(oldSecret).issueToken({ sub: "reader", role: "VIEWER", scopeTenant: "a", nowSec: now - 60, ttlSec: 1800 });
  const headers = { authorization: `Bearer ${token}` };
  assert.equal(gatewayPrincipal(new Request("https://gateway/v1/tasks/t", { headers }), newSecret, oldSecret, now + 840)?.scopeTenant, "a");
  assert.equal(gatewayPrincipal(new Request("https://gateway/v1/tasks", { method: "POST", headers }), newSecret, oldSecret, now + 840), null);
  assert.equal(gatewayPrincipal(new Request("https://gateway/v1/tasks/t", { headers }), newSecret, oldSecret, now - 1), null);
  const forgedAfterRotation = new ConsoleRbacAuthGuard(oldSecret).issueToken({ sub: "reader", role: "VIEWER", scopeTenant: "a", nowSec: now, ttlSec: 1800 });
  assert.equal(gatewayPrincipal(new Request("https://gateway/v1/tasks/t", { headers: { authorization: `Bearer ${forgedAfterRotation}` } }), newSecret, oldSecret, now + 840), null);
});

it("rejects anonymous, expired, viewer writes and cross-tenant operations; filters reads", async () => {
  const secret = "gateway-user-test-secret-at-least-32-chars";
  const guard = new ConsoleRbacAuthGuard(secret);
  const gateway = new AnstsGatewayCell(new MockDurableObjectState("gateway"));
  const scheduler = new AnstsSchedulerCell(new MockDurableObjectState("scheduler"));
  const env = { ANSTS_USER_AUTH_SECRET: secret, ANSTS_SCHEDULER_URL: "https://scheduler", ANSTS_GATEWAY_CELL: { idFromName: (name: string) => ({ toString: () => name }), get: () => gateway } } as never;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init) => scheduler.fetch(new Request(input, init));
  const token = (tenant: string, role: ConsoleRole) => `Bearer ${guard.issueToken({ sub: "test-user", role, scopeTenant: tenant, ttlSec: 900 })}`;
  const request = (route: string, auth?: string, body?: unknown) => gatewayWorker.fetch(new Request(`https://gateway${route}`, { method: body ? "POST" : "GET", headers: auth ? { authorization: auth } : {}, body: body ? JSON.stringify(body) : undefined }), env);
  try {
    assert.equal((await request("/v1/tasks/same-name")).status, 401);
    const body = (tenant: string) => ({ task_id: "same-name", tenant_id: tenant, prompt: "same work" });
    assert.equal((await request("/v1/tasks", token("a", "VIEWER"), body("a"))).status, 403);
    assert.equal((await request("/v1/tasks", token("a", "OPERATOR"), body("b"))).status, 403);
    for (const tenant of ["a", "b"]) assert.equal((await request("/v1/tasks", token(tenant, "OPERATOR"), body(tenant))).status, 202);
    const me = await request("/v1/auth/me", token("a", "VIEWER"));
    assert.equal(me.status, 200);
    assert.equal(me.headers.get("cache-control"), "no-store");
    const identity = await me.json();
    assert.equal(identity.user.scopeTenant, "a");
    assert.equal(identity.token, undefined);
    const consoleTasks = await (await request("/v1/console/tasks", token("a", "VIEWER"))).json();
    assert.equal(consoleTasks.total, 1);
    assert.equal(consoleTasks.tasks[0].tenant_id, "a");
    assert.equal((await request("/v1/console/tasks?tenantId=b", token("a", "VIEWER"))).status, 403);
    assert.equal((await request("/v1/tasks/same-name?tenantId=b", token("a", "VIEWER"))).status, 403);
    const task = await (await request("/v1/tasks/same-name", token("a", "VIEWER"))).json();
    assert.equal(task.tenant_id, "a");
    const state = await (await request("/v1beta1/gateway/state", token("a", "VIEWER"))).json();
    assert.equal(state.dispatches.length, 1);
    assert.equal(state.dispatches[0].tenant_id, "a");
    assert.deepEqual(state.sessions, []);
    const overview = await (await request("/v1beta1/console/gateway", token("a", "VIEWER"))).json();
    assert.equal(overview.totalTasks, 1);
    const expired = `Bearer ${guard.issueToken({ sub: "expired", role: "VIEWER", scopeTenant: "a", nowSec: 1000, ttlSec: 1 })}`;
    assert.equal((await request("/v1/tasks/same-name", expired)).status, 401);
    assert.equal((await request("/health")).status, 200);
  } finally { globalThis.fetch = originalFetch; }
});
