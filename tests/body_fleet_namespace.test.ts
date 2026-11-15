import { it } from "node:test";
import * as assert from "node:assert/strict";
import { handleBodyRequest } from "../src/cloudflare/body_worker_routes.js";
import { AnstsBodyCell } from "../src/cloudflare/body_cell.js";
import { MockDurableObjectState } from "../src/cloudflare/env.js";
import { bodyServiceToken } from "../src/cloudflare/body_auth.js";
import worker from "../src/cloudflare/worker.js";
it("isolates migrated Body cells in a new namespace and preserves exact imported evidence", async () => {
  const secret = "f".repeat(64), cells = new Map<string, AnstsBodyCell>();
  const env = { ANSTS_BODY_AUTH_SECRET: secret, ANSTS_BODY_NAMESPACE_PREFIX: "fleet-v2:", ANSTS_BODY_CELL: { idFromName: (name: string) => ({ toString: () => name }), get: (id: { toString(): string }) => {
    const name = id.toString(); if (!cells.has(name)) cells.set(name, new AnstsBodyCell(new MockDurableObjectState(name)));
    return { id, fetch: (request: string | URL | Request) => cells.get(name)!.fetch(new Request(request)) };
  } } };
  const headers = { authorization: `Operator ${bodyServiceToken(secret, "operator:reconcile")}` };
  const snapshot = { schema_version: 1, rows: [{ id: "heartbeat", payload: JSON.stringify({ node_id: "host", healthy: false, updated_at: 1 }) }] };
  assert.equal((await handleBodyRequest(new Request("https://body/v1/body/import?hostId=host", { method: "POST", headers, body: JSON.stringify(snapshot) }), env)).status, 200);
  const exported = await handleBodyRequest(new Request("https://body/v1/body/export?hostId=host", { headers }), env);
  assert.deepEqual(await exported.json(), snapshot);
  assert.deepEqual([...cells.keys()], ["fleet-v2:host"]);
  const lease = { task_id: "task", tenant_id: "tenant", node_id: "host", fencing_token: 1, attempt_id: "attempt", input_digest: "digest" };
  const execution = new AnstsBodyCell(new MockDurableObjectState("fleet-v2:execution:task"));
  cells.set("fleet-v2:execution:task", execution);
  for (const [route, expected_version] of [["acquire", 0], ["start", 1], ["uncertain", 2]] as const) assert.equal((await execution.fetch(new Request(`https://cell/v1/body/${route}`, { method: "POST", body: JSON.stringify({ ...lease, expected_version }) }))).status, 200);
  const scheduler = { id: { toString: () => "scheduler" }, fetch: async (request: Request) => new URL(request.url).pathname.endsWith("/tasks") ? Response.json({ tasks: [{ ...lease, assigned_node_id: "host" }] }) : Response.json({ suspended: true }) };
  const result = await worker.fetch(new Request("https://brain/v1/scheduler/suspend-uncertain", { method: "POST", headers: { authorization: `Body ${bodyServiceToken(secret, "host")}`, "x-ansts-body-id": "host" }, body: JSON.stringify(lease) }), { ...env, ANSTS_SCHEDULER_CELL: { idFromName: () => scheduler.id, get: () => scheduler } } as never);
  assert.equal(result.status, 200, 'suspension must consult the migrated namespace');
});
