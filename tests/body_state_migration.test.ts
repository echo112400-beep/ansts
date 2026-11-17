/** @fileoverview Migration preserves attempts and consumed grants and resumes after partial import. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import { AnstsBodyCell } from "../src/cloudflare/body_cell.js";
import { MockDurableObjectState } from "../src/cloudflare/env.js";

it("preserves versioned state and consumed grants and never overwrites an initialized target", async () => {
  const source = new AnstsBodyCell(new MockDurableObjectState("old"));
  const owner = { tenant_id: "tenant", task_id: "task", attempt_id: "a1", node_id: "host", input_digest: "input", fencing_token: 1 };
  const post = (cell: AnstsBodyCell, route: string, payload: unknown, headers: Record<string, string> = {}) => cell.fetch(new Request(`https://cell/v1/body/${route}`, { method: "POST", headers, body: JSON.stringify(payload) }));
  await post(source, "acquire", owner);
  await post(source, "start", { ...owner, expected_version: 1 });
  await post(source, "consume-grant", { ...owner, nonce: "used-nonce" }, { "x-ansts-verified-grant": "true" });
  const snapshot = await (await source.fetch(new Request("https://cell/v1/body/export", { headers: { "x-ansts-verified-operator": "migration" } }))).json();
  const target = new AnstsBodyCell(new MockDurableObjectState("new"));
  assert.equal((await post(target, "import", snapshot)).status, 401);
  assert.equal((await post(target, "import", snapshot, { "x-ansts-verified-operator": "migration" })).status, 200);
  const state = await (await target.fetch(new Request("https://cell/v1/body/state"))).json();
  assert.equal(state.execution.phase, "RUNNING");
  assert.equal(state.execution.version, 2);
  assert.equal((await post(target, "consume-grant", { ...owner, nonce: "used-nonce" }, { "x-ansts-verified-grant": "true" })).status, 409);
  const again = await (await post(target, "import", { schema_version: 1, rows: [] }, { "x-ansts-verified-operator": "migration" })).json();
  assert.equal(again.already_initialized, true);
});
it("finishes an interrupted snapshot materialization before serving reads", async () => {
  const storage = new MockDurableObjectState("interrupted");
  storage.storage.sql.exec("CREATE TABLE IF NOT EXISTS body_state (id TEXT PRIMARY KEY, payload TEXT)");
  const rows = [{ id: "heartbeat", payload: JSON.stringify({ node_id: "host", updated_at: Date.now() }) }, { id: "receipt:task:1", payload: JSON.stringify({ task_id: "task", fencing_token: 1, result: { status: "COMPLETED" } }) }];
  storage.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", "__migration_pending", JSON.stringify(rows));
  storage.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", rows[0].id, rows[0].payload);
  const cell = new AnstsBodyCell(storage);
  const state = await (await cell.fetch(new Request("https://cell/v1/body/state"))).json();
  assert.equal(state.heartbeat.node_id, "host");
  assert.equal(state.receipts.length, 1);
  assert.equal(storage.storage.sql.exec("SELECT * FROM body_state WHERE id = ?", "__migration_pending").toArray().length, 0);
});
