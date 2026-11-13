/** @fileoverview Body cell heartbeat and execution receipt persistence contract. */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import { AnstsBodyCell } from "../src/cloudflare/body_cell.js";
import { MockDurableObjectState } from "../src/cloudflare/env.js";

describe("Body Cell", () => {
  it("prevents another host from replaying an uncertain execution and rejects stale completion", async () => {
    const cell = new AnstsBodyCell(new MockDurableObjectState("execution:task"));
    const post = (path: string, body: unknown) => cell.fetch(new Request(`https://cell/v1/body/${path}`, { method: "POST", body: JSON.stringify(body) }));
    const owner = { tenant_id: "tenant", task_id: "task", attempt_id: "attempt-1", input_digest: "input", node_id: "host-a", fencing_token: 1 };
    assert.equal((await post("acquire", owner)).status, 200);
    assert.equal((await post("acquire", { ...owner, node_id: "host-b", fencing_token: 2 })).status, 409);
    assert.equal((await post("finish", { ...owner, fencing_token: 2, result: {} })).status, 409);
    await post("start", { ...owner, expected_version: 1 });
    await post("finish", { ...owner, expected_version: 2, result: { status: "COMPLETED" } });
    const recovered = await (await post("acquire", { ...owner, node_id: "host-b", fencing_token: 2 })).json();
    assert.equal(recovered.acquired, false);
    assert.equal(recovered.result.status, "COMPLETED");
  });
  it("preserves immutable results, rejects stale versions and exposes execution phase", async () => {
    const state = new MockDurableObjectState("execution:versioned");
    const cell = new AnstsBodyCell(state);
    const post = (path: string, body: unknown) => cell.fetch(new Request(`https://cell/v1/body/${path}`, { method: "POST", body: JSON.stringify(body) }));
    const owner = { tenant_id: "tenant", task_id: "task", attempt_id: "attempt", input_digest: "digest", node_id: "host", fencing_token: 1 };
    await post("acquire", owner);
    assert.equal((await post("start", { ...owner, expected_version: 0 })).status, 409);
    await post("start", { ...owner, expected_version: 1 });
    assert.equal((await post("acquire", { ...owner, tenant_id: "another" })).status, 409);
    await post("finish", { ...owner, expected_version: 2, result: { a: 1, b: 2 } });
    assert.equal((await post("finish", { ...owner, expected_version: 2, result: { b: 2, a: 1 } })).status, 200);
    assert.equal((await post("finish", { ...owner, expected_version: 3, result: { a: 99 } })).status, 409);
    await post("acknowledge", { ...owner, expected_version: 3 });
    const result = await (await new AnstsBodyCell(state).fetch(new Request("https://cell/v1/body/state"))).json();
    assert.equal(result.execution.phase, "COMPLETED");
    assert.equal(result.execution.version, 4);
    assert.deepEqual(result.execution.result, { a: 1, b: 2 });
  });
  it("migrates legacy admission to uncertain and rejects unknown write routes", async () => {
    const state = new MockDurableObjectState("legacy");
    state.storage.sql.exec("CREATE TABLE IF NOT EXISTS body_state (id TEXT PRIMARY KEY, payload TEXT)");
    state.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", "execution", JSON.stringify({ task_id: "task", node_id: "host", fencing_token: 1 }));
    const cell = new AnstsBodyCell(state);
    const response = await (await cell.fetch(new Request("https://cell/v1/body/state"))).json();
    assert.equal(response.execution.phase, "UNCERTAIN");
    assert.equal(response.execution.reason, "LEGACY_RECORD_REQUIRES_RECONCILIATION");
    assert.equal((await cell.fetch(new Request("https://cell/v1/body/typo", { method: "POST", body: "{}" }))).status, 404);
    assert.equal((await cell.fetch(new Request("https://cell/v1/body/acquire"))).status, 405);
    assert.equal((await cell.fetch(new Request("https://cell/v1/body/heartbeat", { method: "POST", body: "{" }))).status, 400);
  });
  it("keeps heartbeat and fenced receipts across cell reconstruction", async () => {
    const state = new MockDurableObjectState("body-01");
    const cell = new AnstsBodyCell(state);
    const post = (path: string, body: unknown) => cell.fetch(new Request(`https://cell/v1/body/${path}`, { method: "POST", body: JSON.stringify(body) }));
    await post("heartbeat", { node_id: "body-01" });
    await post("receipt", { task_id: "task-1", fencing_token: 1, result: { status: "COMPLETED" } });
    await post("receipt", { task_id: "task-1", fencing_token: 2, result: { status: "COMPLETED" } });
    const restored = new AnstsBodyCell(state);
    const result = await (await restored.fetch(new Request("https://cell/v1/body/state"))).json();
    assert.equal(result.online, true);
    assert.equal(result.receipts.length, 2);
    assert.equal((await post("receipt", { task_id: "bad" })).status, 400);
  });
});
