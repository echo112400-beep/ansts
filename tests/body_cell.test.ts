/** @fileoverview Body cell heartbeat and execution receipt persistence contract. */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import { AnstsBodyCell } from "../src/cloudflare/body_cell.js";
import { MockDurableObjectState } from "../src/cloudflare/env.js";

describe("Body Cell", () => {
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
