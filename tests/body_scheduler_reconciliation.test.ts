/** @fileoverview Uncertain tasks stop consuming leases and resume only after reconciliation. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell.js";
import { MockDurableObjectState } from "../src/cloudflare/env.js";
import { NodeDaemon } from "../src/node_daemon.js";

it("persists suspended uncertainty and fences the replacement after explicit resolution", async () => {
  const storage = new MockDurableObjectState("scheduler");
  let cell = new AnstsSchedulerCell(storage);
  const post = (route: string, body: unknown) => cell.fetch(new Request(`https://cell/v1/scheduler/${route}`, { method: "POST", body: JSON.stringify(body) }));
  await post("register-node", new NodeDaemon("host").emitHeartbeat());
  await post("submit", { tenant_id: "tenant", task_id: "task", prompt: "work", priority: 2 });
  const tick = await (await post("schedule-tick", {})).json();
  const fence = tick.placements[0][2];
  const lease = { task_id: "task", tenant_id: "tenant", node_id: "host", fencing_token: fence };
  assert.equal((await post("suspend-uncertain", lease)).status, 200);
  assert.equal((await post("lease-renew", lease)).status, 409);
  cell = new AnstsSchedulerCell(storage);
  const tasks = await (await cell.fetch(new Request("https://cell/v1/scheduler/tasks"))).json();
  assert.equal(tasks.tasks[0].state, "SUSPENDED_RECONCILIATION");
  assert.deepEqual((await (await post("schedule-tick", {})).json()).placements, []);
  assert.equal((await post("resolve-uncertain", { ...lease, tenant_id: "other", retry: true })).status, 409);
  assert.equal((await post("resolve-uncertain", { ...lease, retry: true })).status, 200);
  const replacement = await (await post("schedule-tick", {})).json();
  assert.ok(replacement.placements[0][2] > fence);
});
