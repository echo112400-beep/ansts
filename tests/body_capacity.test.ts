import { it } from "node:test";
import * as assert from "node:assert/strict";
import { BodyCapacity } from "../src/runtime/body_capacity.js";
it("never oversells slots or releases capacity twice", () => {
  const capacity = new BodyCapacity(2);
  assert.equal(capacity.reserve("first"), true);
  assert.equal(capacity.reserve("first"), false);
  assert.equal(capacity.reserve("second"), true);
  assert.equal(capacity.reserve("third"), false);
  assert.equal(capacity.release("first"), true);
  assert.equal(capacity.release("first"), false);
  assert.equal(capacity.reserve("third"), true);
  assert.deepEqual(capacity.snapshot(), { slots: 2, reserved: 2, available: 0 });
});
it("rejects invalid slot configuration", () => {
  for (const slots of [0, -1, 1.5, NaN, 257]) assert.throws(() => new BodyCapacity(slots));
});
import { bodyResourceBudget, cgroupResourceLimits } from "../src/runtime/body_resource_budget.js";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell.js";
import { MockDurableObjectState } from "../src/cloudflare/env.js";
import { NodeDaemon } from "../src/node_daemon.js";
it("bounds configured budgets by runtime limits and rejects invalid budgets", () => {
  assert.deepEqual(bodyResourceBudget(2, 4096, "32", "65536"), { cpu: 2, memoryMb: 4096 });
  assert.deepEqual(bodyResourceBudget(8, 8192, "1.5", "2048"), { cpu: 1.5, memoryMb: 2048 });
  assert.throws(() => bodyResourceBudget(2, 4096, "NaN"));
  assert.throws(() => bodyResourceBudget(2, 4096, undefined, "0"));
});
it("retains lease slot reservations across idle heartbeats", async () => {
  const cell = new AnstsSchedulerCell(new MockDurableObjectState("slots"));
  const post = (route: string, body: unknown) => cell.fetch(new Request(`https://cell/v1/scheduler/${route}`, { method: "POST", body: JSON.stringify(body) }));
  const heartbeat = { ...new NodeDaemon("host").emitHeartbeat(0), execution_slots: 1 };
  await post("register-node", heartbeat);
  for (const task of ["one", "two"]) await post("submit", { tenant_id: "tenant", task_id: task, prompt: "work", priority: 2 });
  const first = await (await post("schedule-tick", {})).json();
  assert.equal(first.placements.length, 1);
  await post("register-node", heartbeat);
  assert.deepEqual((await (await post("schedule-tick", {})).json()).placements, []);
  const [task_id, node_id, fencing_token] = first.placements[0];
  assert.equal((await post("complete", { task_id, tenant_id: "tenant", node_id, fencing_token })).status, 200);
  assert.equal((await (await post("schedule-tick", {})).json()).placements.length, 1);
});
it("detects fractional cgroup v2 quotas and v1 limits without treating unlimited as zero", () => {
  const detect = (files: Record<string, string>) => cgroupResourceLimits(file => files[file]);
  assert.deepEqual(detect({ "/sys/fs/cgroup/cpu.max": "150000 100000", "/sys/fs/cgroup/memory.max": "536870912" }), { cpu: 1.5, memoryBytes: 536870912 });
  assert.deepEqual(detect({ "/sys/fs/cgroup/cpu/cpu.cfs_quota_us": "50000", "/sys/fs/cgroup/cpu/cpu.cfs_period_us": "100000" }), { cpu: 0.5, memoryBytes: undefined });
  for (const cpu of ["max 100000", "-1 100000", "broken", "10000 0"]) assert.equal(detect({ "/sys/fs/cgroup/cpu.max": cpu }).cpu, undefined);
  assert.deepEqual(detect({}), { cpu: undefined, memoryBytes: undefined });
});