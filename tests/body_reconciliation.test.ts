/** @fileoverview Operator reconciliation evidence, CAS, immutable results and durable audit. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import { AnstsBodyCell } from "../src/cloudflare/body_cell.js";
import { MockDurableObjectState } from "../src/cloudflare/env.js";
import { executionDigest } from "../src/cloudflare/execution_attempt.js";
import { authenticateBodyOperator, bodyServiceToken } from "../src/cloudflare/body_auth.js";
import worker from "../src/cloudflare/worker.js";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell.js";
import { CloudflareCelldEnv } from "../src/cloudflare/env.js";
import { NodeDaemon } from "../src/node_daemon.js";

it("requires operator credentials distinct from host service credentials", () => {
  const secret = "c".repeat(64);
  assert.equal(authenticateBodyOperator(new Request("https://cell", { headers: { authorization: `Body ${bodyServiceToken(secret, "host")}` } }), secret), false);
  assert.equal(authenticateBodyOperator(new Request("https://cell", { headers: { authorization: `Operator ${bodyServiceToken(secret, "operator:reconcile")}` } }), secret), true);
});
it("reconciles uncertainty with evidence and version checks, preserving hash-linked audit", async () => {
  const state = new MockDurableObjectState("execution:task");
  const cell = new AnstsBodyCell(state);
  const post = (route: string, body: unknown, operator = false) => cell.fetch(new Request(`https://cell/v1/body/${route}`, {
    method: "POST", body: JSON.stringify(body), headers: operator ? { "x-ansts-verified-operator": "operator" } : {},
  }));
  const owner = { tenant_id: "tenant", task_id: "task", attempt_id: "a1", input_digest: "input", node_id: "host", fencing_token: 1 };
  await post("acquire", owner);
  await post("uncertain", { ...owner, expected_version: 1 });
  const review = { ...owner, expected_version: 2, action: "retain_block", evidence: "log:attempt-a1", reason: "No completion evidence" };
  assert.equal((await post("reconcile", review)).status, 401);
  assert.equal((await post("reconcile", { ...review, evidence: "" }, true)).status, 400);
  assert.equal((await post("reconcile", { ...review, expected_version: 1 }, true)).status, 409);
  await post("reconcile", review, true);
  const response = await post("reconcile", { ...review, expected_version: 3, action: "confirm_not_started", replacement: { ...owner, attempt_id: "a2", fencing_token: 2 } }, true);
  assert.equal(response.status, 200);
  const recovered = await (await new AnstsBodyCell(state).fetch(new Request("https://cell/v1/body/state"))).json();
  assert.equal(recovered.execution.phase, "ADMITTED");
  assert.equal(recovered.execution.attempt_id, "a2");
  assert.equal(recovered.execution.version, 4);
  const [first, second] = recovered.execution.audit;
  assert.equal(second.previous_hash, first.hash);
  const { hash, ...payload } = second;
  assert.equal(hash, executionDigest(payload));
  assert.equal((await post("start", { ...owner, expected_version: 4 })).status, 409);
});
it("coordinates operator-confirmed completion with scheduler suspension and durable acknowledgement", async () => {
  const scheduler = new AnstsSchedulerCell(new MockDurableObjectState("scheduler"));
  const body = new AnstsBodyCell(new MockDurableObjectState("execution:task"));
  const post = (cell: { fetch(request: Request): Promise<Response> }, prefix: string, route: string, payload: unknown) => cell.fetch(new Request(`https://cell/v1/${prefix}/${route}`, { method: "POST", body: JSON.stringify(payload) }));
  await post(scheduler, "scheduler", "register-node", new NodeDaemon("host").emitHeartbeat());
  await post(scheduler, "scheduler", "submit", { tenant_id: "tenant", task_id: "task", prompt: "work" });
  const placement = await (await post(scheduler, "scheduler", "schedule-tick", {})).json();
  const owner = { tenant_id: "tenant", task_id: "task", node_id: "host", fencing_token: placement.placements[0][2], input_digest: "input", attempt_id: "a1" };
  await post(body, "body", "acquire", owner);
  await post(body, "body", "uncertain", { ...owner, expected_version: 1 });
  const secret = "d".repeat(64);
  const namespace = (cell: { fetch(request: Request): Promise<Response> }) => ({ idFromName: (name: string) => ({ toString: () => name }), get: () => cell });
  const env = { ANSTS_BODY_AUTH_SECRET: secret, ANSTS_BODY_CELL: namespace(body), ANSTS_SCHEDULER_CELL: namespace(scheduler) } as unknown as CloudflareCelldEnv;
  const review = { ...owner, expected_version: 2, action: "confirm_completed", evidence: "committed artifact verified", reason: "recovery test", result: { status: "COMPLETED" } };
  const request = () => new Request("https://cell/v1/body/reconcile?taskId=task", { method: "POST", body: JSON.stringify(review), headers: { authorization: `Operator ${bodyServiceToken(secret, "operator:reconcile")}` } });
  const response = await worker.fetch(request(), env);
  assert.equal(response.status, 200);
  assert.equal((await response.json()).execution.phase, "COMPLETED");
  assert.equal((await worker.fetch(request(), env)).status, 200);
  const tasks = await (await scheduler.fetch(new Request("https://cell/v1/scheduler/tasks"))).json();
  assert.equal(tasks.tasks[0].state, "COMPLETED");
});
