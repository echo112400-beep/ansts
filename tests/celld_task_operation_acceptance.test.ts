import { strict as assert } from "node:assert";
import { test } from "node:test";
import worker from "../src/cloudflare/worker";
import { MockDurableObjectState } from "../src/cloudflare/env";
import { AgentTaskHarnessCell } from "../src/cloudflare/harness_cell";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell";
import { NodeCapacityCell } from "../src/cloudflare/node_capacity_cell";
import { ToolEffectClass } from "../src/models";

async function runningTask(executor: { fetch(request: Request): Promise<Response> }) {
  const taskId = "task-operation-acceptance";
  const tenantId = "tenant-operation-acceptance";
  const env: any = { TASK_OPERATION_EXECUTOR: executor, ANSTS_CHECKPOINTS_R2: {},
    PRIMARY_MODEL_POOL: "DeepSeek", FAST_UTILITY_POOL: "GLM-5.3", FALLBACK_MODEL_POOL: "Qwen-3.8" };
  function namespace(Cell: any) {
    const owners = new Map<string, { state: MockDurableObjectState; cell: any }>();
    return { idFromName: (name: string) => ({ name, toString: () => name }), get(id: any) {
      if (!owners.has(id.name)) {
        const state = new MockDurableObjectState(id.name);
        owners.set(id.name, { state, cell: new Cell(state, env) });
      }
      return { fetch: (input: RequestInfo, init?: RequestInit) => owners.get(id.name)!.cell.fetch(new Request(input, init)) };
    }, reactivate() {
      for (const owner of owners.values()) owner.cell = new Cell(owner.state, env);
    } };
  }
  env.SCHEDULER_CELL = namespace(AnstsSchedulerCell);
  env.AGENT_HARNESS_CELL = namespace(AgentTaskHarnessCell);
  env.NODE_CAPACITY_CELL = namespace(NodeCapacityCell);
  const request = (path: string, body?: unknown) => worker.fetch(new Request(`https://ansts${path}`, {
    method: body === undefined ? "GET" : "POST", headers: { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  }), env);
  const scheduled = await request("/v1beta1/scheduler/tick", {
    nodes: [{ node_id: "node", incarnation: "boot", capacity: { cpu: 4, memory_mb: 2048, burst_cpu: 2, sessions: 2, tps: 100 } }],
    candidates: [{ task_id: taskId, tenant_id: tenantId, dispatch_id: "dispatch", node_id: "node", node_incarnation: "boot",
      resources: { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 },
      task_spec: { spec: { tools_and_saga_config: { transactional_policies: [
        { tool_name: "orders/create", effect_class: ToolEffectClass.SAGA_COMPENSATABLE, compensation_tool: "orders/cancel" },
        { tool_name: "deploy/promote", effect_class: ToolEffectClass.STAGED_COMMIT, require_human_approval: true },
      ] } } } }],
  });
  assert.equal(scheduled.status, 200, await scheduled.clone().text());
  const dispatch = ((await scheduled.json()) as any).dispatches[0];
  assert.equal(dispatch.stage, "AUTHORIZED");
  const started = await request("/v1beta1/scheduler/started", { ...dispatch, start_receipt: "node-start-receipt" });
  assert.equal(started.status, 200, await started.clone().text());
  const base = `/v1beta1/tasks/${taskId}`;
  const operation = (id = "call-create") => request(`${base}/operations?tenant_id=${tenantId}`, {
    tool_call_id: id, tool_name: "orders/create", args: { order_id: "order-1" }, epoch: 1, turn_seq: 1,
  });
  const compensate = () => request(`${base}/compensations/run?tenant_id=${tenantId}`, {
    epoch: 1, turn_seq: 1, after_turn_exclusive: 0,
  });
  return { env, request, operation, compensate, base, tenantId, reactivate: () => env.AGENT_HARNESS_CELL.reactivate() };
}

test("failed authoritative lookup cannot repeat an uncertain external effect after reactivation", async () => {
  let effects = 0;
  const fixture = await runningTask({ async fetch(request) {
    if (request.method === "POST") { effects += 1; throw new Error("accepted but response lost"); }
    return new Response(JSON.stringify({ status: "NOT_FOUND", authoritative: true }), { status: 503 });
  } });
  const first = await fixture.operation();
  assert.equal(((await first.json()) as any).status, "OUTCOME_UNKNOWN");
  fixture.reactivate();
  const retry = await fixture.operation();
  assert.equal(((await retry.json()) as any).status, "OUTCOME_UNKNOWN");
  assert.equal(effects, 1, "HTTP failure is not evidence that the first effect was absent");
});

test("compensation cannot execute for a forward operation that was only prepared", async () => {
  const effects: string[] = [];
  const executor = { async fetch(request: Request) {
    effects.push(new URL(request.url).pathname);
    return new Response(JSON.stringify({ status: "COMPENSATED" }));
  } };
  const fixture = await runningTask(executor);
  fixture.env.TASK_OPERATION_EXECUTOR = undefined;
  const prepared = await fixture.operation();
  assert.equal(prepared.status, 503);
  assert.equal(((await prepared.json()) as any).operation.status, "READY");
  fixture.env.TASK_OPERATION_EXECUTOR = executor;
  fixture.reactivate();
  const rollback = await fixture.compensate();
  assert.equal(rollback.status, 200, await rollback.clone().text());
  assert.deepEqual(effects, [], "a durable intent alone does not establish an effect to undo");
  assert.equal(((await rollback.json()) as any).results[0].status, "SKIPPED");
});

test("unknown forward effects wait for lookup and unknown compensation remains visible without repetition", async () => {
  const effects: string[] = [];
  let found = false;
  const fixture = await runningTask({ async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "GET") return new Response(JSON.stringify(found
      ? { status: "FOUND", result: { order_id: "order-1", created: true } }
      : { status: "UNKNOWN" }));
    effects.push(url.pathname);
    throw new Error("external effect accepted; response lost");
  } });
  assert.equal(((await (await fixture.operation()).json()) as any).status, "OUTCOME_UNKNOWN");
  const blocked = await fixture.compensate();
  assert.equal(((await blocked.json()) as any).results[0].status, "SKIPPED");
  assert.deepEqual(effects, ["/v1/operations/execute"]);
  found = true;
  fixture.reactivate();
  const resolved = await fixture.operation();
  const resolvedBody = await resolved.json() as any;
  assert.equal(resolvedBody.status, "SUCCEEDED");
  assert.equal(resolvedBody.operation.attempts.length, 1);
  assert.equal(resolvedBody.operation.attempts[0].outcomeSource, "PROVIDER_LOOKUP");
  const rollback = await fixture.compensate();
  assert.equal(((await rollback.json()) as any).results[0].status, "OUTCOME_UNKNOWN");
  fixture.reactivate();
  const replay = await fixture.compensate();
  assert.equal(((await replay.json()) as any).results[0].status, "SKIPPED");
  assert.deepEqual(effects, ["/v1/operations/execute", "/v1/operations/compensate"]);
  const query = await fixture.request(`${fixture.base}/compensations?tenant_id=${fixture.tenantId}&after_turn_exclusive=0`);
  const compensation = ((await query.json()) as any).compensations[0];
  assert.equal(compensation.status, "OUTCOME_UNKNOWN");
  assert.equal(compensation.attempts, 1);
});

test("approval cannot authorize changed arguments after TaskCell reactivation", async () => {
  const effects: unknown[] = [];
  const fixture = await runningTask({ async fetch(request) {
    effects.push(await request.json());
    return new Response(JSON.stringify({ result: { promoted: true } }));
  } });
  const body = { tool_call_id: "promote", tool_name: "deploy/promote", args: { release: "v1" }, epoch: 1, turn_seq: 1 };
  const path = `${fixture.base}/operations?tenant_id=${fixture.tenantId}`;
  const staged = await fixture.request(path, body);
  assert.equal(staged.status, 202);
  const operationId = ((await staged.json()) as any).operation.operationId;
  const approved = await fixture.request(`${fixture.base}/operations/${encodeURIComponent(operationId)}/approval?tenant_id=${fixture.tenantId}`, {
    approval_id: "approval-promote", approved_by: "tenant-admin", expires_at: Date.now() + 60_000,
  });
  assert.equal(approved.status, 200);
  fixture.reactivate();
  const changed = await fixture.request(path, { ...body, args: { release: "v2" } });
  assert.equal(changed.status, 409);
  assert.deepEqual(effects, []);
  const executed = await fixture.request(path, body);
  assert.equal(executed.status, 200);
  assert.equal(effects.length, 1);
});

test("reactivation exposes an interrupted compensation as unknown and accepts its late receipt", async () => {
  let started!: () => void;
  const effectStarted = new Promise<void>((resolve) => { started = resolve; });
  let finish!: () => void;
  const effectFinished = new Promise<void>((resolve) => { finish = resolve; });
  let compensations = 0;
  const fixture = await runningTask({ async fetch(request) {
    if (new URL(request.url).pathname.endsWith("/compensate")) {
      compensations += 1;
      started(); await effectFinished;
      return new Response(JSON.stringify({ status: "COMPENSATED" }));
    }
    return new Response(JSON.stringify({ result: { created: true } }));
  } });
  assert.equal((await fixture.operation()).status, 200);
  const original = fixture.compensate();
  await effectStarted;
  fixture.reactivate();
  try {
    const recovered = await fixture.compensate();
    assert.equal(recovered.status, 200);
    assert.equal(((await recovered.json()) as any).results[0].status, "OUTCOME_UNKNOWN");
    assert.equal(compensations, 1);
  } finally { finish(); }
  const late = await original;
  assert.equal(late.status, 200, await late.clone().text());
  assert.equal(((await late.json()) as any).results[0].status, "COMPENSATED");
  const query = await fixture.request(`${fixture.base}/compensations?tenant_id=${fixture.tenantId}&after_turn_exclusive=0`);
  assert.equal(((await query.json()) as any).compensations[0].status, "COMPENSATED");
});
