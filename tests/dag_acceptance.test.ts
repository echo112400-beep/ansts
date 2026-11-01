import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import * as path from "node:path";
import { test } from "node:test";
import * as yaml from "js-yaml";
import worker from "../src/cloudflare/worker";
import { MockDurableObjectState } from "../src/cloudflare/env";
import { AgentTaskHarnessCell } from "../src/cloudflare/harness_cell";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell";
import { NodeCapacityCell } from "../src/cloudflare/node_capacity_cell";
import { DagCell } from "../src/cloudflare/dag_cell";
import { TenantDirectoryCell } from "../src/cloudflare/tenant_directory_cell";
import { TenantLedgerCell } from "../src/persistence/tenant";

async function fixture(childIds = ["child"], startChildren = true) {
  const tenantId = "tenant-dag-acceptance", dagId = "dag-acceptance", parentId = "parent";
  const env: any = { DAG_VOUCHER_SECRET: "dedicated-test-dag-voucher-secret-0123456789", ANSTS_CHECKPOINTS_R2: {} };
  let loseReturnResponse = false, loseBoostResponse = false, rejectDagSync = false, hideQuotaAssignment = false;
  function namespace(Cell: any, kind: string) {
    const owners = new Map<string, { state: MockDurableObjectState; cell: any }>();
    return { idFromName: (name: string) => ({ name, toString: () => name }), get(id: any) {
      if (!owners.has(id.name)) {
        const state = new MockDurableObjectState(id.name);
        if (kind === "dag") state.storage.sync = async () => { if (rejectDagSync) throw new Error("dag proof unavailable"); };
        owners.set(id.name, { state, cell: new Cell(state, env) });
      }
      return { async fetch(input: RequestInfo, init?: RequestInit) {
        const request = new Request(input, init);
        const body = kind === "ledger" && request.method === "POST" ? await request.clone().json().catch(() => ({})) as any : {};
        const result = await owners.get(id.name)!.cell.fetch(request);
        if (hideQuotaAssignment && kind === "task" && new URL(request.url).pathname === "/execution/state" && result.ok) {
          const body = await result.json() as any;
          for (const field of ["quota_dag_id", "quota_voucher_id", "quota_tokens_used", "quota_tokens_reserved"]) delete body[field];
          return new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });
        }
        if (loseReturnResponse && String(body.transfer_id ?? "").startsWith("dag-return:")) {
          loseReturnResponse = false; throw new Error("committed return response lost");
        }
        if (loseBoostResponse && String(body.transfer_id ?? "").startsWith("dag-boost-transfer:")) {
          loseBoostResponse = false; throw new Error("committed boost response lost");
        }
        return result;
      } };
    }, reactivate() { for (const owner of owners.values()) owner.cell = new Cell(owner.state, env); } };
  }
  env.SCHEDULER_CELL = namespace(AnstsSchedulerCell, "scheduler");
  env.AGENT_HARNESS_CELL = namespace(AgentTaskHarnessCell, "task");
  env.NODE_CAPACITY_CELL = namespace(NodeCapacityCell, "node");
  env.TENANT_LEDGER_CELL = namespace(TenantLedgerCell, "ledger");
  env.TENANT_DIRECTORY_CELL = namespace(TenantDirectoryCell, "directory");
  env.DAG_CELL = namespace(DagCell, "dag");
  const request = (pathname: string, body?: unknown, method = body === undefined ? "GET" : "POST") => worker.fetch(new Request(`https://ansts${pathname}`, {
    method, headers: { "content-type": "application/json" }, body: body === undefined ? undefined : JSON.stringify(body),
  }), env);
  const check = async (result: Promise<Response>, status = 200) => {
    const response = await result; assert.equal(response.status, status, await response.clone().text()); return response.json() as Promise<any>;
  };
  const tenant = `/v1beta1/tenants/${tenantId}`;
  const base = `/v1beta1/dags/${dagId}`;
  for (const taskId of [parentId, ...childIds]) {
    const spec = yaml.load(readFileSync(path.join(__dirname, "../../specs/taskspec_v1beta1_example.yaml"), "utf8")) as any;
    Object.assign(spec.metadata, { tenant_id: tenantId, task_id: taskId, idempotency_key: `admit-${taskId}` });
    await check(request("/v1beta1/tasks", spec), 202);
  }
  await check(request(`${tenant}/budget-periods/2026-11`, { currency: "USD", hard_limit_minor: 1000, price_version: "price-v1" }, "PUT"), 201);
  await check(request(`${tenant}/budget-grants`, { task_id: parentId, period_key: "2026-11", grant_id: "parent-grant", amount_minor: 800, currency: "USD" }), 201);
  const dispatches = new Map<string, any>();
  async function start(taskId: string, voucher?: any, suffix = "") {
    const resources = { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 };
    const id = `${taskId}${suffix}`;
    const tick = await check(request("/v1beta1/scheduler/tick", {
      nodes: [{ node_id: `${id}-node`, incarnation: "boot", capacity: resources }],
      candidates: [{ task_id: taskId, tenant_id: tenantId, dispatch_id: `${id}-dispatch`, node_id: `${id}-node`, node_incarnation: "boot",
        kind: voucher ? "dag" : suffix ? "recovery" : "ordinary", resources,
        ...(voucher ? { dag_id: dagId, voucher_id: voucher.voucher_id } : {}),
        task_spec: { metadata: { tenant_id: tenantId, task_id: taskId }, spec: {} } }],
    }));
    const dispatch = tick.dispatches[0]; assert.equal(dispatch.stage, "AUTHORIZED", JSON.stringify(dispatch));
    await check(request("/v1beta1/scheduler/started", { ...dispatch, start_receipt: `${id}-start` }));
    dispatches.set(taskId, dispatch); return dispatch;
  }
  await start(parentId);
  const created = await check(request(base, { tenant_id: tenantId, dag_id: dagId, parent_task_id: parentId,
    parent_epoch: 1, parent_turn_seq: 0, period_key: "2026-11", currency: "USD",
    children: childIds.map((task_id) => ({ task_id, delegated_tps: 10, delegated_tokens: 1000, allowance_minor: 200 })) }));
  for (const child of created.children) {
    if (!startChildren) continue;
    await check(request(`${base}/children/${child.task_id}/redeem`, { tenant_id: tenantId, voucher_id: child.voucher_id, signature: child.voucher_signature }));
    await start(child.task_id, child);
    await check(request(`${base}/children/${child.task_id}/observations`, { tenant_id: tenantId, event_id: `${child.task_id}-running`, aggregate_version: 1, state: "RUNNING" }));
  }
  const complete = async (taskId: string) => {
    await check(request(`/v1beta1/tasks/${taskId}/complete?tenant_id=${tenantId}`, { epoch: dispatches.get(taskId).business_epoch, status: "SUCCEEDED" }));
    await check(request(`${base}/children/${taskId}/observations`, { tenant_id: tenantId, event_id: `${taskId}-succeeded`, aggregate_version: 2, state: "SUCCEEDED" }));
  };
  const stop = (taskId: string) => check(request("/v1beta1/scheduler/stopped", { ...dispatches.get(taskId), tenant_id: tenantId,
    evidence_type: "STOPPED", evidence_id: `${taskId}-stopped` }));
  const account = (taskId: string) => check(request(`${tenant}/budget-accounts/${taskId}?tenant_id=${tenantId}&period_key=2026-11`));
  return { env, request, check, base, tenantId, parentId, created, start, complete, stop, account,
    returnAllowance: (taskId: string) => request(`${base}/children/${taskId}/return-allowance`, { tenant_id: tenantId }),
    reconcile: () => request(`${base}/reconcile`, { tenant_id: tenantId }),
    state: () => check(request(`${base}/state?tenant_id=${tenantId}`)),
    reactivate: () => env.DAG_CELL.reactivate(),
    loseReturn: () => { loseReturnResponse = true; }, loseBoost: () => { loseBoostResponse = true; },
    rejectProof: (value: boolean) => { rejectDagSync = value; }, legacySourceState: () => { hideQuotaAssignment = true; } };
}

test("DAG allowance return recovers its original amount after a committed transfer response is lost", async () => {
  const f = await fixture();
  await f.complete("child"); await f.stop("child");
  f.loseReturn();
  assert.equal((await f.returnAllowance("child")).status, 503);
  f.reactivate();
  const replay = await f.check(f.returnAllowance("child"));
  assert.equal(replay.amount_minor, 200);
  assert.equal((await f.account("parent")).available_minor, 800);
  assert.equal((await f.account("child")).available_minor, 0);
});

test("completed DAG replays stay complete after the parent has a newer execution epoch", async () => {
  const f = await fixture();
  await f.complete("child");
  assert.equal((await f.state()).state, "COMPLETED");
  const resumed = await f.start("parent", undefined, "-resumed");
  assert.equal(resumed.business_epoch, 2);
  f.reactivate();
  const replay = await f.check(f.reconcile());
  assert.equal(replay.state, "COMPLETED");
  const late = await f.check(f.request(`${f.base}/children/child/observations`, {
    tenant_id: f.tenantId, event_id: "child-succeeded", aggregate_version: 2, state: "SUCCEEDED",
  }));
  assert.equal(late.duplicate, true);
  assert.equal(late.dag.state, "COMPLETED");
});

test("DAG reconciliation resumes a pending boost without a client repeating the boost command", async () => {
  const f = await fixture(["source", "target"]);
  await f.complete("source"); await f.stop("source");
  f.loseBoost();
  const interrupted = await f.request(`${f.base}/boosts`, { tenant_id: f.tenantId, boost_id: "boost-retry",
    source_task_id: "source", target_task_id: "target", amount_minor: 50, delegated_tokens: 200, delegated_tps: 2 });
  assert.equal(interrupted.status, 503);
  f.reactivate();
  const recovered = await f.check(f.reconcile());
  assert.equal(recovered.boosts[0].status, "TRANSFERRED");
  assert.equal((await f.account("source")).available_minor, 150);
  assert.equal((await f.account("target")).available_minor, 250);
});

test("an unconfirmed voucher redemption cannot authorize a child dispatch", async () => {
  const f = await fixture(["child"], false);
  const voucher = f.created.children[0];
  f.rejectProof(true);
  const failed = await f.request(`${f.base}/children/child/redeem`, { tenant_id: f.tenantId,
    voucher_id: voucher.voucher_id, signature: voucher.voucher_signature });
  assert.equal(failed.status, 503);
  const resources = { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 };
  const blocked = await f.request("/v1beta1/scheduler/tick", {
    nodes: [{ node_id: "unconfirmed-node", incarnation: "boot", capacity: resources }],
    candidates: [{ task_id: "child", tenant_id: f.tenantId, dispatch_id: "unconfirmed-dispatch", node_id: "unconfirmed-node", node_incarnation: "boot",
      kind: "dag", dag_id: "dag-acceptance", voucher_id: voucher.voucher_id, resources,
      task_spec: { metadata: { task_id: "child", tenant_id: f.tenantId }, spec: {} } }],
  });
  assert.equal(blocked.status, 503, "local redemption row is not a durability proof");
  f.rejectProof(false);
  f.reactivate();
  const started = await f.start("child", voucher);
  assert.equal(started.business_epoch, 1);
});

test("distinct boosts cannot delegate the same donor token and TPS allocation twice", async () => {
  const f = await fixture(["source", "target"]);
  await f.complete("source"); await f.stop("source");
  const boost = (boost_id: string, delegated_tokens: number, delegated_tps: number) => f.request(`${f.base}/boosts`, {
    tenant_id: f.tenantId, boost_id, source_task_id: "source", target_task_id: "target",
    amount_minor: 50, delegated_tokens, delegated_tps,
  });
  await f.check(boost("first", 800, 8), 201);
  const duplicateAllocation = await boost("second", 300, 3);
  assert.equal(duplicateAllocation.status, 409);
  assert.equal((await f.account("source")).available_minor, 150);
  assert.equal((await f.account("target")).available_minor, 250);
});

test("legacy source state without durable quota accounting cannot delegate additional tokens", async () => {
  const f = await fixture(["source", "target"]);
  await f.complete("source"); await f.stop("source");
  f.legacySourceState();
  const boost = await f.request(`${f.base}/boosts`, { tenant_id: f.tenantId, boost_id: "legacy-boost",
    source_task_id: "source", target_task_id: "target", amount_minor: 50, delegated_tokens: 100, delegated_tps: 0 });
  assert.equal(boost.status, 409);
  assert.equal(((await boost.json()) as any).error, "DAG_BOOST_SOURCE_USAGE_UNKNOWN");
  assert.equal((await f.account("source")).available_minor, 200);
  assert.equal((await f.account("target")).available_minor, 200);
});
