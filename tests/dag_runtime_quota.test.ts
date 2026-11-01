import { strict as assert } from "node:assert";
import { test } from "node:test";
import worker from "../src/cloudflare/worker";
import { MockDurableObjectState } from "../src/cloudflare/env";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell";
import { AgentTaskHarnessCell } from "../src/cloudflare/harness_cell";
import { NodeCapacityCell } from "../src/cloudflare/node_capacity_cell";
import { DelegatedTaskQuota } from "../src/persistence/task-core/delegated_quota";
import { TaskCore, taskCoreMigration } from "../src/persistence/task-core/task_core";
import { initializeCellProtocol } from "../src/persistence/schema";

test("delegated token reservations survive activation and cannot spend unknown usage again", async () => {
  const state = new MockDurableObjectState("child");
  initializeCellProtocol(state.storage, { type: "task", key: "child" }, [taskCoreMigration]);
  const core = new TaskCore(state.storage);
  const request = { dispatch_id: "dispatch", task_id: "child", tenant_id: "tenant", kind: "dag" as const,
    node_id: "node", node_incarnation: "boot", reservation_id: "dispatch", reservation_token: "reservation", task_spec: {} };
  await core.prepareDispatch(request);
  await core.recordReservation(request);
  const authorization = await core.authorizeDispatch(request);
  await core.markStarted(authorization, "receipt");
  let quota = new DelegatedTaskQuota(state.storage, "child");
  await quota.assign("dag", "voucher");
  await quota.refresh(100, 20, 10);
  await quota.reserve("call-1", 80, 1);
  quota = new DelegatedTaskQuota(state.storage, "child");
  await assert.rejects(quota.reserve("call-2", 30, 1), { code: "DAG_TOKEN_QUOTA_EXCEEDED" });
  assert.equal(quota.state().quota_tokens_reserved, 80);
  assert.equal(quota.state().effective_delegated_tps, 10);
  assert.equal(quota.state().pending_delegated_tps, 10);
  await quota.settle("call-1", 10);
  await quota.settle("call-1", 10);
  await quota.reserve("call-2", 90, 1);
  assert.equal(quota.state().quota_tokens_used, 10);
  assert.equal(quota.state().quota_tokens_reserved, 90);
  await assert.rejects(quota.reserve("old-executor", 0, 0), { code: "STALE_EXECUTION_EPOCH" });
});

test("DAG dispatch cannot activate delegated TPS without matching physical node reservation", async () => {
  const env: any = { ANSTS_CHECKPOINTS_R2: {} };
  function namespace(Cell: any) {
    const cells = new Map();
    return { idFromName: (name: string) => ({ name, toString: () => name }), get(id: any) {
      if (!cells.has(id.name)) cells.set(id.name, new Cell(new MockDurableObjectState(id.name), env));
      return { fetch: (input: RequestInfo, init?: RequestInit) => cells.get(id.name).fetch(new Request(input, init)) };
    } };
  }
  env.SCHEDULER_CELL = namespace(AnstsSchedulerCell);
  env.AGENT_HARNESS_CELL = namespace(AgentTaskHarnessCell);
  env.NODE_CAPACITY_CELL = namespace(NodeCapacityCell);
  env.DAG_CELL = { idFromName: (name: string) => ({ name }), get: () => ({ fetch: async () => new Response(JSON.stringify({
    authorized: true, dag_id: "dag", voucher_id: "voucher", task_id: "child", delegated_tokens: 100, delegated_tps: 20,
  })) }) };
  const result = await worker.fetch(new Request("https://ansts/v1beta1/scheduler/tick", { method: "POST", body: JSON.stringify({
    nodes: [{ node_id: "node", incarnation: "boot", capacity: { cpu: 4, memory_mb: 2048, burst_cpu: 2, sessions: 2, tps: 100 } }],
    candidates: [{ task_id: "child", tenant_id: "tenant", dispatch_id: "dispatch", kind: "dag", dag_id: "dag", voucher_id: "voucher",
      region: "default", resource_pool: "default", node_id: "node", node_incarnation: "boot", resources: { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 } }],
  }) }), env);
  assert.equal(result.status, 200, await result.clone().text());
  assert.equal(((await result.json()) as any).dispatches[0].status, "BLOCKED_DAG_QUOTA");
});
