/**
 * End-to-End Integration Tests for `denoland/celld` & Cloudflare Workers Deployment Support.
 */

import { strict as assert } from "assert";
import * as fs from "fs";
import * as path from "path";
import { test } from "node:test";
import * as yaml from "js-yaml";

import {
  AgentTaskHarnessCell,
  AnstsSchedulerCell,
  CloudflareCelldEnv,
  cloudflareCelldWorker,
  MockDurableObjectState,
  TenantLedgerCell,
  ToolEffectClass,
} from "../src/index";
import { NodeCapacityCell } from "../src/cloudflare/node_capacity_cell";
import { schedulerPoolCellKey } from "../src/persistence/scheduler-pool";

function createMockCloudflareCelldEnv(): {
  env: CloudflareCelldEnv;
  schedulerCell: AnstsSchedulerCell;
  schedulerCells: Map<string, AnstsSchedulerCell>;
  harnessCells: Map<string, AgentTaskHarnessCell>;
  tenantLedgerCells: Map<string, TenantLedgerCell>;
  nodeCapacityCells: Map<string, NodeCapacityCell>;
  d1Rows: Array<any[]>;
  queuedMessages: any[];
} {
  const kvMap = new Map<string, string>();
  const r2Map = new Map<string, string>();
  const d1Rows: Array<any[]> = [];
  const queuedMessages: any[] = [];
  const harnessCells = new Map<string, AgentTaskHarnessCell>();
  const tenantLedgerCells = new Map<string, TenantLedgerCell>();
  const nodeCapacityCells = new Map<string, NodeCapacityCell>();

  const env: CloudflareCelldEnv = {
    PRIMARY_MODEL_POOL: "DeepSeek",
    FAST_UTILITY_POOL: "GLM-5.3",
    FALLBACK_MODEL_POOL: "Qwen-3.8",
    WEBHOOK_HMAC_SECRET: "whsec_celld_test",
    SCHEDULER_CELL: null as any,
    AGENT_HARNESS_CELL: null as any,
    TENANT_LEDGER_CELL: null as any,
    ANSTS_KV: {
      get: async (key: string, type?: "text" | "json") => {
        const val = kvMap.get(key);
        if (!val) return null;
        return type === "json" ? JSON.parse(val) : val;
      },
      put: async (key: string, value: string) => {
        kvMap.set(key, value);
      },
      delete: async (key: string) => {
        kvMap.delete(key);
      },
    },
    ANSTS_CHECKPOINTS_R2: {
      put: async (key: string, value: string) => {
        r2Map.set(key, value);
        return { key };
      },
      get: async (key: string) => {
        const v = r2Map.get(key);
        if (!v) return null;
        return { key, body: v, text: async () => v };
      },
      list: async () => ({
        objects: Array.from(r2Map.keys()).map((key) => ({ key })),
      }),
    },
    ANSTS_D1: {
      prepare: (_query: string) => {
        let bound: any[] = [];
        const stmt = {
          bind: (...values: any[]) => {
            bound = values;
            return stmt;
          },
          run: async () => {
            d1Rows.push(bound);
            return { success: true };
          },
          all: async <T = any>() => ({ results: d1Rows as T[] }),
        };
        return stmt;
      },
    },
    ANSTS_EVENTS_QUEUE: {
      send: async (msg: any) => {
        queuedMessages.push(msg);
      },
    },
  };

  const schedulerState = new MockDurableObjectState("global-5d-scheduler");
  const schedulerCell = new AnstsSchedulerCell(schedulerState, env);
  const schedulerCells = new Map<string, AnstsSchedulerCell>([["global-5d-scheduler", schedulerCell]]);

  env.SCHEDULER_CELL = {
    idFromName: (name: string) => ({ toString: () => `sched_${name}`, name }),
    get: (id: { toString(): string; name?: string }) => {
      const key = id.name ?? id.toString();
      if (!schedulerCells.has(key)) schedulerCells.set(key, new AnstsSchedulerCell(new MockDurableObjectState(key), env));
      const cell = schedulerCells.get(key)!;
      return {
      fetch: (input: RequestInfo, init?: RequestInit) => {
        const req = typeof input === "string" ? new Request(input, init) : input;
        return cell.fetch(req);
      },
      };
    },
  };

  env.AGENT_HARNESS_CELL = {
    idFromName: (name: string) => ({ toString: () => `harness_${name}`, name }),
    get: (id: { toString(): string; name?: string }) => {
      const key = id.name ?? id.toString();
      if (!harnessCells.has(key)) {
        const st = new MockDurableObjectState(key);
        harnessCells.set(key, new AgentTaskHarnessCell(st, env));
      }
      const cell = harnessCells.get(key)!;
      return {
        fetch: (input: RequestInfo, init?: RequestInit) => {
          const req = typeof input === "string" ? new Request(input, init) : input;
          return cell.fetch(req);
        },
      };
    },
  };

  env.TENANT_LEDGER_CELL = {
    idFromName: (name: string) => ({ toString: () => `tenant_${name}`, name }),
    get: (id: { toString(): string; name?: string }) => {
      const tenantId = id.name ?? id.toString();
      if (!tenantLedgerCells.has(tenantId)) {
        tenantLedgerCells.set(tenantId, new TenantLedgerCell(new MockDurableObjectState(tenantId), env));
      }
      const cell = tenantLedgerCells.get(tenantId)!;
      return {
        fetch: (input: RequestInfo, init?: RequestInit) => {
          const req = typeof input === "string" ? new Request(input, init) : input;
          return cell.fetch(req);
        },
      };
    },
  };

  env.NODE_CAPACITY_CELL = {
    idFromName: (name: string) => ({ toString: () => `node_${name}`, name }),
    get: (id: { toString(): string; name?: string }) => {
      const nodeId = id.name ?? id.toString();
      if (!nodeCapacityCells.has(nodeId)) {
        nodeCapacityCells.set(nodeId, new NodeCapacityCell(new MockDurableObjectState(nodeId), env));
      }
      const cell = nodeCapacityCells.get(nodeId)!;
      return {
        fetch: (input: RequestInfo, init?: RequestInit) => {
          const req = typeof input === "string" ? new Request(input, init) : input;
          return cell.fetch(req);
        },
      };
    },
  };

  return { env, schedulerCell, schedulerCells, harnessCells, tenantLedgerCells, nodeCapacityCells, d1Rows, queuedMessages };
}

async function simulateNodeStartReceipt(env: CloudflareCelldEnv, dispatch: Record<string, any>, receipt: string): Promise<Response> {
  // This test adapter stands in for the node callback after the node reports running.
  return cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/scheduler/started", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      dispatch_id: dispatch.dispatch_id,
      node_id: dispatch.node_id,
      node_incarnation: dispatch.node_incarnation,
      region: dispatch.region,
      resource_pool: dispatch.resource_pool,
      authorization_token: dispatch.authorization_token,
      business_epoch: dispatch.business_epoch,
      start_receipt: receipt,
    }),
  }), env);
}

test("07. wrangler.json & D1 SQL migration schema verification for celld and Cloudflare Workers", () => {
  const rootDir = path.join(__dirname, "..", "..");
  const wranglerJson = JSON.parse(fs.readFileSync(path.join(rootDir, "wrangler.json"), "utf8"));
  assert.equal(wranglerJson.main, "src/cloudflare/worker.ts");
  assert.equal(wranglerJson.durable_objects.bindings.length, 4);
  assert.equal(
    wranglerJson.durable_objects.bindings.find((binding: Record<string, string>) => binding.name === "TENANT_LEDGER_CELL")?.class_name,
    "TenantLedgerCell",
  );
  assert.equal(
    wranglerJson.durable_objects.bindings.find((binding: Record<string, string>) => binding.name === "NODE_CAPACITY_CELL")?.class_name,
    "NodeCapacityCell",
  );
  assert.equal(wranglerJson.migrations.at(-1).tag, "v3");
  assert.ok(wranglerJson.migrations.at(-1).new_sqlite_classes.includes("NodeCapacityCell"));
  const wranglerToml = fs.readFileSync(path.join(rootDir, "wrangler.toml"), "utf8");
  assert.match(wranglerToml, /name = "NODE_CAPACITY_CELL"\s+class_name = "NodeCapacityCell"/);
  assert.match(wranglerToml, /tag = "v3"\s+new_sqlite_classes = \["NodeCapacityCell"\]/);
  assert.equal(wranglerJson.d1_databases[0].binding, "ANSTS_D1");
  assert.equal(wranglerJson.kv_namespaces[0].binding, "ANSTS_KV");
  assert.equal(wranglerJson.r2_buckets[0].binding, "ANSTS_CHECKPOINTS_R2");

  const sqlMig = fs.readFileSync(path.join(rootDir, "migrations", "0001_init_ansts.sql"), "utf8");
  assert.ok(sqlMig.includes("CREATE TABLE IF NOT EXISTS task_audit_events"));
});

test("08. TenantLedger admission + AnstsSchedulerCell 5D W-DRF & DAG Escrow", async () => {
  const { env, queuedMessages, d1Rows } = createMockCloudflareCelldEnv();
  const specPath = path.join(__dirname, "..", "..", "specs", "taskspec_v1beta1_example.yaml");
  const sampleSpec = yaml.load(fs.readFileSync(specPath, "utf8")) as Record<string, any>;

  // First submission -> 202 Accepted; initial queue intent and outbox are owned by TenantLedgerCell.
  const res1 = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks", {
      method: "POST",
      body: JSON.stringify(sampleSpec),
    }),
    env,
  );
  assert.equal(res1.status, 202);
  const body1 = (await res1.json()) as Record<string, any>;
  assert.equal(body1.deduplicated, false);
  assert.equal(body1.runnable_status, "PENDING_DISPATCH");
  assert.equal(queuedMessages.length, 0);

  // Duplicate submission -> the durable tenant ledger returns the same acceptance identity.
  const res2 = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks", {
      method: "POST",
      body: JSON.stringify(sampleSpec),
    }),
    env,
  );
  assert.equal(res2.status, 200);
  const body2 = (await res2.json()) as Record<string, any>;
  assert.equal(body2.deduplicated, true);
  assert.equal(body2.task_id, body1.task_id);

  // Admission is not falsely treated as scheduler dispatch; a separate reconciler consumes its durable intent.
  await cloudflareCelldWorker.scheduled({ scheduledTime: Date.now(), cron: "* * * * *" }, env);

  const topRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/cluster/top"),
    env,
  );
  const topBody = (await topRes.json()) as Record<string, any>;
  assert.equal(topBody.celld_sqlite_leases_count, 0);

  // The generic Queue consumer remains independently testable with a synthetic outbox delivery.
  let acked = false;
  await cloudflareCelldWorker.queue(
    {
      queue: "ansts-cloudevents-outbox",
      messages: [
        {
          id: "msg_01",
          timestamp: new Date(),
          body: {
            task_id: body1.task_id,
            tenant_id: body1.tenant_id,
            event_type: "task.admitted",
            payload: { task_id: body1.task_id },
          },
          ack: () => {
            acked = true;
          },
          retry: () => {},
        },
      ],
    },
    env,
  );
  assert.equal(acked, true);
  assert.equal(d1Rows.length, 1);
});

test("08B. Public task route resolves the logical TaskCell binding by stable task key", async () => {
  const { env } = createMockCloudflareCelldEnv();
  const routedNames: string[] = [];
  env.TASK_CELL = {
    idFromName: (name: string) => ({ toString: () => `task_${name}`, name }),
    get: (id: { toString(): string; name?: string }) => ({
      fetch: async (input: RequestInfo, init?: RequestInit) => {
        routedNames.push(id.name ?? id.toString());
        return new Response(JSON.stringify({ accepted_route: String(input), method: init?.method }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      },
    }),
  };

  const response = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks/task-registry-7/turn", {
      method: "POST",
      body: JSON.stringify({ turn_seq: 1 }),
    }),
    env,
  );

  assert.equal(response.status, 200);
  assert.deepEqual(routedNames, ["task-registry-7"]);
});

test("scheduler tick replays dispatch stages and reserves shared node capacity once", async () => {
  const { env, harnessCells, nodeCapacityCells } = createMockCloudflareCelldEnv();
  const candidate = {
    task_id: "dispatch-task-1",
    tenant_id: "tenant-a",
    dispatch_id: "dispatch-stable-1",
    node_id: "node-capacity-1",
    region: "us-west-1",
    resource_pool: "cpu-small",
    kind: "ordinary",
    task_spec: { task_id: "dispatch-task-1", tenant_id: "tenant-a", execution_policy: { retry: 3 } },
    resources: { cpu: 2, memory_mb: 1024, burst_cpu: 1, sessions: 1, tps: 10 },
  };
  const poolKey = schedulerPoolCellKey("us-west-1", "cpu-small");
  const tick = () => cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/scheduler/tick?shard=${encodeURIComponent(poolKey)}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        nodes: [{ node_id: "node-capacity-1", incarnation: "boot-1", capacity: { cpu: 2, memory_mb: 1024, burst_cpu: 1, sessions: 1, tps: 10 } }],
        candidates: [candidate],
      }),
    }),
    env,
  );

  const first = await tick();
  assert.equal(first.status, 200);
  const firstBody = (await first.json()) as any;
  assert.equal(firstBody.dispatches[0].stage, "AUTHORIZED");
  assert.equal(firstBody.dispatches[0].status, "AWAITING_NODE_START");
  assert.equal(firstBody.dispatches[0].business_epoch, 1);
  assert.equal(nodeCapacityCells.get("node-capacity-1")!.capacity.getReservation("dispatch-stable-1")!.status, "STARTING");
  const taskCell = harnessCells.get("dispatch-task-1")!;
  const pendingCommands = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks/dispatch-task-1/start-commands"), env);
  const startCommandBody = await pendingCommands.json() as any;
  assert.equal(startCommandBody.commands.length, 1);
  assert.equal(startCommandBody.commands[0].payload.authorization_token, firstBody.dispatches[0].authorization_token);
  assert.equal(startCommandBody.commands[0].payload.region, "us-west-1");
  assert.equal(startCommandBody.commands[0].payload.resource_pool, "cpu-small");
  const commandReplay = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks/dispatch-task-1/start-commands"), env);
  assert.deepEqual(((await commandReplay.json()) as any).commands[0], startCommandBody.commands[0]);
  const authorizationReplay = await tick();
  const authorizationReplayBody = await authorizationReplay.json() as any;
  assert.equal(authorizationReplayBody.dispatches[0].authorization_token, firstBody.dispatches[0].authorization_token);
  const changedReservation = await taskCell.fetch(new Request("http://task.internal/execution/authorize", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      dispatch_id: candidate.dispatch_id,
      task_id: candidate.task_id,
      tenant_id: candidate.tenant_id,
      kind: candidate.kind,
      node_id: candidate.node_id,
      node_incarnation: candidate.node_incarnation,
      region: candidate.region,
      resource_pool: candidate.resource_pool,
      reservation_id: candidate.dispatch_id,
      reservation_token: "changed-reservation-token",
    }),
  }));
  assert.equal(changedReservation.status, 409);
  const commandAfterConflict = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks/dispatch-task-1/start-commands"), env);
  assert.deepEqual(((await commandAfterConflict.json()) as any).commands[0], startCommandBody.commands[0]);
  const prematureTurn = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks/dispatch-task-1/turn", {
    method: "POST", body: JSON.stringify({ epoch: 1, turn_seq: 1, is_read_only_turn: true }),
  }), env);
  assert.equal(prematureTurn.status, 409);

  const started = await simulateNodeStartReceipt(env, firstBody.dispatches[0], "node-receipt-dispatch-stable-1");
  assert.equal(started.status, 200);
  assert.equal(((await started.json()) as any).stage, "STARTED");
  const acknowledgedCommands = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks/dispatch-task-1/start-commands"), env);
  assert.equal(((await acknowledgedCommands.json()) as any).commands.length, 0);

  const replay = await tick();
  assert.equal(replay.status, 200);
  const replayBody = (await replay.json()) as any;
  assert.equal(replayBody.dispatches[0].stage, "STARTED");
  assert.equal(replayBody.dispatches[0].business_epoch, 1);
  const conflictingReceipt = await simulateNodeStartReceipt(env, firstBody.dispatches[0], "conflicting-receipt");
  assert.equal(conflictingReceipt.status, 409);

  const capacityCell = nodeCapacityCells.get("node-capacity-1")!;
  const capacity = await capacityCell.fetch(new Request("https://node.internal/allocations"));
  const capacityBody = (await capacity.json()) as any;
  assert.equal(capacityBody.allocations.length, 1);
  assert.equal(capacityBody.allocations[0].dispatch_id, "dispatch-stable-1");
});

test("dispatch retry repairs TaskCore RESERVED after a failure between node reserve and SchedulerPool advance", async () => {
  const { env, harnessCells, nodeCapacityCells } = createMockCloudflareCelldEnv();
  const taskId = "dispatch-reservation-replay-task";
  const dispatchId = "dispatch-reservation-replay";
  env.AGENT_HARNESS_CELL!.get(env.AGENT_HARNESS_CELL!.idFromName(taskId));
  const taskCell = harnessCells.get(taskId)!;
  const originalFetch = taskCell.fetch.bind(taskCell);
  let failReservedOnce = true;
  taskCell.fetch = async (request: Request) => {
    if (failReservedOnce && new URL(request.url).pathname === "/execution/reserved") {
      failReservedOnce = false;
      return new Response(JSON.stringify({ error: "INJECTED_TRANSIENT_FAILURE" }), { status: 503 });
    }
    return originalFetch(request);
  };
  const candidate = {
    task_id: taskId,
    tenant_id: "tenant-replay",
    dispatch_id: dispatchId,
    node_id: "reservation-replay-node",
    node_incarnation: "reservation-replay-boot-1",
    kind: "ordinary",
    task_spec: { metadata: { task_id: taskId, tenant_id: "tenant-replay" }, spec: {} },
    resources: { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 },
  };
  const tick = () => cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/scheduler/tick", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      nodes: [{ node_id: candidate.node_id, incarnation: candidate.node_incarnation, capacity: candidate.resources }],
      candidates: [candidate],
    }),
  }), env);

  assert.equal((await tick()).status, 503);
  assert.equal(taskCell.taskCore.getDispatch(dispatchId)!.stage, "PREPARING");
  assert.equal(nodeCapacityCells.get(candidate.node_id)!.capacity.getReservations().length, 1);

  const retry = await tick();
  assert.equal(retry.status, 200);
  const retryBody = await retry.json() as any;
  assert.equal(retryBody.dispatches[0].stage, "AUTHORIZED");
  assert.equal(taskCell.taskCore.getDispatch(dispatchId)!.stage, "AUTHORIZED");
  assert.equal(nodeCapacityCells.get(candidate.node_id)!.capacity.getReservations().length, 1);
});

test("public Worker splits mixed scheduler ticks by stable region and resource-pool owner", async () => {
  const { env, schedulerCells } = createMockCloudflareCelldEnv();
  const resourceCapacity = { cpu: 4, memory_mb: 2048, burst_cpu: 2, sessions: 2, tps: 20 };
  const candidates = [
    {
      task_id: "mixed-pool-task-a",
      tenant_id: "tenant-mixed-a",
      dispatch_id: "mixed-pool-dispatch-a",
      node_id: "mixed-pool-node-a",
      node_incarnation: "mixed-boot-a",
      region: "us-west-2",
      resource_pool: "cpu-general",
      kind: "ordinary",
      task_spec: { metadata: { task_id: "mixed-pool-task-a", tenant_id: "tenant-mixed-a" }, spec: {} },
      resources: { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 5 },
    },
    {
      task_id: "mixed-pool-task-b",
      tenant_id: "tenant-mixed-b",
      dispatch_id: "mixed-pool-dispatch-b",
      node_id: "mixed-pool-node-b",
      node_incarnation: "mixed-boot-b",
      region: "eu-west-1",
      resource_pool: "gpu-burst",
      kind: "ordinary",
      task_spec: { metadata: { task_id: "mixed-pool-task-b", tenant_id: "tenant-mixed-b" }, spec: {} },
      resources: { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 5 },
    },
  ];
  const response = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/scheduler/tick", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      nodes: [
        { node_id: candidates[0].node_id, incarnation: candidates[0].node_incarnation, capacity: resourceCapacity },
        { node_id: candidates[1].node_id, incarnation: candidates[1].node_incarnation, capacity: resourceCapacity },
      ],
      candidates,
    }),
  }), env);

  assert.equal(response.status, 200);
  const body = await response.json() as any;
  assert.deepEqual(body.dispatches.map((dispatch: any) => dispatch.dispatch_id).sort(), [
    "mixed-pool-dispatch-a",
    "mixed-pool-dispatch-b",
  ]);
  assert.ok(schedulerCells.has(schedulerPoolCellKey("us-west-2", "cpu-general")));
  assert.ok(schedulerCells.has(schedulerPoolCellKey("eu-west-1", "gpu-burst")));
  assert.equal(schedulerCells.get(schedulerPoolCellKey("us-west-2", "cpu-general"))!.schedulerPool.getDispatch("mixed-pool-dispatch-a")!.stage, "AUTHORIZED");
  assert.equal(schedulerCells.get(schedulerPoolCellKey("eu-west-1", "gpu-burst"))!.schedulerPool.getDispatch("mixed-pool-dispatch-b")!.stage, "AUTHORIZED");
});

test("independent SchedulerPool cells cannot reserve the same node capacity concurrently", async () => {
  const { env, nodeCapacityCells } = createMockCloudflareCelldEnv();
  const capacity = { cpu: 2, memory_mb: 1024, burst_cpu: 1, sessions: 1, tps: 10 };
  const candidate = (taskId: string, region: string) => ({
    task_id: taskId,
    tenant_id: `tenant-${taskId}`,
    dispatch_id: `dispatch-${taskId}`,
    node_id: "contended-node",
    node_incarnation: "contended-boot-1",
    region,
    resource_pool: "cpu",
    kind: "ordinary",
    task_spec: { metadata: { task_id: taskId, tenant_id: `tenant-${taskId}` }, spec: {} },
    resources: capacity,
  });
  const tick = (region: string, taskId: string) => cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/scheduler/tick?shard=${encodeURIComponent(schedulerPoolCellKey(region, "cpu"))}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        nodes: [{ node_id: "contended-node", incarnation: "contended-boot-1", capacity }],
        candidates: [candidate(taskId, region)],
      }),
    }),
    env,
  );

  const [left, right] = await Promise.all([tick("region-a", "task-left"), tick("region-b", "task-right")]);
  const results = await Promise.all([left.json() as Promise<any>, right.json() as Promise<any>]);
  const stages = results.map((result) => result.dispatches[0].stage);
  const statuses = results.map((result) => result.dispatches[0].status);
  assert.equal(stages.filter((stage) => stage === "AUTHORIZED").length, 1);
  assert.equal(statuses.filter((status) => status === "BLOCKED_CAPACITY").length, 1);
  assert.equal(nodeCapacityCells.get("contended-node")!.capacity.getReservations().length, 1);
  const admitted = results.find((result) => result.dispatches[0].stage === "AUTHORIZED")!;
  const pool = schedulerPoolCellKey(admitted.dispatches[0].region, admitted.dispatches[0].resource_pool);
  const started = await simulateNodeStartReceipt(env, admitted.dispatches[0], "node-receipt-contended", pool);
  assert.equal(started.status, 200);
  assert.equal(nodeCapacityCells.get("contended-node")!.capacity.getReservation(admitted.dispatches[0].dispatch_id)!.status, "RUNNING");
});

test("ordinary and DAG starts use the same durable NodeCapacity reservation path", async () => {
  const { env, nodeCapacityCells } = createMockCloudflareCelldEnv();
  const resources = { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 };
  const candidates = ["ordinary", "dag"].map((kind) => ({
    task_id: `${kind}-capacity-task`,
    tenant_id: "tenant-shared",
    dispatch_id: `${kind}-capacity-dispatch`,
    node_id: "ordinary-dag-node",
    node_incarnation: "ordinary-dag-boot-1",
    kind,
    task_spec: { metadata: { task_id: `${kind}-capacity-task`, tenant_id: "tenant-shared" }, spec: {} },
    resources,
  }));
  const response = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/scheduler/tick", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        nodes: [{ node_id: "ordinary-dag-node", incarnation: "ordinary-dag-boot-1", capacity: { cpu: 2, memory_mb: 1024, burst_cpu: 2, sessions: 2, tps: 20 } }],
        candidates,
      }),
    }),
    env,
  );
  assert.equal(response.status, 200);
  const body = (await response.json()) as any;
  assert.deepEqual(body.dispatches.map((dispatch: any) => dispatch.stage), ["AUTHORIZED", "AUTHORIZED"]);
  for (const dispatch of body.dispatches) {
    const started = await simulateNodeStartReceipt(env, dispatch, `node-receipt-${dispatch.dispatch_id}`);
    assert.equal(started.status, 200);
  }
  const allocations = nodeCapacityCells.get("ordinary-dag-node")!.capacity.getReservations();
  assert.deepEqual(allocations.map((allocation) => allocation.kind).sort(), ["dag", "ordinary"]);
  assert.deepEqual(allocations.map((allocation) => allocation.status).sort(), ["RUNNING", "RUNNING"]);
});

test("SchedulerPool greedily orders weighted multi-resource demand against configured node capacity", async () => {
  const { env } = createMockCloudflareCelldEnv();
  const poolKey = schedulerPoolCellKey("eu-central-1", "cpu-memory");
  const candidates = [
    { task_id: "alpha-1", tenant_id: "tenant-alpha", dispatch_id: "dispatch-a1", tenant_weight: 1,
      node_id: "fair-node", node_incarnation: "fair-boot-1", region: "eu-central-1", resource_pool: "cpu-memory", kind: "ordinary",
      task_spec: { metadata: { task_id: "alpha-1", tenant_id: "tenant-alpha" }, spec: {} },
      resources: { cpu: 4, memory_mb: 10, burst_cpu: 1, sessions: 1, tps: 10 } },
    { task_id: "alpha-2", tenant_id: "tenant-alpha", dispatch_id: "dispatch-a2", tenant_weight: 1,
      node_id: "fair-node", node_incarnation: "fair-boot-1", region: "eu-central-1", resource_pool: "cpu-memory", kind: "ordinary",
      task_spec: { metadata: { task_id: "alpha-2", tenant_id: "tenant-alpha" }, spec: {} },
      resources: { cpu: 4, memory_mb: 10, burst_cpu: 1, sessions: 1, tps: 10 } },
    { task_id: "beta-1", tenant_id: "tenant-beta", dispatch_id: "dispatch-b1", tenant_weight: 2,
      node_id: "fair-node", node_incarnation: "fair-boot-1", region: "eu-central-1", resource_pool: "cpu-memory", kind: "ordinary",
      task_spec: { metadata: { task_id: "beta-1", tenant_id: "tenant-beta" }, spec: {} },
      resources: { cpu: 1, memory_mb: 180, burst_cpu: 1, sessions: 1, tps: 10 } },
  ];
  const response = await cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/scheduler/tick?shard=${encodeURIComponent(poolKey)}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      nodes: [{ node_id: "fair-node", incarnation: "fair-boot-1", capacity: { cpu: 10, memory_mb: 200, burst_cpu: 3, sessions: 3, tps: 30 } }],
      candidates,
    }),
  }), env);
  assert.equal(response.status, 200);
  const body = await response.json() as any;
  assert.deepEqual(body.dispatches.map((dispatch: any) => dispatch.dispatch_id), ["dispatch-a1", "dispatch-b1", "dispatch-a2"]);
  assert.deepEqual(body.dispatches.map((dispatch: any) => dispatch.stage), ["AUTHORIZED", "AUTHORIZED", "AUTHORIZED"]);
});

test("NodeCapacity keeps reservations until stop or incarnation isolation evidence", async () => {
  const { env } = createMockCloudflareCelldEnv();
  const stub = env.NODE_CAPACITY_CELL!.get(env.NODE_CAPACITY_CELL!.idFromName("incarnation-node"));
  const call = (path: string, body?: unknown) => stub.fetch(`http://node.internal${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const capacity = { cpu: 2, memory_mb: 1024, burst_cpu: 1, sessions: 1, tps: 10 };
  assert.equal((await call("/nodes/register", { incarnation: "boot-old", capacity })).status, 200);
  assert.equal((await call("/reservations", {
    dispatch_id: "incarnation-dispatch",
    task_id: "incarnation-task",
    tenant_id: "tenant-incarnation",
    kind: "ordinary",
    node_incarnation: "boot-old",
    resources: capacity,
  })).status, 201);

  const unsupportedRelease = await call("/reservations/incarnation-dispatch/release", { node_incarnation: "boot-old" });
  assert.equal(unsupportedRelease.status, 400);
  assert.equal((await call("/allocations").then((response) => response.json()) as any).allocations[0].status, "RESERVED");
  assert.equal((await call("/nodes/register", { incarnation: "boot-new", capacity })).status, 409);

  assert.equal((await call("/nodes/register", {
    incarnation: "boot-new",
    capacity,
    reboot_evidence_id: "host-reboot-991",
  })).status, 200);
  const allocations = (await call("/allocations").then((response) => response.json()) as any).allocations;
  assert.equal(allocations[0].status, "ISOLATED");
  assert.equal(allocations[0].release_evidence_id.includes("host-reboot-991"), true);
  const staleIncarnationReservation = await call("/reservations", {
    dispatch_id: "old-incarnation-dispatch",
    task_id: "another-task",
    tenant_id: "tenant-incarnation",
    kind: "recovery",
    node_incarnation: "boot-old",
    resources: capacity,
  });
  assert.equal(staleIncarnationReservation.status, 409);
});

test("public Worker admission is stored in TenantLedgerCell and remains queryable by tenant", async () => {
  const { env } = createMockCloudflareCelldEnv();
  const cells = new Map<string, TenantLedgerCell>();
  env.TENANT_LEDGER_CELL = {
    idFromName: (name: string) => ({ toString: () => `tenant_${name}`, name }),
    get: (id: { toString(): string; name?: string }) => {
      const tenantId = id.name ?? id.toString();
      if (!cells.has(tenantId)) {
        cells.set(tenantId, new TenantLedgerCell(new MockDurableObjectState(tenantId), env));
      }
      const cell = cells.get(tenantId)!;
      return {
        fetch: (input: RequestInfo, init?: RequestInit) => {
          const req = typeof input === "string" ? new Request(input, init) : input;
          return cell.fetch(req);
        },
      };
    },
  };
  const sampleSpec = yaml.load(
    fs.readFileSync(path.join(__dirname, "..", "..", "specs", "taskspec_v1beta1_example.yaml"), "utf8"),
  ) as Record<string, any>;

  const accepted = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(sampleSpec),
    }),
    env,
  );
  assert.equal(accepted.status, 202);
  const admission = (await accepted.json()) as Record<string, any>;
  assert.equal(admission.accepted, true);
  assert.equal(admission.task_id, sampleSpec.metadata.task_id);
  assert.equal(admission.tenant_id, sampleSpec.metadata.tenant_id);
  assert.equal(admission.idempotency_key, sampleSpec.metadata.idempotency_key);
  assert.equal(admission.runnable_status, "PENDING_DISPATCH");

  const task = await cloudflareCelldWorker.fetch(
    new Request(
      `https://ansts.edge/v1beta1/tasks/${encodeURIComponent(sampleSpec.metadata.task_id)}?tenant_id=${encodeURIComponent(sampleSpec.metadata.tenant_id)}`,
    ),
    env,
  );
  assert.equal(task.status, 200);
  const taskBody = (await task.json()) as Record<string, any>;
  assert.equal(taskBody.task_id, sampleSpec.metadata.task_id);
  assert.equal(taskBody.task_spec.spec.tools_and_saga_config.transactional_policies[3].tool_name, "settlement_ledger_post");

  const replay = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks", { method: "POST", body: JSON.stringify(sampleSpec) }),
    env,
  );
  assert.equal(replay.status, 200);
  assert.equal(((await replay.json()) as any).task_id, sampleSpec.metadata.task_id);

  const changedPayload = structuredClone(sampleSpec);
  changedPayload.metadata.priority_score = Number(changedPayload.metadata.priority_score ?? 0) + 1;
  const conflict = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks", { method: "POST", body: JSON.stringify(changedPayload) }),
    env,
  );
  assert.equal(conflict.status, 409);

  const admissionByKey = await cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/admissions/by-key/${encodeURIComponent(sampleSpec.metadata.idempotency_key)}?tenant_id=${encodeURIComponent(sampleSpec.metadata.tenant_id)}`),
    env,
  );
  assert.equal(admissionByKey.status, 200);
  assert.equal(((await admissionByKey.json()) as any).task_id, sampleSpec.metadata.task_id);

  const concurrentSpec = structuredClone(sampleSpec);
  concurrentSpec.metadata.task_id = "tsk_concurrent_tenant_admission_01";
  concurrentSpec.metadata.idempotency_key = "idem_concurrent_tenant_admission_01";
  const concurrent = await Promise.all(
    Array.from({ length: 8 }, () =>
      cloudflareCelldWorker.fetch(
        new Request("https://ansts.edge/v1beta1/tasks", { method: "POST", body: JSON.stringify(concurrentSpec) }),
        env,
      ),
    ),
  );
  const concurrentBodies = await Promise.all(concurrent.map((response) => response.json() as Promise<Record<string, any>>));
  assert.equal(concurrent.every((response) => response.status === 202 || response.status === 200), true);
  assert.equal(new Set(concurrentBodies.map((body) => body.task_id)).size, 1);
  assert.equal(concurrentBodies.filter((body) => body.deduplicated === false).length, 1);

  const wrongTenant = await cloudflareCelldWorker.fetch(
    new Request(
      `https://ansts.edge/v1beta1/tasks/${encodeURIComponent(sampleSpec.metadata.task_id)}?tenant_id=other-tenant`,
    ),
    env,
  );
  assert.equal(wrongTenant.status, 404);
});

test("public Worker budget routes conserve integer allowances across grant, hold, unknown, settlement and transfer", async () => {
  const { env } = createMockCloudflareCelldEnv();
  const tenantId = "tenant-ledger-budget-test";
  const base = `https://ansts.edge/v1beta1/tenants/${tenantId}`;
  const request = (path: string, method: string, body?: Record<string, unknown>) =>
    cloudflareCelldWorker.fetch(
      new Request(`${base}${path}`, {
        method,
        headers: body ? { "content-type": "application/json" } : undefined,
        body: body ? JSON.stringify(body) : undefined,
      }),
      env,
    );

  const period = await request("/budget-periods/2026-11", "PUT", {
    currency: "USD",
    hard_limit_minor: 1000,
    price_version: "price-v1",
  });
  assert.equal(period.status, 201);

  const grant = {
    period_key: "2026-11",
    task_id: "parent-task",
    grant_id: "grant-parent-1",
    amount_minor: 800,
    currency: "USD",
  };
  assert.equal((await request("/budget-grants", "POST", grant)).status, 201);
  assert.equal((await request("/budget-grants", "POST", grant)).status, 200);
  const overCap = await request("/budget-grants", "POST", {
    ...grant,
    grant_id: "grant-parent-over-cap",
    amount_minor: 300,
  });
  assert.equal(overCap.status, 402);

  const hold = {
    period_key: "2026-11",
    task_id: "parent-task",
    hold_id: "hold-primary-1",
    amount_minor: 300,
    cost_bound_minor: 300,
    currency: "USD",
  };
  assert.equal((await request("/budget-holds", "POST", hold)).status, 201);
  assert.equal((await request("/budget-holds", "POST", hold)).status, 200);
  const heldAccount = await request("/budget-accounts/parent-task?period_key=2026-11", "GET");
  assert.deepEqual(
    (({ available_minor, reserved_minor }: any) => ({ available_minor, reserved_minor }))(await heldAccount.json()),
    { available_minor: 500, reserved_minor: 300 },
  );
  const changedHold = await request("/budget-holds", "POST", { ...hold, amount_minor: 250, cost_bound_minor: 250 });
  assert.equal(changedHold.status, 409);
  assert.equal((await request("/budget-holds/hold-primary-1/unknown", "POST", { reason: "provider timed out" })).status, 201);
  assert.equal((await request("/budget-holds/hold-primary-1/unknown", "POST", { reason: "provider timed out" })).status, 200);
  const unknown = await request("/budget-holds?period_key=2026-11&status=UNKNOWN", "GET");
  assert.equal(unknown.status, 200);
  assert.equal(((await unknown.json()) as any).holds[0].hold_id, "hold-primary-1");

  const settlement = {
    settlement_id: "settlement-primary-1",
    actual_minor: 275,
    currency: "USD",
    usage_reference: "provider-usage-42",
  };
  assert.equal((await request("/budget-holds/hold-primary-1/settlements", "POST", settlement)).status, 201);
  assert.equal((await request("/budget-holds/hold-primary-1/settlements", "POST", settlement)).status, 200);
  assert.equal(
    (await request("/budget-holds/hold-primary-1/settlements", "POST", { ...settlement, actual_minor: 250 })).status,
    409,
  );

  const transfer = {
    period_key: "2026-11",
    transfer_id: "parent-to-child-1",
    source_task_id: "parent-task",
    target_task_id: "child-task",
    amount_minor: 200,
    currency: "USD",
  };
  assert.equal((await request("/allowance-transfers", "POST", transfer)).status, 201);
  assert.equal((await request("/allowance-transfers", "POST", transfer)).status, 200);

  const parent = await request("/budget-accounts/parent-task?period_key=2026-11", "GET");
  const child = await request("/budget-accounts/child-task?period_key=2026-11", "GET");
  assert.deepEqual(
    { available: (await parent.json() as any).available_minor, reserved: 0, spent: 275 },
    { available: 325, reserved: 0, spent: 275 },
  );
  assert.equal((await child.json() as any).available_minor, 200);

  const noBound = await request("/budget-holds", "POST", {
    period_key: "2026-11",
    task_id: "parent-task",
    hold_id: "hold-without-bound",
    amount_minor: 10,
    currency: "USD",
    strict_hard_budget: true,
  });
  assert.equal(noBound.status, 422);
  const fractionalAmount = await request("/budget-holds", "POST", {
    period_key: "2026-11",
    task_id: "parent-task",
    hold_id: "hold-fractional",
    amount_minor: 1.5,
    cost_bound_minor: 1.5,
    currency: "USD",
  });
  assert.equal(fractionalAmount.status, 400);
  const crossTenant = await cloudflareCelldWorker.fetch(
    new Request(`${base}/budget-periods/2026-11?tenant_id=somebody-else`),
    env,
  );
  assert.equal(crossTenant.status, 404);
});

test("09. AgentTaskHarnessCell Brain-Body Disaggregation, Companion Container Burst, SQLite WAL & Fencing", async () => {
  const { env, harnessCells, nodeCapacityCells } = createMockCloudflareCelldEnv();
  const taskId = "tsk_celld_agent_01";
  const taskSpec = {
    metadata: { task_id: taskId, tenant_id: "fin-enterprise" },
    spec: { execution_policy: { max_retries: 3, checkpoint: "turn-boundary" } },
  };
  const dispatch = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/scheduler/tick", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        nodes: [
          { node_id: "celld-node-01", incarnation: "agent-boot-1", capacity: { cpu: 32, memory_mb: 65536, burst_cpu: 16, sessions: 16, tps: 1000 } },
          { node_id: "celld-node-02", incarnation: "agent-boot-1", capacity: { cpu: 32, memory_mb: 65536, burst_cpu: 16, sessions: 16, tps: 1000 } },
        ],
        candidates: [{
          task_id: taskId,
          tenant_id: "fin-enterprise",
          dispatch_id: `ordinary:${taskId}:1`,
          node_id: "celld-node-01",
          node_incarnation: "agent-boot-1",
          kind: "ordinary",
          task_spec: taskSpec,
          resources: { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 },
        }],
      }),
    }),
    env,
  );
  assert.equal(dispatch.status, 200);
  const initialDispatch = ((await dispatch.json()) as any).dispatches[0];
  assert.equal(initialDispatch.stage, "AUTHORIZED");
  const initialReceipt = await simulateNodeStartReceipt(env, initialDispatch, `node-receipt-${initialDispatch.dispatch_id}`);
  assert.equal(initialReceipt.status, 200);
  assert.equal(harnessCells.get(taskId)!.taskCore.getDispatch(initialDispatch.dispatch_id)!.stage, "STARTED");

  // Turn 1: Read-only utility turn -> stays in V8 Cell (no container burst) and routes to GLM-5.3
  const turn1Res = await cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/tasks/${taskId}/turn`, {
      method: "POST",
      body: JSON.stringify({
        epoch: 1,
        turn_seq: 1,
        is_read_only_turn: true,
        workspace_merkle_hash: "merkle_v1",
      }),
    }),
    env,
  );
  assert.equal(turn1Res.status, 200);
  const turn1Body = (await turn1Res.json()) as Record<string, any>;
  assert.equal(turn1Body.routed_model, "GLM-5.3");
  assert.equal(turn1Body.container_burst_triggered, false);

  // Turn 2: Heavy compilation + SAGA_COMPENSATABLE tool -> routes to DeepSeek and bursts companion container
  const turn2Res = await cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/tasks/${taskId}/turn`, {
      method: "POST",
      body: JSON.stringify({
        epoch: 1,
        turn_seq: 2,
        is_read_only_turn: false,
        tool_call: {
          tool_call_id: "call_02",
          tool_name: "git_branch_push",
          effect_class: ToolEffectClass.SAGA_COMPENSATABLE,
          payload: "branch=feat-celld",
          compensation_tool: "git_branch_delete",
          requires_heavy_container: true,
        },
        workspace_merkle_hash: "merkle_v2",
      }),
    }),
    env,
  );
  const turn2Body = (await turn2Res.json()) as Record<string, any>;
  assert.equal(turn2Body.routed_model, "DeepSeek");
  assert.equal(turn2Body.container_burst_triggered, true);
  assert.equal(turn2Body.sqlite_wal_rows, 2);

  // Zero-Token Time-Travel Replay to Turn 1 directly from Cell state
  const replayRes = await cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/tasks/${taskId}/replay`, {
      method: "POST",
      body: JSON.stringify({ target_turn_seq: 1 }),
    }),
    env,
  );
  const replayBody = (await replayRes.json()) as Record<string, any>;
  assert.equal(replayBody.replayed_up_to_turn, 1);
  assert.equal(replayBody.tokens_billed_during_replay, 0);
  assert.equal(replayBody.restored_workspace_hash, "merkle_v1");

  // Simulate recovery dispatch. TaskCell issues business epoch 2; the old allocation stays held as suspect.
  const foRes = await cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/tasks/${taskId}/failover`, {
      method: "POST",
      body: JSON.stringify({ new_node_id: "celld-node-02" }),
    }),
    env,
  );
  const foBody = (await foRes.json()) as Record<string, any>;
  assert.equal(foRes.status, 202);
  assert.equal(foBody.new_epoch, 2);
  const oldAllocation = await nodeCapacityCells.get("celld-node-01")!.fetch(
    new Request(`http://node.internal/reservations/ordinary:${taskId}:1`),
  );
  assert.equal(((await oldAllocation.json()) as any).status, "SUSPECT");
  const recoveryAllocation = await nodeCapacityCells.get("celld-node-02")!.fetch(
    new Request(`http://node.internal/reservations/recovery:${taskId}:2`),
  );
  assert.equal(((await recoveryAllocation.json()) as any).status, "STARTING");
  const recoveryReceipt = await simulateNodeStartReceipt(env, foBody, "node-receipt-recovery-2");
  assert.equal(recoveryReceipt.status, 200);
  assert.equal(((await nodeCapacityCells.get("celld-node-02")!.fetch(
    new Request(`http://node.internal/reservations/recovery:${taskId}:2`),
  ).then((response) => response.json())) as any).status, "RUNNING");

  const recoveredTurn = await cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/tasks/${taskId}/turn`, {
      method: "POST",
      body: JSON.stringify({ epoch: foBody.new_epoch, turn_seq: 3, is_read_only_turn: true, workspace_merkle_hash: "merkle_after_recovery" }),
    }),
    env,
  );
  assert.equal(recoveredTurn.status, 200);
  assert.equal(((await recoveredTurn.json()) as any).task_id, taskId);

  const oldDispatchReplay = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/scheduler/tick", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ candidates: [{
        task_id: taskId,
        tenant_id: "fin-enterprise",
        dispatch_id: `ordinary:${taskId}:1`,
        node_id: "celld-node-01",
        node_incarnation: "agent-boot-1",
        kind: "ordinary",
        task_spec: taskSpec,
        resources: { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 },
      }] }),
    }),
    env,
  );
  assert.equal(oldDispatchReplay.status, 409, await oldDispatchReplay.clone().text());
  assert.equal(((await oldDispatchReplay.json()) as any).error, "STALE_DISPATCH_AUTHORIZATION");

  const zombieRes = await cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/tasks/${taskId}/turn`, {
      method: "POST",
      body: JSON.stringify({
        epoch: 1,
        turn_seq: 4,
        tool_call: {
          tool_call_id: "stale-call-03",
          tool_name: "git_branch_push",
          effect_class: ToolEffectClass.IDEMPOTENT_WRITE,
          payload: "branch=must-not-execute",
        },
      }),
    }),
    env,
  );
  assert.equal(zombieRes.status, 409, await zombieRes.clone().text());
  assert.equal(((await zombieRes.json()) as any).error, "STALE_EXECUTION_EPOCH");

  // Test Hibernatable WebSocket credit ACK & alarm() hibernation on the Cell
  const cell = harnessCells.get(taskId)!;
  const sentFrames: string[] = [];
  await cell.webSocketMessage(
    { send: (d: string) => sentFrames.push(d), close: () => {} },
    JSON.stringify({ type: "CREDIT_ACK", credits: 25 }),
  );
  assert.equal(sentFrames.length, 1);
  assert.ok(sentFrames[0].includes("CREDIT_WINDOW_UPDATED"));

  await cell.alarm();
  assert.equal(cell.task.state, "HIBERNATED");
});


test("10. RustFS + celld --bucket startup specimen & Docker Compose manifest verification", () => {
  const rootDir = path.join(__dirname, "..", "..");
  const scriptPath = path.join(rootDir, "deploy", "rustfs-celld", "start-rustfs-celld.sh");
  const composePath = path.join(rootDir, "deploy", "rustfs-celld", "docker-compose.rustfs-celld.yml");
  const systemdPath = path.join(rootDir, "deploy", "rustfs-celld", "systemd", "celld.service");

  const scriptText = fs.readFileSync(scriptPath, "utf8");
  assert.ok(scriptText.includes('CELLD_BUCKET="${CELLD_BUCKET:-s3://ansts-fleet}"'));
  assert.ok(scriptText.includes('--bucket "${CELLD_BUCKET}"'));
  assert.ok(scriptText.includes('--endpoint "${S3_ENDPOINT}"'));
  assert.ok(scriptText.includes("--internal-listen"));
  assert.ok(scriptText.includes("--advertise"));

  const composeObj = yaml.load(fs.readFileSync(composePath, "utf8")) as Record<string, any>;
  assert.equal(composeObj.services.rustfs.image, "rustfs/rustfs:latest");
  assert.equal(composeObj.services["celld-node-01"].image, "ghcr.io/denoland/celld:latest");
  assert.ok(composeObj.services["celld-node-01"].command.includes("http://rustfs:9000"));

  const systemdText = fs.readFileSync(systemdPath, "utf8");
  assert.ok(systemdText.includes("CELLD_DURABILITY=fleet"));
});
