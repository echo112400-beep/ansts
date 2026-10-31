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

function createMockCloudflareCelldEnv(): {
  env: CloudflareCelldEnv;
  schedulerCell: AnstsSchedulerCell;
  harnessCells: Map<string, AgentTaskHarnessCell>;
  tenantLedgerCells: Map<string, TenantLedgerCell>;
  d1Rows: Array<any[]>;
  queuedMessages: any[];
} {
  const kvMap = new Map<string, string>();
  const r2Map = new Map<string, string>();
  const d1Rows: Array<any[]> = [];
  const queuedMessages: any[] = [];
  const harnessCells = new Map<string, AgentTaskHarnessCell>();
  const tenantLedgerCells = new Map<string, TenantLedgerCell>();

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

  env.SCHEDULER_CELL = {
    idFromName: (name: string) => ({ toString: () => `sched_${name}`, name }),
    get: () => ({
      fetch: (input: RequestInfo, init?: RequestInit) => {
        const req = typeof input === "string" ? new Request(input, init) : input;
        return schedulerCell.fetch(req);
      },
    }),
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

  return { env, schedulerCell, harnessCells, tenantLedgerCells, d1Rows, queuedMessages };
}

test("07. wrangler.json & D1 SQL migration schema verification for celld and Cloudflare Workers", () => {
  const rootDir = path.join(__dirname, "..", "..");
  const wranglerJson = JSON.parse(fs.readFileSync(path.join(rootDir, "wrangler.json"), "utf8"));
  assert.equal(wranglerJson.main, "src/cloudflare/worker.ts");
  assert.equal(wranglerJson.durable_objects.bindings.length, 3);
  assert.equal(
    wranglerJson.durable_objects.bindings.find((binding: Record<string, string>) => binding.name === "TENANT_LEDGER_CELL")?.class_name,
    "TenantLedgerCell",
  );
  assert.equal(wranglerJson.migrations.at(-1).tag, "v2");
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
  const { env, harnessCells } = createMockCloudflareCelldEnv();
  const taskId = "tsk_celld_agent_01";

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

  // Simulate node failover (Epoch 1 -> 2) and verify stale Epoch 1 request gets HTTP 409 StaleFenceRejected
  const foRes = await cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/tasks/${taskId}/failover`, {
      method: "POST",
      body: JSON.stringify({ new_node_id: "celld-node-02" }),
    }),
    env,
  );
  const foBody = (await foRes.json()) as Record<string, any>;
  assert.equal(foBody.new_epoch, 2);

  const zombieRes = await cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/tasks/${taskId}/turn`, {
      method: "POST",
      body: JSON.stringify({ epoch: 1, turn_seq: 3 }),
    }),
    env,
  );
  assert.equal(zombieRes.status, 409);

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
