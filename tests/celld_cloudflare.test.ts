/**
 * End-to-End Integration Tests for `denoland/celld` & Cloudflare Workers Deployment Support.
 */

import { strict as assert } from "assert";
import { createHash } from "node:crypto";
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
  ProviderQuotaCell,
  RoutingConfigCell,
  DagCell,
  TenantDirectoryCell,
} from "../src/index";
import { NodeCapacityCell } from "../src/cloudflare/node_capacity_cell";
import { schedulerPoolCellKey } from "../src/persistence/scheduler-pool";
import { stableTaskOperationId } from "../src/persistence/task-operations";
import { providerQuotaCellKey } from "../src/persistence/provider-quota";
import { TENANT_DIRECTORY_SHARD_COUNT, tenantDirectoryShardIndex } from "../src/cloudflare/tenant_directory_cell";

test("admission rejects names beyond Celld's UTF-8 limit and preserves boundary-length task identity", async () => {
  const sample = yaml.load(fs.readFileSync(path.join(__dirname, "../../specs/taskspec_v1beta1_example.yaml"), "utf8")) as any;
  for (const field of ["tenant_id", "task_id"]) {
    const { env } = createMockCloudflareCelldEnv();
    const spec = JSON.parse(JSON.stringify(sample));
    spec.metadata[field] = "界".repeat(400);
    const result = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks", {
      method: "POST", body: JSON.stringify(spec),
    }), env);
    assert.equal(result.status, 400, `${field} must reject before accepting an identity Celld cannot retain`);
  }
  const { env } = createMockCloudflareCelldEnv();
  const boundary = JSON.parse(JSON.stringify(sample));
  boundary.metadata.task_id = "界".repeat(341) + "x";
  const accepted = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks", {
    method: "POST", body: JSON.stringify(boundary),
  }), env);
  assert.equal(accepted.status, 202);
  const task = await cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/tasks/${encodeURIComponent(boundary.metadata.task_id)}?tenant_id=${encodeURIComponent(boundary.metadata.tenant_id)}`), env);
  assert.equal(task.status, 200);
  assert.equal(((await task.json()) as any).task_id, boundary.metadata.task_id);
});

test("scheduled query repair survives an unavailable scheduler owner", async () => {
  const { env } = createMockCloudflareCelldEnv();
  const spec = yaml.load(fs.readFileSync(path.join(__dirname, "../../specs/taskspec_v1beta1_example.yaml"), "utf8")) as any;
  const accepted = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks", {
    method: "POST", body: JSON.stringify(spec),
  }), env);
  assert.equal(accepted.status, 202);
  env.SCHEDULER_CELL = { idFromName: (name) => ({ name, toString: () => name }),
    get: () => ({ fetch: async () => { throw new Error("scheduler offline"); } }) };
  await cloudflareCelldWorker.scheduled({ scheduledTime: tenantDirectoryShardIndex(spec.metadata.tenant_id) * 60_000, cron: "* * * * *" }, env);
  const dashboard = await cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/tenants/${encodeURIComponent(spec.metadata.tenant_id)}/dashboard/tasks`), env);
  assert.equal(dashboard.status, 200);
  assert.equal(((await dashboard.json()) as any).tasks[0].task_id, spec.metadata.task_id);
});

test("admission response loss remains UNKNOWN and retries recover the original accepted task", async () => {
  const { env, tenantLedgerCells } = createMockCloudflareCelldEnv();
  const spec = yaml.load(fs.readFileSync(path.join(__dirname, "../../specs/taskspec_v1beta1_example.yaml"), "utf8")) as any;
  const owner = env.TENANT_LEDGER_CELL;
  let loseResponse = true;
  env.TENANT_LEDGER_CELL = { idFromName: (name) => owner.idFromName(name), get(id) {
    const stub = owner.get(id);
    return { async fetch(input, init) {
      const result = await stub.fetch(input, init);
      if (loseResponse && String(input).endsWith("/admissions")) {
        loseResponse = false; throw new Error("accepted response lost");
      }
      return result;
    } };
  } };
  const submit = () => cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks", {
    method: "POST", body: JSON.stringify(spec),
  }), env);
  const uncertain = await submit();
  assert.equal(uncertain.status, 503);
  assert.equal(((await uncertain.json()) as any).outcome, "UNKNOWN");
  assert.equal(tenantLedgerCells.size, 1);
  const query = await cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/admissions/by-key/${encodeURIComponent(spec.metadata.idempotency_key)}?tenant_id=${encodeURIComponent(spec.metadata.tenant_id)}`), env);
  const recovered = await query.json() as any;
  assert.equal(recovered.outcome, "ACCEPTED");
  const retried = await submit();
  assert.equal(retried.status, 200);
  assert.equal(((await retried.json()) as any).task_id, recovered.task_id);
});

test("directory failure rejects admission before contacting the tenant ledger", async () => {
  const { env, tenantLedgerCells } = createMockCloudflareCelldEnv();
  const spec = yaml.load(fs.readFileSync(path.join(__dirname, "../../specs/taskspec_v1beta1_example.yaml"), "utf8")) as any;
  env.TENANT_DIRECTORY_CELL = { idFromName: (name) => ({ name, toString: () => name }),
    get: () => ({ fetch: async () => { throw new Error("directory unavailable"); } }) };
  const result = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks", {
    method: "POST", body: JSON.stringify(spec),
  }), env);
  assert.equal(result.status, 503);
  assert.equal(((await result.json()) as any).outcome, "NOT_ACCEPTED");
  assert.equal(tenantLedgerCells.size, 0);
});

function createMockCloudflareCelldEnv(): {
  env: CloudflareCelldEnv;
  schedulerCell: AnstsSchedulerCell;
  schedulerCells: Map<string, AnstsSchedulerCell>;
  harnessCells: Map<string, AgentTaskHarnessCell>;
  tenantLedgerCells: Map<string, TenantLedgerCell>;
  nodeCapacityCells: Map<string, NodeCapacityCell>;
  providerQuotaCells: Map<string, ProviderQuotaCell>;
  routingConfigCells: Map<string, RoutingConfigCell>;
  dagCells: Map<string, DagCell>;
  tenantDirectoryCells: Map<string, TenantDirectoryCell>;
  d1Rows: Array<any[]>;
  tenantTaskProjections: Map<string, Record<string, any>>;
  taskEventProjections: Map<string, Record<string, any>>;
  queuedMessages: any[];
  taskOperationResults: Map<string, unknown>;
  taskExecutorCalls: { execute: number; compensate: number };
  modelProviderCalls: { calls: number };
} {
  const kvMap = new Map<string, string>();
  const r2Map = new Map<string, Uint8Array>();
  const d1Rows: Array<any[]> = [];
  const tenantTaskProjections = new Map<string, Record<string, any>>();
  const taskEventProjections = new Map<string, Record<string, any>>();
  const queuedMessages: any[] = [];
  const taskOperationResults = new Map<string, unknown>();
  const taskExecutorCalls = { execute: 0, compensate: 0 };
  const modelProviderCalls = { calls: 0 };
  const harnessCells = new Map<string, AgentTaskHarnessCell>();
  const tenantLedgerCells = new Map<string, TenantLedgerCell>();
  const nodeCapacityCells = new Map<string, NodeCapacityCell>();
  const providerQuotaCells = new Map<string, ProviderQuotaCell>();
  const routingConfigCells = new Map<string, RoutingConfigCell>();
  const dagCells = new Map<string, DagCell>();
  const tenantDirectoryCells = new Map<string, TenantDirectoryCell>();

  const env: CloudflareCelldEnv = {
    TEST_SIMULATOR_MODE: true,
    PRIMARY_MODEL_POOL: "DeepSeek",
    FAST_UTILITY_POOL: "GLM-5.3",
    FALLBACK_MODEL_POOL: "Qwen-3.8",
    WEBHOOK_HMAC_SECRET: "whsec_celld_test",
    DAG_VOUCHER_SECRET: "test-only-dag-voucher-secret-0123456789abcdef",
    DEEPSEEK_CHAT_COMPLETIONS_URL: "https://provider.internal/v1/chat/completions",
    DEEPSEEK_API_KEY: "test-deepseek-key",
    GLM53_CHAT_COMPLETIONS_URL: "https://provider.internal/v1/chat/completions",
    GLM53_API_KEY: "test-glm-key",
    QWEN_CHAT_COMPLETIONS_URL: "https://provider.internal/v1/chat/completions",
    QWEN_API_KEY: "test-qwen-key",
    MODEL_PROVIDER_PROXY: {
      fetch: async (_input: RequestInfo, _init?: RequestInit) => {
        modelProviderCalls.calls += 1;
        const requestId = `provider-request-${modelProviderCalls.calls}`;
        return new Response([
          `data: ${JSON.stringify({ id: requestId, model: "test-model", choices: [{ delta: { content: "durable answer" } }] })}\n\n`,
          `data: ${JSON.stringify({ id: requestId, choices: [], usage: { prompt_tokens: 7, completion_tokens: 3 } })}\n\n`,
          "data: [DONE]\n\n",
        ].join(""), { headers: { "content-type": "text/event-stream", "x-request-id": requestId } });
      },
    },
    SCHEDULER_CELL: null as any,
    AGENT_HARNESS_CELL: null as any,
    TENANT_LEDGER_CELL: null as any,
    TENANT_DIRECTORY_CELL: null as any,
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
      put: async (key: string, value: string | ArrayBuffer | ArrayBufferView, options?: { onlyIf?: { etagDoesNotMatch?: string } }) => {
        if (options?.onlyIf?.etagDoesNotMatch === "*" && r2Map.has(key)) return null;
        const bytes = typeof value === "string"
          ? new TextEncoder().encode(value)
          : value instanceof ArrayBuffer
            ? new Uint8Array(value)
            : new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
        r2Map.set(key, new Uint8Array(bytes));
        return { key };
      },
      get: async (key: string) => {
        const v = r2Map.get(key);
        if (!v) return null;
        const copy = new Uint8Array(v);
        return {
          key,
          body: new TextDecoder().decode(copy),
          text: async () => new TextDecoder().decode(copy),
          arrayBuffer: async () => copy.buffer.slice(copy.byteOffset, copy.byteOffset + copy.byteLength),
        };
      },
      list: async () => ({
        objects: Array.from(r2Map.keys()).map((key) => ({ key })),
      }),
      delete: async (key: string) => { r2Map.delete(key); },
    },
    ANSTS_D1: {
      prepare: (query: string) => {
        let bound: any[] = [];
        const stmt = {
          bind: (...values: any[]) => {
            bound = values;
            return stmt;
          },
          run: async () => {
            if (query.includes("INSERT INTO tenant_task_projection")) {
              const [tenant_id, task_id, admission_status, runnable_status, accepted_at, lifecycle_state,
                business_epoch, last_turn_seq, execution_version, projected_at] = bound;
              const key = `${tenant_id}:${task_id}`;
              const prior = tenantTaskProjections.get(key);
              if (!prior || Number(execution_version) >= Number(prior.execution_version)) {
                tenantTaskProjections.set(key, { tenant_id, task_id, admission_status, runnable_status, accepted_at,
                  lifecycle_state, business_epoch, last_turn_seq, execution_version, projected_at });
              }
            } else if (query.includes("INSERT OR IGNORE INTO task_event_projection")) {
              const [event_id, tenant_id, task_id, event_type, aggregate_version, payload_digest, payload_json, created_at] = bound;
              if (!taskEventProjections.has(String(event_id))) taskEventProjections.set(String(event_id), {
                event_id, tenant_id, task_id, event_type, aggregate_version, payload_digest, payload_json, created_at,
              });
            }
            d1Rows.push(bound);
            return { success: true };
          },
          all: async <T = any>() => {
            if (query.includes("FROM tenant_task_projection")) {
              if (query.includes("GROUP BY tenant_id")) {
                const rows = Array.from(tenantTaskProjections.values()).sort((a, b) => Number(a.projected_at) - Number(b.projected_at));
                return { results: [...new Set(rows.map((row) => row.tenant_id))].slice(0, Number(bound[0])).map((tenant_id) => ({ tenant_id })) as T[] };
              }
              const rows = Array.from(tenantTaskProjections.values()).filter((row) => row.tenant_id === bound[0])
                .sort((a, b) => Number(b.accepted_at) - Number(a.accepted_at)).slice(0, Number(bound[1]));
              return { results: rows as T[] };
            }
            if (query.includes("FROM task_event_projection")) {
              return { results: (taskEventProjections.has(String(bound[0])) ? [taskEventProjections.get(String(bound[0]))!] : []) as T[] };
            }
            return { results: d1Rows as T[] };
          },
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

  env.TASK_OPERATION_EXECUTOR = {
    fetch: async (input: RequestInfo, init?: RequestInit) => {
      const request = input instanceof Request ? input : new Request(input, init);
      const url = new URL(request.url);
      if (request.method === "POST" && url.pathname === "/v1/operations/execute") {
        const body = await request.json() as Record<string, any>;
        taskExecutorCalls.execute += 1;
        const prior = taskOperationResults.get(String(body.operation_id));
        if (prior !== undefined) return new Response(JSON.stringify({ result: prior }), { headers: { "content-type": "application/json" } });
        const result = { tool_name: body.tool_name, args: body.args, accepted: true };
        taskOperationResults.set(String(body.operation_id), result);
        return new Response(JSON.stringify({ result }), { headers: { "content-type": "application/json" } });
      }
      if (request.method === "POST" && url.pathname === "/v1/operations/compensate") {
        taskExecutorCalls.compensate += 1;
        return new Response(JSON.stringify({ status: "COMPENSATED" }), { headers: { "content-type": "application/json" } });
      }
      const operationMatch = url.pathname.match(/^\/v1\/operations\/([^/]+)$/);
      if (request.method === "GET" && operationMatch) {
        const result = taskOperationResults.get(decodeURIComponent(operationMatch[1]));
        const body = result === undefined ? { status: "NOT_FOUND", authoritative: true } : { status: "FOUND", result };
        return new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });
      }
      return new Response(JSON.stringify({ error: "NotFound" }), { status: 404, headers: { "content-type": "application/json" } });
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

  env.TENANT_DIRECTORY_CELL = {
    idFromName: (name: string) => ({ toString: () => `tenant_directory_${name}`, name }),
    get: (id: { toString(): string; name?: string }) => {
      const shardKey = id.name ?? id.toString();
      if (!tenantDirectoryCells.has(shardKey)) {
        tenantDirectoryCells.set(shardKey, new TenantDirectoryCell(new MockDurableObjectState(shardKey), env));
      }
      const cell = tenantDirectoryCells.get(shardKey)!;
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

  env.PROVIDER_QUOTA_CELL = {
    idFromName: (name: string) => ({ toString: () => `quota_${name}`, name }),
    get: (id: { toString(): string; name?: string }) => {
      const key = id.name ?? id.toString();
      if (!providerQuotaCells.has(key)) providerQuotaCells.set(key, new ProviderQuotaCell(new MockDurableObjectState(key), env));
      const cell = providerQuotaCells.get(key)!;
      return {
        fetch: (input: RequestInfo, init?: RequestInit) => {
          const req = typeof input === "string" ? new Request(input, init) : input;
          return cell.fetch(req);
        },
      };
    },
  };

  env.ROUTING_CONFIG_CELL = {
    idFromName: (name: string) => ({ toString: () => `route_${name}`, name }),
    get: (id: { toString(): string; name?: string }) => {
      const key = id.name ?? id.toString();
      if (!routingConfigCells.has(key)) routingConfigCells.set(key, new RoutingConfigCell(new MockDurableObjectState(key), env));
      const cell = routingConfigCells.get(key)!;
      return {
        fetch: (input: RequestInfo, init?: RequestInit) => {
          const req = typeof input === "string" ? new Request(input, init) : input;
          return cell.fetch(req);
        },
      };
    },
  };

  env.DAG_CELL = {
    idFromName: (name: string) => ({ toString: () => `dag_${name}`, name }),
    get: (id: { toString(): string; name?: string }) => {
      const key = id.name ?? id.toString();
      if (!dagCells.has(key)) dagCells.set(key, new DagCell(new MockDurableObjectState(key), env));
      const cell = dagCells.get(key)!;
      return {
        fetch: (input: RequestInfo, init?: RequestInit) => {
          const req = typeof input === "string" ? new Request(input, init) : input;
          return cell.fetch(req);
        },
      };
    },
  };

  return { env, schedulerCell, schedulerCells, harnessCells, tenantLedgerCells, nodeCapacityCells, providerQuotaCells, routingConfigCells, dagCells, tenantDirectoryCells, d1Rows, tenantTaskProjections, taskEventProjections, queuedMessages, taskOperationResults, taskExecutorCalls, modelProviderCalls };
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
  assert.equal(wranglerJson.durable_objects.bindings.length, 8);
  assert.equal(
    wranglerJson.durable_objects.bindings.find((binding: Record<string, string>) => binding.name === "TENANT_LEDGER_CELL")?.class_name,
    "TenantLedgerCell",
  );
  assert.equal(
    wranglerJson.durable_objects.bindings.find((binding: Record<string, string>) => binding.name === "NODE_CAPACITY_CELL")?.class_name,
    "NodeCapacityCell",
  );
  assert.equal(wranglerJson.migrations.at(-1).tag, "v6");
  assert.ok(wranglerJson.migrations.at(-4).new_sqlite_classes.includes("NodeCapacityCell"));
  assert.ok(wranglerJson.migrations.at(-3).new_sqlite_classes.includes("ProviderQuotaCell"));
  assert.ok(wranglerJson.migrations.at(-3).new_sqlite_classes.includes("RoutingConfigCell"));
  assert.ok(wranglerJson.migrations.at(-2).new_sqlite_classes.includes("DagCell"));
  assert.ok(wranglerJson.migrations.at(-1).new_sqlite_classes.includes("TenantDirectoryCell"));
  const wranglerToml = fs.readFileSync(path.join(rootDir, "wrangler.toml"), "utf8");
  assert.match(wranglerToml, /name = "NODE_CAPACITY_CELL"\s+class_name = "NodeCapacityCell"/);
  assert.match(wranglerToml, /tag = "v3"\s+new_sqlite_classes = \["NodeCapacityCell"\]/);
  assert.match(wranglerToml, /name = "PROVIDER_QUOTA_CELL"\s+class_name = "ProviderQuotaCell"/);
  assert.match(wranglerToml, /name = "ROUTING_CONFIG_CELL"\s+class_name = "RoutingConfigCell"/);
  assert.match(wranglerToml, /name = "DAG_CELL"\s+class_name = "DagCell"/);
  assert.match(wranglerToml, /name = "TENANT_DIRECTORY_CELL"\s+class_name = "TenantDirectoryCell"/);
  assert.equal(wranglerJson.d1_databases[0].binding, "ANSTS_D1");
  assert.equal(wranglerJson.kv_namespaces[0].binding, "ANSTS_KV");
  assert.equal(wranglerJson.r2_buckets[0].binding, "ANSTS_CHECKPOINTS_R2");

  const sqlMig = fs.readFileSync(path.join(rootDir, "migrations", "0001_init_ansts.sql"), "utf8");
  assert.ok(sqlMig.includes("CREATE TABLE IF NOT EXISTS task_audit_events"));
  const projectionMigration = fs.readFileSync(path.join(rootDir, "migrations", "0002_rebuildable_task_projections.sql"), "utf8");
  assert.ok(projectionMigration.includes("CREATE TABLE IF NOT EXISTS tenant_task_projection"));
  assert.ok(projectionMigration.includes("CREATE TABLE IF NOT EXISTS task_event_projection"));
  assert.equal(TENANT_DIRECTORY_SHARD_COUNT, 16);
});

test("public Worker routes versioned model pricing and quota policy to stable durable owners", async () => {
  const { env, providerQuotaCells, routingConfigCells } = createMockCloudflareCelldEnv();
  const tenantId = "tenant-routing-1";
  const routeConfig = {
    tenantId,
    policyVersion: "model-policy-7",
    routes: [{
      poolName: "DeepSeek",
      provider: "openai-compatible",
      modelId: "deepseek-v4",
      quotaDomain: "shared-account-a",
      quotaDemandTokensPerSecond: 40,
      maxInputTokens: 8000,
      maxOutputTokens: 2000,
      pricing: { priceVersion: "usd-2026-10", currency: "USD", inputMinorPerMillionTokens: 125, outputMinorPerMillionTokens: 450 },
    }],
  };
  const publish = (operationId: string, expectedVersion: number, configuration: unknown) => cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/tenants/${tenantId}/routing-config/versions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ operationId, expectedVersion, configuration }),
    }),
    env,
  );
  const version1 = await publish("routing-publish-1", 0, routeConfig);
  assert.equal(version1.status, 201, await version1.clone().text());
  const replacement = { ...routeConfig, policyVersion: "model-policy-8", routes: routeConfig.routes.map((route) => ({
    ...route,
    pricing: { ...route.pricing, priceVersion: "usd-2026-11", inputMinorPerMillionTokens: 150 },
  })) };
  const version2 = await publish("routing-publish-2", 1, replacement);
  assert.equal(version2.status, 201);
  const historical = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tenants/${tenantId}/routing-config/versions/1`,
  ), env);
  assert.equal(((await historical.json()) as any).routes[0].pricing.priceVersion, "usd-2026-10");
  assert.equal(routingConfigCells.size, 1);
  const wrongTenant = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tenants/${tenantId}/routing-config/current?tenant_id=another-tenant`,
  ), env);
  assert.equal(wrongTenant.status, 404);

  const policy = { requestsPerMinute: 120, tokensPerSecond: 800, grantTtlMs: 500 };
  const configureQuota = (quotaDomain: string, operationId: string) => cloudflareCelldWorker.fetch(
    new Request(`https://ansts.edge/v1beta1/providers/openai-compatible/quota-domains/${quotaDomain}/policy`, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ operationId, expectedVersion: 0, policy }),
    }),
    env,
  );
  assert.equal((await configureQuota("shared-account-a", "quota-policy-a")).status, 201);
  assert.equal((await configureQuota("shared-account-b", "quota-policy-b")).status, 201);
  assert.equal(providerQuotaCells.size, 2, "each provider quota domain has a separate durable Cell owner");
  const keyA = providerQuotaCellKey("openai-compatible", "shared-account-a");
  assert.equal(providerQuotaCells.get(keyA)?.owner.getPolicy()?.policy.tokensPerSecond, 800);

  const grant = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/providers/openai-compatible/quota-domains/shared-account-a/grants`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ requestId: "tenant-routing-1:task-1:call-1", tenantId, provider: "openai-compatible", routeVersion: 2, requests: 1, tokensPerSecond: 40 }),
    }),
    env,
  );
  assert.equal(grant.status, 201, await grant.clone().text());
  const queryGrant = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/providers/openai-compatible/quota-domains/shared-account-a/grants?request_id=tenant-routing-1%3Atask-1%3Acall-1`,
  ), env);
  assert.equal(((await queryGrant.json()) as any).routeVersion, 2);
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
  assert.equal(d1Rows.filter((row) => row[3] === "task.admitted").length, 1,
    "the scheduled projection refresh may also write D1 before the synthetic queue event is consumed");
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
  const eventProjection = await cloudflareCelldWorker.fetch(new Request(
    "https://ansts.edge/v1beta1/tasks/dispatch-task-1/events?tenant_id=tenant-a"), env);
  assert.equal(eventProjection.status, 200);
  const commandReplay = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks/dispatch-task-1/start-commands"), env);
  assert.deepEqual(((await commandReplay.json()) as any).commands[0], startCommandBody.commands[0]);
  const projectionAckForStart = await taskCell.fetch(new Request("http://task.internal/events/ack", {
    method: "POST", body: JSON.stringify({ tenant_id: candidate.tenant_id,
      event_ids: [startCommandBody.commands[0].eventId] }),
  }));
  assert.equal(projectionAckForStart.status, 409, "only a NodeAgent start receipt can acknowledge a start command");
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

test("scheduler rejects DAG starts that do not present a redeemed durable voucher", async () => {
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
  assert.equal(body.dispatches.filter((dispatch: any) => dispatch.stage === "AUTHORIZED").length, 1);
  assert.equal(body.dispatches.filter((dispatch: any) => dispatch.status === "BLOCKED_DAG_VOUCHER").length, 1);
  const ordinary = body.dispatches.find((dispatch: any) => dispatch.stage === "AUTHORIZED");
  assert.equal((await simulateNodeStartReceipt(env, ordinary, `node-receipt-${ordinary.dispatch_id}`)).status, 200);
  const allocations = nodeCapacityCells.get("ordinary-dag-node")!.capacity.getReservations();
  assert.deepEqual(allocations.map((allocation) => allocation.kind), ["ordinary"]);
  assert.deepEqual(allocations.map((allocation) => allocation.status), ["RUNNING"]);
});

test("public DagCell yields its parent before transferring allowance and gates DAG dispatch through NodeCapacity", async () => {
  const { env, dagCells, tenantLedgerCells, nodeCapacityCells } = createMockCloudflareCelldEnv();
  const tenantId = "tenant-dag-public";
  const periodKey = "2026-11";
  const parentTaskId = "dag-parent-public";
  const childTaskId = "dag-child-public";
  const tenantBase = `https://ansts.edge/v1beta1/tenants/${tenantId}`;
  const ledgerRequest = (path: string, method: string, body?: unknown) => cloudflareCelldWorker.fetch(new Request(`${tenantBase}${path}`, {
    method, headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  }), env);
  const admissionSpec = (taskId: string, idempotencyKey: string) => {
    const spec = structuredClone(yaml.load(fs.readFileSync(path.join(__dirname, "..", "..", "specs", "taskspec_v1beta1_example.yaml"), "utf8")) as any);
    spec.metadata.task_id = taskId;
    spec.metadata.tenant_id = tenantId;
    spec.metadata.idempotency_key = idempotencyKey;
    return spec;
  };
  for (const [taskId, key] of [[parentTaskId, "dag-parent-idem"], [childTaskId, "dag-child-idem"]]) {
    const accepted = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/tasks", {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(admissionSpec(taskId, key)),
    }), env);
    assert.equal(accepted.status, 202, await accepted.clone().text());
  }
  assert.equal((await ledgerRequest(`/budget-periods/${periodKey}`, "PUT", { currency: "USD", hard_limit_minor: 1000, price_version: "dag-price-v1" })).status, 201);
  assert.equal((await ledgerRequest("/budget-grants", "POST", { period_key: periodKey, task_id: parentTaskId, grant_id: "dag-parent-grant", amount_minor: 800, currency: "USD" })).status, 201);

  const resources = { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 };
  const parentDispatchResponse = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/scheduler/tick", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ nodes: [{ node_id: "dag-parent-node", incarnation: "dag-parent-boot", capacity: resources }], candidates: [{
      task_id: parentTaskId, tenant_id: tenantId, dispatch_id: "dag-parent-dispatch", node_id: "dag-parent-node",
      node_incarnation: "dag-parent-boot", kind: "ordinary", resources,
      task_spec: { metadata: { task_id: parentTaskId, tenant_id: tenantId }, spec: {} },
    }] }),
  }), env);
  assert.equal(parentDispatchResponse.status, 200, await parentDispatchResponse.clone().text());
  const parentDispatch = ((await parentDispatchResponse.json()) as any).dispatches[0];
  assert.equal((await simulateNodeStartReceipt(env, parentDispatch, "dag-parent-start-receipt")).status, 200);

  const ledgerCell = tenantLedgerCells.get(tenantId)!;
  const originalLedgerFetch = ledgerCell.fetch.bind(ledgerCell);
  let rejectFirstTransfer = true;
  ledgerCell.fetch = async (request: Request) => {
    if (rejectFirstTransfer && new URL(request.url).pathname === "/allowance-transfers") {
      rejectFirstTransfer = false;
      return new Response(JSON.stringify({ error: "INJECTED_TRANSFER_RESPONSE_LOSS" }), { status: 503 });
    }
    return originalLedgerFetch(request);
  };

  const createBody = {
    tenant_id: tenantId, dag_id: "dag-public-1", parent_task_id: parentTaskId,
    parent_epoch: parentDispatch.business_epoch, parent_turn_seq: 0, period_key: periodKey, currency: "USD",
    children: [{ task_id: childTaskId, delegated_tps: 10, delegated_tokens: 1000, allowance_minor: 600 }],
  };
  const interruptedCreate = await cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/dags/${createBody.dag_id}`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(createBody),
  }), env);
  assert.equal(interruptedCreate.status, 503, "a failed allowance transfer remains an unresolved durable DAG intent");
  const interruptedState = await cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/dags/${createBody.dag_id}/state?tenant_id=${tenantId}`), env);
  const interruptedDag = (await interruptedState.json()) as any;
  assert.equal(interruptedDag.parent_yielded, true, "parent suspension is confirmed before any child allowance transfer");
  assert.equal(interruptedDag.state, "TRANSFERRING");
  const notYetTransferred = await ledgerRequest(`/budget-accounts/${childTaskId}?period_key=${periodKey}`, "GET");
  assert.equal(((await notYetTransferred.json()) as any).available_minor, 0);

  const resumedCreate = await cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/dags/${createBody.dag_id}`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(createBody),
  }), env);
  assert.equal(resumedCreate.status, 200, await resumedCreate.clone().text());
  const created = resumedCreate;
  assert.equal(created.status, 200, await created.clone().text());
  const createdBody = (await created.json()) as any;
  assert.equal(createdBody.parent_yielded, true);
  assert.equal(createdBody.state, "ACTIVE");
  assert.equal(createdBody.children[0].transfer_status, "TRANSFERRED");
  const parentAccount = await ledgerRequest(`/budget-accounts/${parentTaskId}?period_key=${periodKey}`, "GET");
  assert.equal(parentAccount.status, 200);
  assert.equal(((await parentAccount.json()) as any).available_minor, 200);
  const childAccount = await ledgerRequest(`/budget-accounts/${childTaskId}?period_key=${periodKey}`, "GET");
  assert.equal(((await childAccount.json()) as any).available_minor, 600);

  const voucher = createdBody.children[0];
  const redeemUrl = `https://ansts.edge/v1beta1/dags/${createBody.dag_id}/children/${childTaskId}/redeem`;
  const invalidVoucher = await cloudflareCelldWorker.fetch(new Request(redeemUrl, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ tenant_id: tenantId, voucher_id: voucher.voucher_id, signature: "invalid" }),
  }), env);
  assert.equal(invalidVoucher.status, 403);
  const redeemed = await cloudflareCelldWorker.fetch(new Request(redeemUrl, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ tenant_id: tenantId, voucher_id: voucher.voucher_id, signature: voucher.voucher_signature }),
  }), env);
  assert.equal(redeemed.status, 200, await redeemed.clone().text());
  assert.equal(((await redeemed.json()) as any).status, "REDEEMED");

  const childDispatchResponse = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/scheduler/tick", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ nodes: [{ node_id: "dag-child-node", incarnation: "dag-child-boot", capacity: { ...resources, cpu: 2, memory_mb: 1024, sessions: 2, tps: 20 } }], candidates: [{
      task_id: childTaskId, tenant_id: tenantId, dispatch_id: "dag-child-dispatch", node_id: "dag-child-node",
      node_incarnation: "dag-child-boot", kind: "dag", dag_id: createBody.dag_id, voucher_id: voucher.voucher_id,
      resources, task_spec: { metadata: { task_id: childTaskId, tenant_id: tenantId }, spec: {} },
    }] }),
  }), env);
  assert.equal(childDispatchResponse.status, 200, await childDispatchResponse.clone().text());
  const childDispatch = ((await childDispatchResponse.json()) as any).dispatches[0];
  assert.equal(childDispatch.stage, "AUTHORIZED");
  assert.equal((await simulateNodeStartReceipt(env, childDispatch, "dag-child-start-receipt")).status, 200);
  assert.equal(nodeCapacityCells.get("dag-child-node")!.capacity.getReservation("dag-child-dispatch")!.status, "RUNNING");

  const childComplete = await cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/tasks/${childTaskId}/complete?tenant_id=${tenantId}`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ tenant_id: tenantId, epoch: childDispatch.business_epoch, status: "SUCCEEDED" }),
  }), env);
  assert.equal(childComplete.status, 200, await childComplete.clone().text());
  const childObservation = () => cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/dags/${createBody.dag_id}/children/${childTaskId}/observations`, {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ tenant_id: tenantId, event_id: "dag-child-succeeded-1", aggregate_version: 1, state: "SUCCEEDED" }),
  }), env);
  const joined = await childObservation();
  assert.equal(joined.status, 200, await joined.clone().text());
  assert.equal(((await joined.json()) as any).dag.state, "COMPLETED");
  const duplicateJoin = await childObservation();
  assert.equal(duplicateJoin.status, 200, await duplicateJoin.clone().text());
  assert.equal(((await duplicateJoin.json()) as any).duplicate, true);

  const stopped = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/scheduler/stopped", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ dispatch_id: childDispatch.dispatch_id, task_id: childTaskId, tenant_id: tenantId,
      node_id: childDispatch.node_id, node_incarnation: childDispatch.node_incarnation,
      region: childDispatch.region, resource_pool: childDispatch.resource_pool, business_epoch: childDispatch.business_epoch,
      evidence_type: "STOPPED", evidence_id: "dag-child-stop-proof" }),
  }), env);
  assert.equal(stopped.status, 200, await stopped.clone().text());
  assert.equal(nodeCapacityCells.get("dag-child-node")!.capacity.getReservation("dag-child-dispatch")!.status, "RELEASED");
  const returnUrl = `https://ansts.edge/v1beta1/dags/${createBody.dag_id}/children/${childTaskId}/return-allowance`;
  const returned = await cloudflareCelldWorker.fetch(new Request(returnUrl, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ tenant_id: tenantId }),
  }), env);
  assert.equal(returned.status, 200, await returned.clone().text());
  assert.equal(((await returned.json()) as any).amount_minor, 600);
  const duplicateReturn = await cloudflareCelldWorker.fetch(new Request(returnUrl, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ tenant_id: tenantId }),
  }), env);
  assert.equal(duplicateReturn.status, 200);
  assert.equal(((await duplicateReturn.json()) as any).amount_minor, 600);

  const replayCreate = await cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/dags/${createBody.dag_id}`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(createBody),
  }), env);
  assert.equal(replayCreate.status, 200);
  const replayParentAccount = await ledgerRequest(`/budget-accounts/${parentTaskId}?period_key=${periodKey}`, "GET");
  assert.equal(((await replayParentAccount.json()) as any).available_minor, 800);
  const replayChildAccount = await ledgerRequest(`/budget-accounts/${childTaskId}?period_key=${periodKey}`, "GET");
  assert.equal(((await replayChildAccount.json()) as any).available_minor, 0);
  const crossTenant = await cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/dags/${createBody.dag_id}/state?tenant_id=another-tenant`), env);
  assert.equal(crossTenant.status, 404);
  assert.equal(dagCells.size, 1);

  const resumedParentDispatch = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/scheduler/tick", {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({
      nodes: [{ node_id: "dag-parent-resume-node", incarnation: "dag-parent-resume-boot", capacity: resources }],
      candidates: [{ task_id: parentTaskId, tenant_id: tenantId, dispatch_id: "dag-parent-resume-dispatch",
        node_id: "dag-parent-resume-node", node_incarnation: "dag-parent-resume-boot", kind: "recovery",
        resources, task_spec: { metadata: { task_id: parentTaskId, tenant_id: tenantId }, spec: {} } }],
    }),
  }), env);
  assert.equal(resumedParentDispatch.status, 200, await resumedParentDispatch.clone().text());
  assert.equal(((await resumedParentDispatch.json()) as any).dispatches[0].stage, "AUTHORIZED");
});

test("DAG boost retries a committed TenantLedger transfer under the same durable identity", async () => {
  const { env, tenantLedgerCells } = createMockCloudflareCelldEnv();
  const tenantId = "tenant-dag-boost-replay";
  const periodKey = "2026-11";
  const dagId = "dag-boost-replay";
  const parentTaskId = "boost-parent";
  const sourceTaskId = "boost-source";
  const targetTaskId = "boost-target";
  const request = (url: string, method = "GET", body?: unknown) => cloudflareCelldWorker.fetch(new Request(url, {
    method,
    headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  }), env);
  const taskSpec = (taskId: string) => {
    const spec = structuredClone(yaml.load(fs.readFileSync(path.join(__dirname, "..", "..", "specs", "taskspec_v1beta1_example.yaml"), "utf8")) as any);
    spec.metadata.task_id = taskId;
    spec.metadata.tenant_id = tenantId;
    spec.metadata.idempotency_key = `${taskId}-admission`;
    return spec;
  };
  for (const taskId of [parentTaskId, sourceTaskId, targetTaskId]) {
    const accepted = await request("https://ansts.edge/v1beta1/tasks", "POST", taskSpec(taskId));
    assert.equal(accepted.status, 202, await accepted.clone().text());
  }
  const tenantUrl = `${tenantId}`;
  const budgetPeriod = await request(`https://ansts.edge/v1beta1/tenants/${tenantUrl}/budget-periods/${periodKey}`, "PUT", {
    currency: "USD", hard_limit_minor: 1000, price_version: "dag-boost-test",
  });
  assert.equal(budgetPeriod.status, 201);
  const grant = await request(`https://ansts.edge/v1beta1/tenants/${tenantId}/budget-grants`, "POST", {
    period_key: periodKey, task_id: parentTaskId, grant_id: "boost-parent-grant", amount_minor: 800, currency: "USD",
  });
  assert.equal(grant.status, 201);

  const resources = { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 };
  const parentTick = await request("https://ansts.edge/v1beta1/scheduler/tick", "POST", {
    nodes: [{ node_id: "boost-parent-node", incarnation: "boost-parent-boot", capacity: resources }],
    candidates: [{ task_id: parentTaskId, tenant_id: tenantId, dispatch_id: "boost-parent-dispatch",
      node_id: "boost-parent-node", node_incarnation: "boost-parent-boot", kind: "ordinary", resources,
      task_spec: { metadata: { task_id: parentTaskId, tenant_id: tenantId }, spec: {} } }],
  });
  assert.equal(parentTick.status, 200, await parentTick.clone().text());
  const parentDispatch = ((await parentTick.json()) as any).dispatches[0];
  assert.equal((await simulateNodeStartReceipt(env, parentDispatch, "boost-parent-start")).status, 200);

  const created = await request(`https://ansts.edge/v1beta1/dags/${dagId}`, "POST", {
    tenant_id: tenantId, dag_id: dagId, parent_task_id: parentTaskId, parent_epoch: parentDispatch.business_epoch,
    parent_turn_seq: 0, period_key: periodKey, currency: "USD",
    children: [
      { task_id: sourceTaskId, branch_id: "source", delegated_tps: 10, delegated_tokens: 1000, allowance_minor: 300 },
      { task_id: targetTaskId, branch_id: "target", delegated_tps: 10, delegated_tokens: 1000, allowance_minor: 300 },
    ],
  });
  assert.equal(created.status, 200, await created.clone().text());
  const dag = await created.json() as any;
  const sourceVoucher = dag.children.find((child: any) => child.task_id === sourceTaskId);
  const targetVoucher = dag.children.find((child: any) => child.task_id === targetTaskId);
  for (const [taskId, voucher] of [[sourceTaskId, sourceVoucher], [targetTaskId, targetVoucher]] as const) {
    const redeemed = await request(`https://ansts.edge/v1beta1/dags/${dagId}/children/${taskId}/redeem`, "POST", {
      tenant_id: tenantId, voucher_id: voucher.voucher_id, signature: voucher.voucher_signature,
    });
    assert.equal(redeemed.status, 200, await redeemed.clone().text());
  }

  const childTaskIds = [sourceTaskId, targetTaskId];
  const childTick = await request("https://ansts.edge/v1beta1/scheduler/tick", "POST", {
    nodes: childTaskIds.map((taskId) => ({ node_id: `${taskId}-node`, incarnation: `${taskId}-boot`,
      capacity: { ...resources, cpu: 2, memory_mb: 1024, sessions: 2, tps: 20 } })),
    candidates: childTaskIds.map((taskId) => ({ task_id: taskId, tenant_id: tenantId,
      dispatch_id: `${taskId}-dispatch`, node_id: `${taskId}-node`, node_incarnation: `${taskId}-boot`,
      kind: "dag", dag_id: dagId,
      voucher_id: taskId === sourceTaskId ? sourceVoucher.voucher_id : targetVoucher.voucher_id,
      resources, task_spec: { metadata: { task_id: taskId, tenant_id: tenantId }, spec: {} } })),
  });
  assert.equal(childTick.status, 200, await childTick.clone().text());
  const dispatches = ((await childTick.json()) as any).dispatches;
  const sourceDispatch = dispatches.find((dispatch: any) => dispatch.task_id === sourceTaskId);
  const targetDispatch = dispatches.find((dispatch: any) => dispatch.task_id === targetTaskId);
  assert.equal((await simulateNodeStartReceipt(env, sourceDispatch, "boost-source-start")).status, 200);
  assert.equal((await simulateNodeStartReceipt(env, targetDispatch, "boost-target-start")).status, 200);
  const targetRunning = await request(`https://ansts.edge/v1beta1/dags/${dagId}/children/${targetTaskId}/observations`, "POST", {
    tenant_id: tenantId, event_id: "boost-target-running", aggregate_version: 1, state: "RUNNING",
  });
  assert.equal(targetRunning.status, 200, await targetRunning.clone().text());
  const sourceComplete = await request(`https://ansts.edge/v1beta1/tasks/${sourceTaskId}/complete?tenant_id=${tenantId}`, "POST", {
    tenant_id: tenantId, epoch: sourceDispatch.business_epoch, status: "SUCCEEDED",
  });
  assert.equal(sourceComplete.status, 200, await sourceComplete.clone().text());
  const sourceSucceeded = await request(`https://ansts.edge/v1beta1/dags/${dagId}/children/${sourceTaskId}/observations`, "POST", {
    tenant_id: tenantId, event_id: "boost-source-succeeded", aggregate_version: 1, state: "SUCCEEDED",
  });
  assert.equal(sourceSucceeded.status, 200, await sourceSucceeded.clone().text());
  const sourceStopped = await request("https://ansts.edge/v1beta1/scheduler/stopped", "POST", {
    dispatch_id: sourceDispatch.dispatch_id, task_id: sourceTaskId, tenant_id: tenantId,
    node_id: sourceDispatch.node_id, node_incarnation: sourceDispatch.node_incarnation,
    region: sourceDispatch.region, resource_pool: sourceDispatch.resource_pool, business_epoch: sourceDispatch.business_epoch,
    evidence_type: "STOPPED", evidence_id: "boost-source-stopped",
  });
  assert.equal(sourceStopped.status, 200, await sourceStopped.clone().text());

  const ledger = tenantLedgerCells.get(tenantId)!;
  const originalLedgerFetch = ledger.fetch.bind(ledger);
  let loseCommittedResponse = true;
  ledger.fetch = async (ledgerRequest: Request) => {
    const response = await originalLedgerFetch(ledgerRequest);
    if (loseCommittedResponse && new URL(ledgerRequest.url).pathname === "/allowance-transfers") {
      loseCommittedResponse = false;
      return new Response(JSON.stringify({ error: "INJECTED_COMMITTED_TRANSFER_RESPONSE_LOSS" }), { status: 503 });
    }
    return response;
  };
  const boostUrl = `https://ansts.edge/v1beta1/dags/${dagId}/boosts`;
  const boostBody = { tenant_id: tenantId, boost_id: "straggler-1", source_task_id: sourceTaskId,
    target_task_id: targetTaskId, amount_minor: 100, delegated_tps: 2, delegated_tokens: 200 };
  const interrupted = await request(boostUrl, "POST", boostBody);
  assert.equal(interrupted.status, 503);
  const pending = await request(`https://ansts.edge/v1beta1/dags/${dagId}/state?tenant_id=${tenantId}`);
  assert.equal(((await pending.json()) as any).boosts[0].status, "PENDING");
  const retried = await request(boostUrl, "POST", boostBody);
  assert.equal(retried.status, 200, await retried.clone().text());
  assert.equal(((await retried.json()) as any).boost.status, "TRANSFERRED");
  const replay = await request(boostUrl, "POST", boostBody);
  assert.equal(replay.status, 200);
  const sourceAccount = await request(`https://ansts.edge/v1beta1/tenants/${tenantId}/budget-accounts/${sourceTaskId}?period_key=${periodKey}`);
  const targetAccount = await request(`https://ansts.edge/v1beta1/tenants/${tenantId}/budget-accounts/${targetTaskId}?period_key=${periodKey}`);
  assert.equal(((await sourceAccount.json()) as any).available_minor, 200);
  assert.equal(((await targetAccount.json()) as any).available_minor, 400);
});

test("empty DAG barrier yields and resumes its parent exactly once", async () => {
  const { env } = createMockCloudflareCelldEnv();
  const taskId = "dag-empty-parent";
  const tenantId = "tenant-dag-empty";
  const resources = { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 };
  const spec = { metadata: { task_id: taskId, tenant_id: tenantId }, spec: {} };
  const firstDispatchResponse = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/scheduler/tick", {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({
      nodes: [{ node_id: "dag-empty-node", incarnation: "dag-empty-boot", capacity: resources }],
      candidates: [{ task_id: taskId, tenant_id: tenantId, dispatch_id: "dag-empty-parent-first", node_id: "dag-empty-node",
        node_incarnation: "dag-empty-boot", kind: "ordinary", task_spec: spec, resources }],
    }),
  }), env);
  const firstDispatch = ((await firstDispatchResponse.json()) as any).dispatches[0];
  assert.equal((await simulateNodeStartReceipt(env, firstDispatch, "dag-empty-first-start")).status, 200);

  const createBody = { tenant_id: tenantId, dag_id: "dag-empty-1", parent_task_id: taskId,
    parent_epoch: firstDispatch.business_epoch, parent_turn_seq: 0, period_key: "2026-11", currency: "USD", children: [] };
  const create = () => cloudflareCelldWorker.fetch(new Request(`https://ansts.edge/v1beta1/dags/${createBody.dag_id}`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(createBody),
  }), env);
  const completed = await create();
  assert.equal(completed.status, 200, await completed.clone().text());
  const dag = (await completed.json()) as any;
  assert.equal(dag.state, "COMPLETED");
  assert.equal(dag.children.length, 0);
  assert.equal(dag.parent_yielded, true);
  const replay = await create();
  assert.equal(replay.status, 200);
  assert.equal(((await replay.json()) as any).state, "COMPLETED");

  const resumedDispatch = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/v1beta1/scheduler/tick", {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({
      nodes: [{ node_id: "dag-empty-node-2", incarnation: "dag-empty-boot-2", capacity: resources }],
      candidates: [{ task_id: taskId, tenant_id: tenantId, dispatch_id: "dag-empty-parent-resumed", node_id: "dag-empty-node-2",
        node_incarnation: "dag-empty-boot-2", kind: "recovery", task_spec: spec, resources }],
    }),
  }), env);
  assert.equal(resumedDispatch.status, 200, await resumedDispatch.clone().text());
  assert.equal(((await resumedDispatch.json()) as any).dispatches[0].stage, "AUTHORIZED");
});

test("race winner is single, loser operations stay fenced, and allowance waits for stop evidence", async () => {
  const { env, taskExecutorCalls, nodeCapacityCells } = createMockCloudflareCelldEnv();
  const tenantId = "tenant-dag-race";
  const periodKey = "2026-11";
  const dagId = "dag-race-public";
  const parentTaskId = "dag-race-parent";
  const winnerTaskId = "dag-race-winner";
  const loserTaskId = "dag-race-loser";
  const base = `https://ansts.edge/v1beta1/tenants/${tenantId}`;
  const request = (url: string, method = "GET", body?: unknown) => cloudflareCelldWorker.fetch(new Request(url, {
    method, headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  }), env);
  const ledger = (path: string, method: string, body?: unknown) => request(`${base}${path}`, method, body);
  const admit = async (taskId: string, key: string) => {
    const spec = structuredClone(yaml.load(fs.readFileSync(path.join(__dirname, "..", "..", "specs", "taskspec_v1beta1_example.yaml"), "utf8")) as any);
    spec.metadata.task_id = taskId;
    spec.metadata.tenant_id = tenantId;
    spec.metadata.idempotency_key = key;
    const response = await request("https://ansts.edge/v1beta1/tasks", "POST", spec);
    assert.equal(response.status, 202, await response.clone().text());
  };
  await admit(parentTaskId, "race-parent-idem");
  await admit(winnerTaskId, "race-winner-idem");
  await admit(loserTaskId, "race-loser-idem");
  assert.equal((await ledger(`/budget-periods/${periodKey}`, "PUT", { currency: "USD", hard_limit_minor: 1000, price_version: "race-price-v1" })).status, 201);
  assert.equal((await ledger("/budget-grants", "POST", { period_key: periodKey, task_id: parentTaskId, grant_id: "race-parent-grant", amount_minor: 800, currency: "USD" })).status, 201);

  const resources = { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 };
  const policies = { tools_and_saga_config: { transactional_policies: [{ tool_name: "payments/submit", effect_class: ToolEffectClass.IDEMPOTENT_WRITE }] } };
  const parentSpec = { metadata: { task_id: parentTaskId, tenant_id: tenantId }, spec: {} };
  const candidates = [{ task_id: parentTaskId, tenant_id: tenantId, dispatch_id: "race-parent-dispatch", node_id: "race-parent-node",
    node_incarnation: "race-parent-boot", kind: "ordinary", task_spec: parentSpec, resources }];
  const parentDispatchResponse = await request("https://ansts.edge/v1beta1/scheduler/tick", "POST", {
    nodes: [{ node_id: "race-parent-node", incarnation: "race-parent-boot", capacity: resources }], candidates,
  });
  const parentDispatch = ((await parentDispatchResponse.json()) as any).dispatches[0];
  assert.equal((await simulateNodeStartReceipt(env, parentDispatch, "race-parent-start")).status, 200);

  const children = [
    { task_id: winnerTaskId, branch_id: "branch-winner", delegated_tps: 10, delegated_tokens: 500, allowance_minor: 300 },
    { task_id: loserTaskId, branch_id: "branch-loser", delegated_tps: 10, delegated_tokens: 500, allowance_minor: 300 },
  ];
  const dagResponse = await request(`https://ansts.edge/v1beta1/dags/${dagId}`, "POST", {
    tenant_id: tenantId, dag_id: dagId, parent_task_id: parentTaskId, parent_epoch: parentDispatch.business_epoch,
    parent_turn_seq: 0, period_key: periodKey, currency: "USD", race_mode: true, children,
  });
  assert.equal(dagResponse.status, 200, await dagResponse.clone().text());
  const dag = (await dagResponse.json()) as any;
  const childDispatches = [] as any[];
  for (const [index, child] of dag.children.entries()) {
    const redeemed = await request(`https://ansts.edge/v1beta1/dags/${dagId}/children/${child.task_id}/redeem`, "POST", {
      tenant_id: tenantId, voucher_id: child.voucher_id, signature: child.voucher_signature,
    });
    assert.equal(redeemed.status, 200);
    const nodeId = `race-child-node-${index}`;
    const bootId = `race-child-boot-${index}`;
    const dispatchResponse = await request("https://ansts.edge/v1beta1/scheduler/tick", "POST", {
      nodes: [{ node_id: nodeId, incarnation: bootId, capacity: resources }],
      candidates: [{ task_id: child.task_id, tenant_id: tenantId, dispatch_id: `race-dispatch-${index}`,
        node_id: nodeId, node_incarnation: bootId, kind: "dag", dag_id: dagId, voucher_id: child.voucher_id,
        task_spec: { metadata: { task_id: child.task_id, tenant_id: tenantId }, spec: policies }, resources }],
    });
    const dispatch = ((await dispatchResponse.json()) as any).dispatches[0];
    assert.equal(dispatch.stage, "AUTHORIZED");
    assert.equal((await simulateNodeStartReceipt(env, dispatch, `race-child-start-${index}`)).status, 200);
    childDispatches.push(dispatch);
  }

  const blocked = await request(`https://ansts.edge/v1beta1/tasks/${loserTaskId}/operations?tenant_id=${tenantId}`, "POST", {
    tool_call_id: "loser-before-winner", tool_name: "payments/submit", effect_class: ToolEffectClass.IDEMPOTENT_WRITE,
    args: { order_id: "order-1" }, epoch: childDispatches[1].business_epoch, turn_seq: 1,
    race: { race_id: dagId, branch_id: "branch-loser" },
  });
  assert.equal(blocked.status, 202);
  assert.equal(((await blocked.json()) as any).status, "RACE_BLOCKED");
  assert.equal(taskExecutorCalls.execute, 0);

  const winnerCompletion = await request(`https://ansts.edge/v1beta1/tasks/${winnerTaskId}/complete?tenant_id=${tenantId}`, "POST", {
    tenant_id: tenantId, epoch: childDispatches[0].business_epoch, status: "SUCCEEDED",
  });
  assert.equal(winnerCompletion.status, 200, await winnerCompletion.clone().text());
  const selected = await request(`https://ansts.edge/v1beta1/dags/${dagId}/children/${winnerTaskId}/observations`, "POST", {
    tenant_id: tenantId, event_id: "race-winner-success", aggregate_version: 1, state: "SUCCEEDED",
  });
  assert.equal(selected.status, 409, "parent remains yielded while loser capacity has no stop evidence");
  assert.equal(((await selected.json()) as any).error, "DAG_CHILD_STOP_UNCONFIRMED");

  const loserOperation = await request(`https://ansts.edge/v1beta1/tasks/${loserTaskId}/operations?tenant_id=${tenantId}`, "GET");
  const operations = (await loserOperation.json()) as any;
  assert.equal(operations.operations[0].race.selectedWinner, "branch-winner");
  const afterWinner = await request(`https://ansts.edge/v1beta1/tasks/${loserTaskId}/operations?tenant_id=${tenantId}`, "POST", {
    tool_call_id: "loser-after-winner", tool_name: "payments/submit", effect_class: ToolEffectClass.IDEMPOTENT_WRITE,
    args: { order_id: "order-2" }, epoch: childDispatches[1].business_epoch, turn_seq: 1,
    race: { race_id: dagId, branch_id: "branch-loser" },
  });
  assert.equal(afterWinner.status, 409);
  assert.equal(((await afterWinner.json()) as any).error, "DAG_RACE_LOST");
  assert.equal(taskExecutorCalls.execute, 0);

  const stopLoser = await request("https://ansts.edge/v1beta1/scheduler/stopped", "POST", {
    dispatch_id: childDispatches[1].dispatch_id, task_id: loserTaskId, tenant_id: tenantId,
    node_id: childDispatches[1].node_id, node_incarnation: childDispatches[1].node_incarnation,
    region: childDispatches[1].region, resource_pool: childDispatches[1].resource_pool,
    business_epoch: childDispatches[1].business_epoch, evidence_type: "STOPPED", evidence_id: "race-loser-stopped-proof",
  });
  assert.equal(stopLoser.status, 200, await stopLoser.clone().text());
  const reconciled = await request(`https://ansts.edge/v1beta1/dags/${dagId}/reconcile`, "POST", { tenant_id: tenantId });
  assert.equal(reconciled.status, 200, await reconciled.clone().text());
  const raceState = (await reconciled.json()) as any;
  assert.equal(raceState.winner_task_id, winnerTaskId);
  assert.equal(raceState.state, "COMPLETED");
  assert.equal(raceState.children.find((child: any) => child.task_id === loserTaskId).observed_state, "STOPPED");
  assert.equal(nodeCapacityCells.get(childDispatches[1].node_id)!.capacity.getReservation(childDispatches[1].dispatch_id)!.release_evidence_id, "race-loser-stopped-proof");
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
  const { env, harnessCells, nodeCapacityCells, taskExecutorCalls } = createMockCloudflareCelldEnv();
  const taskId = "tsk_celld_agent_01";
  const taskSpec = {
    metadata: { task_id: taskId, tenant_id: "fin-enterprise" },
    spec: {
      execution_policy: { max_retries: 3, checkpoint: "turn-boundary" },
      tools_and_saga_config: {
        transactional_policies: [
          { tool_name: "git_branch_push", effect_class: ToolEffectClass.SAGA_COMPENSATABLE, compensation_tool: "git_branch_delete" },
          { tool_name: "settlement_ledger_post", effect_class: ToolEffectClass.STAGED_COMMIT, require_human_approval: true },
        ],
      },
    },
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
          args: { branch: "feat-celld" },
          compensation_tool: "git_branch_delete",
          requires_heavy_container: true,
        },
        workspace_merkle_hash: "merkle_v2",
      }),
    }),
    env,
  );
  assert.equal(turn2Res.status, 200, await turn2Res.clone().text());
  const turn2Body = (await turn2Res.json()) as Record<string, any>;
  assert.equal(turn2Body.routed_model, "DeepSeek");
  assert.equal(turn2Body.container_burst_triggered, true);
  assert.equal(turn2Body.sqlite_wal_rows, 2);

  const turn2OperationId = stableTaskOperationId(taskId, "call_02");
  const listedOperations = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tasks/${taskId}/operations?tenant_id=fin-enterprise`,
  ), env);
  assert.equal(listedOperations.status, 200);
  const operationsBody = await listedOperations.json() as any;
  assert.equal(operationsBody.operations[0].operationId, turn2OperationId);
  assert.equal(operationsBody.operations[0].status, "SUCCEEDED");
  assert.equal(operationsBody.operations[0].compensation.status, "PENDING");
  assert.equal(taskExecutorCalls.execute, 1);

  const existingCellState = harnessCells.get(taskId)!.ctx;
  harnessCells.set(taskId, new AgentTaskHarnessCell(existingCellState, env));
  const afterActivation = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tasks/${taskId}/operations/${encodeURIComponent(turn2OperationId)}?tenant_id=fin-enterprise`,
  ), env);
  assert.equal(afterActivation.status, 200);
  assert.equal(((await afterActivation.json()) as any).operation.status, "SUCCEEDED");

  const stagedRequest = {
    tool_call_id: "approval-call-01",
    tool_name: "settlement_ledger_post",
    args: { settlement_id: "settle-01", amount_minor: 500 },
    epoch: 1,
    turn_seq: 3,
  };
  const stagedResponse = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tasks/${taskId}/operations?tenant_id=fin-enterprise`,
    { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(stagedRequest) },
  ), env);
  assert.equal(stagedResponse.status, 202);
  const stagedBody = await stagedResponse.json() as any;
  assert.equal(stagedBody.status, "APPROVAL_REQUIRED");
  assert.equal(taskExecutorCalls.execute, 1, "staged operations must wait for approval before provider execution");
  const stagedOperationId = stableTaskOperationId(taskId, stagedRequest.tool_call_id);

  const crossTenantOperation = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tasks/${taskId}/operations/${encodeURIComponent(stagedOperationId)}?tenant_id=another-tenant`,
  ), env);
  assert.equal(crossTenantOperation.status, 404);

  const approval = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tasks/${taskId}/operations/${encodeURIComponent(stagedOperationId)}/approval?tenant_id=fin-enterprise`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ approval_id: "approval-01", approved_by: "tenant-admin", expires_at: Date.now() + 60_000 }),
    },
  ), env);
  assert.equal(approval.status, 200, await approval.clone().text());
  const executeApproved = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tasks/${taskId}/operations?tenant_id=fin-enterprise`,
    { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(stagedRequest) },
  ), env);
  assert.equal(executeApproved.status, 200, await executeApproved.clone().text());
  assert.equal(((await executeApproved.json()) as any).status, "SUCCEEDED");
  assert.equal(taskExecutorCalls.execute, 2);

  const compensation = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tasks/${taskId}/compensations/run?tenant_id=fin-enterprise`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ epoch: 1, turn_seq: 3, after_turn_exclusive: 1 }),
    },
  ), env);
  assert.equal(compensation.status, 200, await compensation.clone().text());
  assert.equal(taskExecutorCalls.compensate, 1);
  const compensationList = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tasks/${taskId}/compensations?tenant_id=fin-enterprise&after_turn_exclusive=1`,
  ), env);
  assert.equal(((await compensationList.json()) as any).compensations[0].status, "COMPENSATED");

  env.TASK_OPERATION_EXECUTOR = undefined;
  const unavailable = await cloudflareCelldWorker.fetch(new Request(
    `https://ansts.edge/v1beta1/tasks/${taskId}/operations?tenant_id=fin-enterprise`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        tool_call_id: "no-executor-call",
        tool_name: "git_branch_push",
        args: { branch: "must-not-be-started" },
        epoch: 1,
        turn_seq: 3,
      }),
    },
  ), env);
  assert.equal(unavailable.status, 503);
  assert.equal(taskExecutorCalls.execute, 2, "missing executor must persist intent without starting an external attempt");

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


for (const dagRuntime of [false, true]) test(`public Worker meters a provider call, commits a verified snapshot, and recovers the same Turn after activation${dagRuntime ? " with delegated DAG quota" : ""}`, async () => {
  const { env, harnessCells, modelProviderCalls, tenantTaskProjections, taskEventProjections } = createMockCloudflareCelldEnv();
  env.TEST_SIMULATOR_MODE = false;
  const tenantId = "tenant-durable-turn";
  const taskId = "task-durable-turn";
  const delegation = { delegated_tokens: 2000, delegated_tps: 8 };
  if (dagRuntime) env.DAG_CELL = { idFromName: (name: string) => ({ name, toString: () => name }),
    get: () => ({ fetch: async () => new Response(JSON.stringify({ authorized: true, dag_id: "runtime-dag",
      task_id: taskId, voucher_id: "runtime-voucher", ...delegation })) }) };
  const periodKey = new Date().toISOString().slice(0, 7);
  const base = "https://ansts.edge";
  const request = (pathName: string, method = "GET", body?: unknown, headers: Record<string, string> = {}) =>
    cloudflareCelldWorker.fetch(new Request(`${base}${pathName}`, {
      method,
      headers: { ...(body === undefined ? {} : { "content-type": "application/json" }), ...headers },
      body: body === undefined ? undefined : typeof body === "string" ? body : JSON.stringify(body),
    }), env);

  const routeConfig = {
    tenantId,
    policyVersion: "durable-turn-policy-v1",
    routes: [{
      poolName: "DeepSeek",
      provider: "openai-compatible",
      modelId: "test-deepseek",
      quotaDomain: "shared-account-a",
      quotaDemandTokensPerSecond: 8,
      maxInputTokens: 1024,
      maxOutputTokens: 64,
      pricing: { priceVersion: "turn-price-v1", currency: "USD", inputMinorPerMillionTokens: 1_000_000, outputMinorPerMillionTokens: 1_000_000 },
    }],
  };
  const routing = await request(`/v1beta1/tenants/${tenantId}/routing-config/versions`, "POST", {
    operationId: "durable-turn-routing-v1", expectedVersion: 0, configuration: routeConfig,
  });
  assert.equal(routing.status, 201, await routing.clone().text());
  const quotaPolicy = await request("/v1beta1/providers/openai-compatible/quota-domains/shared-account-a/policy", "PUT", {
    operationId: "durable-turn-quota-v1", expectedVersion: 0,
    policy: { requestsPerMinute: 100, tokensPerSecond: 1000, grantTtlMs: 60_000 },
  });
  assert.equal(quotaPolicy.status, 201, await quotaPolicy.clone().text());
  const period = await request(`/v1beta1/tenants/${tenantId}/budget-periods/${periodKey}`, "PUT", {
    currency: "USD", hard_limit_minor: 10_000, price_version: "turn-price-v1",
  });
  assert.equal(period.status, 201, await period.clone().text());
  const grant = await request(`/v1beta1/tenants/${tenantId}/budget-grants`, "POST", {
    period_key: periodKey, task_id: taskId, grant_id: "durable-turn-budget", amount_minor: 10_000, currency: "USD",
  });
  assert.equal(grant.status, 201, await grant.clone().text());

  const taskSpec = {
    metadata: { task_id: taskId, tenant_id: tenantId, idempotency_key: "durable-turn-admission" },
    spec: {
      cognitive_policy: {
        primary_model: "DeepSeek", fast_utility_model: "GLM-5.3", fallback_models: ["Qwen-3.8"], default_reasoning_effort: "medium",
      },
      quota_and_dag_policy: { max_total_tokens: 2000, reserved_tps: 8 },
      tools_and_saga_config: { transactional_policies: [] },
    },
  };
  const admissionSpec = structuredClone(yaml.load(fs.readFileSync(path.join(__dirname, "..", "..", "specs", "taskspec_v1beta1_example.yaml"), "utf8")) as any);
  admissionSpec.metadata.task_id = taskId;
  admissionSpec.metadata.tenant_id = tenantId;
  admissionSpec.metadata.idempotency_key = "durable-turn-admission";
  const accepted = await request("/v1beta1/tasks", "POST", admissionSpec);
  assert.equal(accepted.status, 202, await accepted.clone().text());
  const dispatchResponse = await request("/v1beta1/scheduler/tick", "POST", {
    nodes: [{ node_id: "durable-turn-node", incarnation: "durable-turn-boot", capacity: { cpu: 4, memory_mb: 2048, burst_cpu: 2, sessions: 2, tps: 100 } }],
    candidates: [{
      task_id: taskId, tenant_id: tenantId, dispatch_id: "durable-turn-dispatch", node_id: "durable-turn-node",
      node_incarnation: "durable-turn-boot", kind: dagRuntime ? "dag" : "ordinary", task_spec: taskSpec,
      ...(dagRuntime ? { dag_id: "runtime-dag", voucher_id: "runtime-voucher" } : {}),
      resources: { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 8 },
    }],
  });
  assert.equal(dispatchResponse.status, 200, await dispatchResponse.clone().text());
  const dispatch = ((await dispatchResponse.json()) as any).dispatches[0];
  assert.equal(dispatch.stage, "AUTHORIZED");
  assert.equal((await simulateNodeStartReceipt(env, dispatch, "durable-turn-start-receipt")).status, 200);

  const block = new TextEncoder().encode("workspace-at-turn-1");
  const blockSha256 = createHash("sha256").update(block).digest("hex");
  const upload = await request(`/v1beta1/tasks/${taskId}/turns/snapshot-uploads?tenant_id=${tenantId}`, "POST", {
    epoch: 1,
    turn_seq: 1,
    upload_id: "durable-turn-upload-1",
    chunks: [{ index: 0, sha256: blockSha256, size_bytes: block.byteLength }],
  });
  assert.equal(upload.status, 200, await upload.clone().text());
  const turnPath = `/v1beta1/tasks/${taskId}/turn?tenant_id=${tenantId}`;
  const turnBody = {
    epoch: 1,
    turn_seq: 1,
    snapshot_upload_id: "durable-turn-upload-1",
    messages: [{ role: "user", content: "Summarize the durable workspace." }],
    max_output_tokens: 64,
    workspace_merkle_hash: blockSha256,
  };
  const incompleteSnapshotTurn = await request(turnPath, "POST", turnBody);
  assert.equal(incompleteSnapshotTurn.status, 409);
  assert.equal(modelProviderCalls.calls, 0, "an incomplete snapshot must block before provider billing or execution");
  const chunk = await cloudflareCelldWorker.fetch(new Request(
    `${base}/v1beta1/tasks/${taskId}/turns/snapshot-uploads/durable-turn-upload-1/chunks/0?tenant_id=${tenantId}`,
    { method: "PUT", headers: { "content-type": "application/octet-stream" }, body: block },
  ), env);
  assert.equal(chunk.status, 200, await chunk.clone().text());

  const turn = await request(turnPath, "POST", turnBody);
  assert.equal(turn.status, 200, await turn.clone().text());
  const committed = await turn.json() as any;
  assert.equal(committed.turn_committed, true);
  assert.equal(committed.model_response.content_text, "durable answer");
  assert.equal(modelProviderCalls.calls, 1);

  const taskList = await request(`/v1beta1/tenants/${tenantId}/tasks`);
  assert.equal(taskList.status, 200, await taskList.clone().text());
  const listedTasks = await taskList.json() as any;
  assert.equal(listedTasks.tasks[0].lifecycle_state, "RUNNING");
  assert.equal(listedTasks.projection_stored, true);
  const dashboardTasks = await request(`/v1beta1/tenants/${tenantId}/dashboard/tasks`);
  assert.equal(dashboardTasks.status, 200, await dashboardTasks.clone().text());
  assert.equal(((await dashboardTasks.json()) as any).tasks[0].task_id, taskId);
  tenantTaskProjections.clear();
  taskEventProjections.clear();
  const lostProjection = await request(`/v1beta1/tenants/${tenantId}/dashboard/tasks`);
  assert.deepEqual(((await lostProjection.json()) as any).tasks, [], "D1 projection loss does not erase authoritative task state");
  const currentMinute = Math.floor(Date.now() / 60_000);
  const shard = tenantDirectoryShardIndex(tenantId);
  const scheduledMinute = currentMinute + ((shard - (currentMinute % TENANT_DIRECTORY_SHARD_COUNT) + TENANT_DIRECTORY_SHARD_COUNT) % TENANT_DIRECTORY_SHARD_COUNT);
  await cloudflareCelldWorker.scheduled({ scheduledTime: scheduledMinute * 60_000, cron: "* * * * *" }, env);
  assert.ok(Array.from(taskEventProjections.values()).some((event) => event.event_type === "task.turn.committed"), "retained TaskCell outbox rebuilds the D1 event view");
  const rebuiltTaskView = await request(`/v1beta1/tenants/${tenantId}/dashboard/tasks`);
  assert.equal(((await rebuiltTaskView.json()) as any).tasks[0].task_id, taskId);

  const eventsPath = `/v1beta1/tasks/${taskId}/events?tenant_id=${tenantId}`;
  const eventFeed = await request(eventsPath);
  assert.equal(eventFeed.status, 200, await eventFeed.clone().text());
  const feed = await eventFeed.json() as any;
  assert.ok(feed.events.some((event: any) => event.eventType === "task.turn.committed"));
  assert.ok(feed.next_cursor);
  assert.ok(feed.events.some((event: any) => event.status === "DELIVERED"), "projection success durably acknowledges the source outbox event");
  assert.ok(Array.from(taskEventProjections.values()).some((event) => event.event_type === "task.turn.committed"));
  const eventStream = await request(`${eventsPath}&snapshot=1`, "GET", undefined, { accept: "text/event-stream" });
  assert.equal(eventStream.status, 200);
  assert.match(eventStream.headers.get("content-type") ?? "", /text\/event-stream/);
  assert.match(await eventStream.text(), /event: task\.turn\.committed/);
  const followStream = await request(eventsPath, "GET", undefined, { accept: "text/event-stream" });
  assert.equal(followStream.status, 200);
  const streamReader = followStream.body!.getReader();
  let streamFrames = "";
  for (let i = 0; i < feed.events.length; i += 1) {
    const frame = await streamReader.read();
    if (frame.done) break;
    streamFrames += new TextDecoder().decode(frame.value);
    if (streamFrames.includes("event: task.turn.committed")) break;
  }
  await streamReader.cancel();
  assert.match(streamFrames, /event: task\.turn\.committed/);
  const expiredCursor = `${feed.events[0].aggregateVersion}~${Date.now() - 8 * 24 * 60 * 60 * 1000}~old-event`;
  const expiredFeed = await request(eventsPath, "GET", undefined, { "last-event-id": expiredCursor });
  assert.equal(expiredFeed.status, 410);
  assert.equal(((await expiredFeed.json()) as any).resync_required, true);

  const recoveryPath = `/v1beta1/tasks/${taskId}/turns/recovery?tenant_id=${tenantId}`;
  const recovery = await request(recoveryPath);
  assert.equal(recovery.status, 200, await recovery.clone().text());
  const recovered = await recovery.json() as any;
  assert.equal(recovered.turnSeq, 1);
  assert.equal(recovered.response.model_response.content_text, "durable answer");
  assert.equal(recovered.snapshotBlocks.length, 1);

  const crossTenant = await request(`/v1beta1/tasks/${taskId}/turns/recovery?tenant_id=other-tenant`);
  assert.equal(crossTenant.status, 404);
  const existingState = harnessCells.get(taskId)!.ctx;
  harnessCells.set(taskId, new AgentTaskHarnessCell(existingState, env));
  const afterActivation = await request(recoveryPath);
  assert.equal(afterActivation.status, 200, await afterActivation.clone().text());
  assert.equal(((await afterActivation.json()) as any).response.model_response.content_text, "durable answer");

  const replay = await request(turnPath, "POST", turnBody);
  assert.equal(replay.status, 200, await replay.clone().text());
  assert.equal(((await replay.json()) as any).replayed, true);
  assert.equal(modelProviderCalls.calls, 1, "replaying a committed Turn must not call the model again");
  const changedInput = await request(turnPath, "POST", { ...turnBody, messages: [{ role: "user", content: "changed request" }] });
  assert.equal(changedInput.status, 409);
  if (dagRuntime) {
    const state = async () => (await (await harnessCells.get(taskId)!.fetch(new Request("http://task/execution/state"))).json()) as any;
    assert.equal((await state()).quota_tokens_used, 10);
    delegation.delegated_tps = 16;
    assert.equal((await state()).effective_delegated_tps, 8);
    assert.equal((await state()).pending_delegated_tps, 8, "a monetary boost cannot create physical TPS capacity");
    const upload2 = await request(`/v1beta1/tasks/${taskId}/turns/snapshot-uploads?tenant_id=${tenantId}`, "POST", {
      epoch: 1, turn_seq: 2, upload_id: "dag-upload-2", chunks: [{ index: 0, sha256: blockSha256, size_bytes: block.byteLength }],
    });
    assert.equal(upload2.status, 200);
    await cloudflareCelldWorker.fetch(new Request(`${base}/v1beta1/tasks/${taskId}/turns/snapshot-uploads/dag-upload-2/chunks/0?tenant_id=${tenantId}`,
      { method: "PUT", body: block }), env);
    let unknownCalls = 0;
    env.MODEL_PROVIDER_PROXY = { fetch: async () => { unknownCalls += 1; throw new Error("provider accepted; usage unknown"); } };
    const unknownBody = { ...turnBody, turn_seq: 2, snapshot_upload_id: "dag-upload-2" };
    assert.equal((await request(turnPath, "POST", unknownBody)).status, 503);
    const reserved = (await state()).quota_tokens_reserved;
    assert.ok(reserved > 0);
    harnessCells.set(taskId, new AgentTaskHarnessCell(existingState, env));
    assert.equal((await request(turnPath, "POST", unknownBody)).status, 503);
    assert.equal(unknownCalls, 1, "unknown consumption cannot refill tokens or retry an outward provider attempt");
    assert.equal((await state()).quota_tokens_reserved, reserved);
  }
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
