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
  ToolEffectClass,
} from "../src/index";

function createMockCloudflareCelldEnv(): {
  env: CloudflareCelldEnv;
  schedulerCell: AnstsSchedulerCell;
  harnessCells: Map<string, AgentTaskHarnessCell>;
  d1Rows: Array<any[]>;
  queuedMessages: any[];
} {
  const kvMap = new Map<string, string>();
  const r2Map = new Map<string, string>();
  const d1Rows: Array<any[]> = [];
  const queuedMessages: any[] = [];
  const harnessCells = new Map<string, AgentTaskHarnessCell>();

  const env: CloudflareCelldEnv = {
    PRIMARY_MODEL_POOL: "DeepSeek",
    FAST_UTILITY_POOL: "GLM-5.3",
    FALLBACK_MODEL_POOL: "Qwen-3.8",
    WEBHOOK_HMAC_SECRET: "whsec_celld_test",
    SCHEDULER_CELL: null as any,
    AGENT_HARNESS_CELL: null as any,
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

  return { env, schedulerCell, harnessCells, d1Rows, queuedMessages };
}

test("07. wrangler.json & D1 SQL migration schema verification for celld and Cloudflare Workers", () => {
  const rootDir = path.join(__dirname, "..", "..");
  const wranglerJson = JSON.parse(fs.readFileSync(path.join(rootDir, "wrangler.json"), "utf8"));
  assert.equal(wranglerJson.main, "src/cloudflare/worker.ts");
  assert.equal(wranglerJson.durable_objects.bindings.length, 2);
  assert.equal(wranglerJson.d1_databases[0].binding, "ANSTS_D1");
  assert.equal(wranglerJson.kv_namespaces[0].binding, "ANSTS_KV");
  assert.equal(wranglerJson.r2_buckets[0].binding, "ANSTS_CHECKPOINTS_R2");

  const sqlMig = fs.readFileSync(path.join(rootDir, "migrations", "0001_init_ansts.sql"), "utf8");
  assert.ok(sqlMig.includes("CREATE TABLE IF NOT EXISTS task_audit_events"));
});

test("08. Stateless Edge Worker + KV Idempotency + AnstsSchedulerCell 5D W-DRF & DAG Escrow", async () => {
  const { env, queuedMessages, d1Rows } = createMockCloudflareCelldEnv();
  const specPath = path.join(__dirname, "..", "..", "specs", "taskspec_v1beta1_example.yaml");
  const sampleSpec = yaml.load(fs.readFileSync(specPath, "utf8")) as Record<string, any>;

  // First submission -> 202 Accepted and queued into ANSTS_EVENTS_QUEUE
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
  assert.equal(queuedMessages.length, 1);

  // Duplicate submission -> intercepted at edge by ANSTS_KV in ~0.2ms
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

  // Cron scheduled() trigger runs 5D W-DRF tick and persists lease into Scheduler Cell SQLite
  await cloudflareCelldWorker.scheduled({ scheduledTime: Date.now(), cron: "* * * * *" }, env);

  const topRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/cluster/top"),
    env,
  );
  const topBody = (await topRes.json()) as Record<string, any>;
  assert.equal(topBody.celld_sqlite_leases_count, 1);

  // Process queue() batch into ANSTS_D1 audit table
  let acked = false;
  await cloudflareCelldWorker.queue(
    {
      queue: "ansts-cloudevents-outbox",
      messages: [
        {
          id: "msg_01",
          timestamp: new Date(),
          body: queuedMessages[0],
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
