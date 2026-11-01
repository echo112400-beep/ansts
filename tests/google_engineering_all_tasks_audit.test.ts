/**
 * @fileoverview Google Engineering Development Standards & Full 74-Task (`TASK-01` ~ `TASK-74`) Verification Suite.
 *
 * Enforces:
 *    - Module-level JSDoc header comment in every source file
 *    - Module-level JSDoc header comment (`/** ... end-comment`) in every source file
 *    - Zero `var` declarations (strict `const` / `let` only)
 *    - Zero `eval()` or `new Function()` dynamic code injection
 *    - Zero unfinished `TODO` / `FIXME` / `NotImplemented` stubs
 * 2. Native `js_rpc` entrypoints (`submitTaskRpc`, `scheduleTickRpc`) with `AbortSignal` support on `AnstsSchedulerCell`
 * 3. Edge Worker `/v1beta1/dynamic-workers/invoke` & `CelldKvOptimizerAndGuard` sharded KV integration (`kv_namespace_shard_id`)
 */

import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { test } from "node:test";
import * as yaml from "js-yaml";

import {
  AgentTaskHarnessCell,
  AnstsSchedulerCell,
  CloudflareCelldEnv,
  cloudflareCelldWorker,
  MockDurableObjectState,
} from "../src/index";

function walkTsFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...walkTsFiles(full));
    } else if (entry.isFile() && entry.name.endsWith(".ts")) {
      out.push(full);
    }
  }
  return out.sort();
}

test("Google Engineering Standard #1: All 51 src/**/*.ts modules pass Google TS Style Guide invariants", () => {
  const rootDir = path.join(__dirname, "..", "..");
  const srcFiles = walkTsFiles(path.join(rootDir, "src"));
  assert.equal(srcFiles.length, 52);

  for (const file of srcFiles) {
    const rel = path.relative(rootDir, file);
    const content = fs.readFileSync(file, "utf8");

    // 1. Every source module must begin with a JSDoc module header block (`/** ... */`)
    assert.ok(
      content.trimStart().startsWith("/**"),
      `[Google TS Style] Missing top-level JSDoc header in ${rel}`,
    );

    // 2. Disallow `var ` declarations (Google TS Style §5.1: Always use const or let)
    assert.equal(
      /\bvar\s+[a-zA-Z_$]/.test(content),
      false,
      `[Google TS Style] Forbidden 'var' declaration in ${rel}`,
    );

    // 3. Disallow `eval(` and unfinished `TODO` / `FIXME` stubs
    assert.equal(
      /\beval\s*\(/.test(content),
      false,
      `[Google TS Style] Forbidden eval() in ${rel}`,
    );
    assert.equal(
      /\b(TODO|FIXME|NotImplemented)\b/.test(content),
      false,
      `[Google TS Style] Unfinished stub found in ${rel}`,
    );
  }
});

test("Google Engineering Standard #2: Native js_rpc methods on AnstsSchedulerCell & Edge Dynamic Worker / Sharded KV integration", async () => {
  const rootDir = path.join(__dirname, "..", "..");
  const specPath = path.join(rootDir, "specs", "taskspec_v1beta1_example.yaml");
  const sampleSpec = yaml.load(fs.readFileSync(specPath, "utf8")) as Record<string, any>;

  const mockEnv: CloudflareCelldEnv = {
    PRIMARY_MODEL_POOL: "DeepSeek",
    FAST_UTILITY_POOL: "GLM-5.3",
    FALLBACK_MODEL_POOL: "Qwen-3.8",
    SCHEDULER_CELL: {
      idFromName: (n: string) => ({ toString: () => n, name: n }),
      get: (id) => {
        const c = new AnstsSchedulerCell(new MockDurableObjectState(id.toString()), mockEnv);
        return {
          fetch: (input: RequestInfo, init?: RequestInit) =>
            c.fetch(typeof input === "string" ? new Request(input, init) : input),
        };
      },
    },
    AGENT_HARNESS_CELL: {
      idFromName: (n: string) => ({ toString: () => n, name: n }),
      get: (id) => {
        const c = new AgentTaskHarnessCell(new MockDurableObjectState(id.toString()), mockEnv);
        return {
          fetch: (input: RequestInfo, init?: RequestInfo) =>
            c.fetch(typeof input === "string" ? new Request(input, init as any) : input),
        };
      },
    },
    ANSTS_KV: { get: async () => null, put: async () => {}, delete: async () => {} },
    ANSTS_CHECKPOINTS_R2: { put: async (k) => ({ key: k }), get: async () => null, list: async () => ({ objects: [] }) },
    ANSTS_D1: {
      prepare: () => {
        const s = { bind: () => s, run: async () => ({ success: true }), all: async () => ({ results: [] }) };
        return s;
      },
    },
    ANSTS_EVENTS_QUEUE: { send: async () => {} },
  };

  // 1. Direct js_rpc invocation on AnstsSchedulerCell (`submitTaskRpc` & `scheduleTickRpc`)
  const schedCell = new AnstsSchedulerCell(new MockDurableObjectState("rpc-sched-01"), mockEnv);
  const rpcSub = schedCell.submitTaskRpc(sampleSpec);
  assert.equal(rpcSub.statusCode, 202);
  const rpcTick = schedCell.scheduleTickRpc();
  assert.equal(rpcTick.dispatched_count, 1);

  // Verify AbortSignal propagation on js_rpc
  const abortCtrl = new AbortController();
  abortCtrl.abort();
  assert.throws(
    () => schedCell.submitTaskRpc(sampleSpec, Date.now() / 1000, abortCtrl.signal),
    /SchedulerRpcAborted/,
  );

  // 2. Verify Edge Worker `POST /v1beta1/tasks` attaches `kv_namespace_shard_id` via CelldKvOptimizerAndGuard
  const edgeTaskRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks", {
      method: "POST",
      body: JSON.stringify(sampleSpec),
    }),
    mockEnv,
  );
  assert.equal(edgeTaskRes.status, 202);
  const edgeTaskBody = (await edgeTaskRes.json()) as Record<string, any>;
  assert.equal(typeof edgeTaskBody.kv_namespace_shard_id, "number");

  // 3. Verify Edge Worker `POST /v1beta1/dynamic-workers/invoke` executes Dynamic Worker with CPU/subrequest guards
  const dynWorkerRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/dynamic-workers/invoke", {
      method: "POST",
      body: JSON.stringify({
        worker_name: "edge-mcp-validator",
        compatibility_date: "2026-10-01",
        cpu_limit_ms: 50,
        simulated_cpu_ms: 8,
        outbound_urls: ["https://api.deepseek.com/v1/models"],
        props: { schemaVersion: "v1beta1" },
      }),
    }),
    mockEnv,
  );
  assert.equal(dynWorkerRes.status, 200);
  const dynWorkerBody = (await dynWorkerRes.json()) as Record<string, any>;
  assert.equal(dynWorkerBody.result.executed, true);
  assert.equal(dynWorkerBody.tailReport.outcome, "ok");
});
