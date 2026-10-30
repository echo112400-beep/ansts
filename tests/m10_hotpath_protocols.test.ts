/**
 * M10 Integration Test Suite (TASK-49 ~ TASK-52):
 * - TASK-49: Edge Worker + PartitionedSchedulerRouter + DynamicQueueAutoSharder + Harness RPO=0 & Durable Workflow hot path
 * - TASK-50: Real RustFS S3 HTTP protocol driver (AWS SigV4, If-Match / If-None-Match CAS, RPO=0 LTX upload, HTTP Range Paged Restore)
 * - TASK-51: celld Internal Admin Client (:9001 GET /state, POST /shutdown?handoff=preserve, rolling restart gate) & anstsctl fleet CLI
 * - TASK-52: Live OpenAI-Compatible SSE Streaming Adapter (DeepSeek / GLM-5.3 / Qwen-3.8, reasoning_content CoT parsing, TTFT, 429 failover)
 */

import * as assert from "node:assert/strict";
import * as http from "node:http";
import { AddressInfo } from "node:net";
import { test } from "node:test";

import {
  AgentTask,
  AgentTaskHarnessCell,
  AnstsSchedulerCell,
  CelldAdminClient,
  CloudflareCelldEnv,
  cloudflareCelldWorker,
  LiveOpenAiCompatibleSseAdapter,
  MockDurableObjectState,
  runCli,
  RustFsS3LtxDriver,
  S3CasPreconditionFailedError,
} from "../src/index";

test("M10 (TASK-49): Edge Worker hot-path multi-shard routing, dynamic queue auto-sharding, RPO=0 turn proof & Durable Workflow", async () => {
  const kvMap = new Map<string, string>();
  const schedulerShards = new Map<string, AnstsSchedulerCell>();
  const harnessCells = new Map<string, AgentTaskHarnessCell>();
  const queuedEvents: any[] = [];

  const env: CloudflareCelldEnv = {
    PRIMARY_MODEL_POOL: "DeepSeek",
    FAST_UTILITY_POOL: "GLM-5.3",
    FALLBACK_MODEL_POOL: "Qwen-3.8",
    WEBHOOK_HMAC_SECRET: "whsec_m10_test",
    SCHEDULER_CELL: {
      idFromName: (name: string) => ({ toString: () => name, name }),
      get: (id: { toString(): string; name?: string }) => {
        const key = id.name ?? id.toString();
        if (!schedulerShards.has(key)) {
          schedulerShards.set(key, new AnstsSchedulerCell(new MockDurableObjectState(key), env));
        }
        const cell = schedulerShards.get(key)!;
        return {
          fetch: (input: RequestInfo, init?: RequestInit) => {
            const req = typeof input === "string" ? new Request(input, init) : input;
            return cell.fetch(req);
          },
        };
      },
    },
    AGENT_HARNESS_CELL: {
      idFromName: (name: string) => ({ toString: () => name, name }),
      get: (id: { toString(): string; name?: string }) => {
        const key = id.name ?? id.toString();
        if (!harnessCells.has(key)) {
          harnessCells.set(key, new AgentTaskHarnessCell(new MockDurableObjectState(key), env));
        }
        const cell = harnessCells.get(key)!;
        return {
          fetch: (input: RequestInfo, init?: RequestInit) => {
            const req = typeof input === "string" ? new Request(input, init) : input;
            return cell.fetch(req);
          },
        };
      },
    },
    ANSTS_KV: {
      get: async (k: string, t?: "text" | "json") => {
        const v = kvMap.get(k);
        if (!v) return null;
        return t === "json" ? JSON.parse(v) : v;
      },
      put: async (k: string, v: string) => {
        kvMap.set(k, v);
      },
      delete: async (k: string) => {
        kvMap.delete(k);
      },
    },
    ANSTS_CHECKPOINTS_R2: {
      put: async (key: string) => ({ key }),
      get: async () => null,
      list: async () => ({ objects: [] }),
    },
    ANSTS_D1: {
      prepare: () => {
        const stmt = {
          bind: () => stmt,
          run: async () => ({ success: true }),
          all: async () => ({ results: [] }),
        };
        return stmt;
      },
    },
    ANSTS_EVENTS_QUEUE: {
      send: async (msg: any) => {
        queuedEvents.push(msg);
      },
    },
  };

  // 1. Submit 40 concurrent tasks for "fin-enterprise" -> triggers hot-tenant P2C multi-shard routing
  const hitShards = new Set<string>();
  for (let i = 0; i < 40; i++) {
    const resp = await cloudflareCelldWorker.fetch(
      new Request("https://ansts.edge/v1beta1/tasks", {
        method: "POST",
        body: JSON.stringify({
          apiVersion: "scheduler.agent.io/v1beta1",
          kind: "AgentTask",
          metadata: {
            task_id: `m10-hot-task-${i}`,
            tenant_id: "fin-enterprise",
            idempotency_key: `idem-m10-${i}`,
            priority_class: "P1",
          },
          spec: {
          },
        }),
      }),
      env,
    );
    assert.equal(resp.status, 202);
    const body = (await resp.json()) as Record<string, any>;
    assert.ok(body.scheduler_shard_id.startsWith("scheduler-cell-shard-"));
    hitShards.add(body.scheduler_shard_id);
  }
  assert.ok(hitShards.size >= 2, "Hot tenant submissions must distribute across multiple SchedulerCell shards");

  // 2. Scale out scheduler shards online via POST /v1beta1/scheduler/shards
  const scaleShardRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/scheduler/shards", {
      method: "POST",
      body: JSON.stringify({ add_shards: 4 }),
    }),
    env,
  );
  const scaleShardBody = (await scaleShardRes.json()) as Record<string, any>;
  assert.equal(scaleShardBody.added_shards.length, 4);
  assert.ok(scaleShardBody.total_shards >= 12);

  // 3. Execute turn on AgentTaskHarnessCell -> verifies embedded RPO=0 proof in response
  const turnRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks/m10-hot-task-0/turn", {
      method: "POST",
      body: JSON.stringify({
        epoch: 1,
        turn_seq: 1,
        is_read_only_turn: false,
        workspace_merkle_hash: "merkle_m10_1",
      }),
    }),
    env,
  );
  assert.equal(turnRes.status, 200);
  const turnBody = (await turnRes.json()) as Record<string, any>;
  assert.equal(turnBody.rpo_zero_proof.acknowledged, true);
  assert.equal(turnBody.rpo_zero_proof.proofType, "bucket");

  // 4. Execute Durable Workflow on AgentTaskHarnessCell with early event buffering
  const wfRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks/m10-hot-task-0/workflow", {
      method: "POST",
      body: JSON.stringify({
        event_to_deliver: { type: "sandbox.ready", payload: { ip: "10.240.0.15" } },
        sleep_duration_ms: 7200_000, // >1h -> hibernates V8 isolate
      }),
    }),
    env,
  );
  assert.equal(wfRes.status, 200);
  const wfBody = (await wfRes.json()) as Record<string, any>;
  assert.equal(wfBody.workflow_steps.step1.sandboxId, "sbx-m10-hot-task-0");
  assert.equal(wfBody.workflow_steps.step2.ip, "10.240.0.15");
  assert.equal(wfBody.workflow_steps.sleepInfo.hibernatedIsolate, true);
  assert.ok(wfBody.cached_step_count >= 2);
});

test("M10 (TASK-50): Real RustFS S3 HTTP driver (AWS SigV4, If-Match CAS, RPO=0 LTX upload & HTTP Range Paged Restore)", async () => {
  const objects = new Map<string, { body: Buffer; etag: string }>();
  let etagCounter = 1;
  let simulateConcurrentTakeoverBeforeReRead = false;

  const s3Server = http.createServer((req, res) => {
    const auth = req.headers.authorization ?? "";
    if (!auth.startsWith("AWS4-HMAC-SHA256 Credential=")) {
      res.writeHead(403);
      res.end("Missing AWS SigV4");
      return;
    }

    const key = req.url ?? "/";
    if (req.method === "PUT") {
      const chunks: Buffer[] = [];
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => {
        const body = Buffer.concat(chunks);
        const existing = objects.get(key);
        const ifNoneMatch = req.headers["if-none-match"];
        const ifMatch = req.headers["if-match"];

        if (ifNoneMatch === "*" && existing) {
          res.writeHead(412);
          res.end("Precondition Failed: Object exists");
          return;
        }
        if (ifMatch && (!existing || existing.etag !== ifMatch)) {
          res.writeHead(412);
          res.end("Precondition Failed: ETag mismatch");
          return;
        }

        const newEtag = `"etag-v${etagCounter++}"`;
        objects.set(key, { body, etag: newEtag });

        // If test flag is set and an LTX file was just uploaded, simulate a concurrent node stealing ownership.json
        if (simulateConcurrentTakeoverBeforeReRead && key.endsWith(".ltx")) {
          const ownKey = "/ansts-fleet/cells/default/cell-split-01/ownership.json";
          objects.set(ownKey, {
            body: Buffer.from(
              JSON.stringify({
                cellId: "cell-split-01",
                namespace: "default",
                ownerNodeId: "celld-node-02",
                epoch: 2,
                leaseExpiresAtMs: Date.now() + 10000,
                restoreBaseEpoch: 1,
              }),
              "utf8",
            ),
            etag: `"etag-v${etagCounter++}"`,
          });
        }

        res.writeHead(200, { ETag: newEtag });
        res.end();
      });
      return;
    }

    if (req.method === "GET") {
      const existing = objects.get(key);
      if (!existing) {
        res.writeHead(404);
        res.end();
        return;
      }
      const rangeHeader = req.headers.range;
      if (rangeHeader && rangeHeader.startsWith("bytes=")) {
        const [startStr, endStr] = rangeHeader.replace("bytes=", "").split("-");
        const start = Number(startStr);
        const end = Number(endStr);
        const slice = existing.body.subarray(start, end + 1);
        res.writeHead(206, {
          ETag: existing.etag,
          "Content-Range": `bytes ${start}-${end}/${existing.body.length}`,
          "Content-Length": String(slice.length),
        });
        res.end(slice);
        return;
      }

      res.writeHead(200, {
        ETag: existing.etag,
        "Content-Length": String(existing.body.length),
      });
      res.end(existing.body);
      return;
    }

    res.writeHead(405);
    res.end();
  });

  await new Promise<void>((resolve) => s3Server.listen(0, "127.0.0.1", resolve));
  const port = (s3Server.address() as AddressInfo).port;

  try {
    const driver = new RustFsS3LtxDriver({
      endpointUrl: `http://127.0.0.1:${port}`,
      bucket: "ansts-fleet",
      accessKeyId: "rustfsadmin",
      secretAccessKey: "rustfssecret123",
    });

    // 1. Initial CAS ownership acquisition (epoch = 1)
    const own1 = await driver.acquireOrRenewOwnershipCas({
      namespace: "default",
      cellId: "cell-split-01",
      nodeId: "celld-node-01",
    });
    assert.equal(own1.record.epoch, 1);

    // 2. Upload LTX frame and verify RPO=0 post-upload ownership re-read succeeds
    const ltxBytes = Buffer.from("LTX_HEADER_HOT_PAGE_0001_COLD_PAGE_0002_PAYLOAD_DATA", "utf8");
    const uploadRes = await driver.uploadLtxFrameWithRpoZeroProof({
      namespace: "default",
      cellId: "cell-split-01",
      nodeId: "celld-node-01",
      epoch: 1,
      txSeq: 1,
      ltxPayload: ltxBytes,
    });
    assert.equal(uploadRes.rpoZeroVerified, true);

    // 3. Paged Restore via HTTP Range (fetch hot header bytes 0..23)
    const rangeRes = await driver.fetchLtxPageRange(uploadRes.key, 0, 23);
    assert.equal(rangeRes.statusCode, 206);
    assert.equal(rangeRes.chunk.toString("utf8"), "LTX_HEADER_HOT_PAGE_0001");

    // 4. Simulate concurrent takeover during LTX upload -> RPO=0 post-upload re-read MUST abort!
    simulateConcurrentTakeoverBeforeReRead = true;
    await assert.rejects(
      async () => {
        await driver.uploadLtxFrameWithRpoZeroProof({
          namespace: "default",
          cellId: "cell-split-01",
          nodeId: "celld-node-01",
          epoch: 1,
          txSeq: 2,
          ltxPayload: Buffer.from("STALE_WRITE"),
        });
      },
      (err: any) => err instanceof S3CasPreconditionFailedError && err.message.includes("split-brain fence triggered"),
    );

    // 5. Write retired.json marker for Safe Epoch GC
    const retiredRes = await driver.writeRetiredEpochsMarker({
      namespace: "default",
      cellId: "cell-split-01",
      restoreBaseEpoch: 2,
      retiredEpochs: [1],
    });
    assert.ok(retiredRes.etag.includes("etag-v"));
  } finally {
    s3Server.close();
  }
});

test("M10 (TASK-51): CelldAdminClient (:9001 GET /state, POST /shutdown?handoff=preserve, rolling restart) & anstsctl fleet CLI", async () => {
  const nodeStates = [
    {
      node_id: "celld-node-01",
      healthy: true,
      draining: false,
      node_load: {
        owned_cells: 300,
        resident_cells: 60,
        restoring: 0,
        pressured: false,
        memory_headroom: 0.65,
        shed_cells: 0,
      },
    },
    {
      node_id: "celld-node-02",
      healthy: true,
      draining: false,
      node_load: {
        owned_cells: 120,
        resident_cells: 30,
        restoring: 0,
        pressured: false,
        memory_headroom: 0.80,
        shed_cells: 0,
      },
    },
  ];

  const adminServers = nodeStates.map((st) =>
    http.createServer((req, res) => {
      if (req.method === "GET" && req.url === "/state") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(st));
        return;
      }
      if (req.method === "POST" && req.url === "/shutdown?handoff=preserve") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            node_id: st.node_id,
            handoff_mode: "preserve",
            cold_activations_cancelled: 3,
            stopped_dbs_uploaded: st.node_load.resident_cells,
            ownership_records_preserved: true,
          }),
        );
        return;
      }
      res.writeHead(404);
      res.end();
    }),
  );

  for (const srv of adminServers) {
    await new Promise<void>((resolve) => srv.listen(0, "127.0.0.1", resolve));
  }
  const urls = adminServers.map((srv) => `http://127.0.0.1:${(srv.address() as AddressInfo).port}`);

  try {
    const client = new CelldAdminClient();
    const restartResult = await client.executeRollingRestart(urls);
    assert.deepEqual(restartResult.completedNodes, ["celld-node-01", "celld-node-02"]);
    assert.ok(restartResult.rebalancePlanBeforeRestart);
    assert.equal(restartResult.rebalancePlanBeforeRestart!.movedCells, 32);

    // Verify CLI `anstsctl fleet status | rebalance | rolling-restart`
    const cliStatus = runCli(["fleet", "status"]);
    assert.equal(cliStatus.totalOwnedCells, 600);
    const cliRebalance = runCli(["fleet", "rebalance"]);
    assert.equal(cliRebalance.movedCells, 32);
    const cliRolling = runCli(["fleet", "rolling-restart"]);
    assert.equal(cliRolling.rolling_gate.safeToAdvanceRollingUpdate, true);
    assert.equal(cliRolling.preserve_handoff.status, "HANDOFF_PRESERVE_READY");
  } finally {
    for (const srv of adminServers) {
      srv.close();
    }
  }
});

test("M10 (TASK-52): LiveOpenAiCompatibleSseAdapter streams reasoning_content + content, measures TTFT, and fails over on 429", async () => {
  let deepSeekReturn429 = false;

  const deepSeekServer = http.createServer((_req, res) => {
    if (deepSeekReturn429) {
      res.writeHead(429, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "Rate limit exceeded" }));
      return;
    }
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    });
    res.write(`data: ${JSON.stringify({ choices: [{ delta: { reasoning_content: "Analyzing AST lock..." } }] })}\n\n`);
    res.write(`data: ${JSON.stringify({ choices: [{ delta: { content: "Patch applied cleanly." } }] })}\n\n`);
    res.write("data: [DONE]\n\n");
    res.end();
  });

  const qwenFallbackServer = http.createServer((_req, res) => {
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.write(`data: ${JSON.stringify({ choices: [{ delta: { reasoning_content: "Fallback CoT..." } }] })}\n\n`);
    res.write(`data: ${JSON.stringify({ choices: [{ delta: { content: "Recovered via Qwen-3.8!" } }] })}\n\n`);
    res.write("data: [DONE]\n\n");
    res.end();
  });

  await new Promise<void>((r) => deepSeekServer.listen(0, "127.0.0.1", r));
  await new Promise<void>((r) => qwenFallbackServer.listen(0, "127.0.0.1", r));

  const dsPort = (deepSeekServer.address() as AddressInfo).port;
  const qwPort = (qwenFallbackServer.address() as AddressInfo).port;

  try {
    const adapter = new LiveOpenAiCompatibleSseAdapter([
      {
        poolName: "DeepSeek",
        baseUrl: `http://127.0.0.1:${dsPort}/v1/chat/completions`,
        apiKey: "sk-ds-test",
        modelId: "deepseek-reasoner",
      },
      {
        poolName: "Qwen-3.8",
        baseUrl: `http://127.0.0.1:${qwPort}/v1/chat/completions`,
        apiKey: "sk-qw-test",
        modelId: "qwen-3.8",
      },
    ]);

    const task = new AgentTask({ taskId: "tsk-sse-01", tenantId: "fin-enterprise" });
    const deltas: string[] = [];

    // 1. Primary DeepSeek SSE stream succeeds and separates reasoning_content from content
    const turn1 = await adapter.streamChatCompletionTurn({
      task,
      preferredPool: "DeepSeek",
      messages: [{ role: "user", content: "Fix race condition" }],
      onDelta: (d) => deltas.push(`${d.type}:${d.text}`),
    });
    assert.equal(turn1.usedPool, "DeepSeek");
    assert.equal(turn1.failoverAttempted, false);
    assert.equal(turn1.reasoningText, "Analyzing AST lock...");
    assert.equal(turn1.contentText, "Patch applied cleanly.");
    assert.ok(turn1.ttftMs >= 0);
    assert.equal(deltas.length, 2);

    // 2. Trip 429 on DeepSeek -> automatically fails over to Qwen-3.8 SSE stream
    deepSeekReturn429 = true;
    const turn2 = await adapter.streamChatCompletionTurn({
      task,
      preferredPool: "DeepSeek",
      fallbackPools: ["Qwen-3.8"],
      messages: [{ role: "user", content: "Continue verification" }],
    });
    assert.equal(turn2.usedPool, "Qwen-3.8");
    assert.equal(turn2.failoverAttempted, true);
    assert.equal(turn2.failoverFromPool, "DeepSeek");
    assert.equal(turn2.contentText, "Recovered via Qwen-3.8!");
    assert.ok(task.usedTokens > 0);
    assert.ok(task.activeComputeSec > 0);
  } finally {
    deepSeekServer.close();
    qwenFallbackServer.close();
  }
});
