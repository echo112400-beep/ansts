/**
 * M15 Integration Test Suite (`TASK-69` ~ `TASK-74`):
 * Verifies optimizations grounded in `celld.dev/docs/services/{workers,dynamic-workers,durable-object-facets,kv,durable-objects}`:
 * - `TASK-69`: Durable Object Facets (`ctx.facets.get/abort/delete`, isolated SQLite DB, max depth 4,
 *              `storage.setAlarm()` throws inside facet & delegates to root cell, WebSocket `1001` close & hibernation pin check)
 * - `TASK-70`: Dynamic Workers (`worker_loaders`, `compatibilityDate` parity, `limits: { cpuMs, subrequests }`,
 *              `AbortSignal.timeout()` propagation, custom outbound `fetch` firewall, `getDurableObjectClass()`, and Tail Worker reports)
 * - `TASK-71`: `celld` KV Single-Writer Bypass & Sharded Namespace Guard (512B key, 25MiB value, >1MiB fleet bucket check,
 *              `expirationTtl >= 60s`, `<= 100` bulk get limit, request-local cache for `cacheStatus: null`)
 * - `TASK-72`: V8 Heap (`128 MB` limit, 90% WebSocket/Cursor.toArray refusal, 75% proactive GC) & Per-Statement SQL Budget Watchdog
 */

import * as assert from "node:assert/strict";
import { test } from "node:test";

import {
  AgentTaskHarnessCell,
  AnstsSchedulerCell,
  CelldKvOptimizerAndGuard,
  CloudflareCelldEnv,
  cloudflareCelldWorker,
  DurableObjectFacetSupervisor,
  DynamicWorkerSandboxLoader,
  MockDurableObjectState,
  V8HeapAndSqlBudgetGuard,
} from "../src/index";

test("M15 (TASK-69): DurableObjectFacetSupervisor isolates child SQLite DB, enforces depth<=4, throws on inner setAlarm, and closes WebSockets with 1001", () => {
  const supervisor = new DurableObjectFacetSupervisor("root-task-01", 2, 1);

  // 1. Start a child facet (`ctx.facets.get`) with structured-cloned startup props
  const originalProps = { subAgentRole: "auditor", maxTurns: 5 };
  const f1 = supervisor.getFacet("subagent-auditor-01", () => ({
    className: "SubAgentFacet",
    props: originalProps,
  }));
  // Mutating originalProps after startup must NOT mutate facet ctx.props
  originalProps.maxTurns = 999;
  assert.equal(f1.facet.props.maxTurns, 5);
  assert.equal(f1.facet.depth, 2);

  // 2. Write to the facet's isolated SQLite database
  f1.execSql("CREATE TABLE IF NOT EXISTS findings (id TEXT, severity TEXT)");
  f1.execSql("INSERT INTO findings (id, severity) VALUES (?, ?)", "f-01", "HIGH");
  assert.equal(f1.execSql("SELECT * FROM findings").length, 1);

  // 3. Per docs/services/durable-object-facets.md: storage.setAlarm() MUST throw inside a facet!
  assert.throws(
    () => f1.setAlarmInsideFacet(Date.now() + 5000),
    /FacetAlarmUnsupported: storage\.setAlarm\(\) throws inside a facet/,
  );
  // Delegating alarm to root supervisor succeeds
  f1.delegateAlarmToRoot(Date.now() + 5000);
  assert.equal(supervisor.rootAlarmScheduleMs.has("subagent-auditor-01"), true);

  // 4. Facet WebSocket keeps root cell resident (cannot hibernate) until closed; abort() closes socket with code 1001 & keeps DB
  const ws = f1.openWebSocket() as any;
  assert.equal(supervisor.isRootCellPinnedByFacetWebSockets(), true);

  const abortRes = supervisor.abortFacet("subagent-auditor-01", "SubAgentTurnTimeout");
  assert.equal(abortRes.stopped, true);
  assert.equal(abortRes.dbPreserved, true);
  assert.equal(abortRes.closedSockets, 1);
  assert.equal(ws.closedCode, 1001);
  assert.equal(supervisor.isRootCellPinnedByFacetWebSockets(), false);

  // Calling a stopped facet stub throws the abort reason
  assert.throws(
    () => supervisor.getFacet("subagent-auditor-01", () => ({ className: "SubAgentFacet" })),
    /FacetAborted: SubAgentTurnTimeout/,
  );

  // 5. delete() removes the facet and its SQLite database completely
  const delRes = supervisor.deleteFacet("subagent-auditor-01");
  assert.equal(delRes.deleted, true);
  assert.equal(supervisor.facets.has("subagent-auditor-01"), false);

  // 6. Enforce max facet depth = 4 (counting root object)
  const depth4Supervisor = new DurableObjectFacetSupervisor("deep-cell", 1, 4);
  assert.throws(
    () => depth4Supervisor.getFacet("too-deep", () => ({ className: "Leaf" })),
    /FacetMaxDepthExceeded/,
  );
});

test("M15 (TASK-70): DynamicWorkerSandboxLoader enforces compatibilityDate, cpuMs/subrequests limits, AbortSignal, outbound firewall & Tail Worker", async () => {
  const loader = new DynamicWorkerSandboxLoader();

  // 1. Missing compatibilityDate fails workerd parity check
  assert.throws(
    () =>
      loader.loadWorkerCode("bad-worker", {
        compatibilityDate: "",
        mainModule: "index.js",
        modules: { "index.js": "export default {}" },
      }),
    /WorkerCode requires compatibilityDate/,
  );

  // 2. Load valid Dynamic Worker with WASM module & strict CPU/subrequest limits
  loader.loadWorkerCode("ai-tool-plugin-01", {
    compatibilityDate: "2026-10-01",
    mainModule: "plugin.js",
    modules: {
      "plugin.js": "export class ToolPlugin {}",
      "fast_math.wasm": { wasm: new Uint8Array([0x00, 0x61, 0x73, 0x6d]) },
    },
    limits: { cpuMs: 50, subrequests: 2 },
  });

  // 3. Normal execution within limits succeeds and emits Tail Worker report
  const okRun = await loader.invokeEntrypoint({
    workerName: "ai-tool-plugin-01",
    props: { tenantId: "fin-enterprise" },
    simulatedCpuMs: 12,
    outboundSubrequests: ["https://api.deepseek.com/v1/models"],
    outboundFetchFirewall: (url) => url.startsWith("https://api.deepseek.com"),
    handler: (props) => ({ status: "PLUGIN_OK", tenant: props.tenantId }),
  });
  assert.equal(okRun.result.status, "PLUGIN_OK");
  assert.equal(okRun.tailReport.outcome, "ok");
  assert.equal(okRun.tailReport.logs[0].severity, "INFO");

  // 4. Exceeding `limits.cpuMs` throws catchable CPU limit error and records `exceededCpu` in Tail Worker
  await assert.rejects(
    async () => {
      await loader.invokeEntrypoint({
        workerName: "ai-tool-plugin-01",
        simulatedCpuMs: 120, // > 50ms limit!
        outboundSubrequests: [],
        handler: () => "never",
      });
    },
    /Worker exceeded CPU limit of 50 ms/,
  );
  assert.equal(loader.tailReports[loader.tailReports.length - 1].outcome, "exceededCpu");

  // 5. Custom outbound fetch firewall blocks unauthorized URLs
  await assert.rejects(
    async () => {
      await loader.invokeEntrypoint({
        workerName: "ai-tool-plugin-01",
        simulatedCpuMs: 5,
        outboundSubrequests: ["https://evil.example.com/exfil"],
        outboundFetchFirewall: (url) => url.startsWith("https://api.deepseek.com"),
        handler: () => "never",
      });
    },
    /DynamicWorkerOutboundBlocked/,
  );

  // 6. Extract Durable Object class for Facet instantiation & dispose Dynamic Worker
  const doClass = loader.getDurableObjectClass("ai-tool-plugin-01", "ToolPlugin");
  assert.equal(doClass.className, "ToolPlugin");
  assert.equal(loader.dispose("ai-tool-plugin-01"), true);
});

test("M15 (TASK-71 & TASK-72): CelldKvOptimizerAndGuard sharding/limits & V8HeapAndSqlBudgetGuard (128MB Heap & SQL Statement Watchdog)", () => {
  // 1. celld KV sharding, >=60s TTL rule, >1MiB fleet bucket rule, and request-local cache
  const kvOpt = new CelldKvOptimizerAndGuard(8, true);
  assert.throws(
    () => kvOpt.putSharded("short-ttl-key", "val", { expirationTtl: 30 }),
    /must be at least 60 seconds/,
  );

  const put1 = kvOpt.putSharded("idem:fin-enterprise:001", JSON.stringify({ status: "QUEUED" }), {
    expirationTtl: 120,
    metadata: { version: 1 },
  });
  assert.ok(put1.namespaceShardId >= 0 && put1.namespaceShardId < 8);
  assert.equal(put1.storedInFleetBucket, false);

  const reqCache = new Map<string, string>();
  const batchRes = kvOpt.bulkGetWithRequestCache(["idem:fin-enterprise:001", "missing-key"], reqCache);
  assert.ok(batchRes["idem:fin-enterprise:001"]?.includes("QUEUED"));
  assert.equal(batchRes["missing-key"], null);
  assert.equal(reqCache.has("idem:fin-enterprise:001"), true);

  // Value > 1 MiB without a fleet bucket must fail with `KV large values need a fleet bucket`
  const kvNoBucket = new CelldKvOptimizerAndGuard(4, false);
  const largeValue = "x".repeat(1024 * 1024 + 64);
  assert.throws(
    () => kvNoBucket.putSharded("large-checkpoint", largeValue),
    /KV large values need a fleet bucket/,
  );

  // 2. V8 Heap (128 MB) & SQL Statement Budget Guard (Issue #274 protection)
  const guard = new V8HeapAndSqlBudgetGuard({ heapLimitMb: 128, sqlStatementBudgetMs: 100 });
  const normalHeap = guard.checkIsolateHeapAdmission(64); // 50%
  assert.equal(normalHeap.acceptNewHibernatableWebSocket, true);
  assert.equal(normalHeap.forceGarbageCollection, false);

  const highHeap = guard.checkIsolateHeapAdmission(100); // 78.1% (>= 75% triggers GC, < 90% still allows WS)
  assert.equal(highHeap.forceGarbageCollection, true);
  assert.equal(highHeap.acceptNewHibernatableWebSocket, true);

  const criticalHeap = guard.checkIsolateHeapAdmission(120); // 93.75% (>= 90% refuses WS & Cursor.toArray!)
  assert.equal(criticalHeap.acceptNewHibernatableWebSocket, false);
  assert.equal(criticalHeap.allowCursorToArrayMaterialization, false);

  // Runaway recursive CTE exceeding 100ms budget is interrupted cleanly without poisoning cell
  assert.throws(
    () =>
      guard.executeBoundedSql({
        query: "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c) SELECT count(*) FROM c",
        simulatedDurationMs: 1500,
        executor: () => [],
      }),
    /CelldSqlBudgetExceeded/,
  );
});

test("M15 (TASK-73 & TASK-74): Edge Worker -> AgentTaskHarnessCell /facet endpoint end-to-end integration", async () => {
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
          fetch: (input: RequestInfo, init?: RequestInit) =>
            c.fetch(typeof input === "string" ? new Request(input, init) : input),
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

  const facetRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks/tsk-facet-e2e-01/facet", {
      method: "POST",
      body: JSON.stringify({
        facet_name: "dag-child-verifier",
        props: { role: "unit-test-runner" },
        sql_query: "INSERT INTO facet_kv (k, v) VALUES ('status', 'VERIFIED')",
        delegate_alarm_ms: Date.now() + 10_000,
      }),
    }),
    mockEnv,
  );
  assert.equal(facetRes.status, 200);
  const facetBody = (await facetRes.json()) as Record<string, any>;
  assert.equal(facetBody.facet_name, "dag-child-verifier");
  assert.equal(facetBody.facet_depth, 2);
  assert.equal(facetBody.facet_rows_count, 1);
  assert.equal(facetBody.root_alarms_count, 1);
});
