/**
 * @fileoverview M15 denoland/celld Native Durable Object Facets, Dynamic Workers, KV Sharding & V8 Budget Suite (4 tests).
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
} from "../src/index.js";

describe("M15 denoland/celld Native Capabilities Suite", () => {
  it("1. DurableObjectFacetSupervisor spawns isolated sub-actor Facet with dedicated SQLite and supports abort/delete", async () => {
    const sup = new DurableObjectFacetSupervisor();
    const { facetRecord, facetSql } = sup.spawnToolFacet(
      "task-f1",
      1,
      "python_repl",
      () => new InMemoryCellSqlStorage()
    );
    assert.equal(facetRecord.status, "SPAWNED");
    facetSql.exec("CREATE TABLE facet_local (k TEXT PRIMARY KEY, v TEXT)");

    const aborted = sup.abortFaultyFacet(facetRecord.facetName, new Error("Tool panic"));
    assert.equal(aborted.status, "ABORTED");
    const deleted = await sup.deleteCompletedFacet(facetRecord.facetName);
    assert.equal(deleted, true);
  });

  it("2. DynamicWorkerSandboxLoader executes untrusted JS in millisecond V8 isolate with globalOutbound: null", async () => {
    const loader = new DynamicWorkerSandboxLoader();
    const res = await loader.executeUntrustedCodeInIsolate({
      taskId: "task-dw-1",
      turnIndex: 2,
      jsSourceCode: "export default { async fetch() { return new Response('{}'); } }",
      allowInternetEgress: false,
      inputPayload: { x: 42 },
    });
    assert.equal(res.globalOutboundBlocked, true);
    assert.ok(res.coldStartLatencyMs < 10);
    const spec = loader.getLoadedSpec(res.isolateId);
    assert.equal(spec?.globalOutbound, null);
  });

  it("3. CelldKvOptimizerAndGuard shards hot keys across 32 KV cells and enforces 25 MiB value limit", async () => {
    const kv = new InMemoryKvNamespace();
    const guard = new CelldKvOptimizerAndGuard(kv, 32);
    const putRes = await guard.putSafe("idem", "key-1", JSON.stringify({ ok: true }), 3600);
    assert.ok(putRes.shardId >= 0 && putRes.shardId < 32);
    const val = await guard.getSafe("idem", "key-1");
    assert.equal(val, JSON.stringify({ ok: true }));
  });

  it("4. V8HeapAndSqlBudgetGuard enforces 128 MB V8 isolate memory watermarks and 5,000ms SQL timeout", () => {
    const guard = new V8HeapAndSqlBudgetGuard(96, 128, 5000);
    assert.equal(guard.evaluateHeapPressure(64).status, "HEALTHY");
    assert.equal(guard.evaluateHeapPressure(105).status, "EVICT_WARM_BUFFERS");
    assert.equal(guard.evaluateHeapPressure(122).status, "REJECT_NEW_STREAMS");

    const sql = new InMemoryCellSqlStorage();
    assert.throws(() => {
      guard.execWithBudget(sql, "SELECT * FROM slow_table", 6200);
    }, /exceeded celld per-statement timeout/);
  });
});
