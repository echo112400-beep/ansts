"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M15 denoland/celld Native Durable Object Facets, Dynamic Workers, KV Sharding & V8 Budget Suite (4 tests).
 */






(0, _nodeTest.describe)("M15 denoland/celld Native Capabilities Suite", () => {
  (0, _nodeTest.it)("1. DurableObjectFacetSupervisor spawns isolated sub-actor Facet with dedicated SQLite and supports abort/delete", async () => {
    const sup = new _index.DurableObjectFacetSupervisor();
    const { facetRecord, facetSql } = sup.spawnToolFacet(
    "task-f1",
    1,
    "python_repl",
    () => new _index.InMemoryCellSqlStorage());

    assert.equal(facetRecord.status, "SPAWNED");
    facetSql.exec("CREATE TABLE facet_local (k TEXT PRIMARY KEY, v TEXT)");

    const aborted = sup.abortFaultyFacet(facetRecord.facetName, new Error("Tool panic"));
    assert.equal(aborted.status, "ABORTED");
    const deleted = await sup.deleteCompletedFacet(facetRecord.facetName);
    assert.equal(deleted, true);
  });

  (0, _nodeTest.it)("2. DynamicWorkerSandboxLoader executes untrusted JS in millisecond V8 isolate with globalOutbound: null", async () => {
    const loader = new _index.DynamicWorkerSandboxLoader();
    const res = await loader.executeUntrustedCodeInIsolate({
      taskId: "task-dw-1",
      turnIndex: 2,
      jsSourceCode: "export default { async fetch() { return new Response('{}'); } }",
      allowInternetEgress: false,
      inputPayload: { x: 42 }
    });
    assert.equal(res.globalOutboundBlocked, true);
    assert.ok(res.coldStartLatencyMs < 10);
    const spec = loader.getLoadedSpec(res.isolateId);
    assert.equal(spec?.globalOutbound, null);
  });

  (0, _nodeTest.it)("3. CelldKvOptimizerAndGuard shards hot keys across 32 KV cells and enforces 25 MiB value limit", async () => {
    const kv = new _index.InMemoryKvNamespace();
    const guard = new _index.CelldKvOptimizerAndGuard(kv, 32);
    const putRes = await guard.putSafe("idem", "key-1", JSON.stringify({ ok: true }), 3600);
    assert.ok(putRes.shardId >= 0 && putRes.shardId < 32);
    const val = await guard.getSafe("idem", "key-1");
    assert.equal(val, JSON.stringify({ ok: true }));
  });

  (0, _nodeTest.it)("4. V8HeapAndSqlBudgetGuard enforces 128 MB V8 isolate memory watermarks and 5,000ms SQL timeout", () => {
    const guard = new _index.V8HeapAndSqlBudgetGuard(96, 128, 5000);
    assert.equal(guard.evaluateHeapPressure(64).status, "HEALTHY");
    assert.equal(guard.evaluateHeapPressure(105).status, "EVICT_WARM_BUFFERS");
    assert.equal(guard.evaluateHeapPressure(122).status, "REJECT_NEW_STREAMS");

    const sql = new _index.InMemoryCellSqlStorage();
    assert.throws(() => {
      guard.execWithBudget(sql, "SELECT * FROM slow_table", 6200);
    }, /exceeded celld per-statement timeout/);
  });
});