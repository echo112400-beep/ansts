"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M10 Hot-Path S3 CAS, celld Admin & Live LLM SSE Parser Suite (3 tests).
 */





(0, _nodeTest.describe)("M10 Hot-Path Protocols Suite", () => {
  (0, _nodeTest.it)("1. RustFsS3LtxDriver signs SigV4 PUT and enforces If-None-Match:* CAS", async () => {
    const bucket = new _index.InMemoryR2Bucket();
    const driver = new _index.RustFsS3LtxDriver(bucket);
    const res = await driver.putImmutableLtxSegment({
      cellId: "cell-1",
      txId: 1,
      fencingToken: 1,
      ltxBytes: new Uint8Array([10, 20, 30])
    });
    assert.ok(res.authHeader.startsWith("AWS4-HMAC-SHA256"));

    await assert.rejects(async () => {
      await driver.putImmutableLtxSegment({
        cellId: "cell-1",
        txId: 1,
        fencingToken: 1,
        ltxBytes: new Uint8Array([99])
      });
    }, _index.S3CasPreconditionFailedError);
  });

  (0, _nodeTest.it)("2. CelldAdminClient migrates cell across bare-metal nodes and increments epoch fencing token", async () => {
    const admin = new _index.CelldAdminClient();
    admin.registerCell("cell-mig-1", "node-1", 42);
    const mig = await admin.migrateCellToNode("cell-mig-1", "node-2");
    assert.equal(mig.previousNodeId, "node-1");
    assert.equal(mig.newNodeId, "node-2");
    assert.equal(mig.newEpochFencingToken, 2);
  });

  (0, _nodeTest.it)("3. LiveOpenAiCompatibleSseAdapter parses streaming SSE tokens and computes TTFT/TPOT", () => {
    const adapter = new _index.LiveOpenAiCompatibleSseAdapter();
    const frames = [
    'data: {"choices":[{"delta":{"content":"Hello"}}]}',
    'data: {"choices":[{"delta":{"content":" world"}}],"usage":{"prompt_tokens":100,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":80}}}',
    "data: [DONE]"];

    const summary = adapter.parseSseTranscript(frames, 1000, [1040, 1090]);
    assert.equal(summary.assembledContent, "Hello world");
    assert.equal(summary.ttftMs, 40);
    assert.equal(summary.cachedPromptTokens, 80);
  });
});