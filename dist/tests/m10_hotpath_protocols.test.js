"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
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