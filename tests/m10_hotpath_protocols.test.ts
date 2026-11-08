/**
 * @fileoverview M10 Hot-Path S3 CAS, celld Admin & Live LLM SSE Parser Suite (3 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  InMemoryR2Bucket,
  RustFsS3LtxDriver,
  S3CasPreconditionFailedError,
  CelldAdminClient,
  LiveOpenAiCompatibleSseAdapter,
} from "../src/index.js";

describe("M10 Hot-Path Protocols Suite", () => {
  it("1. RustFsS3LtxDriver signs SigV4 PUT and enforces If-None-Match:* CAS", async () => {
    const bucket = new InMemoryR2Bucket();
    const driver = new RustFsS3LtxDriver(bucket);
    const res = await driver.putImmutableLtxSegment({
      cellId: "cell-1",
      txId: 1,
      fencingToken: 1,
      ltxBytes: new Uint8Array([10, 20, 30]),
    });
    assert.ok(res.authHeader.startsWith("AWS4-HMAC-SHA256"));

    await assert.rejects(async () => {
      await driver.putImmutableLtxSegment({
        cellId: "cell-1",
        txId: 1,
        fencingToken: 1,
        ltxBytes: new Uint8Array([99]),
      });
    }, S3CasPreconditionFailedError);
  });

  it("2. CelldAdminClient migrates cell across bare-metal nodes and increments epoch fencing token", async () => {
    const admin = new CelldAdminClient();
    admin.registerCell("cell-mig-1", "node-1", 42);
    const mig = await admin.migrateCellToNode("cell-mig-1", "node-2");
    assert.equal(mig.previousNodeId, "node-1");
    assert.equal(mig.newNodeId, "node-2");
    assert.equal(mig.newEpochFencingToken, 2);
  });

  it("3. LiveOpenAiCompatibleSseAdapter parses streaming SSE tokens and computes TTFT/TPOT", () => {
    const adapter = new LiveOpenAiCompatibleSseAdapter();
    const frames = [
      'data: {"choices":[{"delta":{"content":"Hello"}}]}',
      'data: {"choices":[{"delta":{"content":" world"}}],"usage":{"prompt_tokens":100,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":80}}}',
      "data: [DONE]",
    ];
    const summary = adapter.parseSseTranscript(frames, 1000, [1040, 1090]);
    assert.equal(summary.assembledContent, "Hello world");
    assert.equal(summary.ttftMs, 40);
    assert.equal(summary.cachedPromptTokens, 80);
  });
});
