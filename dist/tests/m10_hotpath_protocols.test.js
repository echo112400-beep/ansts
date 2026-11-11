"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview M10 Hot-Path S3 CAS, celld Admin & Live LLM SSE Parser Suite (3 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("M10 Hot-Path Protocols Suite", () => {
    (0, node_test_1.it)("1. RustFsS3LtxDriver signs SigV4 PUT and enforces If-None-Match:* CAS", async () => {
        const bucket = new index_js_1.InMemoryR2Bucket();
        const driver = new index_js_1.RustFsS3LtxDriver(bucket);
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
        }, index_js_1.S3CasPreconditionFailedError);
    });
    (0, node_test_1.it)("2. CelldAdminClient migrates cell across bare-metal nodes and increments epoch fencing token", async () => {
        const admin = new index_js_1.CelldAdminClient();
        admin.registerCell("cell-mig-1", "node-1", 42);
        const mig = await admin.migrateCellToNode("cell-mig-1", "node-2");
        assert.equal(mig.previousNodeId, "node-1");
        assert.equal(mig.newNodeId, "node-2");
        assert.equal(mig.newEpochFencingToken, 2);
    });
    (0, node_test_1.it)("3. LiveOpenAiCompatibleSseAdapter parses streaming SSE tokens and computes TTFT/TPOT", () => {
        const adapter = new index_js_1.LiveOpenAiCompatibleSseAdapter();
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
