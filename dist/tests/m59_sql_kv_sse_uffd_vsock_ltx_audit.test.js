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
 * @fileoverview M59 End-to-End & Unit Verification Suite:
 * 1. [M59-1] `InMemoryCellSqlStorage` & `DurableDiskCellSqlStorage`:
 *    - Supports composite `PRIMARY KEY (col1, col2)` so multi-turn/multi-file rows sharing the first column are preserved rather than overwritten
 *    - Flushes `DELETE FROM` mutations to disk (`this.flushToDisk()`) so deleted rows never resurrect across cold-start rehydration
 *    - Enforces `SELECT ... LIMIT N` (both literal and `?` parameter bindings)
 * 2. [M59-2] `InMemoryKvNamespace` & `CelldKvOptimizerAndGuard`:
 *    - Enforces `expirationTtl` expiration on `get()` and rejects invalid/non-positive `expirationTtl`
 * 3. [M59-3] `LiveOpenAiCompatibleSseAdapter`:
 *    - Gracefully skips malformed JSON SSE data frames (`malformedFramesSkipped`), prevents negative `tpotMs` under clock skew, and preserves prompt token accounting on empty token arrays
 * 4. [M59-4] `UffdLazyPageFaultRestorer`:
 *    - Invalidates cached `faultedPages` when `stageRemoteSnapshotPage` re-stages a page index for a new task/turn (eliminating cross-task page digest pollution) and verifies backing SHA-256 integrity on page fault
 * 5. [M59-5] `ZeroTrustVsockBridge`:
 *    - Rejects non-HTTP/HTTPS schemes (`file://`, `ftp://`) in `HTTP_EGRESS` and blocks `rm -rf --no-preserve-root /`, `/proc/self/environ`, and cloud IMDS (`169.254.169.254`) in `EXEC_CMD`
 * 6. [M59-6] `DurableObjectFacetSupervisor` & `RustFsS3LtxDriver`:
 *    - Prunes terminal (`ABORTED`/`DELETED`) facet metadata history above `maxRetainedFacetRecords`
 *    - Rejects negative/non-integer `txId`, non-positive `fencingToken`, and traversal `cellId` in `putImmutableLtxSegment`
 * 7. Live HTTP `POST /v1beta1/management/system/sql-kv-sse-uffd-audit` & CLI `anstsctl mgmt-sql-kv-uffd-audit`.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const env_js_1 = require("../src/cloudflare/env.js");
const celld_native_optimizations_js_1 = require("../src/cloudflare/celld_native_optimizations.js");
const live_llm_adapter_js_1 = require("../src/router/live_llm_adapter.js");
const uffd_lazy_restorer_js_1 = require("../src/runtime/uffd_lazy_restorer.js");
const vsock_bridge_js_1 = require("../src/runtime/vsock_bridge.js");
const s3_ltx_driver_js_1 = require("../src/checkpoint/s3_ltx_driver.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M59: SQLite Composite-PK/DELETE-Flush/LIMIT Engine Hardening, KV TTL Expiration, SSE Malformed-Frame/Clock-Skew Resilience, UFFD Page Cache Invalidation, Vsock Scheme/IMDS Guard & LTX/Facet Metadata Hygiene", () => {
    (0, node_test_1.it)("validates all 6 storage/kv/sse/uffd/vsock/ltx invariants & live HTTP/CLI sql-kv-uffd-audit", async () => {
        // 1. [M59-1] Composite PRIMARY KEY, DELETE FROM disk flush & SELECT LIMIT
        const sqlPath = path.join(os.tmpdir(), `ansts-sql-m59-${process.pid}-${Date.now()}.json`);
        const diskSql1 = new env_js_1.DurableDiskCellSqlStorage(sqlPath);
        diskSql1.exec("CREATE TABLE IF NOT EXISTS harness_turns (task_id TEXT, turn_index INTEGER, ltx_key TEXT, PRIMARY KEY (task_id, turn_index))");
        diskSql1.exec("INSERT INTO harness_turns (task_id, turn_index, ltx_key) VALUES (?, ?, ?)", "task-1", 1, "k1");
        diskSql1.exec("INSERT INTO harness_turns (task_id, turn_index, ltx_key) VALUES (?, ?, ?)", "task-1", 2, "k2");
        diskSql1.exec("INSERT INTO harness_turns (task_id, turn_index, ltx_key) VALUES (?, ?, ?)", "task-1", 3, "k3");
        // All 3 turns for task-1 must coexist without overwriting each other!
        const allRows = diskSql1.exec("SELECT * FROM harness_turns WHERE task_id = ?", "task-1").toArray();
        assert.equal(allRows.length, 3);
        // SELECT ... LIMIT 2 must return exactly 2 rows
        const limRows = diskSql1
            .exec("SELECT * FROM harness_turns WHERE task_id = ? ORDER BY turn_index ASC LIMIT 2", "task-1")
            .toArray();
        assert.equal(limRows.length, 2);
        // DELETE turn 2 and rehydrate from disk: turn 2 must NOT resurrect!
        diskSql1.exec("DELETE FROM harness_turns WHERE task_id = ? AND turn_index = ?", "task-1", 2);
        const diskSql2 = new env_js_1.DurableDiskCellSqlStorage(sqlPath);
        const afterRestart = diskSql2.exec("SELECT * FROM harness_turns WHERE task_id = ?", "task-1").toArray();
        assert.equal(afterRestart.length, 2);
        // 2. [M59-2] InMemoryKvNamespace expirationTtl & CelldKvOptimizerAndGuard validation
        const kv = new env_js_1.InMemoryKvNamespace();
        await kv.put("k-ttl", "val-1", { expirationTtl: 10, nowMs: 1000 });
        assert.equal(await kv.get("k-ttl", 5000), "val-1");
        assert.equal(await kv.get("k-ttl", 11_000), null);
        const kvGuard = new celld_native_optimizations_js_1.CelldKvOptimizerAndGuard(kv, 16);
        await assert.rejects(() => kvGuard.putSafe("ns", "k1", "v1", -5), /expirationTtl must be a positive number/);
        // 3. [M59-3] LiveOpenAiCompatibleSseAdapter malformed JSON resilience, clock-skew guard & empty tokens
        const sse = new live_llm_adapter_js_1.LiveOpenAiCompatibleSseAdapter();
        const parsed = sse.parseSseTranscript([
            "data: {broken-json\n\n",
            'data: {"choices":[{"delta":{"content":"Hello"}}]}\n\n',
            'data: {"choices":[{"delta":{"content":" world"}}],"usage":{"prompt_tokens":12,"completion_tokens":2}}\n\n',
            "data: [DONE]\n\n",
        ], 1000, [1050, 1020] // simulated clock skew where second timestamp < first timestamp
        );
        assert.equal(parsed.assembledContent, "Hello world");
        assert.equal(parsed.malformedFramesSkipped, 1);
        assert.ok(parsed.tpotMs >= 0);
        const emptyCompletion = sse.streamAndParseCompletion({
            modelName: "gpt-4o",
            tokens: [],
            promptTokens: 42,
            cachedPromptTokens: 20,
            baseTtftMs: 30,
        });
        assert.equal(emptyCompletion.promptTokens, 42);
        // 4. [M59-4] UffdLazyPageFaultRestorer cache invalidation on re-staging
        const uffd = new uffd_lazy_restorer_js_1.UffdLazyPageFaultRestorer();
        const pageA = new Uint8Array(4096).fill(0xaa);
        const pageB = new Uint8Array(4096).fill(0xbb);
        const digA = uffd.stageRemoteSnapshotPage(0, pageA);
        const faultA = uffd.handlePageFault(0);
        assert.equal(faultA.sha256Digest, digA);
        assert.equal(faultA.fetchedFromRemote, true);
        // Re-staging page 0 with pageB must invalidate cached faultA and serve digB!
        const digB = uffd.stageRemoteSnapshotPage(0, pageB);
        const faultB = uffd.handlePageFault(0);
        assert.equal(faultB.sha256Digest, digB);
        assert.equal(faultB.fetchedFromRemote, true);
        // 5. [M59-5] ZeroTrustVsockBridge scheme & command hardening
        const vsock = new vsock_bridge_js_1.ZeroTrustVsockBridge(["api.openai.com"]);
        assert.throws(() => vsock.sendRequest({
            requestId: "r1",
            vmId: "vm-1",
            method: "HTTP_EGRESS",
            payload: { url: "ftp://api.openai.com/resource" },
        }), /Zero-trust egress policy blocked/);
        assert.throws(() => vsock.sendRequest({
            requestId: "r2",
            vmId: "vm-1",
            method: "EXEC_CMD",
            payload: { command: "rm -rf --no-preserve-root /", executeRealSubprocess: false },
        }), /blocked unsafe command/);
        assert.throws(() => vsock.sendRequest({
            requestId: "r3",
            vmId: "vm-1",
            method: "EXEC_CMD",
            payload: { command: "curl http://169.254.169.254/latest/meta-data/", executeRealSubprocess: false },
        }), /blocked unsafe command/);
        // 6. [M59-6] DurableObjectFacetSupervisor terminal history pruning & RustFsS3LtxDriver validation
        const facetSup = new celld_native_optimizations_js_1.DurableObjectFacetSupervisor(8);
        for (let i = 1; i <= 12; i++) {
            const { facetRecord } = facetSup.spawnToolFacet("t1", i, "tool", () => new env_js_1.InMemoryCellSqlStorage());
            await facetSup.deleteCompletedFacet(facetRecord.facetName);
        }
        assert.ok(facetSup.listFacets().length <= 8);
        const ltxDriver = new s3_ltx_driver_js_1.RustFsS3LtxDriver(new env_js_1.InMemoryR2Bucket());
        await assert.rejects(() => ltxDriver.putImmutableLtxSegment({
            cellId: "cell-1",
            txId: -1,
            fencingToken: 1,
            ltxBytes: new Uint8Array([1, 2, 3]),
        }), /Invalid LTX txId/);
        await assert.rejects(() => ltxDriver.putImmutableLtxSegment({
            cellId: "../escape",
            txId: 1,
            fencingToken: 1,
            ltxBytes: new Uint8Array([1, 2, 3]),
        }), /Invalid LTX cellId/);
        // 7. Verify live HTTP endpoint & CLI `mgmt-sql-kv-uffd-audit`
        const server = new server_main_js_1.AnstsLocalAndComposeServer({ nodeId: "gw-m59-test" });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-sql-kv-uffd-audit"], baseUrl);
            assert.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            assert.equal(body.allM59RisksEliminated, true);
            assert.equal(body.sqlCompositePkAndFlush1.sqlCompositePkAndFlushVerified, true);
            assert.equal(body.kvTtlExpiration2.kvTtlExpirationVerified, true);
            assert.equal(body.sseMalformedAndSkew3.sseMalformedAndSkewVerified, true);
            assert.equal(body.uffdCacheInvalidation4.uffdCacheInvalidationVerified, true);
            assert.equal(body.vsockSchemeAndCmdGuard5.vsockSchemeAndCmdGuardVerified, true);
            assert.equal(body.facetPruneAndLtxGuard6.facetPruneAndLtxGuardVerified, true);
        }
        finally {
            await server.stop();
        }
    });
});
