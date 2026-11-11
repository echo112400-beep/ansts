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
 * @fileoverview M43 End-to-End Verification: denoland/celld v0.6.3 Native Runtime Engine Endpoints,
 * LayerFS 2.0 Rule D9 Store Synchronizer, Closed-Loop Quota Admission Gate & Management Audit SSE Stream.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M43 denoland/celld v0.6.3 Native Runtime, LayerFS Rule D9, Quota Gate & Audit SSE Suite", () => {
    (0, node_test_1.it)("executes V8 isolate sandbox, DO Facets, 64-fetch Overload Guard, L0->L1 LTX compaction, Rule D9 409 guard, Quota 429 gate & Audit SSE", async () => {
        const tmpDir = `/tmp/ansts-m43-${process.pid}-${Date.now()}`;
        fs.mkdirSync(tmpDir, { recursive: true });
        const server = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-01",
            nodeRole: "gateway",
            storageDir: tmpDir,
        });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            // 1. [M43-1] Dynamic Worker V8 Isolate via CLI `celld-isolate` (200) and Egress Block (403)
            const isoCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["celld-isolate", "task-iso-1"], baseUrl);
            assert.equal(isoCli.exitCode, 0);
            const isoData = JSON.parse(isoCli.stdout);
            assert.equal(isoData.globalOutboundBlocked, true);
            assert.equal(isoData.output.val, 42);
            assert.equal(isoData.heapStatus.status, "HEALTHY");
            // Attempt outbound fetch inside isolate when allowInternetEgress=false -> 403 ISOLATE_EGRESS_BLOCKED
            const blockedIsoResp = await fetch(`${baseUrl}/v1beta1/celld/isolate`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "task-iso-blocked",
                    turnIndex: 1,
                    allowInternetEgress: false,
                    jsSourceCode: `export default { async fetch() { await fetch("https://example.com"); return new Response("{}"); } };`,
                }),
            });
            assert.equal(blockedIsoResp.status, 403);
            const blockedIsoBody = (await blockedIsoResp.json());
            assert.equal(blockedIsoBody.error, "ISOLATE_EGRESS_BLOCKED");
            // 2. [M43-1] DO Sub-Actor Facets & 32-Shard KV Optimizer (`POST /v1beta1/celld/facets`)
            const facetResp = await fetch(`${baseUrl}/v1beta1/celld/facets`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "spawnAndShardKv",
                    taskId: "task-facet-88",
                    turnIndex: 2,
                    toolName: "sqlite_analyzer",
                }),
            });
            assert.equal(facetResp.status, 200);
            const facetBody = (await facetResp.json());
            assert.equal(facetBody.facetRecord.status, "SPAWNED");
            assert.ok(facetBody.kvShard.physicalKey.startsWith("kv-cell-"));
            // 3. [M43-1] Hot-Cell 64-Fetch Overload Guard (`POST /v1beta1/celld/overload-guard`)
            const overloadResp = await fetch(`${baseUrl}/v1beta1/celld/overload-guard`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    cellScope: "AnstsSchedulerCell:hot-shard-01",
                    concurrentRequests: 65,
                    producerCount: 128,
                }),
            });
            assert.equal(overloadResp.status, 503);
            assert.equal(overloadResp.headers.get("retry-after"), "1");
            assert.equal(overloadResp.headers.get("x-celld-overload"), "cell");
            // 4. [M43-1] L0->L1 LTX Compaction, 128-Page WAL Truncation & UFFD Restore via CLI `ltx-compact`
            const compactCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["ltx-compact", "task-ns-8841"], baseUrl);
            assert.equal(compactCli.exitCode, 0);
            const compactData = JSON.parse(compactCli.stdout);
            assert.equal(compactData.l1Compaction.compactionTriggered, true);
            assert.equal(compactData.walTruncation.shouldTruncateWalAtCheckpoint, true);
            assert.ok(compactData.uffdRestore.eagerPagesLoaded > 0);
            // 5. [M43-2] LayerFS 2.0 Remote Store Rule D9 Synchronizer (`layerfs-store-sync` CLI & 409 stale push refusal)
            const push1Cli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["layerfs-store-sync", "push", "0"], baseUrl);
            assert.equal(push1Cli.exitCode, 0);
            const push1Data = JSON.parse(push1Cli.stdout);
            assert.equal(push1Data.remoteGeneration, 1);
            // Pushing again with stale generation 0 must be refused with HTTP 409 (Rule D9)
            const stalePushCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["layerfs-store-sync", "push", "0"], baseUrl);
            assert.equal(stalePushCli.exitCode, 1);
            assert.ok(stalePushCli.stdout.includes("LAYERFS_RULE_D9_STALE_PUSH_REFUSED"));
            // 6. [M43-3] Closed-Loop Management Quota Gate on `POST /v1beta1/tasks/dispatch` (429 -> Boost -> 202)
            await fetch(`${baseUrl}/v1beta1/management/scenario`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ scenario: "quota", actor: "chen" }),
            });
            const throttledDisp = await fetch(`${baseUrl}/v1beta1/tasks/dispatch`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "task-ns-throttled",
                    tenantId: "Northstar",
                    priority: 1,
                    promptText: "Should be blocked by Northstar 120/120 quota",
                }),
            });
            assert.equal(throttledDisp.status, 429);
            assert.equal(throttledDisp.headers.get("retry-after"), "30");
            // Boost Northstar quota to 160 in Management Plane, then re-dispatch -> 202 Accepted!
            await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-quota", "Northstar", "160", "1500", "14", "chen"], baseUrl);
            const admittedDisp = await fetch(`${baseUrl}/v1beta1/tasks/dispatch`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "task-ns-admitted",
                    tenantId: "Northstar",
                    priority: 1,
                    promptText: "Admitted after management quota boost",
                }),
            });
            assert.equal(admittedDisp.status, 202);
            // 7. [M43-3] Real-Time Management Audit SSE Stream (`GET /v1beta1/management/events/stream`)
            const sseResp = await fetch(`${baseUrl}/v1beta1/management/events/stream`);
            assert.equal(sseResp.status, 200);
            assert.ok((sseResp.headers.get("content-type") ?? "").includes("text/event-stream"));
            const sseText = await sseResp.text();
            assert.ok(sseText.includes("event: management.audit"));
            assert.ok(sseText.includes("Northstar"));
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
