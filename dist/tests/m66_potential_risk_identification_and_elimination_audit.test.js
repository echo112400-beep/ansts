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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview M66 End-to-End Verification Suite:
 * Deep Potential Risk Identification & Elimination Suite:
 * - [M66-1] `TransactionalOutboxDispatcher` Timing-Safe HMAC Verification, Constructor Overload/MaxAttempts Guard & Dead-Letter Retry
 * - [M66-2] `CloudEventsBus` Bounded Idempotency Key Eviction + SQLite FinOps Persistence & `SseEventStreamGateway` CRLF Frame-Injection Sanitization + Orphan Subscription GC
 * - [M66-3] `LayerFsWorkspaceDriver` Path-Traversal Guard, Unreachable Commit/B-Tree GC & `LayerFsContentStoreEngine` Logical-Byte Accounting
 * - [M66-4] `WarmSandboxPoolManager` Target Clamping, `LayerFsNumberedSandboxMultiplexer` Closed-Sandbox Pruning & `ZeroTrustVsockHostDaemon` Client Frame/Timeout Guard
 * - [M66-5] Management Plane Endpoint (`POST /v1beta1/management/system/potential-risks-audit`) & CLI (`anstsctl mgmt-potential-risks-audit`)
 */
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const env_js_1 = require("../src/cloudflare/env.js");
const outbox_webhook_js_1 = require("../src/telemetry/outbox_webhook.js");
const cloudevents_js_1 = require("../src/telemetry/cloudevents.js");
const sse_stream_js_1 = require("../src/gateway/sse_stream.js");
const layerfs_workspace_driver_js_1 = require("../src/runtime/layerfs_workspace_driver.js");
const distributed_gc_sweeper_js_1 = require("../src/storage/distributed_gc_sweeper.js");
const warm_pool_js_1 = require("../src/runtime/warm_pool.js");
const vsock_daemon_js_1 = require("../src/runtime/vsock_daemon.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M66: Deep Potential Risk Identification & Elimination Audit", () => {
    (0, node_test_1.it)("[M66-1] TransactionalOutboxDispatcher enforces timingSafeEqual HMAC verification, constructor overload/maxAttempts guard, and dead-letter retry", () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        // Test 2-arg overload (signingSecret, sql) AND negative maxAttempts fallback
        const outboxOverloaded = new outbox_webhook_js_1.TransactionalOutboxDispatcher(undefined, sql);
        strict_1.default.equal(outboxOverloaded.maxAttempts, 4);
        const outboxInvalidMax = new outbox_webhook_js_1.TransactionalOutboxDispatcher("sec-m66", -5, sql);
        strict_1.default.equal(outboxInvalidMax.maxAttempts, 4);
        const outbox = new outbox_webhook_js_1.TransactionalOutboxDispatcher("sec-m66", 2, sql);
        strict_1.default.equal(outbox.maxAttempts, 2);
        const payloadJson = JSON.stringify({ event: "task.completed", taskId: "t-m66-1" });
        const sig = outbox.signPayload(payloadJson, 1700000000);
        // Standard signature and whitespace-separated signature must both verify
        strict_1.default.equal(outbox.verifySignature(payloadJson, sig, 1700000100, 300), true);
        const spacedSig = sig.replace(",", ", ");
        strict_1.default.equal(outbox.verifySignature(payloadJson, spacedSig, 1700000100, 300), true);
        // Tampered hex and non-hex must fail cleanly without throwing
        strict_1.default.equal(outbox.verifySignature(payloadJson, "t=1700000000,v1=zzzz", 1700000100, 300), false);
        strict_1.default.equal(outbox.verifySignature(payloadJson, sig.slice(0, -2) + "00", 1700000100, 300), false);
        // Enqueue and exhaust maxAttempts=2 -> deadLettered=true
        outbox.enqueueOutbox("msg-dlq-1", "t-m66-1", "https://bad.example/hook", { ok: 1 });
        outbox.processPending(() => false);
        const step2 = outbox.processPending(() => false);
        strict_1.default.equal(step2.deadLetterCount, 1);
        strict_1.default.equal(outbox.listMessages()[0].deadLettered, true);
        // Retry dead-lettered message with new target URL and deliver
        const retried = outbox.retryDeadLetteredMessage("msg-dlq-1", "https://good.example/hook");
        strict_1.default.ok(retried);
        strict_1.default.equal(retried.deadLettered, false);
        strict_1.default.equal(retried.attempts, 0);
        strict_1.default.equal(retried.targetUrl, "https://good.example/hook");
        const step3 = outbox.processPending(() => true);
        strict_1.default.equal(step3.deliveredCount, 1);
        // Cold-start SQLite rehydration preserves delivered state
        const outboxRehydrated = new outbox_webhook_js_1.TransactionalOutboxDispatcher("sec-m66", 2, sql);
        strict_1.default.equal(outboxRehydrated.listMessages()[0].delivered, true);
    });
    (0, node_test_1.it)("[M66-2] CloudEventsBus bounds seenIdempotencyKeys + persists FinOps to SQLite & SseEventStreamGateway sanitizes CRLF + GCs orphan task subscriptions", async () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const bus1 = new cloudevents_js_1.CloudEventsBus(10, sql); // maxIdempotencyEntries = 40
        for (let i = 0; i < 65; i++) {
            bus1.emitEvent({
                type: "ansts.turn.completed",
                subject: `task-${i}`,
                tenantId: "Northstar",
                idempotencyKey: `idem-ce-${i}`,
                llmCostUsd: 0.01,
                sandboxCpuCoreSeconds: 10,
            });
        }
        strict_1.default.equal(bus1.allEvents().length, 10);
        strict_1.default.ok(bus1.getIdempotencyCacheSize() <= 40);
        const bus2 = new cloudevents_js_1.CloudEventsBus(10, sql);
        const report = bus2.getTenantReport("Northstar");
        strict_1.default.ok(report);
        strict_1.default.equal(report.totalLlmCostUsd, 0.65);
        const sse = new sse_stream_js_1.SseEventStreamGateway(10);
        // Subscribe to task-orphan (which will never publish any event) and task-crlf
        const orphanSub = sse.subscribe("task-orphan-0-events", "sub-orphan-1");
        const crlfSub = sse.subscribe("task-crlf", "sub-crlf-1");
        const chunks = [];
        crlfSub.stream.on("data", (c) => chunks.push(c.toString("utf-8")));
        sse.publish({
            id: "evt-1\r\nevent: injected\r\ndata: {\"hack\":true}",
            event: "turn.ok\r\nid: fake",
            taskId: "task-crlf",
            turnIndex: 1,
            timestampMs: Date.now(),
            data: { safe: true },
        });
        const rawFrame = chunks.join("");
        const lines = rawFrame.trim().split("\n");
        // Must only have 3 header lines: id, event, data (no injected extra SSE lines)
        strict_1.default.equal(lines.length, 3);
        strict_1.default.ok(lines[0].startsWith("id: evt-1event: injected"));
        strict_1.default.ok(lines[1].startsWith("event: turn.okid: fake"));
        // GC completed task streams: task-orphan-0-events had 0 history events, but its open subscription MUST be closed and removed!
        strict_1.default.equal(sse.getActiveSubscriptionCount("task-orphan-0-events"), 1);
        const cleaned = sse.gcCompletedTaskStreams(new Set(["task-crlf"]));
        strict_1.default.equal(cleaned, 1);
        strict_1.default.equal(orphanSub.closed, true);
        strict_1.default.equal(sse.getActiveSubscriptionCount("task-orphan-0-events"), 0);
    });
    (0, node_test_1.it)("[M66-3] LayerFsWorkspaceDriver blocks path traversal, GCs unreachable commits & B-Tree pages, and maintains accurate logicalTotalBytes", () => {
        const driver = new layerfs_workspace_driver_js_1.LayerFsWorkspaceDriver();
        strict_1.default.throws(() => driver.executeInProcessFileTool({
            branchName: "main",
            agentId: "agent-1",
            stepId: "s-traversal",
            operation: "write",
            filePath: "../../etc/passwd",
            newContent: "hacked",
        }), /Unsafe workspace filePath rejected/);
        strict_1.default.throws(() => driver.executeInProcessFileTool({
            branchName: "main",
            agentId: "agent-1",
            stepId: "s-abs",
            operation: "write",
            filePath: "/tmp/escape.txt",
            newContent: "hacked",
        }), /Unsafe workspace filePath rejected/);
        strict_1.default.throws(() => driver.executeEphemeralBashWorkspace({
            branchName: "main",
            agentId: "agent-1",
            stepId: "s-bash-traversal",
            command: "",
            overlayMutations: [{ filePath: "../escape.sh", content: "echo bad" }],
        }), /Unsafe workspace filePath rejected/);
        const baselineStats = driver.storeEngine.getStorageStats();
        driver.forkBranch("main", "temp-feature");
        driver.executeInProcessFileTool({
            branchName: "temp-feature",
            agentId: "agent-1",
            stepId: "s-temp-1",
            operation: "write",
            filePath: "src/temp_module.ts",
            newContent: "export const tempValue = 'ephemeral-branch-payload-1234567890';",
        });
        const statsBeforeGc = driver.storeEngine.getStorageStats();
        strict_1.default.ok(statsBeforeGc.logicalTotalBytes > baselineStats.logicalTotalBytes);
        strict_1.default.equal(driver.getCommitCount(), 2);
        strict_1.default.equal(driver.storeEngine.getTrackedCommitPageCount(), 1);
        // Delete branch and sweep via DistributedCasGcSweeper
        driver.deleteBranch("temp-feature");
        const sweeper = new distributed_gc_sweeper_js_1.DistributedCasGcSweeper();
        const sweepRes = sweeper.sweepLayerFsWorkspaceStore(driver);
        strict_1.default.equal(sweepRes.prunedCommitIds.length, 1);
        strict_1.default.equal(sweepRes.prunedBTreeCommitCount, 1);
        strict_1.default.equal(sweepRes.sweptBlobIds.length, 1);
        strict_1.default.equal(driver.getCommitCount(), 1);
        strict_1.default.equal(driver.storeEngine.getTrackedCommitPageCount(), 0);
        const statsAfterGc = driver.storeEngine.getStorageStats();
        strict_1.default.equal(statsAfterGc.logicalTotalBytes, baselineStats.logicalTotalBytes);
        strict_1.default.equal(statsAfterGc.storedCompressedBytes, baselineStats.storedCompressedBytes);
    });
    (0, node_test_1.it)("[M66-4] WarmSandboxPoolManager clamps invalid targetWarmCount, LayerFsNumberedSandboxMultiplexer prunes closed sandboxes & ZeroTrustVsockHostDaemon enforces frame guards", async () => {
        const pool = new warm_pool_js_1.WarmSandboxPoolManager("node-m66", -10);
        strict_1.default.equal(pool.targetWarmCount, 8);
        strict_1.default.equal(pool.stats().warmCount, 8);
        const mux = new warm_pool_js_1.LayerFsNumberedSandboxMultiplexer();
        const m1 = mux.mountWorkspaceIntoSandbox("ws-1", 1000);
        mux.unmountWorkspace(m1.sandboxNumber, "ws-1", 2000);
        strict_1.default.equal(mux.getActiveSandboxCount(), 1);
        const closed = mux.sweepIdleSandboxes(2000 + warm_pool_js_1.LayerFsNumberedSandboxMultiplexer.IDLE_CLOSE_TIMEOUT_MS + 10);
        strict_1.default.deepEqual(closed, [m1.sandboxNumber]);
        strict_1.default.equal(mux.getActiveSandboxCount(), 0);
        const sockPath = path.join(os.tmpdir(), `vsock-m66-${process.pid}-${Date.now()}.sock`);
        const daemon = new vsock_daemon_js_1.ZeroTrustVsockHostDaemon(["api.openai.com"], 4096);
        await daemon.startUnixSocketServer(sockPath);
        try {
            const resp = await daemon.sendClientFrameOverUnixSocket(sockPath, { type: "EGRESS_CHECK", requestId: "req-m66", hostname: "api.openai.com" }, 2000);
            strict_1.default.equal(resp.allowed, true);
            strict_1.default.equal(resp.status, 200);
        }
        finally {
            await daemon.stop();
        }
    });
    (0, node_test_1.it)("[M66-5] Live server exposes POST /v1beta1/management/system/potential-risks-audit & CLI anstsctl mgmt-potential-risks-audit", async () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m66-live-"));
        const srv = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            apiPort: 0,
            dashboardPort: 0,
            storageDir: tmpDir,
            tickIntervalMs: 60_000,
        });
        const { apiPort } = await srv.start();
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-potential-risks-audit"], `http://127.0.0.1:${apiPort}`);
            strict_1.default.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            strict_1.default.equal(body.outboxSecurityAndDlq1.outboxVerified, true);
            strict_1.default.equal(body.cloudEventsAndSse2.cloudEventsAndSseVerified, true);
            strict_1.default.equal(body.layerFsTraversalAndGc3.layerFsVerified, true);
            strict_1.default.equal(body.sandboxPoolAndVsock4.sandboxAndVsockVerified, true);
            strict_1.default.equal(body.allM66PotentialRiskChecksPassed, true);
        }
        finally {
            await srv.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
