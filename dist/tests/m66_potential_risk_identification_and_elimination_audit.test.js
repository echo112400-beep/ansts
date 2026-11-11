"use strict";








var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));

var _env = require("../src/cloudflare/env.js");
var _outbox_webhook = require("../src/telemetry/outbox_webhook.js");
var _cloudevents = require("../src/telemetry/cloudevents.js");
var _sse_stream = require("../src/gateway/sse_stream.js");
var _layerfs_workspace_driver = require("../src/runtime/layerfs_workspace_driver.js");
var _distributed_gc_sweeper = require("../src/storage/distributed_gc_sweeper.js");
var _warm_pool = require("../src/runtime/warm_pool.js");
var _vsock_daemon = require("../src/runtime/vsock_daemon.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M66 End-to-End Verification Suite:
 * Deep Potential Risk Identification & Elimination Suite:
 * - [M66-1] `TransactionalOutboxDispatcher` Timing-Safe HMAC Verification, Constructor Overload/MaxAttempts Guard & Dead-Letter Retry
 * - [M66-2] `CloudEventsBus` Bounded Idempotency Key Eviction + SQLite FinOps Persistence & `SseEventStreamGateway` CRLF Frame-Injection Sanitization + Orphan Subscription GC
 * - [M66-3] `LayerFsWorkspaceDriver` Path-Traversal Guard, Unreachable Commit/B-Tree GC & `LayerFsContentStoreEngine` Logical-Byte Accounting
 * - [M66-4] `WarmSandboxPoolManager` Target Clamping, `LayerFsNumberedSandboxMultiplexer` Closed-Sandbox Pruning & `ZeroTrustVsockHostDaemon` Client Frame/Timeout Guard
 * - [M66-5] Management Plane Endpoint (`POST /v1beta1/management/system/potential-risks-audit`) & CLI (`anstsctl mgmt-potential-risks-audit`)
 */(0, _nodeTest.describe)("M66: Deep Potential Risk Identification & Elimination Audit", () => {(0, _nodeTest.it)("[M66-1] TransactionalOutboxDispatcher enforces timingSafeEqual HMAC verification, constructor overload/maxAttempts guard, and dead-letter retry", () => {const sql = new _env.InMemoryCellSqlStorage(); // Test 2-arg overload (signingSecret, sql) AND negative maxAttempts fallback
    const outboxOverloaded = new _outbox_webhook.TransactionalOutboxDispatcher(undefined, sql);_strict.default.equal(outboxOverloaded.maxAttempts, 4);const outboxInvalidMax = new _outbox_webhook.TransactionalOutboxDispatcher("sec-m66", -5, sql);
    _strict.default.equal(outboxInvalidMax.maxAttempts, 4);

    const outbox = new _outbox_webhook.TransactionalOutboxDispatcher("sec-m66", 2, sql);
    _strict.default.equal(outbox.maxAttempts, 2);

    const payloadJson = JSON.stringify({ event: "task.completed", taskId: "t-m66-1" });
    const sig = outbox.signPayload(payloadJson, 1700000000);
    // Standard signature and whitespace-separated signature must both verify
    _strict.default.equal(outbox.verifySignature(payloadJson, sig, 1700000100, 300), true);
    const spacedSig = sig.replace(",", ", ");
    _strict.default.equal(outbox.verifySignature(payloadJson, spacedSig, 1700000100, 300), true);
    // Tampered hex and non-hex must fail cleanly without throwing
    _strict.default.equal(outbox.verifySignature(payloadJson, "t=1700000000,v1=zzzz", 1700000100, 300), false);
    _strict.default.equal(
    outbox.verifySignature(payloadJson, sig.slice(0, -2) + "00", 1700000100, 300),
    false);


    // Enqueue and exhaust maxAttempts=2 -> deadLettered=true
    outbox.enqueueOutbox("msg-dlq-1", "t-m66-1", "https://bad.example/hook", { ok: 1 });
    outbox.processPending(() => false);
    const step2 = outbox.processPending(() => false);
    _strict.default.equal(step2.deadLetterCount, 1);
    _strict.default.equal(outbox.listMessages()[0].deadLettered, true);

    // Retry dead-lettered message with new target URL and deliver
    const retried = outbox.retryDeadLetteredMessage("msg-dlq-1", "https://good.example/hook");
    _strict.default.ok(retried);
    _strict.default.equal(retried.deadLettered, false);
    _strict.default.equal(retried.attempts, 0);
    _strict.default.equal(retried.targetUrl, "https://good.example/hook");

    const step3 = outbox.processPending(() => true);
    _strict.default.equal(step3.deliveredCount, 1);

    // Cold-start SQLite rehydration preserves delivered state
    const outboxRehydrated = new _outbox_webhook.TransactionalOutboxDispatcher("sec-m66", 2, sql);
    _strict.default.equal(outboxRehydrated.listMessages()[0].delivered, true);
  });

  (0, _nodeTest.it)("[M66-2] CloudEventsBus bounds seenIdempotencyKeys + persists FinOps to SQLite & SseEventStreamGateway sanitizes CRLF + GCs orphan task subscriptions", async () => {
    const sql = new _env.InMemoryCellSqlStorage();
    const bus1 = new _cloudevents.CloudEventsBus(10, sql); // maxIdempotencyEntries = 40
    for (let i = 0; i < 65; i++) {
      bus1.emitEvent({
        type: "ansts.turn.completed",
        subject: `task-${i}`,
        tenantId: "Northstar",
        idempotencyKey: `idem-ce-${i}`,
        llmCostUsd: 0.01,
        sandboxCpuCoreSeconds: 10
      });
    }
    _strict.default.equal(bus1.allEvents().length, 10);
    _strict.default.ok(bus1.getIdempotencyCacheSize() <= 40);

    const bus2 = new _cloudevents.CloudEventsBus(10, sql);
    const report = bus2.getTenantReport("Northstar");
    _strict.default.ok(report);
    _strict.default.equal(report.totalLlmCostUsd, 0.65);

    const sse = new _sse_stream.SseEventStreamGateway(10);
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
      data: { safe: true }
    });

    const rawFrame = chunks.join("");
    const lines = rawFrame.trim().split("\n");
    // Must only have 3 header lines: id, event, data (no injected extra SSE lines)
    _strict.default.equal(lines.length, 3);
    _strict.default.ok(lines[0].startsWith("id: evt-1event: injected"));
    _strict.default.ok(lines[1].startsWith("event: turn.okid: fake"));

    // GC completed task streams: task-orphan-0-events had 0 history events, but its open subscription MUST be closed and removed!
    _strict.default.equal(sse.getActiveSubscriptionCount("task-orphan-0-events"), 1);
    const cleaned = sse.gcCompletedTaskStreams(new Set(["task-crlf"]));
    _strict.default.equal(cleaned, 1);
    _strict.default.equal(orphanSub.closed, true);
    _strict.default.equal(sse.getActiveSubscriptionCount("task-orphan-0-events"), 0);
  });

  (0, _nodeTest.it)("[M66-3] LayerFsWorkspaceDriver blocks path traversal, GCs unreachable commits & B-Tree pages, and maintains accurate logicalTotalBytes", () => {
    const driver = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    _strict.default.throws(
    () =>
    driver.executeInProcessFileTool({
      branchName: "main",
      agentId: "agent-1",
      stepId: "s-traversal",
      operation: "write",
      filePath: "../../etc/passwd",
      newContent: "hacked"
    }),
    /Unsafe workspace filePath rejected/);

    _strict.default.throws(
    () =>
    driver.executeInProcessFileTool({
      branchName: "main",
      agentId: "agent-1",
      stepId: "s-abs",
      operation: "write",
      filePath: "/tmp/escape.txt",
      newContent: "hacked"
    }),
    /Unsafe workspace filePath rejected/);

    _strict.default.throws(
    () =>
    driver.executeEphemeralBashWorkspace({
      branchName: "main",
      agentId: "agent-1",
      stepId: "s-bash-traversal",
      command: "",
      overlayMutations: [{ filePath: "../escape.sh", content: "echo bad" }]
    }),
    /Unsafe workspace filePath rejected/);


    const baselineStats = driver.storeEngine.getStorageStats();
    driver.forkBranch("main", "temp-feature");
    driver.executeInProcessFileTool({
      branchName: "temp-feature",
      agentId: "agent-1",
      stepId: "s-temp-1",
      operation: "write",
      filePath: "src/temp_module.ts",
      newContent: "export const tempValue = 'ephemeral-branch-payload-1234567890';"
    });

    const statsBeforeGc = driver.storeEngine.getStorageStats();
    _strict.default.ok(statsBeforeGc.logicalTotalBytes > baselineStats.logicalTotalBytes);
    _strict.default.equal(driver.getCommitCount(), 2);
    _strict.default.equal(driver.storeEngine.getTrackedCommitPageCount(), 1);

    // Delete branch and sweep via DistributedCasGcSweeper
    driver.deleteBranch("temp-feature");
    const sweeper = new _distributed_gc_sweeper.DistributedCasGcSweeper();
    const sweepRes = sweeper.sweepLayerFsWorkspaceStore(driver);
    _strict.default.equal(sweepRes.prunedCommitIds.length, 1);
    _strict.default.equal(sweepRes.prunedBTreeCommitCount, 1);
    _strict.default.equal(sweepRes.sweptBlobIds.length, 1);
    _strict.default.equal(driver.getCommitCount(), 1);
    _strict.default.equal(driver.storeEngine.getTrackedCommitPageCount(), 0);

    const statsAfterGc = driver.storeEngine.getStorageStats();
    _strict.default.equal(statsAfterGc.logicalTotalBytes, baselineStats.logicalTotalBytes);
    _strict.default.equal(statsAfterGc.storedCompressedBytes, baselineStats.storedCompressedBytes);
  });

  (0, _nodeTest.it)("[M66-4] WarmSandboxPoolManager clamps invalid targetWarmCount, LayerFsNumberedSandboxMultiplexer prunes closed sandboxes & ZeroTrustVsockHostDaemon enforces frame guards", async () => {
    const pool = new _warm_pool.WarmSandboxPoolManager("node-m66", -10);
    _strict.default.equal(pool.targetWarmCount, 8);
    _strict.default.equal(pool.stats().warmCount, 8);

    const mux = new _warm_pool.LayerFsNumberedSandboxMultiplexer();
    const m1 = mux.mountWorkspaceIntoSandbox("ws-1", 1000);
    mux.unmountWorkspace(m1.sandboxNumber, "ws-1", 2000);
    _strict.default.equal(mux.getActiveSandboxCount(), 1);

    const closed = mux.sweepIdleSandboxes(2000 + _warm_pool.LayerFsNumberedSandboxMultiplexer.IDLE_CLOSE_TIMEOUT_MS + 10);
    _strict.default.deepEqual(closed, [m1.sandboxNumber]);
    _strict.default.equal(mux.getActiveSandboxCount(), 0);

    const sockPath = path.join(os.tmpdir(), `vsock-m66-${process.pid}-${Date.now()}.sock`);
    const daemon = new _vsock_daemon.ZeroTrustVsockHostDaemon(["api.openai.com"], 4096);
    await daemon.startUnixSocketServer(sockPath);
    try {
      const resp = await daemon.sendClientFrameOverUnixSocket(
      sockPath,
      { type: "EGRESS_CHECK", requestId: "req-m66", hostname: "api.openai.com" },
      2000);

      _strict.default.equal(resp.allowed, true);
      _strict.default.equal(resp.status, 200);
    } finally {
      await daemon.stop();
    }
  });

  (0, _nodeTest.it)("[M66-5] Live server exposes POST /v1beta1/management/system/potential-risks-audit & CLI anstsctl mgmt-potential-risks-audit", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m66-live-"));
    const srv = new _server_main.AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
      tickIntervalMs: 60_000
    });
    const { apiPort } = await srv.start();
    try {
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(
      ["mgmt-potential-risks-audit"],
      `http://127.0.0.1:${apiPort}`);

      _strict.default.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);






      _strict.default.equal(body.outboxSecurityAndDlq1.outboxVerified, true);
      _strict.default.equal(body.cloudEventsAndSse2.cloudEventsAndSseVerified, true);
      _strict.default.equal(body.layerFsTraversalAndGc3.layerFsVerified, true);
      _strict.default.equal(body.sandboxPoolAndVsock4.sandboxAndVsockVerified, true);
      _strict.default.equal(body.allM66PotentialRiskChecksPassed, true);
    } finally {
      await srv.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});