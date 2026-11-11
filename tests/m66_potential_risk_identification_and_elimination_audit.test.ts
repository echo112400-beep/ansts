/**
 * @fileoverview M66 End-to-End Verification Suite:
 * Deep Potential Risk Identification & Elimination Suite:
 * - [M66-1] `TransactionalOutboxDispatcher` Timing-Safe HMAC Verification, Constructor Overload/MaxAttempts Guard & Dead-Letter Retry
 * - [M66-2] `CloudEventsBus` Bounded Idempotency Key Eviction + SQLite FinOps Persistence & `SseEventStreamGateway` CRLF Frame-Injection Sanitization + Orphan Subscription GC
 * - [M66-3] `LayerFsWorkspaceDriver` Path-Traversal Guard, Unreachable Commit/B-Tree GC & `LayerFsContentStoreEngine` Logical-Byte Accounting
 * - [M66-4] `WarmSandboxPoolManager` Target Clamping, `LayerFsNumberedSandboxMultiplexer` Closed-Sandbox Pruning & `ZeroTrustVsockHostDaemon` Client Frame/Timeout Guard
 * - [M66-5] Management Plane Endpoint (`POST /v1beta1/management/system/potential-risks-audit`) & CLI (`anstsctl mgmt-potential-risks-audit`)
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { InMemoryCellSqlStorage } from "../src/cloudflare/env.js";
import { TransactionalOutboxDispatcher } from "../src/telemetry/outbox_webhook.js";
import { CloudEventsBus } from "../src/telemetry/cloudevents.js";
import { SseEventStreamGateway } from "../src/gateway/sse_stream.js";
import { LayerFsWorkspaceDriver } from "../src/runtime/layerfs_workspace_driver.js";
import { DistributedCasGcSweeper } from "../src/storage/distributed_gc_sweeper.js";
import { WarmSandboxPoolManager, LayerFsNumberedSandboxMultiplexer } from "../src/runtime/warm_pool.js";
import { ZeroTrustVsockHostDaemon } from "../src/runtime/vsock_daemon.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M66: Deep Potential Risk Identification & Elimination Audit", () => {
  it("[M66-1] TransactionalOutboxDispatcher enforces timingSafeEqual HMAC verification, constructor overload/maxAttempts guard, and dead-letter retry", () => {
    const sql = new InMemoryCellSqlStorage();
    // Test 2-arg overload (signingSecret, sql) AND negative maxAttempts fallback
    const outboxOverloaded = new TransactionalOutboxDispatcher(undefined, sql);
    assert.equal(outboxOverloaded.maxAttempts, 4);

    const outboxInvalidMax = new TransactionalOutboxDispatcher("sec-m66", -5, sql);
    assert.equal(outboxInvalidMax.maxAttempts, 4);

    const outbox = new TransactionalOutboxDispatcher("sec-m66", 2, sql);
    assert.equal(outbox.maxAttempts, 2);

    const payloadJson = JSON.stringify({ event: "task.completed", taskId: "t-m66-1" });
    const sig = outbox.signPayload(payloadJson, 1700000000);
    // Standard signature and whitespace-separated signature must both verify
    assert.equal(outbox.verifySignature(payloadJson, sig, 1700000100, 300), true);
    const spacedSig = sig.replace(",", ", ");
    assert.equal(outbox.verifySignature(payloadJson, spacedSig, 1700000100, 300), true);
    // Tampered hex and non-hex must fail cleanly without throwing
    assert.equal(outbox.verifySignature(payloadJson, "t=1700000000,v1=zzzz", 1700000100, 300), false);
    assert.equal(
      outbox.verifySignature(payloadJson, sig.slice(0, -2) + "00", 1700000100, 300),
      false
    );

    // Enqueue and exhaust maxAttempts=2 -> deadLettered=true
    outbox.enqueueOutbox("msg-dlq-1", "t-m66-1", "https://bad.example/hook", { ok: 1 });
    outbox.processPending(() => false);
    const step2 = outbox.processPending(() => false);
    assert.equal(step2.deadLetterCount, 1);
    assert.equal(outbox.listMessages()[0].deadLettered, true);

    // Retry dead-lettered message with new target URL and deliver
    const retried = outbox.retryDeadLetteredMessage("msg-dlq-1", "https://good.example/hook");
    assert.ok(retried);
    assert.equal(retried.deadLettered, false);
    assert.equal(retried.attempts, 0);
    assert.equal(retried.targetUrl, "https://good.example/hook");

    const step3 = outbox.processPending(() => true);
    assert.equal(step3.deliveredCount, 1);

    // Cold-start SQLite rehydration preserves delivered state
    const outboxRehydrated = new TransactionalOutboxDispatcher("sec-m66", 2, sql);
    assert.equal(outboxRehydrated.listMessages()[0].delivered, true);
  });

  it("[M66-2] CloudEventsBus bounds seenIdempotencyKeys + persists FinOps to SQLite & SseEventStreamGateway sanitizes CRLF + GCs orphan task subscriptions", async () => {
    const sql = new InMemoryCellSqlStorage();
    const bus1 = new CloudEventsBus(10, sql); // maxIdempotencyEntries = 40
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
    assert.equal(bus1.allEvents().length, 10);
    assert.ok(bus1.getIdempotencyCacheSize() <= 40);

    const bus2 = new CloudEventsBus(10, sql);
    const report = bus2.getTenantReport("Northstar");
    assert.ok(report);
    assert.equal(report.totalLlmCostUsd, 0.65);

    const sse = new SseEventStreamGateway(10);
    // Subscribe to task-orphan (which will never publish any event) and task-crlf
    const orphanSub = sse.subscribe("task-orphan-0-events", "sub-orphan-1");
    const crlfSub = sse.subscribe("task-crlf", "sub-crlf-1");
    const chunks: string[] = [];
    crlfSub.stream.on("data", (c: Buffer) => chunks.push(c.toString("utf-8")));

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
    assert.equal(lines.length, 3);
    assert.ok(lines[0].startsWith("id: evt-1event: injected"));
    assert.ok(lines[1].startsWith("event: turn.okid: fake"));

    // GC completed task streams: task-orphan-0-events had 0 history events, but its open subscription MUST be closed and removed!
    assert.equal(sse.getActiveSubscriptionCount("task-orphan-0-events"), 1);
    const cleaned = sse.gcCompletedTaskStreams(new Set(["task-crlf"]));
    assert.equal(cleaned, 1);
    assert.equal(orphanSub.closed, true);
    assert.equal(sse.getActiveSubscriptionCount("task-orphan-0-events"), 0);
  });

  it("[M66-3] LayerFsWorkspaceDriver blocks path traversal, GCs unreachable commits & B-Tree pages, and maintains accurate logicalTotalBytes", () => {
    const driver = new LayerFsWorkspaceDriver();
    assert.throws(
      () =>
        driver.executeInProcessFileTool({
          branchName: "main",
          agentId: "agent-1",
          stepId: "s-traversal",
          operation: "write",
          filePath: "../../etc/passwd",
          newContent: "hacked",
        }),
      /Unsafe workspace filePath rejected/
    );
    assert.throws(
      () =>
        driver.executeInProcessFileTool({
          branchName: "main",
          agentId: "agent-1",
          stepId: "s-abs",
          operation: "write",
          filePath: "/tmp/escape.txt",
          newContent: "hacked",
        }),
      /Unsafe workspace filePath rejected/
    );
    assert.throws(
      () =>
        driver.executeEphemeralBashWorkspace({
          branchName: "main",
          agentId: "agent-1",
          stepId: "s-bash-traversal",
          command: "",
          overlayMutations: [{ filePath: "../escape.sh", content: "echo bad" }],
        }),
      /Unsafe workspace filePath rejected/
    );

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
    assert.ok(statsBeforeGc.logicalTotalBytes > baselineStats.logicalTotalBytes);
    assert.equal(driver.getCommitCount(), 2);
    assert.equal(driver.storeEngine.getTrackedCommitPageCount(), 1);

    // Delete branch and sweep via DistributedCasGcSweeper
    driver.deleteBranch("temp-feature");
    const sweeper = new DistributedCasGcSweeper();
    const sweepRes = sweeper.sweepLayerFsWorkspaceStore(driver);
    assert.equal(sweepRes.prunedCommitIds.length, 1);
    assert.equal(sweepRes.prunedBTreeCommitCount, 1);
    assert.equal( sweepRes.sweptBlobIds.length, 1);
    assert.equal(driver.getCommitCount(), 1);
    assert.equal(driver.storeEngine.getTrackedCommitPageCount(), 0);

    const statsAfterGc = driver.storeEngine.getStorageStats();
    assert.equal(statsAfterGc.logicalTotalBytes, baselineStats.logicalTotalBytes);
    assert.equal(statsAfterGc.storedCompressedBytes, baselineStats.storedCompressedBytes);
  });

  it("[M66-4] WarmSandboxPoolManager clamps invalid targetWarmCount, LayerFsNumberedSandboxMultiplexer prunes closed sandboxes & ZeroTrustVsockHostDaemon enforces frame guards", async () => {
    const pool = new WarmSandboxPoolManager("node-m66", -10);
    assert.equal(pool.targetWarmCount, 8);
    assert.equal(pool.stats().warmCount, 8);

    const mux = new LayerFsNumberedSandboxMultiplexer();
    const m1 = mux.mountWorkspaceIntoSandbox("ws-1", 1000);
    mux.unmountWorkspace(m1.sandboxNumber, "ws-1", 2000);
    assert.equal(mux.getActiveSandboxCount(), 1);

    const closed = mux.sweepIdleSandboxes(2000 + LayerFsNumberedSandboxMultiplexer.IDLE_CLOSE_TIMEOUT_MS + 10);
    assert.deepEqual(closed, [m1.sandboxNumber]);
    assert.equal(mux.getActiveSandboxCount(), 0);

    const sockPath = path.join(os.tmpdir(), `vsock-m66-${process.pid}-${Date.now()}.sock`);
    const daemon = new ZeroTrustVsockHostDaemon(["api.openai.com"], 4096);
    await daemon.startUnixSocketServer(sockPath);
    try {
      const resp = await daemon.sendClientFrameOverUnixSocket(
        sockPath,
        { type: "EGRESS_CHECK", requestId: "req-m66", hostname: "api.openai.com" },
        2000
      );
      assert.equal(resp.allowed, true);
      assert.equal(resp.status, 200);
    } finally {
      await daemon.stop();
    }
  });

  it("[M66-5] Live server exposes POST /v1beta1/management/system/potential-risks-audit & CLI anstsctl mgmt-potential-risks-audit", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m66-live-"));
    const srv = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
      tickIntervalMs: 60_000,
    });
    const { apiPort } = await srv.start();
    try {
      const cliRes = await runAnstsCtlRemote(
        ["mgmt-potential-risks-audit"],
        `http://127.0.0.1:${apiPort}`
      );
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout) as {
        allM66PotentialRiskChecksPassed: boolean;
        outboxSecurityAndDlq1: { outboxVerified: boolean };
        cloudEventsAndSse2: { cloudEventsAndSseVerified: boolean };
        layerFsTraversalAndGc3: { layerFsVerified: boolean };
        sandboxPoolAndVsock4: { sandboxAndVsockVerified: boolean };
      };
      assert.equal(body.outboxSecurityAndDlq1.outboxVerified, true);
      assert.equal(body.cloudEventsAndSse2.cloudEventsAndSseVerified, true);
      assert.equal(body.layerFsTraversalAndGc3.layerFsVerified, true);
      assert.equal(body.sandboxPoolAndVsock4.sandboxAndVsockVerified, true);
      assert.equal(body.allM66PotentialRiskChecksPassed, true);
    } finally {
      await srv.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
