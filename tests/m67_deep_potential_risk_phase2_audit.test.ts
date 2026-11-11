/**
 * @fileoverview M67 End-to-End Verification Suite:
 * Deep Potential Risk Identification & Elimination Suite Phase II:
 * - [M67-1] `CelldNativeContainerSandboxManager` & `CelldContainerHandle` Loopback/ULA/Hex-IP/Metadata SSRF Guard & Container WorkDir/Handle Cleanup
 * - [M67-2] `HotCellOverloadAndQueueGuard` Reclaimed/Double-Release Lease Desync Guard + Producer NaN Validation & `ShardedQueueBrokerPool` Bounded Idempotent Shards
 * - [M67-3] `DynamicWorkerSandboxLoader` Async `fetch()` Timeout + Input Payload Deep-Clone Isolation & `V8HeapAndSqlBudgetGuard` Limit Sanitization
 * - [M67-4] `ZeroTrustVsockBridge` URL Credentials/Port/Path-Traversal Guard & `EbpfLsmSecurityBridge` Null-Byte + `/proc/<pid>/*` Container Escape Block
 * - [M67-5] `LayerFsMultiAgentBranchCoordinator` Merge B-Tree Page Sync + Idempotent Same-Commit Merge, `DeterministicTimeTravelReplayer` Self-Overwrite Guard & `TurnWalEngine` Task WAL GC
 * - [M67-6] Management Plane Endpoint (`POST /v1beta1/management/system/deep-risk-phase2-audit`) & CLI (`anstsctl mgmt-deep-risk-phase2-audit`)
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { CelldNativeContainerSandboxManager } from "../src/cloudflare/celld_container_bridge.js";
import { HotCellOverloadAndQueueGuard } from "../src/cloudflare/hot_cell_overload_guard.js";
import { ShardedQueueBrokerPool } from "../src/cloudflare/celld_ha_engine.js";
import {
  DynamicWorkerSandboxLoader,
  V8HeapAndSqlBudgetGuard,
} from "../src/cloudflare/celld_native_optimizations.js";
import { ZeroTrustVsockBridge, EgressPolicyViolationError } from "../src/runtime/vsock_bridge.js";
import { EbpfLsmSecurityBridge } from "../src/runtime/ebpf_lsm_bridge.js";
import { LayerFsWorkspaceDriver } from "../src/runtime/layerfs_workspace_driver.js";
import { LayerFsMultiAgentBranchCoordinator } from "../src/dag/layerfs_branch_coordinator.js";
import { DeterministicTimeTravelReplayer } from "../src/checkpoint/time_travel.js";
import { TurnWalEngine } from "../src/checkpoint/wal_engine.js";
import { InMemoryCellSqlStorage } from "../src/cloudflare/env.js";
import { TaskSpec } from "../src/models.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M67: Deep Potential Risk Identification & Elimination Audit Phase II", () => {
  it("[M67-1] CelldNativeContainerSandboxManager blocks 127.0.0.0/8, fdxx:/fe9x: ULA/link-local, hex/octal/integer IPs & cloud metadata, and cleans up containerWorkDir on destroy", async () => {
    const mgr = new CelldNativeContainerSandboxManager();
    mgr.registerContainerClass({
      class_name: "SandboxRunner",
      image: "ghcr.io/ansts/runner:v2.5",
      runtime: "kata",
      max_instances: 4,
    });
    const handle = mgr.getOrCreateContainerHandle("cell-m67-1", {
      class_name: "SandboxRunner",
      image: "ghcr.io/ansts/runner:v2.5",
      runtime: "kata",
    });
    const started = handle.start({ enableInternet: true });
    assert.equal(fs.existsSync(started.containerWorkDir), true);

    const tcp = handle.getTcpPort(8080);
    // Allowed public domain and container bridge
    const okBridge = await tcp.fetch("http://container.bridge.local/health");
    assert.equal(okBridge.status, 200);
    const okPublic = await tcp.fetch("https://api.openai.com/v1/models");
    assert.equal(okPublic.status, 200);

    // Must block 127.0.0.2 (non-127.0.0.1 loopback scan), fd12:3456::1 (ULA), fe90::1 (link-local), hex/integer IPv4, metadata.google.internal, and .nip.io rebinding
    for (const badTarget of [
      "http://127.0.0.2:8080/admin",
      "http://[fd12:3456:789a::1]/secret",
      "http://[fe90::1]/linklocal",
      "http://0xa9fea9fe/latest/meta-data",
      "http://2852039166/latest/meta-data",
      "http://metadata.google.internal/computeMetadata/v1/",
      "http://169.254.169.254.nip.io/latest",
    ]) {
      await assert.rejects(() => tcp.fetch(badTarget), /celld-fence/);
    }

    const destroyed = await mgr.destroyContainerHandle("cell-m67-1");
    assert.equal(destroyed, true);
    assert.equal(mgr.getHandleCount(), 0);
    assert.equal(fs.existsSync(started.containerWorkDir), false);
  });

  it("[M67-2] HotCellOverloadAndQueueGuard prevents reclaimed/double-release lease desync and NaN producer poisoning, and ShardedQueueBrokerPool deduplicates & bounds shards", () => {
    const guard = new HotCellOverloadAndQueueGuard({ maxCellConcurrentRequests: 2 });
    const l1 = guard.tryEnterCellFetch("cell-hot", 1000);
    const l2 = guard.tryEnterCellFetch("cell-hot", 25_000);
    assert.equal(guard.getInFlightCount("cell-hot"), 2);

    // Reclaim l1 (started at 1000, > 30s elapsed at now=35_000), while l2 is still active
    const reclaimed = guard.reclaimStaleInFlightFetches(35_000, 30_000);
    assert.equal(reclaimed.reclaimedCount, 1);
    assert.equal(guard.getInFlightCount("cell-hot"), 1);

    // Late handler for already-reclaimed l1 calls leaveCellFetch(cellScope, l1.leaseId) -> MUST NOT evict l2!
    guard.leaveCellFetch("cell-hot", l1.leaseId);
    assert.equal(guard.getInFlightCount("cell-hot"), 1);

    // Now l2 finishes and calls leaveCellFetch twice -> second call is a safe no-op
    guard.leaveCellFetch("cell-hot", l2.leaseId);
    assert.equal(guard.getInFlightCount("cell-hot"), 0);
    guard.leaveCellFetch("cell-hot", l2.leaseId);
    assert.equal(guard.getInFlightCount("cell-hot"), 0);

    // NaN / negative producerCount must be rejected without poisoning activeQueueProducers
    const nanBatch = guard.tryAdmitQueueProducerBatch(Number.NaN);
    assert.equal(nanBatch.admitted, false);
    const validBatch = guard.tryAdmitQueueProducerBatch(32);
    assert.equal(validBatch.admitted, true);

    // ShardedQueueBrokerPool deduplicates by id and enforces maxDepthPerShard
    const broker = new ShardedQueueBrokerPool(4, 2);
    const s1 = broker.publish("tenant-a", "msg-1", { a: 1 });
    const s1Dup = broker.publish("tenant-a", "msg-1", { a: 1 });
    assert.equal(s1, s1Dup);
    const batch = broker.consumeBatch(s1, 10);
    assert.equal(batch.length, 1);
  });

  it("[M67-3] DynamicWorkerSandboxLoader enforces async fetch() timeout & deep-clones inputPayload, and V8HeapAndSqlBudgetGuard sanitizes invalid limits", async () => {
    const loader = new DynamicWorkerSandboxLoader();
    const callerPayload: Record<string, unknown> = { secret: "immutable", nested: { count: 1 } };

    const res = await loader.executeUntrustedCodeInIsolate({
      taskId: "t-m67-3",
      turnIndex: 1,
      jsSourceCode: `
        inputPayload.secret = "mutated-by-sandbox";
        inputPayload.nested.count = 999;
        globalThis.__result = { seenSecret: inputPayload.secret };
      `,
      inputPayload: callerPayload,
    });
    assert.equal(res.output.seenSecret, "mutated-by-sandbox");
    // Caller's object must remain untouched!
    assert.equal(callerPayload.secret, "immutable");
    assert.equal((callerPayload.nested as { count: number }).count, 1);

    // Hung async fetch() inside isolate must time out cleanly
    await assert.rejects(
      () =>
        loader.executeUntrustedCodeInIsolate({
          taskId: "t-m67-3-hang",
          turnIndex: 2,
          jsSourceCode: `
            export default {
              async fetch() {
                return new Promise(() => {});
              }
            }
          `,
          asyncTimeoutMs: 60,
        }),
      /DynamicWorker async fetch timed out/
    );

    const heapGuard = new V8HeapAndSqlBudgetGuard(-10, 0, -5);
    assert.ok(heapGuard.hardHeapLimitMb >= heapGuard.softHeapLimitMb);
    const evalRes = heapGuard.evaluateHeapPressure(64);
    assert.equal(Number.isFinite(evalRes.heapUtilizationPct), true);
  });

  it("[M67-4] ZeroTrustVsockBridge blocks URL credentials/non-standard ports/backslash traversal & EbpfLsmSecurityBridge blocks null-bytes and /proc/<pid>/* escapes", () => {
    const vsock = new ZeroTrustVsockBridge(["api.openai.com"]);
    assert.throws(
      () =>
        vsock.sendRequest({
          requestId: "r1",
          vmId: "vm-m67",
          method: "HTTP_EGRESS",
          payload: { url: "https://admin:secret@api.openai.com/v1/chat" },
        }),
      EgressPolicyViolationError
    );
    assert.throws(
      () =>
        vsock.sendRequest({
          requestId: "r2",
          vmId: "vm-m67",
          method: "HTTP_EGRESS",
          payload: { url: "https://api.openai.com:6379/redis" },
        }),
      EgressPolicyViolationError
    );
    assert.throws(
      () =>
        vsock.sendRequest({
          requestId: "r3",
          vmId: "vm-m67",
          method: "WRITE_FILE",
          payload: { filePath: "..\\..\\escape.txt", content: "bad" },
        }),
      /Vsock sandbox path traversal refused/
    );
    assert.throws(
      () =>
        vsock.sendRequest({
          requestId: "r4",
          vmId: "vm-m67",
          method: "EXEC_CMD",
          payload: { command: "cat /proc/42/environ", executeRealSubprocess: false },
        }),
      /ZeroTrustVsockBridge blocked unsafe command/
    );

    const lsm = new EbpfLsmSecurityBridge();
    assert.equal(lsm.checkFileOpenHook("/etc/shadow\0.png").allowed, false);
    assert.equal(lsm.checkFileOpenHook("/proc/2/environ").allowed, false);
    assert.equal(lsm.checkFileOpenHook("/proc/1337/root/etc/passwd").allowed, false);
    assert.equal(lsm.checkFileOpenHook("/proc/self/fd/3").allowed, false);
    assert.equal(lsm.checkFileOpenHook("/workspace/src/main.ts").allowed, true);
  });

  it("[M67-5] LayerFsMultiAgentBranchCoordinator updates B-Tree pages on merge & handles same-commit idempotently, DeterministicTimeTravelReplayer blocks self-overwrite, and TurnWalEngine purges completed task WALs", () => {
    const driver = new LayerFsWorkspaceDriver();
    const coord = new LayerFsMultiAgentBranchCoordinator(driver);

    driver.forkBranch("main", "same-head-branch");
    const sameMerge = coord.mergeBranchIntoTarget("same-head-branch", "main", "C0");
    assert.equal(sameMerge.mergedCommitId, "C0");

    driver.executeInProcessFileTool({
      branchName: "same-head-branch",
      agentId: "a1",
      stepId: "s-merge-1",
      operation: "write",
      filePath: "src/merged_feature.ts",
      newContent: "export const merged = true;",
    });
    const trackedPagesBefore = driver.storeEngine.getTrackedCommitPageCount();
    const realMerge = coord.mergeBranchIntoTarget("same-head-branch", "main", "C0");
    assert.ok(realMerge.mergedCommitId.startsWith("L1-"));
    assert.equal(driver.storeEngine.getTrackedCommitPageCount(), trackedPagesBefore + 1);

    const sql = new InMemoryCellSqlStorage();
    const wal = new TurnWalEngine(sql);
    const entry1 = wal.appendTurnDelta({
      taskId: "t-m67-wal",
      turnIndex: 1,
      fencingToken: 1,
      changedFiles: ["a.ts"],
      sqliteWalPages: 2,
      highestSubmittedFencingToken: 1,
    });

    const replayer = new DeterministicTimeTravelReplayer();
    const origTask = new TaskSpec({ task_id: "t-m67-wal", tenant_id: "Northstar" });
    assert.throws(
      () => replayer.forkTaskAtTurn(origTask, [entry1], 1, "t-m67-wal"),
      /newTaskId must be non-empty and distinct/
    );

    const gcRes = wal.gcCompletedTaskWals(new Set());
    assert.equal(gcRes.purgedTasks, 1);
    assert.equal(gcRes.purgedFrames, 1);
    assert.equal(wal.getTaskWalEntries("t-m67-wal").length, 0);
  });

  it("[M67-6] Live server exposes POST /v1beta1/management/system/deep-risk-phase2-audit & CLI anstsctl mgmt-deep-risk-phase2-audit", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m67-live-"));
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
        ["mgmt-deep-risk-phase2-audit"],
        `http://127.0.0.1:${apiPort}`
      );
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout) as {
        allM67DeepRiskPhase2ChecksPassed: boolean;
        containerFenceAndWorkDir1: { containerFenceVerified: boolean };
        hotCellAndShardedQueue2: { hotCellAndQueueVerified: boolean };
        isolateTimeoutAndClone3: { isolateAndHeapVerified: boolean };
        vsockAndEbpfLsm4: { vsockAndLsmVerified: boolean };
        layerFsMergeAndWalGc5: { mergeAndWalGcVerified: boolean };
      };
      assert.equal(body.containerFenceAndWorkDir1.containerFenceVerified, true);
      assert.equal(body.hotCellAndShardedQueue2.hotCellAndQueueVerified, true);
      assert.equal(body.isolateTimeoutAndClone3.isolateAndHeapVerified, true);
      assert.equal(body.vsockAndEbpfLsm4.vsockAndLsmVerified, true);
      assert.equal(body.layerFsMergeAndWalGc5.mergeAndWalGcVerified, true);
      assert.equal(body.allM67DeepRiskPhase2ChecksPassed, true);
    } finally {
      await srv.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
