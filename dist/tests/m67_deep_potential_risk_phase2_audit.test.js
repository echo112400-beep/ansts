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
 * @fileoverview M67 End-to-End Verification Suite:
 * Deep Potential Risk Identification & Elimination Suite Phase II:
 * - [M67-1] `CelldNativeContainerSandboxManager` & `CelldContainerHandle` Loopback/ULA/Hex-IP/Metadata SSRF Guard & Container WorkDir/Handle Cleanup
 * - [M67-2] `HotCellOverloadAndQueueGuard` Reclaimed/Double-Release Lease Desync Guard + Producer NaN Validation & `ShardedQueueBrokerPool` Bounded Idempotent Shards
 * - [M67-3] `DynamicWorkerSandboxLoader` Async `fetch()` Timeout + Input Payload Deep-Clone Isolation & `V8HeapAndSqlBudgetGuard` Limit Sanitization
 * - [M67-4] `ZeroTrustVsockBridge` URL Credentials/Port/Path-Traversal Guard & `EbpfLsmSecurityBridge` Null-Byte + `/proc/<pid>/*` Container Escape Block
 * - [M67-5] `LayerFsMultiAgentBranchCoordinator` Merge B-Tree Page Sync + Idempotent Same-Commit Merge, `DeterministicTimeTravelReplayer` Self-Overwrite Guard & `TurnWalEngine` Task WAL GC
 * - [M67-6] Management Plane Endpoint (`POST /v1beta1/management/system/deep-risk-phase2-audit`) & CLI (`anstsctl mgmt-deep-risk-phase2-audit`)
 */
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const celld_container_bridge_js_1 = require("../src/cloudflare/celld_container_bridge.js");
const hot_cell_overload_guard_js_1 = require("../src/cloudflare/hot_cell_overload_guard.js");
const celld_ha_engine_js_1 = require("../src/cloudflare/celld_ha_engine.js");
const celld_native_optimizations_js_1 = require("../src/cloudflare/celld_native_optimizations.js");
const vsock_bridge_js_1 = require("../src/runtime/vsock_bridge.js");
const ebpf_lsm_bridge_js_1 = require("../src/runtime/ebpf_lsm_bridge.js");
const layerfs_workspace_driver_js_1 = require("../src/runtime/layerfs_workspace_driver.js");
const layerfs_branch_coordinator_js_1 = require("../src/dag/layerfs_branch_coordinator.js");
const time_travel_js_1 = require("../src/checkpoint/time_travel.js");
const wal_engine_js_1 = require("../src/checkpoint/wal_engine.js");
const env_js_1 = require("../src/cloudflare/env.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M67: Deep Potential Risk Identification & Elimination Audit Phase II", () => {
    (0, node_test_1.it)("[M67-1] CelldNativeContainerSandboxManager blocks 127.0.0.0/8, fdxx:/fe9x: ULA/link-local, hex/octal/integer IPs & cloud metadata, and cleans up containerWorkDir on destroy", async () => {
        const mgr = new celld_container_bridge_js_1.CelldNativeContainerSandboxManager();
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
        strict_1.default.equal(fs.existsSync(started.containerWorkDir), true);
        const tcp = handle.getTcpPort(8080);
        // Allowed public domain and container bridge
        const okBridge = await tcp.fetch("http://container.bridge.local/health");
        strict_1.default.equal(okBridge.status, 200);
        const okPublic = await tcp.fetch("https://api.openai.com/v1/models");
        strict_1.default.equal(okPublic.status, 200);
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
            await strict_1.default.rejects(() => tcp.fetch(badTarget), /celld-fence/);
        }
        const destroyed = await mgr.destroyContainerHandle("cell-m67-1");
        strict_1.default.equal(destroyed, true);
        strict_1.default.equal(mgr.getHandleCount(), 0);
        strict_1.default.equal(fs.existsSync(started.containerWorkDir), false);
    });
    (0, node_test_1.it)("[M67-2] HotCellOverloadAndQueueGuard prevents reclaimed/double-release lease desync and NaN producer poisoning, and ShardedQueueBrokerPool deduplicates & bounds shards", () => {
        const guard = new hot_cell_overload_guard_js_1.HotCellOverloadAndQueueGuard({ maxCellConcurrentRequests: 2 });
        const l1 = guard.tryEnterCellFetch("cell-hot", 1000);
        const l2 = guard.tryEnterCellFetch("cell-hot", 25_000);
        strict_1.default.equal(guard.getInFlightCount("cell-hot"), 2);
        // Reclaim l1 (started at 1000, > 30s elapsed at now=35_000), while l2 is still active
        const reclaimed = guard.reclaimStaleInFlightFetches(35_000, 30_000);
        strict_1.default.equal(reclaimed.reclaimedCount, 1);
        strict_1.default.equal(guard.getInFlightCount("cell-hot"), 1);
        // Late handler for already-reclaimed l1 calls leaveCellFetch(cellScope, l1.leaseId) -> MUST NOT evict l2!
        guard.leaveCellFetch("cell-hot", l1.leaseId);
        strict_1.default.equal(guard.getInFlightCount("cell-hot"), 1);
        // Now l2 finishes and calls leaveCellFetch twice -> second call is a safe no-op
        guard.leaveCellFetch("cell-hot", l2.leaseId);
        strict_1.default.equal(guard.getInFlightCount("cell-hot"), 0);
        guard.leaveCellFetch("cell-hot", l2.leaseId);
        strict_1.default.equal(guard.getInFlightCount("cell-hot"), 0);
        // NaN / negative producerCount must be rejected without poisoning activeQueueProducers
        const nanBatch = guard.tryAdmitQueueProducerBatch(Number.NaN);
        strict_1.default.equal(nanBatch.admitted, false);
        const validBatch = guard.tryAdmitQueueProducerBatch(32);
        strict_1.default.equal(validBatch.admitted, true);
        // ShardedQueueBrokerPool deduplicates by id and enforces maxDepthPerShard
        const broker = new celld_ha_engine_js_1.ShardedQueueBrokerPool(4, 2);
        const s1 = broker.publish("tenant-a", "msg-1", { a: 1 });
        const s1Dup = broker.publish("tenant-a", "msg-1", { a: 1 });
        strict_1.default.equal(s1, s1Dup);
        const batch = broker.consumeBatch(s1, 10);
        strict_1.default.equal(batch.length, 1);
    });
    (0, node_test_1.it)("[M67-3] DynamicWorkerSandboxLoader enforces async fetch() timeout & deep-clones inputPayload, and V8HeapAndSqlBudgetGuard sanitizes invalid limits", async () => {
        const loader = new celld_native_optimizations_js_1.DynamicWorkerSandboxLoader();
        const callerPayload = { secret: "immutable", nested: { count: 1 } };
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
        strict_1.default.equal(res.output.seenSecret, "mutated-by-sandbox");
        // Caller's object must remain untouched!
        strict_1.default.equal(callerPayload.secret, "immutable");
        strict_1.default.equal(callerPayload.nested.count, 1);
        // Hung async fetch() inside isolate must time out cleanly
        await strict_1.default.rejects(() => loader.executeUntrustedCodeInIsolate({
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
        }), /DynamicWorker async fetch timed out/);
        const heapGuard = new celld_native_optimizations_js_1.V8HeapAndSqlBudgetGuard(-10, 0, -5);
        strict_1.default.ok(heapGuard.hardHeapLimitMb >= heapGuard.softHeapLimitMb);
        const evalRes = heapGuard.evaluateHeapPressure(64);
        strict_1.default.equal(Number.isFinite(evalRes.heapUtilizationPct), true);
    });
    (0, node_test_1.it)("[M67-4] ZeroTrustVsockBridge blocks URL credentials/non-standard ports/backslash traversal & EbpfLsmSecurityBridge blocks null-bytes and /proc/<pid>/* escapes", () => {
        const vsock = new vsock_bridge_js_1.ZeroTrustVsockBridge(["api.openai.com"]);
        strict_1.default.throws(() => vsock.sendRequest({
            requestId: "r1",
            vmId: "vm-m67",
            method: "HTTP_EGRESS",
            payload: { url: "https://admin:secret@api.openai.com/v1/chat" },
        }), vsock_bridge_js_1.EgressPolicyViolationError);
        strict_1.default.throws(() => vsock.sendRequest({
            requestId: "r2",
            vmId: "vm-m67",
            method: "HTTP_EGRESS",
            payload: { url: "https://api.openai.com:6379/redis" },
        }), vsock_bridge_js_1.EgressPolicyViolationError);
        strict_1.default.throws(() => vsock.sendRequest({
            requestId: "r3",
            vmId: "vm-m67",
            method: "WRITE_FILE",
            payload: { filePath: "..\\..\\escape.txt", content: "bad" },
        }), /Vsock sandbox path traversal refused/);
        strict_1.default.throws(() => vsock.sendRequest({
            requestId: "r4",
            vmId: "vm-m67",
            method: "EXEC_CMD",
            payload: { command: "cat /proc/42/environ", executeRealSubprocess: false },
        }), /ZeroTrustVsockBridge blocked unsafe command/);
        const lsm = new ebpf_lsm_bridge_js_1.EbpfLsmSecurityBridge();
        strict_1.default.equal(lsm.checkFileOpenHook("/etc/shadow\0.png").allowed, false);
        strict_1.default.equal(lsm.checkFileOpenHook("/proc/2/environ").allowed, false);
        strict_1.default.equal(lsm.checkFileOpenHook("/proc/1337/root/etc/passwd").allowed, false);
        strict_1.default.equal(lsm.checkFileOpenHook("/proc/self/fd/3").allowed, false);
        strict_1.default.equal(lsm.checkFileOpenHook("/workspace/src/main.ts").allowed, true);
    });
    (0, node_test_1.it)("[M67-5] LayerFsMultiAgentBranchCoordinator updates B-Tree pages on merge & handles same-commit idempotently, DeterministicTimeTravelReplayer blocks self-overwrite, and TurnWalEngine purges completed task WALs", () => {
        const driver = new layerfs_workspace_driver_js_1.LayerFsWorkspaceDriver();
        const coord = new layerfs_branch_coordinator_js_1.LayerFsMultiAgentBranchCoordinator(driver);
        driver.forkBranch("main", "same-head-branch");
        const sameMerge = coord.mergeBranchIntoTarget("same-head-branch", "main", "C0");
        strict_1.default.equal(sameMerge.mergedCommitId, "C0");
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
        strict_1.default.ok(realMerge.mergedCommitId.startsWith("L1-"));
        strict_1.default.equal(driver.storeEngine.getTrackedCommitPageCount(), trackedPagesBefore + 1);
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const wal = new wal_engine_js_1.TurnWalEngine(sql);
        const entry1 = wal.appendTurnDelta({
            taskId: "t-m67-wal",
            turnIndex: 1,
            fencingToken: 1,
            changedFiles: ["a.ts"],
            sqliteWalPages: 2,
            highestSubmittedFencingToken: 1,
        });
        const replayer = new time_travel_js_1.DeterministicTimeTravelReplayer();
        const origTask = new models_js_1.TaskSpec({ task_id: "t-m67-wal", tenant_id: "Northstar" });
        strict_1.default.throws(() => replayer.forkTaskAtTurn(origTask, [entry1], 1, "t-m67-wal"), /newTaskId must be non-empty and distinct/);
        const gcRes = wal.gcCompletedTaskWals(new Set());
        strict_1.default.equal(gcRes.purgedTasks, 1);
        strict_1.default.equal(gcRes.purgedFrames, 1);
        strict_1.default.equal(wal.getTaskWalEntries("t-m67-wal").length, 0);
    });
    (0, node_test_1.it)("[M67-6] Live server exposes POST /v1beta1/management/system/deep-risk-phase2-audit & CLI anstsctl mgmt-deep-risk-phase2-audit", async () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m67-live-"));
        const srv = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            apiPort: 0,
            dashboardPort: 0,
            storageDir: tmpDir,
            tickIntervalMs: 60_000,
        });
        const { apiPort } = await srv.start();
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-deep-risk-phase2-audit"], `http://127.0.0.1:${apiPort}`);
            strict_1.default.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            strict_1.default.equal(body.containerFenceAndWorkDir1.containerFenceVerified, true);
            strict_1.default.equal(body.hotCellAndShardedQueue2.hotCellAndQueueVerified, true);
            strict_1.default.equal(body.isolateTimeoutAndClone3.isolateAndHeapVerified, true);
            strict_1.default.equal(body.vsockAndEbpfLsm4.vsockAndLsmVerified, true);
            strict_1.default.equal(body.layerFsMergeAndWalGc5.mergeAndWalGcVerified, true);
            strict_1.default.equal(body.allM67DeepRiskPhase2ChecksPassed, true);
        }
        finally {
            await srv.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
