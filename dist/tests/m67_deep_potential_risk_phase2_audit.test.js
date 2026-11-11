"use strict";









var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));

var _celld_container_bridge = require("../src/cloudflare/celld_container_bridge.js");
var _hot_cell_overload_guard = require("../src/cloudflare/hot_cell_overload_guard.js");
var _celld_ha_engine = require("../src/cloudflare/celld_ha_engine.js");
var _celld_native_optimizations = require("../src/cloudflare/celld_native_optimizations.js");



var _vsock_bridge = require("../src/runtime/vsock_bridge.js");
var _ebpf_lsm_bridge = require("../src/runtime/ebpf_lsm_bridge.js");
var _layerfs_workspace_driver = require("../src/runtime/layerfs_workspace_driver.js");
var _layerfs_branch_coordinator = require("../src/dag/layerfs_branch_coordinator.js");
var _time_travel = require("../src/checkpoint/time_travel.js");
var _wal_engine = require("../src/checkpoint/wal_engine.js");
var _env = require("../src/cloudflare/env.js");
var _models = require("../src/models.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M67 End-to-End Verification Suite:
 * Deep Potential Risk Identification & Elimination Suite Phase II:
 * - [M67-1] `CelldNativeContainerSandboxManager` & `CelldContainerHandle` Loopback/ULA/Hex-IP/Metadata SSRF Guard & Container WorkDir/Handle Cleanup
 * - [M67-2] `HotCellOverloadAndQueueGuard` Reclaimed/Double-Release Lease Desync Guard + Producer NaN Validation & `ShardedQueueBrokerPool` Bounded Idempotent Shards
 * - [M67-3] `DynamicWorkerSandboxLoader` Async `fetch()` Timeout + Input Payload Deep-Clone Isolation & `V8HeapAndSqlBudgetGuard` Limit Sanitization
 * - [M67-4] `ZeroTrustVsockBridge` URL Credentials/Port/Path-Traversal Guard & `EbpfLsmSecurityBridge` Null-Byte + `/proc/<pid>/*` Container Escape Block
 * - [M67-5] `LayerFsMultiAgentBranchCoordinator` Merge B-Tree Page Sync + Idempotent Same-Commit Merge, `DeterministicTimeTravelReplayer` Self-Overwrite Guard & `TurnWalEngine` Task WAL GC
 * - [M67-6] Management Plane Endpoint (`POST /v1beta1/management/system/deep-risk-phase2-audit`) & CLI (`anstsctl mgmt-deep-risk-phase2-audit`)
 */(0, _nodeTest.describe)("M67: Deep Potential Risk Identification & Elimination Audit Phase II", () => {(0, _nodeTest.it)("[M67-1] CelldNativeContainerSandboxManager blocks 127.0.0.0/8, fdxx:/fe9x: ULA/link-local, hex/octal/integer IPs & cloud metadata, and cleans up containerWorkDir on destroy", async () => {const mgr = new _celld_container_bridge.CelldNativeContainerSandboxManager();mgr.registerContainerClass({ class_name: "SandboxRunner", image: "ghcr.io/ansts/runner:v2.5", runtime: "kata", max_instances: 4
    });
    const handle = mgr.getOrCreateContainerHandle("cell-m67-1", {
      class_name: "SandboxRunner",
      image: "ghcr.io/ansts/runner:v2.5",
      runtime: "kata"
    });
    const started = handle.start({ enableInternet: true });
    _strict.default.equal(fs.existsSync(started.containerWorkDir), true);

    const tcp = handle.getTcpPort(8080);
    // Allowed public domain and container bridge
    const okBridge = await tcp.fetch("http://container.bridge.local/health");
    _strict.default.equal(okBridge.status, 200);
    const okPublic = await tcp.fetch("https://api.openai.com/v1/models");
    _strict.default.equal(okPublic.status, 200);

    // Must block 127.0.0.2 (non-127.0.0.1 loopback scan), fd12:3456::1 (ULA), fe90::1 (link-local), hex/integer IPv4, metadata.google.internal, and .nip.io rebinding
    for (const badTarget of [
    "http://127.0.0.2:8080/admin",
    "http://[fd12:3456:789a::1]/secret",
    "http://[fe90::1]/linklocal",
    "http://0xa9fea9fe/latest/meta-data",
    "http://2852039166/latest/meta-data",
    "http://metadata.google.internal/computeMetadata/v1/",
    "http://169.254.169.254.nip.io/latest"])
    {
      await _strict.default.rejects(() => tcp.fetch(badTarget), /celld-fence/);
    }

    const destroyed = await mgr.destroyContainerHandle("cell-m67-1");
    _strict.default.equal(destroyed, true);
    _strict.default.equal(mgr.getHandleCount(), 0);
    _strict.default.equal(fs.existsSync(started.containerWorkDir), false);
  });

  (0, _nodeTest.it)("[M67-2] HotCellOverloadAndQueueGuard prevents reclaimed/double-release lease desync and NaN producer poisoning, and ShardedQueueBrokerPool deduplicates & bounds shards", () => {
    const guard = new _hot_cell_overload_guard.HotCellOverloadAndQueueGuard({ maxCellConcurrentRequests: 2 });
    const l1 = guard.tryEnterCellFetch("cell-hot", 1000);
    const l2 = guard.tryEnterCellFetch("cell-hot", 25_000);
    _strict.default.equal(guard.getInFlightCount("cell-hot"), 2);

    // Reclaim l1 (started at 1000, > 30s elapsed at now=35_000), while l2 is still active
    const reclaimed = guard.reclaimStaleInFlightFetches(35_000, 30_000);
    _strict.default.equal(reclaimed.reclaimedCount, 1);
    _strict.default.equal(guard.getInFlightCount("cell-hot"), 1);

    // Late handler for already-reclaimed l1 calls leaveCellFetch(cellScope, l1.leaseId) -> MUST NOT evict l2!
    guard.leaveCellFetch("cell-hot", l1.leaseId);
    _strict.default.equal(guard.getInFlightCount("cell-hot"), 1);

    // Now l2 finishes and calls leaveCellFetch twice -> second call is a safe no-op
    guard.leaveCellFetch("cell-hot", l2.leaseId);
    _strict.default.equal(guard.getInFlightCount("cell-hot"), 0);
    guard.leaveCellFetch("cell-hot", l2.leaseId);
    _strict.default.equal(guard.getInFlightCount("cell-hot"), 0);

    // NaN / negative producerCount must be rejected without poisoning activeQueueProducers
    const nanBatch = guard.tryAdmitQueueProducerBatch(Number.NaN);
    _strict.default.equal(nanBatch.admitted, false);
    const validBatch = guard.tryAdmitQueueProducerBatch(32);
    _strict.default.equal(validBatch.admitted, true);

    // ShardedQueueBrokerPool deduplicates by id and enforces maxDepthPerShard
    const broker = new _celld_ha_engine.ShardedQueueBrokerPool(4, 2);
    const s1 = broker.publish("tenant-a", "msg-1", { a: 1 });
    const s1Dup = broker.publish("tenant-a", "msg-1", { a: 1 });
    _strict.default.equal(s1, s1Dup);
    const batch = broker.consumeBatch(s1, 10);
    _strict.default.equal(batch.length, 1);
  });

  (0, _nodeTest.it)("[M67-3] DynamicWorkerSandboxLoader enforces async fetch() timeout & deep-clones inputPayload, and V8HeapAndSqlBudgetGuard sanitizes invalid limits", async () => {
    const loader = new _celld_native_optimizations.DynamicWorkerSandboxLoader();
    const callerPayload = { secret: "immutable", nested: { count: 1 } };

    const res = await loader.executeUntrustedCodeInIsolate({
      taskId: "t-m67-3",
      turnIndex: 1,
      jsSourceCode: `
        inputPayload.secret = "mutated-by-sandbox";
        inputPayload.nested.count = 999;
        globalThis.__result = { seenSecret: inputPayload.secret };
      `,
      inputPayload: callerPayload
    });
    _strict.default.equal(res.output.seenSecret, "mutated-by-sandbox");
    // Caller's object must remain untouched!
    _strict.default.equal(callerPayload.secret, "immutable");
    _strict.default.equal(callerPayload.nested.count, 1);

    // Hung async fetch() inside isolate must time out cleanly
    await _strict.default.rejects(
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
      asyncTimeoutMs: 60
    }),
    /DynamicWorker async fetch timed out/);


    const heapGuard = new _celld_native_optimizations.V8HeapAndSqlBudgetGuard(-10, 0, -5);
    _strict.default.ok(heapGuard.hardHeapLimitMb >= heapGuard.softHeapLimitMb);
    const evalRes = heapGuard.evaluateHeapPressure(64);
    _strict.default.equal(Number.isFinite(evalRes.heapUtilizationPct), true);
  });

  (0, _nodeTest.it)("[M67-4] ZeroTrustVsockBridge blocks URL credentials/non-standard ports/backslash traversal & EbpfLsmSecurityBridge blocks null-bytes and /proc/<pid>/* escapes", () => {
    const vsock = new _vsock_bridge.ZeroTrustVsockBridge(["api.openai.com"]);
    _strict.default.throws(
    () =>
    vsock.sendRequest({
      requestId: "r1",
      vmId: "vm-m67",
      method: "HTTP_EGRESS",
      payload: { url: "https://admin:secret@api.openai.com/v1/chat" }
    }),
    _vsock_bridge.EgressPolicyViolationError);

    _strict.default.throws(
    () =>
    vsock.sendRequest({
      requestId: "r2",
      vmId: "vm-m67",
      method: "HTTP_EGRESS",
      payload: { url: "https://api.openai.com:6379/redis" }
    }),
    _vsock_bridge.EgressPolicyViolationError);

    _strict.default.throws(
    () =>
    vsock.sendRequest({
      requestId: "r3",
      vmId: "vm-m67",
      method: "WRITE_FILE",
      payload: { filePath: "..\\..\\escape.txt", content: "bad" }
    }),
    /Vsock sandbox path traversal refused/);

    _strict.default.throws(
    () =>
    vsock.sendRequest({
      requestId: "r4",
      vmId: "vm-m67",
      method: "EXEC_CMD",
      payload: { command: "cat /proc/42/environ", executeRealSubprocess: false }
    }),
    /ZeroTrustVsockBridge blocked unsafe command/);


    const lsm = new _ebpf_lsm_bridge.EbpfLsmSecurityBridge();
    _strict.default.equal(lsm.checkFileOpenHook("/etc/shadow\0.png").allowed, false);
    _strict.default.equal(lsm.checkFileOpenHook("/proc/2/environ").allowed, false);
    _strict.default.equal(lsm.checkFileOpenHook("/proc/1337/root/etc/passwd").allowed, false);
    _strict.default.equal(lsm.checkFileOpenHook("/proc/self/fd/3").allowed, false);
    _strict.default.equal(lsm.checkFileOpenHook("/workspace/src/main.ts").allowed, true);
  });

  (0, _nodeTest.it)("[M67-5] LayerFsMultiAgentBranchCoordinator updates B-Tree pages on merge & handles same-commit idempotently, DeterministicTimeTravelReplayer blocks self-overwrite, and TurnWalEngine purges completed task WALs", () => {
    const driver = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    const coord = new _layerfs_branch_coordinator.LayerFsMultiAgentBranchCoordinator(driver);

    driver.forkBranch("main", "same-head-branch");
    const sameMerge = coord.mergeBranchIntoTarget("same-head-branch", "main", "C0");
    _strict.default.equal(sameMerge.mergedCommitId, "C0");

    driver.executeInProcessFileTool({
      branchName: "same-head-branch",
      agentId: "a1",
      stepId: "s-merge-1",
      operation: "write",
      filePath: "src/merged_feature.ts",
      newContent: "export const merged = true;"
    });
    const trackedPagesBefore = driver.storeEngine.getTrackedCommitPageCount();
    const realMerge = coord.mergeBranchIntoTarget("same-head-branch", "main", "C0");
    _strict.default.ok(realMerge.mergedCommitId.startsWith("L1-"));
    _strict.default.equal(driver.storeEngine.getTrackedCommitPageCount(), trackedPagesBefore + 1);

    const sql = new _env.InMemoryCellSqlStorage();
    const wal = new _wal_engine.TurnWalEngine(sql);
    const entry1 = wal.appendTurnDelta({
      taskId: "t-m67-wal",
      turnIndex: 1,
      fencingToken: 1,
      changedFiles: ["a.ts"],
      sqliteWalPages: 2,
      highestSubmittedFencingToken: 1
    });

    const replayer = new _time_travel.DeterministicTimeTravelReplayer();
    const origTask = new _models.TaskSpec({ task_id: "t-m67-wal", tenant_id: "Northstar" });
    _strict.default.throws(
    () => replayer.forkTaskAtTurn(origTask, [entry1], 1, "t-m67-wal"),
    /newTaskId must be non-empty and distinct/);


    const gcRes = wal.gcCompletedTaskWals(new Set());
    _strict.default.equal(gcRes.purgedTasks, 1);
    _strict.default.equal(gcRes.purgedFrames, 1);
    _strict.default.equal(wal.getTaskWalEntries("t-m67-wal").length, 0);
  });

  (0, _nodeTest.it)("[M67-6] Live server exposes POST /v1beta1/management/system/deep-risk-phase2-audit & CLI anstsctl mgmt-deep-risk-phase2-audit", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m67-live-"));
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
      ["mgmt-deep-risk-phase2-audit"],
      `http://127.0.0.1:${apiPort}`);

      _strict.default.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);







      _strict.default.equal(body.containerFenceAndWorkDir1.containerFenceVerified, true);
      _strict.default.equal(body.hotCellAndShardedQueue2.hotCellAndQueueVerified, true);
      _strict.default.equal(body.isolateTimeoutAndClone3.isolateAndHeapVerified, true);
      _strict.default.equal(body.vsockAndEbpfLsm4.vsockAndLsmVerified, true);
      _strict.default.equal(body.layerFsMergeAndWalGc5.mergeAndWalGcVerified, true);
      _strict.default.equal(body.allM67DeepRiskPhase2ChecksPassed, true);
    } finally {
      await srv.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});