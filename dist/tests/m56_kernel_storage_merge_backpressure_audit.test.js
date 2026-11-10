"use strict";



var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var _env = require("../src/cloudflare/env.js");
var _unified_state_store = require("../src/checkpoint/unified_state_store.js");
var _scheduler = require("../src/scheduler.js");
var _models = require("../src/models.js");
var _layerfs_workspace_driver = require("../src/runtime/layerfs_workspace_driver.js");
var _layerfs_branch_coordinator = require("../src/dag/layerfs_branch_coordinator.js");
var _layerfs_store_engine = require("../src/checkpoint/layerfs_store_engine.js");
var _backpressure = require("../src/gateway/backpressure.js");
var _celld_admin_client = require("../src/cloudflare/celld_admin_client.js");
var _celld_native_optimizations = require("../src/cloudflare/celld_native_optimizations.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M56 Unified Cold-Start Lease/Fencing Rehydration, LayerFS 3-Way Merge Deletion & Base-Object Pinning,
 * P0 Emergency Ingress Headroom, CelldAdmin Epoch Guard & Facet/Isolate Memory Leak Elimination Verification Suite.
 */(0, _nodeTest.describe)("M56: Unified Cold-Start Rehydration, LayerFS 3-Way Merge Deletion & Base-Object Pinning, P0 Emergency Headroom, CelldAdmin Epoch Guard & Facet/Isolate Leak Elimination", () => {(0, _nodeTest.it)("validates all 6 kernel/storage/merge/admission invariants & live HTTP/CLI kernel-storage-audit", async () => {
    // 1. [UnifiedPersistence-ColdStart-1] Fencing watermark rehydration & interrupted LEASED -> QUEUED recovery
    const sql = new _env.InMemoryCellSqlStorage();
    const unified = new _unified_state_store.UnifiedComponentPersistenceStore(sql);
    const schedA = new _scheduler.CoreScheduler5D();
    schedA.registerOrUpdateNode(new _models.NodeHeartbeat({ node_id: "node-m56", total_cpu_cores: 16, free_cpu_cores: 16 }));
    const task = new _models.TaskSpec({ task_id: "task-m56-u1", tenant_id: "Northstar", priority: _models.TaskPriority.P1_HIGH });
    schedA.submitTask(task);
    schedA.scheduleTick(100);
    task.lease_fencing_token = 8;
    unified.snapshotSchedulerState(schedA);

    const schedB = new _scheduler.CoreScheduler5D();
    schedB.registerOrUpdateNode(new _models.NodeHeartbeat({ node_id: "node-m56", total_cpu_cores: 16, free_cpu_cores: 16 }));
    unified.rehydrateSchedulerFromSql(schedB);
    _strict.default.equal(schedB.allTasks.get("task-m56-u1")?.state, _models.TaskState.QUEUED);
    const placements = schedB.scheduleTick(110);
    _strict.default.equal(placements[0]?.[2], 9, "Fencing token after cold-start rehydration must advance monotonically from 8 to 9");

    // 2. [LayerFsMerge-DeletionAndConflict-2] Bash & fast-path file deletion + 3-way merge deletion & delete/modify conflict
    const driver = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    driver.executeInProcessFileTool({
      branchName: "main",
      agentId: "agent-1",
      stepId: "s1",
      operation: "write",
      filePath: "temp.txt",
      newContent: "hello\n"
    });
    const baseC = driver.getBranchHeadCommit("main").commitId;
    driver.forkBranch("main", "b-del");
    driver.forkBranch("main", "b-keep");
    driver.executeEphemeralBashWorkspace({
      branchName: "b-del",
      agentId: "agent-del",
      stepId: "s-rm",
      command: "rm -f temp.txt"
    });
    _strict.default.equal(driver.getBranchHeadCommit("b-del").filesTree.has("temp.txt"), false, "rm inside ephemeral bash workspace must remove file from commit tree");

    const coord = new _layerfs_branch_coordinator.LayerFsMultiAgentBranchCoordinator(driver);
    const merged = coord.mergeBranchIntoTarget("b-del", "b-keep", baseC);
    _strict.default.ok(merged.deletedFiles.includes("temp.txt"));
    _strict.default.equal(driver.getBranchHeadCommit("b-keep").filesTree.has("temp.txt"), false);

    // Delete/Modify conflict check
    driver.forkBranch(baseC, "b-del-conflict");
    driver.forkBranch(baseC, "b-mod-conflict");
    driver.executeInProcessFileTool({
      branchName: "b-del-conflict",
      agentId: "a1",
      stepId: "s2",
      operation: "delete",
      filePath: "temp.txt"
    });
    driver.executeInProcessFileTool({
      branchName: "b-mod-conflict",
      agentId: "a2",
      stepId: "s3",
      operation: "write",
      filePath: "temp.txt",
      newContent: "modified\n"
    });
    _strict.default.throws(
    () => coord.mergeBranchIntoTarget("b-del-conflict", "b-mod-conflict", baseC),
    _layerfs_branch_coordinator.LayerFsMergeConflictError);


    // 3. [LayerFsStore-PinAndCycleGuard-3] Base object pinned against deletion while referenced by PREFIX/DELTA object
    const store = new _layerfs_store_engine.LayerFsContentStoreEngine();
    const baseContent = "prefix-0123456789abcdef0123456789abcdef-v1";
    const nextContent = "prefix-0123456789abcdef0123456789abcdef-v2";
    const bObj = store.putFileObject("f.ts", baseContent).object;
    const dObj = store.putFileObject("f.ts", nextContent, baseContent).object;
    _strict.default.equal(store.deleteObjectById(bObj.objectId), 0, "Referenced base object must not be deleted while derived object exists");
    _strict.default.equal(store.reconstructObjectContent(dObj.objectId), nextContent);

    // 4. [IngressBackpressure-P0Headroom-4] P0_EMERGENCY admitted via +10% burst headroom when P2_STANDARD is shed at maxGlobalQueueDepth
    const bp = new _backpressure.IngressBackpressureController({ maxGlobalQueueDepth: 500, p0EmergencyReserveRatio: 0.1 });
    _strict.default.equal(
    bp.evaluateAdmission({
      priority: _models.TaskPriority.P2_STANDARD,
      globalQueueDepth: 500,
      tenantActiveTasks: 5,
      clusterPsiPressurePct: 10
    }).allowed,
    false);

    const p0Dec = bp.evaluateAdmission({
      priority: _models.TaskPriority.P0_EMERGENCY,
      globalQueueDepth: 500,
      tenantActiveTasks: 5,
      clusterPsiPressurePct: 10
    });
    _strict.default.equal(p0Dec.allowed, true);
    _strict.default.equal(p0Dec.usedEmergencyHeadroom, true);

    // 5. [CelldAdmin-NonRegressiveEpoch-5] Cold-start SQLite placement rehydration & non-regressive registerCell
    const adminA = new _celld_admin_client.CelldAdminClient(sql, new _env.InMemoryR2Bucket());
    adminA.registerCell("cell-m56-1", "node-1");
    await adminA.migrateCellToNode("cell-m56-1", "node-2"); // epoch 2
    const adminB = new _celld_admin_client.CelldAdminClient(sql, new _env.InMemoryR2Bucket());
    _strict.default.equal(adminB.getCellPlacement("cell-m56-1")?.epochFencingToken, 2);
    const rereg = adminB.registerCell("cell-m56-1", "node-2");
    _strict.default.equal(rereg.epochFencingToken, 2, "registerCell must not regress epochFencingToken back to 1");

    // 6. [FacetAndIsolate-LeakGuard-6] Aborted facet releases SQLite store & DynamicWorkerSandboxLoader caps spec cache
    const sup = new _celld_native_optimizations.DurableObjectFacetSupervisor();
    const { facetRecord } = sup.spawnToolFacet("t-m56", 1, "sql", () => new _env.InMemoryCellSqlStorage());
    _strict.default.equal(sup.getActiveSqlStoreCount(), 1);
    sup.abortFaultyFacet(facetRecord.facetName, new Error("boom"));
    _strict.default.equal(sup.getActiveSqlStoreCount(), 0);

    const loader = new _celld_native_optimizations.DynamicWorkerSandboxLoader(undefined, 2);
    await loader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 1, jsSourceCode: "globalThis.__result = { a: 1 };" });
    await loader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 2, jsSourceCode: "globalThis.__result = { a: 2 };" });
    await loader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 3, jsSourceCode: "globalThis.__result = { a: 3 };" });
    _strict.default.equal(loader.getCachedSpecCount(), 2);

    // 7. Verify live HTTP endpoint & CLI `mgmt-kernel-storage-audit`
    const server = new _server_main.AnstsLocalAndComposeServer({ nodeId: "gw-m56-test" });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-kernel-storage-audit"], baseUrl);
      _strict.default.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);
      _strict.default.equal(body.allM56RisksEliminated, true);
      _strict.default.equal(body.unifiedColdStart1.unifiedColdStartRehydrationVerified, true);
      _strict.default.equal(body.layerfsDeletionAndConflict2.layerfsDeletionAndConflictVerified, true);
      _strict.default.equal(body.layerfsStorePinGuard3.storePinGuardVerified, true);
      _strict.default.equal(body.ingressP0Headroom4.ingressP0HeadroomVerified, true);
      _strict.default.equal(body.celldAdminEpochGuard5.celldAdminNonRegressiveVerified, true);
      _strict.default.equal(body.facetAndIsolateLeakGuard6.facetAndIsolateLeakGuardVerified, true);
    } finally {
      await server.stop();
    }
  });
});