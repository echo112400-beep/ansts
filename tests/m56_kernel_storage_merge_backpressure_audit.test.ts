/**
 * @fileoverview M56 Unified Cold-Start Lease/Fencing Rehydration, LayerFS 3-Way Merge Deletion & Base-Object Pinning,
 * P0 Emergency Ingress Headroom, CelldAdmin Epoch Guard & Facet/Isolate Memory Leak Elimination Verification Suite.
 */
import "./console_write_auth.js";
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { InMemoryCellSqlStorage, InMemoryR2Bucket } from "../src/cloudflare/env.js";
import { UnifiedComponentPersistenceStore } from "../src/checkpoint/unified_state_store.js";
import { CoreScheduler5D } from "../src/scheduler.js";
import { NodeHeartbeat, TaskPriority, TaskSpec, TaskState } from "../src/models.js";
import { LayerFsWorkspaceDriver } from "../src/runtime/layerfs_workspace_driver.js";
import { LayerFsMultiAgentBranchCoordinator, LayerFsMergeConflictError } from "../src/dag/layerfs_branch_coordinator.js";
import { LayerFsContentStoreEngine } from "../src/checkpoint/layerfs_store_engine.js";
import { IngressBackpressureController } from "../src/gateway/backpressure.js";
import { CelldAdminClient } from "../src/cloudflare/celld_admin_client.js";
import { DurableObjectFacetSupervisor, DynamicWorkerSandboxLoader } from "../src/cloudflare/celld_native_optimizations.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M56: Unified Cold-Start Rehydration, LayerFS 3-Way Merge Deletion & Base-Object Pinning, P0 Emergency Headroom, CelldAdmin Epoch Guard & Facet/Isolate Leak Elimination", () => {
  it("validates all 6 kernel/storage/merge/admission invariants & live HTTP/CLI kernel-storage-audit", async () => {
    // 1. [UnifiedPersistence-ColdStart-1] Fencing watermark rehydration & interrupted LEASED -> QUEUED recovery
    const sql = new InMemoryCellSqlStorage();
    const unified = new UnifiedComponentPersistenceStore(sql);
    const schedA = new CoreScheduler5D();
    schedA.registerOrUpdateNode(new NodeHeartbeat({ node_id: "node-m56", total_cpu_cores: 16, free_cpu_cores: 16 }));
    const task = new TaskSpec({ task_id: "task-m56-u1", tenant_id: "Northstar", priority: TaskPriority.P1_HIGH });
    schedA.submitTask(task);
    schedA.scheduleTick(100);
    task.lease_fencing_token = 8;
    unified.snapshotSchedulerState(schedA);

    const schedB = new CoreScheduler5D();
    schedB.registerOrUpdateNode(new NodeHeartbeat({ node_id: "node-m56", total_cpu_cores: 16, free_cpu_cores: 16 }));
    unified.rehydrateSchedulerFromSql(schedB);
    assert.equal(schedB.allTasks.get("task-m56-u1")?.state, TaskState.QUEUED);
    const placements = schedB.scheduleTick(110);
    assert.equal(placements[0]?.[2], 9, "Fencing token after cold-start rehydration must advance monotonically from 8 to 9");

    // 2. [LayerFsMerge-DeletionAndConflict-2] Bash & fast-path file deletion + 3-way merge deletion & delete/modify conflict
    const driver = new LayerFsWorkspaceDriver();
    driver.executeInProcessFileTool({
      branchName: "main",
      agentId: "agent-1",
      stepId: "s1",
      operation: "write",
      filePath: "temp.txt",
      newContent: "hello\n",
    });
    const baseC = driver.getBranchHeadCommit("main").commitId;
    driver.forkBranch("main", "b-del");
    driver.forkBranch("main", "b-keep");
    driver.executeEphemeralBashWorkspace({
      branchName: "b-del",
      agentId: "agent-del",
      stepId: "s-rm",
      command: "rm -f temp.txt",
    });
    assert.equal(driver.getBranchHeadCommit("b-del").filesTree.has("temp.txt"), false, "rm inside ephemeral bash workspace must remove file from commit tree");

    const coord = new LayerFsMultiAgentBranchCoordinator(driver);
    const merged = coord.mergeBranchIntoTarget("b-del", "b-keep", baseC);
    assert.ok(merged.deletedFiles.includes("temp.txt"));
    assert.equal(driver.getBranchHeadCommit("b-keep").filesTree.has("temp.txt"), false);

    // Delete/Modify conflict check
    driver.forkBranch(baseC, "b-del-conflict");
    driver.forkBranch(baseC, "b-mod-conflict");
    driver.executeInProcessFileTool({
      branchName: "b-del-conflict",
      agentId: "a1",
      stepId: "s2",
      operation: "delete",
      filePath: "temp.txt",
    });
    driver.executeInProcessFileTool({
      branchName: "b-mod-conflict",
      agentId: "a2",
      stepId: "s3",
      operation: "write",
      filePath: "temp.txt",
      newContent: "modified\n",
    });
    assert.throws(
      () => coord.mergeBranchIntoTarget("b-del-conflict", "b-mod-conflict", baseC),
      LayerFsMergeConflictError
    );

    // 3. [LayerFsStore-PinAndCycleGuard-3] Base object pinned against deletion while referenced by PREFIX/DELTA object
    const store = new LayerFsContentStoreEngine();
    const baseContent = "prefix-0123456789abcdef0123456789abcdef-v1";
    const nextContent = "prefix-0123456789abcdef0123456789abcdef-v2";
    const bObj = store.putFileObject("f.ts", baseContent).object;
    const dObj = store.putFileObject("f.ts", nextContent, baseContent).object;
    assert.equal(store.deleteObjectById(bObj.objectId), 0, "Referenced base object must not be deleted while derived object exists");
    assert.equal(store.reconstructObjectContent(dObj.objectId), nextContent);

    // 4. [IngressBackpressure-P0Headroom-4] P0_EMERGENCY admitted via +10% burst headroom when P2_STANDARD is shed at maxGlobalQueueDepth
    const bp = new IngressBackpressureController({ maxGlobalQueueDepth: 500, p0EmergencyReserveRatio: 0.1 });
    assert.equal(
      bp.evaluateAdmission({
        priority: TaskPriority.P2_STANDARD,
        globalQueueDepth: 500,
        tenantActiveTasks: 5,
        clusterPsiPressurePct: 10,
      }).allowed,
      false
    );
    const p0Dec = bp.evaluateAdmission({
      priority: TaskPriority.P0_EMERGENCY,
      globalQueueDepth: 500,
      tenantActiveTasks: 5,
      clusterPsiPressurePct: 10,
    });
    assert.equal(p0Dec.allowed, true);
    assert.equal(p0Dec.usedEmergencyHeadroom, true);

    // 5. [CelldAdmin-NonRegressiveEpoch-5] Cold-start SQLite placement rehydration & non-regressive registerCell
    const adminA = new CelldAdminClient(sql, new InMemoryR2Bucket());
    adminA.registerCell("cell-m56-1", "node-1");
    await adminA.migrateCellToNode("cell-m56-1", "node-2"); // epoch 2
    const adminB = new CelldAdminClient(sql, new InMemoryR2Bucket());
    assert.equal(adminB.getCellPlacement("cell-m56-1")?.epochFencingToken, 2);
    const rereg = adminB.registerCell("cell-m56-1", "node-2");
    assert.equal(rereg.epochFencingToken, 2, "registerCell must not regress epochFencingToken back to 1");

    // 6. [FacetAndIsolate-LeakGuard-6] Aborted facet releases SQLite store & DynamicWorkerSandboxLoader caps spec cache
    const sup = new DurableObjectFacetSupervisor();
    const { facetRecord } = sup.spawnToolFacet("t-m56", 1, "sql", () => new InMemoryCellSqlStorage());
    assert.equal(sup.getActiveSqlStoreCount(), 1);
    sup.abortFaultyFacet(facetRecord.facetName, new Error("boom"));
    assert.equal(sup.getActiveSqlStoreCount(), 0);

    const loader = new DynamicWorkerSandboxLoader(undefined, 2);
    await loader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 1, jsSourceCode: "globalThis.__result = { a: 1 };" });
    await loader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 2, jsSourceCode: "globalThis.__result = { a: 2 };" });
    await loader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 3, jsSourceCode: "globalThis.__result = { a: 3 };" });
    assert.equal(loader.getCachedSpecCount(), 2);

    // 7. Verify live HTTP endpoint & CLI `mgmt-kernel-storage-audit`
    const server = new AnstsLocalAndComposeServer({ nodeId: "gw-m56-test" });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await runAnstsCtlRemote(["mgmt-kernel-storage-audit"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);
      assert.equal(body.allM56RisksEliminated, true);
      assert.equal(body.unifiedColdStart1.unifiedColdStartRehydrationVerified, true);
      assert.equal(body.layerfsDeletionAndConflict2.layerfsDeletionAndConflictVerified, true);
      assert.equal(body.layerfsStorePinGuard3.storePinGuardVerified, true);
      assert.equal(body.ingressP0Headroom4.ingressP0HeadroomVerified, true);
      assert.equal(body.celldAdminEpochGuard5.celldAdminNonRegressiveVerified, true);
      assert.equal(body.facetAndIsolateLeakGuard6.facetAndIsolateLeakGuardVerified, true);
    } finally {
      await server.stop();
    }
  });
});
