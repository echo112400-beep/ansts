"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview M56 Unified Cold-Start Lease/Fencing Rehydration, LayerFS 3-Way Merge Deletion & Base-Object Pinning,
 * P0 Emergency Ingress Headroom, CelldAdmin Epoch Guard & Facet/Isolate Memory Leak Elimination Verification Suite.
 */
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const env_js_1 = require("../src/cloudflare/env.js");
const unified_state_store_js_1 = require("../src/checkpoint/unified_state_store.js");
const scheduler_js_1 = require("../src/scheduler.js");
const models_js_1 = require("../src/models.js");
const layerfs_workspace_driver_js_1 = require("../src/runtime/layerfs_workspace_driver.js");
const layerfs_branch_coordinator_js_1 = require("../src/dag/layerfs_branch_coordinator.js");
const layerfs_store_engine_js_1 = require("../src/checkpoint/layerfs_store_engine.js");
const backpressure_js_1 = require("../src/gateway/backpressure.js");
const celld_admin_client_js_1 = require("../src/cloudflare/celld_admin_client.js");
const celld_native_optimizations_js_1 = require("../src/cloudflare/celld_native_optimizations.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M56: Unified Cold-Start Rehydration, LayerFS 3-Way Merge Deletion & Base-Object Pinning, P0 Emergency Headroom, CelldAdmin Epoch Guard & Facet/Isolate Leak Elimination", () => {
    (0, node_test_1.it)("validates all 6 kernel/storage/merge/admission invariants & live HTTP/CLI kernel-storage-audit", async () => {
        // 1. [UnifiedPersistence-ColdStart-1] Fencing watermark rehydration & interrupted LEASED -> QUEUED recovery
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const unified = new unified_state_store_js_1.UnifiedComponentPersistenceStore(sql);
        const schedA = new scheduler_js_1.CoreScheduler5D();
        schedA.registerOrUpdateNode(new models_js_1.NodeHeartbeat({ node_id: "node-m56", total_cpu_cores: 16, free_cpu_cores: 16 }));
        const task = new models_js_1.TaskSpec({ task_id: "task-m56-u1", tenant_id: "Northstar", priority: models_js_1.TaskPriority.P1_HIGH });
        schedA.submitTask(task);
        schedA.scheduleTick(100);
        task.lease_fencing_token = 8;
        unified.snapshotSchedulerState(schedA);
        const schedB = new scheduler_js_1.CoreScheduler5D();
        schedB.registerOrUpdateNode(new models_js_1.NodeHeartbeat({ node_id: "node-m56", total_cpu_cores: 16, free_cpu_cores: 16 }));
        unified.rehydrateSchedulerFromSql(schedB);
        strict_1.default.equal(schedB.allTasks.get("task-m56-u1")?.state, models_js_1.TaskState.QUEUED);
        const placements = schedB.scheduleTick(110);
        strict_1.default.equal(placements[0]?.[2], 9, "Fencing token after cold-start rehydration must advance monotonically from 8 to 9");
        // 2. [LayerFsMerge-DeletionAndConflict-2] Bash & fast-path file deletion + 3-way merge deletion & delete/modify conflict
        const driver = new layerfs_workspace_driver_js_1.LayerFsWorkspaceDriver();
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
        strict_1.default.equal(driver.getBranchHeadCommit("b-del").filesTree.has("temp.txt"), false, "rm inside ephemeral bash workspace must remove file from commit tree");
        const coord = new layerfs_branch_coordinator_js_1.LayerFsMultiAgentBranchCoordinator(driver);
        const merged = coord.mergeBranchIntoTarget("b-del", "b-keep", baseC);
        strict_1.default.ok(merged.deletedFiles.includes("temp.txt"));
        strict_1.default.equal(driver.getBranchHeadCommit("b-keep").filesTree.has("temp.txt"), false);
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
        strict_1.default.throws(() => coord.mergeBranchIntoTarget("b-del-conflict", "b-mod-conflict", baseC), layerfs_branch_coordinator_js_1.LayerFsMergeConflictError);
        // 3. [LayerFsStore-PinAndCycleGuard-3] Base object pinned against deletion while referenced by PREFIX/DELTA object
        const store = new layerfs_store_engine_js_1.LayerFsContentStoreEngine();
        const baseContent = "prefix-0123456789abcdef0123456789abcdef-v1";
        const nextContent = "prefix-0123456789abcdef0123456789abcdef-v2";
        const bObj = store.putFileObject("f.ts", baseContent).object;
        const dObj = store.putFileObject("f.ts", nextContent, baseContent).object;
        strict_1.default.equal(store.deleteObjectById(bObj.objectId), 0, "Referenced base object must not be deleted while derived object exists");
        strict_1.default.equal(store.reconstructObjectContent(dObj.objectId), nextContent);
        // 4. [IngressBackpressure-P0Headroom-4] P0_EMERGENCY admitted via +10% burst headroom when P2_STANDARD is shed at maxGlobalQueueDepth
        const bp = new backpressure_js_1.IngressBackpressureController({ maxGlobalQueueDepth: 500, p0EmergencyReserveRatio: 0.1 });
        strict_1.default.equal(bp.evaluateAdmission({
            priority: models_js_1.TaskPriority.P2_STANDARD,
            globalQueueDepth: 500,
            tenantActiveTasks: 5,
            clusterPsiPressurePct: 10,
        }).allowed, false);
        const p0Dec = bp.evaluateAdmission({
            priority: models_js_1.TaskPriority.P0_EMERGENCY,
            globalQueueDepth: 500,
            tenantActiveTasks: 5,
            clusterPsiPressurePct: 10,
        });
        strict_1.default.equal(p0Dec.allowed, true);
        strict_1.default.equal(p0Dec.usedEmergencyHeadroom, true);
        // 5. [CelldAdmin-NonRegressiveEpoch-5] Cold-start SQLite placement rehydration & non-regressive registerCell
        const adminA = new celld_admin_client_js_1.CelldAdminClient(sql, new env_js_1.InMemoryR2Bucket());
        adminA.registerCell("cell-m56-1", "node-1");
        await adminA.migrateCellToNode("cell-m56-1", "node-2"); // epoch 2
        const adminB = new celld_admin_client_js_1.CelldAdminClient(sql, new env_js_1.InMemoryR2Bucket());
        strict_1.default.equal(adminB.getCellPlacement("cell-m56-1")?.epochFencingToken, 2);
        const rereg = adminB.registerCell("cell-m56-1", "node-2");
        strict_1.default.equal(rereg.epochFencingToken, 2, "registerCell must not regress epochFencingToken back to 1");
        // 6. [FacetAndIsolate-LeakGuard-6] Aborted facet releases SQLite store & DynamicWorkerSandboxLoader caps spec cache
        const sup = new celld_native_optimizations_js_1.DurableObjectFacetSupervisor();
        const { facetRecord } = sup.spawnToolFacet("t-m56", 1, "sql", () => new env_js_1.InMemoryCellSqlStorage());
        strict_1.default.equal(sup.getActiveSqlStoreCount(), 1);
        sup.abortFaultyFacet(facetRecord.facetName, new Error("boom"));
        strict_1.default.equal(sup.getActiveSqlStoreCount(), 0);
        const loader = new celld_native_optimizations_js_1.DynamicWorkerSandboxLoader(undefined, 2);
        await loader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 1, jsSourceCode: "globalThis.__result = { a: 1 };" });
        await loader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 2, jsSourceCode: "globalThis.__result = { a: 2 };" });
        await loader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 3, jsSourceCode: "globalThis.__result = { a: 3 };" });
        strict_1.default.equal(loader.getCachedSpecCount(), 2);
        // 7. Verify live HTTP endpoint & CLI `mgmt-kernel-storage-audit`
        const server = new server_main_js_1.AnstsLocalAndComposeServer({ nodeId: "gw-m56-test" });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-kernel-storage-audit"], baseUrl);
            strict_1.default.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            strict_1.default.equal(body.allM56RisksEliminated, true);
            strict_1.default.equal(body.unifiedColdStart1.unifiedColdStartRehydrationVerified, true);
            strict_1.default.equal(body.layerfsDeletionAndConflict2.layerfsDeletionAndConflictVerified, true);
            strict_1.default.equal(body.layerfsStorePinGuard3.storePinGuardVerified, true);
            strict_1.default.equal(body.ingressP0Headroom4.ingressP0HeadroomVerified, true);
            strict_1.default.equal(body.celldAdminEpochGuard5.celldAdminNonRegressiveVerified, true);
            strict_1.default.equal(body.facetAndIsolateLeakGuard6.facetAndIsolateLeakGuardVerified, true);
        }
        finally {
            await server.stop();
        }
    });
});
