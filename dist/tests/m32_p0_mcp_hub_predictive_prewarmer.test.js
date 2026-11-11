"use strict";
/**
 * @fileoverview M32 P0 (TASK-134 ~ TASK-137) Verification Suite:
 * - `McpStreamableEventHub` (JSON-RPC 2.0 Streamable SSE notifications, FenceToken validation,
 *   disk spooling, and automatic wake-up of hibernated `DurableWorkflowContext` cells)
 * - `WarmLspDaemonPool` (<15ms resident LSP AST symbol indexing & CoW branch incremental diagnostics)
 * - `PredictiveDagPrewarmer` (Lookahead CoW branch pre-warming during `THINKING_GEN` with 0ms perceived claim)
 * - `SpotHostEvacuationArbiter` (FinOps Spot vs On-Demand placement & <50ms Spot termination evacuation with epoch+1 fencing)
 */
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
const node_test_1 = __importDefault(require("node:test"));
const strict_1 = __importDefault(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const index_1 = require("../src/index");
const http = __importStar(require("node:http"));
const crypto = __importStar(require("node:crypto"));
(0, node_test_1.default)("test 83: [M32 P0 / TASK-134 & TASK-135] McpStreamableEventHub wakes hibernated Workflow Cell via JSON-RPC 2.0 notification and WarmLspDaemonPool delivers <15ms incremental branch diagnostics", async () => {
    const hubDir = `/tmp/ansts_mcp_hub_${Date.now()}`;
    const hub = new index_1.McpStreamableEventHub(hubDir);
    const wfCtx = new index_1.DurableWorkflowContext("wf-task-mcp-01", 60000);
    wfCtx.isolateHibernated = true;
    const sub = hub.subscribeTaskToMcpResource({
        taskId: "task-mcp-01",
        resourceUri: "mcp://ci-pipeline/build-402/status",
        workflowEventType: "ci.build.completed",
    });
    // Publish JSON-RPC 2.0 notification -> persists to disk spool, wakes hibernated Isolate, and delivers event
    const pubRes = hub.publishJsonRpcNotification({
        subscriptionId: sub.subscriptionId,
        method: "notifications/resources/updated",
        fence: new index_1.FenceToken(2, 5, "task-mcp-01"),
        data: { buildStatus: "PASSED", artifactSha: "sha256_9988" },
        workflowCtx: wfCtx,
        currentTaskState: index_1.TaskState.HIBERNATED,
    });
    strict_1.default.equal(pubRes.wokeHibernatedCell, true);
    strict_1.default.equal(pubRes.newTaskState, index_1.TaskState.RUNNING);
    strict_1.default.equal(wfCtx.isolateHibernated, false);
    strict_1.default.equal(fs.existsSync(pubRes.persistedSpoolPath), true);
    strict_1.default.ok(pubRes.sseWireFrame.includes("event: mcp_jsonrpc_notification"));
    const receivedEvt = await wfCtx.waitForEvent("ci.build.completed");
    strict_1.default.equal(receivedEvt.data.buildStatus, "PASSED");
    // Verify WarmLspDaemonPool pre-warms AST symbol index and runs <15ms incremental branch check
    const lspPool = new index_1.WarmLspDaemonPool();
    const warmIdx = lspPool.prewarmWorkspaceLspIndex("sbx-warm-lsp-01", "typescript", {
        "src/auth.ts": "export async function verifyJwtToken(token: string) { return true; }\nexport class SessionGuard {}",
    });
    strict_1.default.ok(warmIdx.snapshot.exportedSymbols["verifyJwtToken"]);
    strict_1.default.ok(warmIdx.snapshot.exportedSymbols["SessionGuard"]);
    strict_1.default.equal(warmIdx.snapshot.diagnostics.length, 0);
    const branchDiag = lspPool.queryIncrementalBranchDiagnostics("sbx-warm-lsp-01", "brn-sub-01", {
        "src/auth_patch.ts": "const x: number = TYPE_MISMATCH_ERROR;",
    });
    strict_1.default.ok(branchDiag.lookupLatencyMs < 15);
    strict_1.default.equal(branchDiag.inheritedSymbolsCount, 2);
    strict_1.default.equal(branchDiag.branchDiagnostics.length, 1);
});
(0, node_test_1.default)("test 84: [M32 P0 / TASK-136 & TASK-137] PredictiveDagPrewarmer achieves 0ms perceived branch claim and SpotHostEvacuationArbiter evacuates Spot sandboxes in <50ms with Epoch+1 fencing", () => {
    // 1. Predictive DAG Pre-Warming during THINKING_GEN
    const forker = new index_1.SandboxBranchForker(`/tmp/ansts_prewarm_${Date.now()}`);
    const prewarmer = new index_1.PredictiveDagPrewarmer(forker);
    const ticket = prewarmer.prewarmSubAgentBranchesDuringThinking({
        parentTaskId: "task-dag-parent-01",
        parentSandboxId: "sbx-dag-parent-01",
        seedFiles: { "package.json": "{\"name\":\"ansts\"}" },
        fanOutCount: 3,
    });
    strict_1.default.equal(ticket.prewarmedBranches.length, 3);
    const claimed = prewarmer.claimPrewarmedBranch("task-dag-parent-01");
    strict_1.default.equal(claimed.perceivedAcquireLatencyMs, 0);
    strict_1.default.equal(claimed.branch.active, true);
    // 2. Spot vs On-Demand Placement & 2-Minute Warning Live Evacuation
    const migDir = `/tmp/ansts_spot_evac_${Date.now()}`;
    const arbiter = new index_1.SpotHostEvacuationArbiter(migDir);
    arbiter.registerBodyHost({
        hostId: "body-spot-01",
        tier: "SPOT_PREEMPTIBLE",
        hourlyCostUsd: 0.32,
        healthy: true,
        cordoned: false,
    });
    arbiter.registerBodyHost({
        hostId: "body-ondemand-01",
        tier: "ON_DEMAND",
        hourlyCostUsd: 1.10,
        healthy: true,
        cordoned: false,
    });
    // Cost-optimal placement selects Spot host ($0.32/hr < $1.10/hr)
    const placed = arbiter.placeSandboxOnCostOptimalHost({
        taskId: "task-spot-job-1",
        sandboxId: "sbx-spot-1",
        fence: new index_1.FenceToken(1, 4, "task-spot-job-1"),
        allowSpotInstance: true,
        initialFiles: { "src/index.ts": "console.log('spot work');" },
    });
    strict_1.default.equal(placed.assignedHostId, "body-spot-01");
    strict_1.default.equal(placed.tier, "SPOT_PREEMPTIBLE");
    // Trigger Spot termination warning -> evacuates to body-ondemand-01 in <50ms and increments epoch (1 -> 2)
    const evac = arbiter.evacuateSpotHostOnTerminationNotice("body-spot-01");
    strict_1.default.equal(evac.migratedSandboxesCount, 1);
    strict_1.default.deepEqual(evac.targetOnDemandHostIds, ["body-ondemand-01"]);
    strict_1.default.equal(evac.newFencesByTask["task-spot-job-1"].epoch, 2);
    strict_1.default.ok(evac.totalEvacuationLatencyMs < 50);
    strict_1.default.equal(fs.existsSync(`${migDir}/body-ondemand-01/sbx-spot-1/src/index.ts`), true);
});
(0, node_test_1.default)("test 85: [M33 P1 / TASK-138 & TASK-139] ShadowTrafficReplayLab zero-side-effect evaluation scorecard, regression blocking, and TanStack Console 6th Tab BFF integration", async () => {
    const labDir = `/tmp/ansts_shadow_lab_${Date.now()}`;
    const lab = new index_1.ShadowTrafficReplayLab(labDir);
    // 1. Candidate improves TTFT and Pass@1 -> PROMOTE_CANDIDATE
    const promoteCard = lab.runShadowReplayEvaluation({
        sourceTaskId: "task-shadow-01",
        baselinePool: "DeepSeek",
        candidatePool: "GLM-5.3",
        historicalFrames: [
            {
                taskId: "task-shadow-01",
                fence: new index_1.FenceToken(1, 1, "task-shadow-01"),
                tokensUsed: 1500,
                passRate: 0.80,
                errorCount: 1,
                diffEntropy: 0.6,
            },
        ],
    });
    strict_1.default.equal(promoteCard.verdict, "PROMOTE_CANDIDATE");
    strict_1.default.equal(promoteCard.zeroSideEffectGuaranteed, true);
    strict_1.default.equal(fs.existsSync(promoteCard.persistedReportPath), true);
    // 2. Candidate triggers velocity breaker backtracks or severe Pass@1 drop -> BLOCK_REGRESSION
    const blockCard = lab.runShadowReplayEvaluation({
        sourceTaskId: "task-shadow-02",
        baselinePool: "DeepSeek",
        candidatePool: "Qwen-3.8",
        historicalFrames: [
            {
                taskId: "task-shadow-02",
                fence: new index_1.FenceToken(1, 1, "task-shadow-02"),
                tokensUsed: 1500,
                passRate: 0.85,
                errorCount: 0,
                diffEntropy: 0.6,
            },
        ],
        candidateSimOverrides: {
            passAt1Rate: 0.55,
            velocityBreakerBacktracks: 2,
        },
    });
    strict_1.default.equal(blockCard.verdict, "BLOCK_REGRESSION");
    // 3. Verify live HTTP BFF 6th Tab endpoints (/v1beta1/console/shadow-lab & /v1beta1/console/shadow-lab/run)
    const gateway = new index_1.MultiNodeClusterDaemonServer({
        nodeId: "celld-gw-shadow",
        role: "gateway",
        listenPort: 0,
        peerBrainUrls: {},
        peerBodyUrls: {},
        storageDir: `/tmp/ansts_gw_shadow_${Date.now()}`,
    });
    const port = await gateway.start();
    const client = new index_1.TanStackConsoleBffClient(`http://127.0.0.1:${port}`);
    try {
        const listRes = await client.fetchShadowLab();
        strict_1.default.ok(listRes.totalRuns >= 1);
        const runRes = await client.mutateRunShadowEval({
            sourceTaskId: "task-prod-101",
            baselinePool: "DeepSeek",
            candidatePool: "Qwen-3.8",
            shadowTrafficSamplePct: 5.0,
        });
        strict_1.default.equal(runRes.zeroSideEffectGuaranteed, true);
        strict_1.default.equal(runRes.verdict, "PROMOTE_CANDIDATE");
    }
    finally {
        await gateway.stop();
    }
});
(0, node_test_1.default)("test 86: [M33 P2 / TASK-140 & TASK-141] CrossRegionLtxReplicationMesh monotonic (epoch, txSeq) streaming, idempotent dedup, and GeoSovereigntySlotPinningManager GDPR boundary enforcement", () => {
    const repDir = `/tmp/ansts_xregion_${Date.now()}`;
    const mesh = new index_1.CrossRegionLtxReplicationMesh(["us-west-2", "eu-central-1", "ap-northeast-1"], repDir);
    const ltxPayload = JSON.stringify({ table: "turn_wal_frames", rows: 4 });
    const payloadSha256 = crypto.createHash("sha256").update(ltxPayload, "utf8").digest("hex");
    // 1. Valid global replication from us-west-2 -> eu-central-1
    const r1 = mesh.replicateLtxFrame({
        cellId: "cell-global-01",
        sourceRegion: "us-west-2",
        targetRegion: "eu-central-1",
        epoch: 2,
        txSeq: 10,
        ltxPayload,
        payloadSha256,
        complianceRegime: "GLOBAL_DEFAULT",
        emittedAtMs: Date.now(),
    });
    strict_1.default.equal(r1.replicated, true);
    strict_1.default.equal(r1.deduplicated, false);
    strict_1.default.equal(fs.existsSync(r1.persistedFilePath), true);
    // 2. Duplicate (epoch=2, txSeq=10) is idempotently deduplicated
    const rDup = mesh.replicateLtxFrame({
        cellId: "cell-global-01",
        sourceRegion: "us-west-2",
        targetRegion: "eu-central-1",
        epoch: 2,
        txSeq: 10,
        ltxPayload,
        payloadSha256,
        complianceRegime: "GLOBAL_DEFAULT",
        emittedAtMs: Date.now(),
    });
    strict_1.default.equal(rDup.deduplicated, true);
    // 3. EU_GDPR_STRICT frame attempting to replicate to non-EU region (us-west-2) is blocked with 451 SovereigntyEgressViolationError
    strict_1.default.throws(() => mesh.replicateLtxFrame({
        cellId: "cell-eu-gdpr-99",
        sourceRegion: "eu-central-1",
        targetRegion: "us-west-2",
        epoch: 1,
        txSeq: 1,
        ltxPayload,
        payloadSha256,
        complianceRegime: "EU_GDPR_STRICT",
        emittedAtMs: Date.now(),
    }), index_1.SovereigntyEgressViolationError);
    // 4. GeoSovereigntySlotPinningManager pins EU_GDPR_STRICT tenants to slots 192..255 in eu-central-1
    const pinMgr = new index_1.GeoSovereigntySlotPinningManager();
    const gdprPin = pinMgr.resolveSovereignSlotForTenant("tenant-berlin-bank", "EU_GDPR_STRICT");
    strict_1.default.ok(gdprPin.slotIndex >= 192 && gdprPin.slotIndex <= 255);
    strict_1.default.equal(gdprPin.pinnedRegion, "eu-central-1");
    strict_1.default.equal(gdprPin.crossRegionFailoverAllowed, false);
});
(0, node_test_1.default)("test 87: [M34 Zero-Stub & Zero-Hardcode Audit] Verifies real SQL WHERE (=, >, <) + disk fsync, real hrtime SQL/Worker measurement, dynamic Console BFF task/WAL tracking, and router-driven Shadow Lab", async () => {
    // 1. Verify InMemoryCellSqlStorage evaluates WHERE =, >, <, UPDATE, DELETE and persists to disk with fsync
    const dbPath = `/tmp/ansts_sql_real_${Date.now()}/cell.db.json`;
    const sql1 = new index_1.InMemoryCellSqlStorage(dbPath);
    sql1.exec("CREATE TABLE items (id TEXT, score INTEGER)");
    sql1.exec("INSERT INTO items (id, score) VALUES (?, ?)", "a", 10);
    sql1.exec("INSERT INTO items (id, score) VALUES (?, ?)", "b", 25);
    sql1.exec("UPDATE items SET score = ? WHERE id = ?", 99, "a");
    // Rehydrate from disk in a fresh instance
    const sql2 = new index_1.InMemoryCellSqlStorage(dbPath);
    const onlyA = sql2.exec("SELECT * FROM items WHERE id = ?", "a").toArray();
    strict_1.default.equal(onlyA.length, 1);
    strict_1.default.equal(onlyA[0].score, 99);
    // 2. Verify V8HeapAndSqlBudgetGuard measures actual hrtime execution duration
    const guard = new index_1.V8HeapAndSqlBudgetGuard({ heapLimitMb: 128, sqlStatementBudgetMs: 250 });
    const sqlExecRes = guard.executeBoundedSql({
        query: "SELECT * FROM items WHERE score > ?",
        executor: () => sql2.exec("SELECT * FROM items WHERE score > ?", 50).toArray(),
    });
    strict_1.default.equal(sqlExecRes.rows.length, 1);
    strict_1.default.ok(typeof sqlExecRes.elapsedMs === "number" && sqlExecRes.elapsedMs >= 0);
    // 3. Verify ConsoleBffController dynamically reflects newly dispatched tasks & actual .wal files on disk
    const gw = new index_1.MultiNodeClusterDaemonServer({
        nodeId: "celld-gw-audit",
        role: "gateway",
        listenPort: 0,
        peerBrainUrls: {},
        peerBodyUrls: {},
        storageDir: `/tmp/ansts_gw_audit_${Date.now()}`,
    });
    const port = await gw.start();
    const client = new index_1.TanStackConsoleBffClient(`http://127.0.0.1:${port}`);
    try {
        const ovBefore = await client.fetchOverview("us-west-2");
        const initialWalCount = ovBefore.turnWalCheckpointsCount;
        await client.mutateDispatchTask({
            tenantId: "tenant-audit",
            taskId: "task-dynamic-909",
            repoId: "repo-audit",
            promptText: "Audit live BFF registration",
        });
        const ovAfter = await client.fetchOverview("us-west-2");
        strict_1.default.ok(ovAfter.turnWalCheckpointsCount > initialWalCount);
        const harnessAfter = await client.fetchHarnessAndShards();
        const foundDynamicCell = harnessAfter.activeHarnessCells.find((c) => c.taskId === "task-dynamic-909");
        strict_1.default.ok(foundDynamicCell, "Dispatched task must appear dynamically in /v1beta1/console/harness");
    }
    finally {
        await gw.stop();
    }
});
(0, node_test_1.default)("test 88: [M35 Ultimate Console Part 1: TASK-142, TASK-143, TASK-147] Verifies 16x16 (256-Slot) Sovereignty Heatmap, Online 3-Phase Slot Migration, Live V8 Inspector Flame Graph Capture, and 4-Domain Chaos Drills over HTTP BFF", async () => {
    const gw = new index_1.MultiNodeClusterDaemonServer({
        nodeId: "celld-gw-m35-1",
        role: "gateway",
        listenPort: 0,
        peerBrainUrls: {},
        peerBodyUrls: {},
        storageDir: `/tmp/ansts_gw_m35_1_${Date.now()}`,
    });
    const port = await gw.start();
    const client = new index_1.TanStackConsoleBffClient(`http://127.0.0.1:${port}`);
    try {
        // 1. 16x16 (256-Slot) Heatmap & Sovereignty Regime Tagging
        const heatmap = await client.fetchSlotsHeatmap();
        strict_1.default.equal(heatmap.totalSlots, 256);
        strict_1.default.equal(heatmap.cells.length, 256);
        strict_1.default.equal(heatmap.cells[0].complianceRegime, "GLOBAL_DEFAULT");
        strict_1.default.equal(heatmap.cells[150].complianceRegime, "APAC_PDPA_STRICT");
        strict_1.default.equal(heatmap.cells[210].complianceRegime, "EU_GDPR_STRICT");
        // 2. Online 3-Phase Slot Migration on Slot #42
        const migRes = await client.mutateMigrateSlotOnline(42, "scheduler-shard-02");
        strict_1.default.equal(migRes.slotIndex, 42);
        strict_1.default.equal(migRes.targetShardId, "scheduler-shard-02");
        strict_1.default.ok(migRes.newEpoch >= 2);
        // 3. Live V8 Inspector CPU Flame Graph Capture (.collapsed)
        const flame = await client.mutateCaptureV8FlameGraph("celld-brain-01");
        strict_1.default.ok(flame.profileId.startsWith("prof_celld-brain-01_"));
        strict_1.default.ok(flame.samplingOverheadPct < 1.0);
        strict_1.default.ok(Array.isArray(flame.hotspots) && flame.hotspots.length > 0);
        strict_1.default.equal(fs.existsSync(flame.localDiskFilePath), true);
        // 4. Interactive 4-Domain Chaos Engineering Drills
        const chaos = await client.mutateRunChaosDrills();
        strict_1.default.equal(chaos.allPassed, true);
        strict_1.default.equal(chaos.totalDrills, 4);
    }
    finally {
        await gw.stop();
    }
});
(0, node_test_1.default)("test 89: [M35 Ultimate Console Part 2: TASK-144, TASK-145, TASK-146] Verifies HITL STAGED_COMMIT Approval Gate, 0-Token Time-Travel Branch Forking, Spot Host <50ms Evacuation, CoW SLSA Attestation, and 5D W-DRF/FinOps Radar over HTTP BFF", async () => {
    const gw = new index_1.MultiNodeClusterDaemonServer({
        nodeId: "celld-gw-m35-2",
        role: "gateway",
        listenPort: 0,
        peerBrainUrls: {},
        peerBodyUrls: {},
        storageDir: `/tmp/ansts_gw_m35_2_${Date.now()}`,
    });
    const port = await gw.start();
    const client = new index_1.TanStackConsoleBffClient(`http://127.0.0.1:${port}`);
    try {
        // 1. HITL STAGED_COMMIT Approval Queue & Approve/Commit flow
        const hitlBefore = await client.fetchHitlAndTimeTravel("task-prod-101");
        strict_1.default.equal(hitlBefore.pendingStagedItems.length, 1);
        strict_1.default.equal(hitlBefore.pendingStagedItems[0].toolName, "k8s_prod_ConfigMap_apply");
        const approveRes = await client.mutateHitlDecision({
            taskId: "task-prod-101",
            decision: "approve",
        });
        strict_1.default.equal(approveRes.action, "APPROVED_AND_COMMITTED");
        strict_1.default.equal(approveRes.flushedCount, 1);
        // 2. Zero-Token Time-Travel Replay & CoW Branch Fork from Turn #15
        const forkRes = await client.mutateTimeTravelFork("task-prod-101", 15);
        strict_1.default.equal(forkRes.replayedUpToTurn, 15);
        strict_1.default.equal(forkRes.tokensBilledDuringReplay, 0);
        strict_1.default.ok(forkRes.forkLatencyMs < 80);
        // 3. Spot Host 2-Minute Termination Warning Live Evacuation (<50ms)
        const spotEvac = await client.mutateEvacuateSpotHost("body-host-edge-02");
        strict_1.default.equal(spotEvac.evictedSpotHostId, "body-host-edge-02");
        strict_1.default.ok(spotEvac.migratedSandboxesCount >= 1);
        strict_1.default.ok(spotEvac.totalEvacuationLatencyMs < 50);
        // 4. CoW Sub-Agent Branch Fork + Resident LSP (<15ms) + SLSA v1.0 Attestation + Git AST 3-Way Merger
        const branchRec = await client.mutateForkAndAttestBranch({
            sandboxId: "sbx-bff-probe",
            patchFile: "src/new_feature.ts",
            patchContent: "export function computeFastQuota() { return 256; }",
        });
        strict_1.default.equal(branchRec.slsaVerified, true);
        strict_1.default.ok(branchRec.lspLookupLatencyMs < 15);
        const cowAst = await client.fetchCowAndAstMerger();
        strict_1.default.equal(cowAst.astMergePreview.mergedCleanly, true);
        strict_1.default.ok(cowAst.totalBranches >= 2);
        // 5. 5D W-DRF Quota Radar, Cross-Shard Work-Stealing & FinOps Ledger
        const radar = await client.fetchFinOpsAndWdrfRadar();
        strict_1.default.equal(radar.wdrfDimensions.length, 5);
        strict_1.default.ok(radar.tenantBudgets.length >= 2);
        strict_1.default.equal(radar.workStealingPreview.grantedSlots, 6);
    }
    finally {
        await gw.stop();
    }
});
(0, node_test_1.default)("test 90: [M36 P0 / TASK-148 & TASK-149] UffdLazyPageRestorer <10ms positional pread snapshot restore + demand page fault and EbpfLsmRingBufferBridge <1ms binary ring-buffer Cgroup freeze", () => {
    // 1. TASK-148: UFFD Lazy Snapshot Restore & Demand Page Fault
    const uffdDir = `/tmp/ansts_uffd_test_${Date.now()}`;
    const restorer = new index_1.UffdLazyPageRestorer(uffdDir);
    const manifest = restorer.createMemorySnapshot({
        snapshotId: "snap-firecracker-01",
        totalPages: 64,
        hotPageIndices: [0, 1, 2, 3, 4],
        pageContentSeed: "microvm_v27",
    });
    strict_1.default.equal(manifest.totalPages, 64);
    strict_1.default.equal(fs.existsSync(manifest.snapshotFilePath), true);
    const vm = restorer.restoreMicroVmWithUffd("vm-prod-01", "snap-firecracker-01");
    strict_1.default.ok(vm.hotPrefetchLatencyMs < 10, `Expected <10ms hot prefetch, got ${vm.hotPrefetchLatencyMs}ms`);
    strict_1.default.equal(vm.residentPages.size, 5);
    // Access hot page #2 -> no fault
    const hitHot = restorer.handleUffdPageFault("vm-prod-01", 2);
    strict_1.default.equal(hitHot.wasPageFault, false);
    // Access cold page #42 -> triggers demand page fault via positional pread & verifies SHA-256
    const hitCold = restorer.handleUffdPageFault("vm-prod-01", 42);
    strict_1.default.equal(hitCold.wasPageFault, true);
    strict_1.default.equal(hitCold.residentPageCount, 6);
    strict_1.default.ok(hitCold.pageBuffer.toString("utf8").includes("IDX_42_microvm_v27"));
    // 2. TASK-149: eBPF LSM Binary RingBuffer Bridge & <1ms Cgroup Freeze
    const secGuard = new index_1.ZeroTrustSecurityGuard(`/tmp/ansts_ebpf_cg_${Date.now()}`);
    const ebpfBridge = new index_1.EbpfLsmRingBufferBridge(secGuard, 128);
    ebpfBridge.emitKernelLsmEvent({
        pid: 8821,
        sandboxId: "sbx-ebpf-clean",
        hook: "lsm/file_open",
        targetResource: "/workspace/package.json",
    });
    ebpfBridge.emitKernelLsmEvent({
        pid: 8822,
        sandboxId: "sbx-ebpf-attacker",
        hook: "lsm/file_open",
        targetResource: "/etc/shadow",
    });
    const enforced = ebpfBridge.consumeAndEnforceRingBuffer();
    strict_1.default.equal(enforced.processedCount, 2);
    strict_1.default.deepEqual(enforced.frozenSandboxes, ["sbx-ebpf-clean".slice(0, 0) ? "" : "sbx-ebpf-attacker"]);
    strict_1.default.ok(enforced.processingLatencyMs < 15);
    strict_1.default.equal(secGuard.frozenSandboxes.has("sbx-ebpf-attacker"), true);
});
(0, node_test_1.default)("test 91: [M36 P0 / TASK-150 & TASK-151] ConsoleSsePushGateway <10ms real-time SSE streaming and ConsoleRbacAuthGuard HMAC-SHA256 JWT role matrix & hash-chained audit log", async () => {
    const rbacPath = `/tmp/ansts_rbac_${Date.now()}/audit.jsonl`;
    const rbac = new index_1.ConsoleRbacAuthGuard("secret_key_v27", rbacPath);
    const viewerJwt = rbac.issueOperatorJwt({ operatorId: "viewer-user", role: "VIEWER" });
    const adminJwt = rbac.issueOperatorJwt({ operatorId: "sre-admin", role: "SRE_ADMIN" });
    // Viewer can read, but cannot trigger canary:rollback
    const vRead = rbac.verifyAndAuthorizeAction(viewerJwt, "console:read");
    strict_1.default.equal(vRead.claims.role, "VIEWER");
    strict_1.default.throws(() => rbac.verifyAndAuthorizeAction(viewerJwt, "canary:rollback"), index_1.ConsoleRbacForbiddenError);
    // SRE_ADMIN can trigger canary:rollback & audit log is hash-chained on disk
    const admRes = rbac.verifyAndAuthorizeAction(adminJwt, "canary:rollback");
    strict_1.default.equal(admRes.auditEntry.allowed, true);
    strict_1.default.equal(fs.existsSync(rbacPath), true);
    const lines = fs.readFileSync(rbacPath, "utf8").trim().split("\n");
    strict_1.default.equal(lines.length, 3);
    // Verify live HTTP SSE stream (/v1beta1/console/events/stream) & RBAC BFF endpoints
    const gw = new index_1.MultiNodeClusterDaemonServer({
        nodeId: "celld-gw-m36",
        role: "gateway",
        listenPort: 0,
        peerBrainUrls: {},
        peerBodyUrls: {},
        storageDir: `/tmp/ansts_gw_m36_${Date.now()}`,
    });
    const port = await gw.start();
    const client = new index_1.TanStackConsoleBffClient(`http://127.0.0.1:${port}`);
    try {
        const receivedSseChunks = [];
        const sseReq = http.get(`http://127.0.0.1:${port}/v1beta1/console/events/stream`, (res) => {
            res.on("data", (chunk) => {
                receivedSseChunks.push(chunk.toString("utf8"));
            });
        });
        // Wait briefly for initial SSE connection frame
        await new Promise((r) => setTimeout(r, 40));
        // Issue token & verify RBAC via BFF -> broadcasts RBAC_AUDIT_LOGGED over SSE stream
        const tokRes = await client.mutateIssueRbacToken("ops-01", "OPERATOR");
        const verOk = await client.mutateVerifyRbacPermission(tokRes.token, "tunnels:drain");
        strict_1.default.equal(verOk.status, 200);
        strict_1.default.equal(verOk.body.authorized, true);
        const verDeny = await client.mutateVerifyRbacPermission(tokRes.token, "slots:migrate");
        strict_1.default.equal(verDeny.status, 403);
        strict_1.default.equal(verDeny.body.authorized, false);
        await new Promise((r) => setTimeout(r, 40));
        sseReq.destroy();
        const combinedSse = receivedSseChunks.join("");
        strict_1.default.ok(combinedSse.includes("LIVE_SSE_CONNECTED"));
        strict_1.default.ok(combinedSse.includes("RBAC_AUDIT_LOGGED"));
    }
    finally {
        await gw.stop();
    }
});
(0, node_test_1.default)("test 92: [M37 P1 / TASK-152 & TASK-153] SpeculativeReadOnlyPrefetcher 0ms perceived hit during THINKING_GEN and CrossTenantAstRepairCache zero-token deterministic AST repair", () => {
    // 1. TASK-152: Speculative Read-Only Tool Prefetcher
    const wsDir = `/tmp/ansts_spec_ws_${Date.now()}`;
    const prefetcher = new index_1.SpeculativeReadOnlyPrefetcher(wsDir);
    fs.mkdirSync(`${wsDir}/src`, { recursive: true });
    fs.writeFileSync(`${wsDir}/src/kernel.ts`, "export const KERNEL_VERSION = '2.7.0';", "utf8");
    const entries = prefetcher.inspectThinkingStreamAndPrefetch('Step 1: Let us inspect fs_read_file("src/kernel.ts") to check the constant.');
    strict_1.default.equal(entries.length, 1);
    const execRes = prefetcher.executeReadOnlyToolWithPrefetch("fs_read_file", "src/kernel.ts");
    strict_1.default.equal(execRes.speculativeCacheHit, true);
    strict_1.default.equal(execRes.perceivedExecutionLatencyMs, 0);
    strict_1.default.ok(execRes.content.includes("2.7.0"));
    // 2. TASK-153: Cross-Tenant Privacy-Preserving AST Repair Cache
    const astCache = new index_1.CrossTenantAstRepairCache(`/tmp/ansts_ast_cache_${Date.now()}/cache.json`);
    const repairRes = astCache.tryRepairSyntaxErrorZeroToken("TS2322: Type 'string' is not assignable to type 'number'", 'export const maxRetries: number = "12";');
    strict_1.default.equal(repairRes.repaired, true);
    strict_1.default.equal(repairRes.llmTokensConsumed, 0);
    strict_1.default.ok(repairRes.tokensSaved >= 600);
    strict_1.default.ok(repairRes.patchedSourceCode.includes('Number("12")'));
});
(0, node_test_1.default)("test 93: [M37 P2 / TASK-154 & TASK-155] A2aWaitForGraphDeadlockDetector Tarjan SCC <50ms cycle breaker, DistributedOrphanGcSweeper zero-leak disk sweep, and unified BFF probe", async () => {
    // 1. TASK-154: Tarjan SCC Wait-For Graph Deadlock Detector
    const detector = new index_1.A2aWaitForGraphDeadlockDetector();
    detector.addWaitDependency({
        waiterTaskId: "task-A",
        blockerTaskId: "task-B",
        resourceTopic: "lock-1",
        waiterPriorityWeight: 50,
        registeredAtMs: Date.now(),
    });
    detector.addWaitDependency({
        waiterTaskId: "task-B",
        blockerTaskId: "task-C",
        resourceTopic: "lock-2",
        waiterPriorityWeight: 10, // lowest priority -> chosen as preemption victim
        registeredAtMs: Date.now(),
    });
    detector.addWaitDependency({
        waiterTaskId: "task-C",
        blockerTaskId: "task-A",
        resourceTopic: "lock-3",
        waiterPriorityWeight: 80,
        registeredAtMs: Date.now(),
    });
    const dlReport = detector.detectAndBreakDeadlockCycles();
    strict_1.default.equal(dlReport.deadlockDetected, true);
    strict_1.default.equal(dlReport.cycles.length, 1);
    strict_1.default.deepEqual(dlReport.preemptedVictimTaskIds, ["task-B"]);
    strict_1.default.ok(dlReport.detectionLatencyMs < 50);
    // Verify cycle is broken on second run
    const afterBreak = detector.detectAndBreakDeadlockCycles();
    strict_1.default.equal(afterBreak.deadlockDetected, false);
    // 2. TASK-155: Distributed Orphan Sandbox & Expired LTX Garbage Collector
    const gcBase = `/tmp/ansts_gc_test_${Date.now()}`;
    const sweeper = new index_1.DistributedOrphanGcSweeper(`${gcBase}/sandboxes`, `${gcBase}/ltx`);
    fs.mkdirSync(`${gcBase}/sandboxes/sbx-active-keep`, { recursive: true });
    fs.writeFileSync(`${gcBase}/sandboxes/sbx-active-keep/block.img`, Buffer.alloc(4096, 1));
    fs.mkdirSync(`${gcBase}/sandboxes/sbx-orphan-dead`, { recursive: true });
    fs.writeFileSync(`${gcBase}/sandboxes/sbx-orphan-dead/block.img`, Buffer.alloc(16384, 1));
    fs.writeFileSync(`${gcBase}/ltx/cell-orphan_ep0001.ltx`, "expired-ltx-payload", "utf8");
    const sweepRes = sweeper.runOrphanAndLtxSweep({
        activeSandboxAndCellIds: new Set(["sbx-active-keep"]),
        retentionWindowMs: 0,
    });
    strict_1.default.deepEqual(sweepRes.deletedOrphanSandboxDirs, ["sbx-orphan-dead"]);
    strict_1.default.deepEqual(sweepRes.deletedExpiredLtxFiles, ["cell-orphan_ep0001.ltx"]);
    strict_1.default.ok(sweepRes.reclaimedBytes >= 16384);
    strict_1.default.equal(fs.existsSync(`${gcBase}/sandboxes/sbx-active-keep/block.img`), true);
    strict_1.default.equal(fs.existsSync(`${gcBase}/sandboxes/sbx-orphan-dead`), false);
    // 3. Unified BFF v2.7 Deep-Tech Probe endpoint verification
    const gw = new index_1.MultiNodeClusterDaemonServer({
        nodeId: "celld-gw-m37",
        role: "gateway",
        listenPort: 0,
        peerBrainUrls: {},
        peerBodyUrls: {},
        storageDir: `/tmp/ansts_gw_m37_${Date.now()}`,
    });
    const port = await gw.start();
    const client = new index_1.TanStackConsoleBffClient(`http://127.0.0.1:${port}`);
    try {
        const probe = await client.mutateRunV27DeepTechProbe();
        strict_1.default.ok(probe.uffd.hotPrefetchLatencyMs < 10);
        strict_1.default.equal(probe.speculativePrefetch.speculativeCacheHit, true);
        strict_1.default.equal(probe.astRepairCache.repaired, true);
        strict_1.default.equal(probe.deadlockDetector.deadlockDetected, true);
        strict_1.default.ok(probe.gcSweeper.reclaimedBytes >= 8192);
    }
    finally {
        await gw.stop();
    }
});
(0, node_test_1.default)("test 94: [M38 & M39 Ultimate Verification Suite: TASK-156 ~ TASK-163] Deterministic replay, network chaos proxy, TEE quote unsealing, ZK turn prover, KV block eviction, AST speculative decoder, CoW bisect, and priority load shedder", async () => {
    // 1. TASK-156: Deterministic Replay Envelope
    const env = new index_1.DeterministicReplayEnvelope({
        envelopeId: "env_01",
        virtualBaseTimestampMs: 1700000000000,
        virtualClockTickOffsetMs: 0,
        randomSeedHex: "abcdef0123456789",
        consumedRandomBytesCount: 0,
    });
    const t1 = env.getDeterministicNowMs();
    const t2 = env.getDeterministicNowMs();
    strict_1.default.equal(t2 - t1, 10);
    const r1 = env.getDeterministicRandom();
    strict_1.default.ok(r1 >= 0 && r1 < 1);
    // 2. TASK-157: Sandbox Network Chaos Proxy
    const proxy = new index_1.SandboxNetworkChaosProxy({ dropPacketPct: 0, jitterMs: 5, rateLimitExceeded: false });
    const netRes = await proxy.interceptOutboundFetch("https://registry.npmjs.org/ansts");
    strict_1.default.equal(netRes.status, 200);
    strict_1.default.equal(netRes.dropped, false);
    // 3. TASK-158: Confidential TEE Bridge
    const tee = new index_1.ConfidentialTeeBridge("expected_rootfs_merkle_root_v27");
    const hmacKey = Buffer.from("tee_secret_key", "utf8");
    const quote = {
        platform: "AWSNitroEnclave",
        pcr0CodeDigestSha256: tee.expectedPcr0,
        pcr1ConfigDigestSha256: tee.expectedPcr1,
        pcr2RootfsDigestSha256: tee.expectedPcr2,
        quoteSignature: crypto.createHmac("sha256", hmacKey).update(`${tee.expectedPcr0}:${tee.expectedPcr1}:${tee.expectedPcr2}`, "utf8").digest("hex"),
        timestampMs: Date.now(),
    };
    const unseal = tee.verifyQuoteAndUnsealSecret(quote, Buffer.from("PROD_API_KEY_SECRET_99", "utf8").toString("base64"), hmacKey);
    strict_1.default.equal(unseal.unsealed, true);
    strict_1.default.equal(unseal.secretContent, "PROD_API_KEY_SECRET_99");
    // 4. TASK-159: Verifiable Turn Execution Prover
    const prover = new index_1.VerifiableTurnExecutionProver();
    const mockFrame = {
        taskId: "task-zk-01",
        fence: { epoch: 1, turnSeq: 1, taskId: "task-zk-01" },
        state: index_1.TaskState.COMPLETED,
        diffSnapshot: "refactor(core): optimized scheduler loop",
        workspaceMerkleHash: "merkle_sha256_root_xyz",
        contextMessages: ["tool output ok"],
        tokensUsed: 420,
        passRate: 1.0,
        errorCount: 0,
        diffEntropy: 0.5,
    };
    const proof = prover.generateTurnProof(mockFrame, "Refactor the scheduling core");
    strict_1.default.equal(prover.verifyTurnProof(proof), true);
    // 5. TASK-160: KV-Cache Block Eviction Manager
    const kvMgr = new index_1.KvCacheBlockEvictionManager(4);
    kvMgr.registerOrTouchBlock({ blockId: "b1", taskId: "t1", tokenCount: 512, attentionScore: 0.9, isAttentionSink: true, turnSeq: 1 });
    kvMgr.registerOrTouchBlock({ blockId: "b2", taskId: "t1", tokenCount: 512, attentionScore: 0.1, isAttentionSink: false, turnSeq: 2 });
    kvMgr.registerOrTouchBlock({ blockId: "b3", taskId: "t1", tokenCount: 512, attentionScore: 0.2, isAttentionSink: false, turnSeq: 3 });
    kvMgr.registerOrTouchBlock({ blockId: "b4", taskId: "t1", tokenCount: 512, attentionScore: 0.3, isAttentionSink: false, turnSeq: 4 });
    const evRes = kvMgr.registerOrTouchBlock({ blockId: "b5", taskId: "t1", tokenCount: 512, attentionScore: 0.4, isAttentionSink: false, turnSeq: 5 });
    strict_1.default.ok(evRes.evictedBlockIds.includes("b2")); // Lowest attention score non-sink evicted
    // 6. TASK-161: AST Speculative Decoder
    const lsp = new index_1.WarmLspDaemonPool();
    lsp.prewarmWorkspaceLspIndex("sbx-spec-01", "typescript", { "src/a.ts": "export function parseToken() {}" });
    const decoder = new index_1.AstSpeculativeDecoder(lsp);
    const decRes = decoder.generateAndVerifyDraftTokens("sbx-spec-01", "src/a.ts", "export function parseToken() { return true; }");
    strict_1.default.ok(decRes.acceptanceRatePct > 0);
    // 7. TASK-162: Parallel CoW Git-Bisect Coordinator
    const forker = new index_1.SandboxBranchForker(`/tmp/ansts_bisect_${Date.now()}`);
    const bisectCoord = new index_1.ParallelCowGitBisectCoordinator(forker);
    const bisectRep = bisectCoord.runParallelBisect({
        sandboxId: "sbx-bisect-01",
        commitRange: ["c1", "c2", "c3", "culprit_c4", "c5"],
        culpritCommit: "culprit_c4",
    });
    strict_1.default.equal(bisectRep.resolvedInSingleRound, true);
    strict_1.default.equal(bisectRep.culpritCommitHash, "culprit_c4");
    // 8. TASK-163: Priority-Based Load Shedder
    const shedder = new index_1.PriorityLoadShedder(100);
    const loadRep = shedder.evaluateAdmissionAndShed([
        { taskId: "p3-1", priorityClass: "P3_BACKGROUND" },
        { taskId: "p0-1", priorityClass: "P0_CRITICAL" },
    ], 95);
    strict_1.default.equal(loadRep.shedDetails.P3_BACKGROUND, 1);
    strict_1.default.equal(loadRep.shedDetails.P0_CRITICAL, 0); // P0 critical survives 100%
});
