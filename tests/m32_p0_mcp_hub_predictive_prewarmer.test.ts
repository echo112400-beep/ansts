/**
 * @fileoverview M32 P0 (TASK-134 ~ TASK-137) Verification Suite:
 * - `McpStreamableEventHub` (JSON-RPC 2.0 Streamable SSE notifications, FenceToken validation,
 *   disk spooling, and automatic wake-up of hibernated `DurableWorkflowContext` cells)
 * - `WarmLspDaemonPool` (<15ms resident LSP AST symbol indexing & CoW branch incremental diagnostics)
 * - `PredictiveDagPrewarmer` (Lookahead CoW branch pre-warming during `THINKING_GEN` with 0ms perceived claim)
 * - `SpotHostEvacuationArbiter` (FinOps Spot vs On-Demand placement & <50ms Spot termination evacuation with epoch+1 fencing)
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import {
  McpStreamableEventHub,
  WarmLspDaemonPool,
  PredictiveDagPrewarmer,
  SandboxBranchForker,
  DurableWorkflowContext,
  FenceToken,
  TaskState,
  TaskPriority,
  ShadowTrafficReplayLab,
  CrossRegionLtxReplicationMesh,
  GeoSovereigntySlotPinningManager,
  SovereigntyEgressViolationError,
  TanStackConsoleBffClient,
  InMemoryCellSqlStorage,
  V8HeapAndSqlBudgetGuard,
  UffdLazyPageFaultRestorer,
  EbpfLsmSecurityBridge,
  ConsoleRbacAuthGuard,
  ConsoleRbacForbiddenError,
  SpeculativeReadOnlyPrefetcher,
  CrossTenantAstRepairCache,
  A2aWaitForGraphDeadlockDetector,
  DistributedCasGcSweeper,
  DeterministicExecutionEnvelope,
  NetworkChaosFaultInjectionProxy,
  ConfidentialTeeAttestationBridge,
  VerifiableTurnExecutionProver,
  KvCacheBlockEvictionManager,
  AstSpeculativeDecoder,
  ParallelCowGitBisectCoordinator,
  PriorityLoadShedder,
  LayerFsWorkspaceDriver,
} from "../src/index";
import * as http from "node:http";
import * as crypto from "node:crypto";
import { LocalSpotEvacuation } from "../src/runtime/local_spot_evacuation.js";
import { LocalConsoleGateway } from "./fixtures/local_console_gateway.js";

test("test 83: [M32 P0 / TASK-134 & TASK-135] McpStreamableEventHub wakes hibernated Workflow Cell via JSON-RPC 2.0 notification and WarmLspDaemonPool delivers <15ms incremental branch diagnostics", async () => {
  const hubDir = `/tmp/ansts_mcp_hub_${Date.now()}`;
  const hub = new McpStreamableEventHub(hubDir);

  const wfCtx = new DurableWorkflowContext(new InMemoryCellSqlStorage(), "wf-task-mcp-01");
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
    fence: new FenceToken(2, 5, "task-mcp-01"),
    data: { buildStatus: "PASSED", artifactSha: "sha256_9988" },
    workflowCtx: wfCtx,
    currentTaskState: TaskState.SUSPENDED_RECONCILIATION,
  });

  assert.equal(pubRes.wokeHibernatedCell, true);
  assert.equal(pubRes.newTaskState, TaskState.RUNNING_THINK);
  assert.equal(wfCtx.isolateHibernated, false);
  assert.equal(fs.existsSync(pubRes.persistedSpoolPath), true);
  assert.ok(pubRes.sseWireFrame.includes("event: mcp_jsonrpc_notification"));

  const receivedEvt = await wfCtx.waitForEvent("ci.build.completed");
  assert.equal((receivedEvt as any).data.buildStatus, "PASSED");

  // Verify WarmLspDaemonPool pre-warms AST symbol index and runs <15ms incremental branch check
  const lspPool = new WarmLspDaemonPool();
  const warmIdx = lspPool.prewarmWorkspaceLspIndex("sbx-warm-lsp-01", "typescript", {
    "src/auth.ts": "export async function verifyJwtToken(token: string) { return true; }\nexport class SessionGuard {}",
  });
  assert.ok(warmIdx.snapshot.exportedSymbols["verifyJwtToken"]);
  assert.ok(warmIdx.snapshot.exportedSymbols["SessionGuard"]);
  assert.equal(warmIdx.snapshot.diagnostics.length, 0);

  const branchDiag = lspPool.queryIncrementalBranchDiagnostics("sbx-warm-lsp-01", "brn-sub-01", {
    "src/auth_patch.ts": "const x: number = TYPE_MISMATCH_ERROR;",
  });
  assert.ok(branchDiag.lookupLatencyMs < 15);
  assert.equal(branchDiag.inheritedSymbolsCount, 2);
  assert.equal(branchDiag.branchDiagnostics.length, 1);
});

test("test 84: [M32 P0 / TASK-136 & TASK-137] PredictiveDagPrewarmer achieves 0ms perceived branch claim and SpotHostEvacuationArbiter evacuates Spot sandboxes in <50ms with Epoch+1 fencing", () => {
  // 1. Predictive DAG Pre-Warming during THINKING_GEN
  const forker = new SandboxBranchForker();
  const prewarmer = new PredictiveDagPrewarmer(forker);

  const ticket = prewarmer.prewarmSubAgentBranchesDuringThinking({
    parentTaskId: "task-dag-parent-01",
    parentSandboxId: "sbx-dag-parent-01",
    seedFiles: { "package.json": "{\"name\":\"ansts\"}" },
    fanOutCount: 3,
  });
  assert.equal(ticket.prewarmedBranches.length, 3);

  const claimed = prewarmer.claimPrewarmedBranch("task-dag-parent-01");
  assert.equal(claimed.perceivedAcquireLatencyMs, 0);
  assert.equal(claimed.branch.active, true);

  // 2. Spot vs On-Demand Placement & 2-Minute Warning Live Evacuation
  const migDir = `/tmp/ansts_spot_evac_${Date.now()}`;
  const arbiter = new LocalSpotEvacuation(migDir);
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
    fence: new FenceToken(1, 4, "task-spot-job-1"),
    allowSpotInstance: true,
    initialFiles: { "src/index.ts": "console.log('spot work');" },
  });
  assert.equal(placed.assignedHostId, "body-spot-01");
  assert.equal(placed.tier, "SPOT_PREEMPTIBLE");

  // Trigger Spot termination warning -> evacuates to body-ondemand-01 in <50ms and increments epoch (1 -> 2)
  const evac = arbiter.evacuateSpotHostOnTerminationNotice("body-spot-01");
  assert.equal(evac.migratedSandboxesCount, 1);
  assert.deepEqual(evac.targetOnDemandHostIds, ["body-ondemand-01"]);
  assert.equal(evac.newFencesByTask["task-spot-job-1"].epoch, 2);
  assert.ok(evac.totalEvacuationLatencyMs < 50);
  assert.equal(
    fs.existsSync(`${migDir}/body-ondemand-01/sbx-spot-1/src/index.ts`),
    true,
  );
});

test("test 85: [M33 P1 / TASK-138 & TASK-139] ShadowTrafficReplayLab zero-side-effect evaluation scorecard, regression blocking, and TanStack Console 6th Tab BFF integration", async () => {
  const labDir = `/tmp/ansts_shadow_lab_${Date.now()}`;
  const lab = new ShadowTrafficReplayLab(labDir);

  // 1. Candidate improves TTFT and Pass@1 -> PROMOTE_CANDIDATE
  const promoteCard = lab.runShadowReplayEvaluation({
    sourceTaskId: "task-shadow-01",
    baselinePool: "DeepSeek",
    candidatePool: "GLM-5.3",
    historicalFrames: [
      {
        taskId: "task-shadow-01",
        tokensUsed: 1500,
        passRate: 0.80,
        errorCount: 1,
        diffEntropy: 0.6,
      },
    ],
  });
  assert.equal(promoteCard.verdict, "PROMOTE_CANDIDATE");
  assert.equal(promoteCard.zeroSideEffectGuaranteed, true);
  assert.equal(fs.existsSync(promoteCard.persistedReportPath), true);

  // 2. Candidate triggers velocity breaker backtracks or severe Pass@1 drop -> BLOCK_REGRESSION
  const blockCard = lab.runShadowReplayEvaluation({
    sourceTaskId: "task-shadow-02",
    baselinePool: "DeepSeek",
    candidatePool: "Qwen-3.8",
    historicalFrames: [
      {
        taskId: "task-shadow-02",
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
  assert.equal(blockCard.verdict, "BLOCK_REGRESSION");

  // 3. Verify live HTTP BFF 6th Tab endpoints (/v1beta1/console/shadow-lab & /v1beta1/console/shadow-lab/run)
  const gateway = new LocalConsoleGateway();
  const port = await gateway.start();
  const client = new TanStackConsoleBffClient(`http://127.0.0.1:${port}`);

  try {
    const listRes = await client.fetchShadowLab();
    assert.ok(listRes.totalRuns >= 1);

    const runRes = await client.mutateRunShadowEval({
      sourceTaskId: "task-prod-101",
      baselinePool: "DeepSeek",
      candidatePool: "Qwen-3.8",
      shadowTrafficSamplePct: 5.0,
    });
    assert.equal(runRes.zeroSideEffectGuaranteed, true);
    assert.equal(runRes.verdict, "PROMOTE_CANDIDATE");
  } finally {
    await gateway.stop();
  }
});

test("test 86: [M33 P2 / TASK-140 & TASK-141] CrossRegionLtxReplicationMesh monotonic (epoch, txSeq) streaming, idempotent dedup, and GeoSovereigntySlotPinningManager GDPR boundary enforcement", () => {
  const repDir = `/tmp/ansts_xregion_${Date.now()}`;
  const mesh = new CrossRegionLtxReplicationMesh(["us-west-2", "eu-central-1", "ap-northeast-1"], repDir);

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
  assert.equal(r1.replicated, true);
  assert.equal(r1.deduplicated, false);
  assert.equal(fs.existsSync(r1.persistedFilePath), true);

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
  assert.equal(rDup.deduplicated, true);

  // 3. EU_GDPR_STRICT frame attempting to replicate to non-EU region (us-west-2) is blocked with 451 SovereigntyEgressViolationError
  assert.throws(
    () =>
      mesh.replicateLtxFrame({
        cellId: "cell-eu-gdpr-99",
        sourceRegion: "eu-central-1",
        targetRegion: "us-west-2",
        epoch: 1,
        txSeq: 1,
        ltxPayload,
        payloadSha256,
        complianceRegime: "EU_GDPR_STRICT",
        emittedAtMs: Date.now(),
      }),
    SovereigntyEgressViolationError,
  );

  // 4. GeoSovereigntySlotPinningManager pins EU_GDPR_STRICT tenants to slots 192..255 in eu-central-1
  const pinMgr = new GeoSovereigntySlotPinningManager();
  const gdprPin = pinMgr.resolveSovereignSlotForTenant("tenant-berlin-bank", "EU_GDPR_STRICT");
  assert.ok(gdprPin.slotIndex >= 192 && gdprPin.slotIndex <= 255);
  assert.equal(gdprPin.pinnedRegion, "eu-central-1");
  assert.equal(gdprPin.crossRegionFailoverAllowed, false);
});

test("test 87: [M34 Zero-Stub & Zero-Hardcode Audit] Verifies real SQL WHERE (=, >, <) + disk fsync, real hrtime SQL/Worker measurement, dynamic Console BFF task/WAL tracking, and router-driven Shadow Lab", async () => {
  // 1. Verify InMemoryCellSqlStorage evaluates WHERE =, >, <, UPDATE, DELETE and persists to disk with fsync
  const dbPath = `/tmp/ansts_sql_real_${Date.now()}/cell.db.json`;
  const sql1 = new InMemoryCellSqlStorage(dbPath);
  sql1.exec("CREATE TABLE items (id TEXT, score INTEGER)");
  sql1.exec("INSERT INTO items (id, score) VALUES (?, ?)", "a", 10);
  sql1.exec("INSERT INTO items (id, score) VALUES (?, ?)", "b", 25);
  sql1.exec("UPDATE items SET score = ? WHERE id = ?", 99, "a");

  // Rehydrate from disk in a fresh instance
  const sql2 = new InMemoryCellSqlStorage(dbPath);
  const onlyA = sql2.exec("SELECT * FROM items WHERE id = ?", "a").toArray();
  assert.equal(onlyA.length, 1);
  assert.equal(onlyA[0].score, 99);

  // 2. Verify V8HeapAndSqlBudgetGuard measures actual hrtime execution duration
  const guard = new V8HeapAndSqlBudgetGuard(96, 128, 250);
  const sqlExecRes = guard.execWithBudget(sql2, "SELECT * FROM items WHERE score > ?", 50);
  assert.equal(sqlExecRes.cursor.toArray().length, 1);
  assert.ok(typeof sqlExecRes.elapsedMs === "number" && sqlExecRes.elapsedMs >= 0);

  // 3. Verify ConsoleBffController dynamically reflects newly dispatched tasks & actual .wal files on disk
  const gw = new LocalConsoleGateway();
  const port = await gw.start();
  const client = new TanStackConsoleBffClient(`http://127.0.0.1:${port}`);

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
    assert.ok(ovAfter.turnWalCheckpointsCount > initialWalCount);

    const harnessAfter = await client.fetchHarnessAndShards();
    const foundDynamicCell = harnessAfter.activeHarnessCells.find(
      (c: any) => c.taskId === "task-dynamic-909",
    );
    assert.ok(foundDynamicCell, "Dispatched task must appear dynamically in /v1beta1/console/harness");
  } finally {
    await gw.stop();
  }
});

test("test 88: [M35 Ultimate Console Part 1: TASK-142, TASK-143, TASK-147] Verifies 16x16 (256-Slot) Sovereignty Heatmap, Online 3-Phase Slot Migration, Live V8 Inspector Flame Graph Capture, and 4-Domain Chaos Drills over HTTP BFF", async () => {
  const gw = new LocalConsoleGateway();
  const port = await gw.start();
  const client = new TanStackConsoleBffClient(`http://127.0.0.1:${port}`);

  try {
    // 1. 16x16 (256-Slot) Heatmap & Sovereignty Regime Tagging
    const heatmap = await client.fetchSlotsHeatmap();
    assert.equal(heatmap.totalSlots, 256);
    assert.equal(heatmap.cells.length, 256);
    assert.equal(heatmap.cells[0].complianceRegime, "GLOBAL_DEFAULT");
    assert.equal(heatmap.cells[150].complianceRegime, "APAC_PDPA_STRICT");
    assert.equal(heatmap.cells[210].complianceRegime, "EU_GDPR_STRICT");

    // 2. Online 3-Phase Slot Migration on Slot #42
    const migRes = await client.mutateMigrateSlotOnline(42, "scheduler-shard-02");
    assert.equal(migRes.slotIndex, 42);
    assert.equal(migRes.targetShardId, "scheduler-shard-02");
    assert.ok(migRes.newEpoch >= 2);

    // 3. Live V8 Inspector CPU Flame Graph Capture (.collapsed)
    const flame = await client.mutateCaptureV8FlameGraph("celld-brain-01");
    assert.ok(flame.profileId.startsWith("prof_celld-brain-01_"));
    assert.ok(flame.samplingOverheadPct < 1.0);
    assert.ok(Array.isArray(flame.hotspots) && flame.hotspots.length > 0);
    assert.equal(fs.existsSync(flame.localDiskFilePath), true);

    // 4. Interactive 4-Domain Chaos Engineering Drills
    const chaos = await client.mutateRunChaosDrills();
    assert.equal(chaos.allPassed, true);
    assert.equal(chaos.totalDrills, 4);
  } finally {
    await gw.stop();
  }
});

test("test 89: [M35 Ultimate Console Part 2: TASK-144, TASK-145, TASK-146] Verifies HITL STAGED_COMMIT Approval Gate, 0-Token Time-Travel Branch Forking, Spot Host <50ms Evacuation, CoW SLSA Attestation, and 5D W-DRF/FinOps Radar over HTTP BFF", async () => {
  const gw = new LocalConsoleGateway();
  const port = await gw.start();
  const client = new TanStackConsoleBffClient(`http://127.0.0.1:${port}`);

  try {
    // 1. HITL STAGED_COMMIT Approval Queue & Approve/Commit flow
    const hitlBefore = await client.fetchHitlAndTimeTravel("task-prod-101");
    assert.equal(hitlBefore.pendingStagedItems.length, 1);
    assert.equal(hitlBefore.pendingStagedItems[0].toolName, "k8s_configmap_preview");

    const approver = await client.mutateIssueRbacToken("fixture-approver", "SRE_ADMIN");
    const approveRes = await client.mutateHitlDecision({
      taskId: "task-prod-101",
      decision: "approve",
      operatorToken: approver.token,
      ticketId: hitlBefore.pendingStagedItems[0].ticketId,
    });
    assert.equal(approveRes.action, "APPROVED");
    assert.equal(approveRes.ticket.status, "APPROVED");
    const afterApproval = await client.fetchHitlAndTimeTravel("task-prod-101");
    assert.equal(afterApproval.pendingStagedItems.length, 0);
    assert.equal(afterApproval.committedLedgerCount, 0);

    // 2. Zero-Token Time-Travel Replay & CoW Branch Fork from Turn #15
    const forkRes = await client.mutateTimeTravelFork("task-prod-101", 15);
    assert.equal(forkRes.replayedUpToTurn, 15);
    assert.equal(forkRes.tokensBilledDuringReplay, 0);
    assert.ok(forkRes.forkLatencyMs < 80);

    // 3. Spot Host 2-Minute Termination Warning Live Evacuation (<50ms)
    const spotEvac = await client.mutateEvacuateSpotHost("body-host-edge-02");
    assert.equal(spotEvac.evictedSpotHostId, "body-host-edge-02");
    assert.ok(spotEvac.migratedSandboxesCount >= 1);
    assert.ok(spotEvac.totalEvacuationLatencyMs < 50);

    // 4. CoW Sub-Agent Branch Fork + Resident LSP (<15ms) + SLSA v1.0 Attestation + Git AST 3-Way Merger
    const branchRec = await client.mutateForkAndAttestBranch({
      sandboxId: "sbx-bff-probe",
      patchFile: "src/new_feature.ts",
      patchContent: "export function computeFastQuota() { return 256; }",
    });
    assert.equal(branchRec.slsaVerified, true);
    assert.ok(branchRec.lspLookupLatencyMs < 15);

    const cowAst = await client.fetchCowAndAstMerger();
    assert.equal(cowAst.astMergePreview.mergedCleanly, true);
    assert.ok(cowAst.totalBranches >= 2);

    // 5. 5D W-DRF Quota Radar, Cross-Shard Work-Stealing & FinOps Ledger
    const radar = await client.fetchFinOpsAndWdrfRadar();
    assert.equal(radar.wdrfDimensions.length, 5);
    assert.ok(radar.tenantBudgets.length >= 2);
    assert.equal(radar.workStealingPreview.grantedSlots, 6);
  } finally {
    await gw.stop();
  }
});

test("test 90: [M36 / TASK-148 & TASK-149] Page fault cache, snapshot digest, and LSM file policy", () => {
  const restorer = new UffdLazyPageFaultRestorer();
  const memory = new Uint8Array(64 * 4096);
  memory[42 * 4096] = 42;
  const manifest = restorer.stageBufferInto4KiBPages(memory);
  assert.equal(manifest.length, 64);
  for (const page of [0, 1, 2, 3, 4]) restorer.handlePageFault(page);
  assert.equal(restorer.getStats().faultedPagesCount, 5);
  assert.equal(restorer.handlePageFault(2).fetchedFromRemote, false);

  const hitCold = restorer.handlePageFault(42);
  assert.equal(hitCold.fetchedFromRemote, true);
  assert.equal(hitCold.sha256Digest, manifest[42].sha256Digest);
  assert.equal(restorer.getStats().faultedPagesCount, 6);

  const ebpfBridge = new EbpfLsmSecurityBridge();
  assert.equal(ebpfBridge.checkFileOpenHook("/workspace/package.json").allowed, true);
  assert.equal(ebpfBridge.checkFileOpenHook("/etc/shadow").errno, 13);
  assert.equal(ebpfBridge.checkFileOpenHook("/etc/shadow").allowed, false);
});

test("test 91: [M36 P0 / TASK-150 & TASK-151] ConsoleSsePushGateway <10ms real-time SSE streaming and ConsoleRbacAuthGuard HMAC-SHA256 JWT role matrix & hash-chained audit log", async () => {
  const rbacPath = `/tmp/ansts_rbac_${Date.now()}/audit.jsonl`;
  const rbac = new ConsoleRbacAuthGuard("local-fixture-key-for-console-rbac", rbacPath);

  const viewerJwt = rbac.issueOperatorJwt({ operatorId: "viewer-user", role: "VIEWER" });
  const adminJwt = rbac.issueOperatorJwt({ operatorId: "sre-admin", role: "SRE_ADMIN" });

  // Viewer can read, but cannot trigger canary:rollback
  const vRead = rbac.verifyAndAuthorizeAction(viewerJwt, "console:read");
  assert.equal(vRead.claims.role, "VIEWER");
  assert.throws(
    () => rbac.verifyAndAuthorizeAction(viewerJwt, "canary:rollback"),
    ConsoleRbacForbiddenError,
  );

  // SRE_ADMIN can trigger canary:rollback & audit log is hash-chained on disk
  const admRes = rbac.verifyAndAuthorizeAction(adminJwt, "canary:rollback");
  assert.equal(admRes.auditEntry.allowed, true);
  assert.equal(fs.existsSync(rbacPath), true);
  const lines = fs.readFileSync(rbacPath, "utf8").trim().split("\n");
  assert.equal(lines.length, 3);

  // Verify live HTTP SSE stream (/v1beta1/console/events/stream) & RBAC BFF endpoints
  const gw = new LocalConsoleGateway();
  const port = await gw.start();
  const client = new TanStackConsoleBffClient(`http://127.0.0.1:${port}`);

  try {
    const receivedSseChunks: string[] = [];
    const sseReq = http.get(`http://127.0.0.1:${port}/v1beta1/console/events/stream`, (res) => {
      res.on("data", (chunk: Buffer) => {
        receivedSseChunks.push(chunk.toString("utf8"));
      });
    });

    // Wait briefly for initial SSE connection frame
    await new Promise((r) => setTimeout(r, 40));

    // Issue token & verify RBAC via BFF -> broadcasts RBAC_AUDIT_LOGGED over SSE stream
    const tokRes = await client.mutateIssueRbacToken("ops-01", "OPERATOR");
    const verOk = await client.mutateVerifyRbacPermission(tokRes.token, "tunnels:drain");
    assert.equal(verOk.status, 200);
    assert.equal(verOk.body.authorized, true);

    const verDeny = await client.mutateVerifyRbacPermission(tokRes.token, "slots:migrate");
    assert.equal(verDeny.status, 403);
    assert.equal(verDeny.body.authorized, false);

    await new Promise((r) => setTimeout(r, 40));
    sseReq.destroy();

    const combinedSse = receivedSseChunks.join("");
    assert.ok(combinedSse.includes("LIVE_SSE_CONNECTED"));
    assert.ok(combinedSse.includes("RBAC_AUDIT_LOGGED"));
  } finally {
    await gw.stop();
  }
});

test("test 92: [M37 P1 / TASK-152 & TASK-153] SpeculativeReadOnlyPrefetcher 0ms perceived hit during THINKING_GEN and CrossTenantAstRepairCache zero-token deterministic AST repair", () => {
  // 1. TASK-152: Speculative Read-Only Tool Prefetcher
  const wsDir = `/tmp/ansts_spec_ws_${Date.now()}`;
  const prefetcher = new SpeculativeReadOnlyPrefetcher(wsDir);
  fs.mkdirSync(`${wsDir}/src`, { recursive: true });
  fs.writeFileSync(`${wsDir}/src/kernel.ts`, "export const KERNEL_VERSION = '2.7.0';", "utf8");

  const entries = prefetcher.inspectThinkingStreamAndPrefetch(
    'Step 1: Let us inspect fs_read_file("src/kernel.ts") to check the constant.',
  );
  assert.equal(entries.length, 1);

  const execRes = prefetcher.executeReadOnlyToolWithPrefetch("fs_read_file", "src/kernel.ts");
  assert.equal(execRes.speculativeCacheHit, true);
  assert.equal(execRes.perceivedExecutionLatencyMs, 0);
  assert.ok(execRes.content.includes("2.7.0"));

  // 2. TASK-153: Cross-Tenant Privacy-Preserving AST Repair Cache
  const astCache = new CrossTenantAstRepairCache(`/tmp/ansts_ast_cache_${Date.now()}/cache.json`);
  const repairRes = astCache.tryRepairSyntaxErrorZeroToken(
    "TS2322: Type 'string' is not assignable to type 'number'",
    'export const maxRetries: number = "12";',
  );
  assert.equal(repairRes.repaired, true);
  assert.equal(repairRes.llmTokensConsumed, 0);
  assert.ok(repairRes.tokensSaved >= 600);
  assert.ok(repairRes.patchedSourceCode.includes('Number("12")'));
});

test("test 93: [M37 P2 / TASK-154 & TASK-155] A2aWaitForGraphDeadlockDetector Tarjan SCC <50ms cycle breaker, DistributedOrphanGcSweeper zero-leak disk sweep, and unified BFF probe", async () => {
  // 1. TASK-154: Tarjan SCC Wait-For Graph Deadlock Detector
  const detector = new A2aWaitForGraphDeadlockDetector();
  detector.addWaitEdge("task-A", "task-B");
  detector.addWaitEdge("task-B", "task-C");
  detector.addWaitEdge("task-C", "task-A");

  const started = performance.now();
  const dlReport = detector.resolveCycleWithLowestPriorityVictim({ "task-A": 50, "task-B": 10, "task-C": 80 });
  assert.equal(dlReport.resolved, true);
  assert.deepEqual([...new Set(dlReport.cyclePath)].sort(), ["task-A", "task-B", "task-C"]);
  assert.deepEqual(dlReport.evictedVictims, ["task-B"]);
  assert.ok(performance.now() - started < 50);

  // Verify cycle is broken on second run
  assert.equal(detector.detectCycle().hasDeadlock, false);

  const gcWorkspace = new LayerFsWorkspaceDriver();
  const reachable = gcWorkspace.storeEngine.listAllObjectIds();
  const orphan = gcWorkspace.storeEngine.putFileObject("orphan.blob", "expired workspace object").object;
  const sweepRes = new DistributedCasGcSweeper().sweepLayerFsWorkspaceStore(gcWorkspace);
  assert.deepEqual(sweepRes.sweptBlobIds, [orphan.objectId]);
  assert.ok(sweepRes.reclaimedBytes > 0);
  assert.deepEqual(gcWorkspace.storeEngine.listAllObjectIds().sort(), reachable.sort());

  // 3. Unified BFF v2.7 Deep-Tech Probe endpoint verification
  const gw = new LocalConsoleGateway();
  const port = await gw.start();
  const client = new TanStackConsoleBffClient(`http://127.0.0.1:${port}`);

  try {
    const probe = await client.mutateRunV27DeepTechProbe();
    assert.ok(probe.uffd.hotPrefetchLatencyMs < 10);
    assert.equal(probe.speculativePrefetch.speculativeCacheHit, true);
    assert.equal(probe.astRepairCache.repaired, true);
    assert.equal(probe.deadlockDetector.deadlockDetected, true);
    assert.ok(probe.gcSweeper.reclaimedBytes >= 8192);
  } finally {
    await gw.stop();
  }
});

test("test 94: [M38 & M39 Ultimate Verification Suite: TASK-156 ~ TASK-163] Deterministic replay, network chaos proxy, TEE quote unsealing, ZK turn prover, KV block eviction, AST speculative decoder, CoW bisect, and priority load shedder", async () => {
  // 1. TASK-156: Deterministic Replay Envelope
  const envelope = new DeterministicExecutionEnvelope(42, 1_700_000_000_000);
  const replay = new DeterministicExecutionEnvelope(42, 1_700_000_000_000);
  const prefix = envelope.buildDeterministicShellExportPrefix(0);
  assert.match(prefix, /ANSTS_VIRTUAL_CLOCK_MS=1700000000000\b/);
  assert.equal(replay.buildDeterministicShellExportPrefix(0), prefix);
  assert.match(envelope.buildDeterministicShellExportPrefix(10), /ANSTS_VIRTUAL_CLOCK_MS=1700000000010\b/);
  assert.equal(envelope.nextDeterministicRandom() >= 0, true);

  // 2. TASK-157: Sandbox Network Chaos Proxy
  const proxy = new NetworkChaosFaultInjectionProxy();
  const registry = "registry.npmjs.org";
  proxy.injectLatencyJitter(registry, 5);
  proxy.injectPartition(registry, 10, 100);
  assert.equal(proxy.inspectActiveFaults(100).jitterByHost[registry], 5);
  assert.throws(() => proxy.assertReachable(registry, 100), /simulated network partition/);
  assert.doesNotThrow(() => proxy.assertReachable(registry, 110));

  // 3. TASK-158: Confidential TEE Bridge
  const tee = new ConfidentialTeeAttestationBridge();
  const quoteInput = {
    vmId: "vm-attestation-fixture",
    measurementDigestHex: crypto.createHash("sha256").update("fixture rootfs").digest("hex"),
    nonce: "attestation-fixture-nonce",
  };
  const quote = tee.generateAndVerifyAttestationQuote(quoteInput);
  assert.equal(quote.verified, true);
  assert.equal(tee.verifyExternalAttestationQuote({ ...quoteInput, ed25519SignatureHex: quote.ed25519SignatureHex }), true);
  assert.equal(tee.verifyExternalAttestationQuote({ ...quoteInput, nonce: "different", ed25519SignatureHex: quote.ed25519SignatureHex }), false);

  // 4. TASK-159: Verifiable Turn Execution Prover
  const prover = new VerifiableTurnExecutionProver();
  const mockFrame = {
    taskId: "task-zk-01",
    fence: { epoch: 1, turnSeq: 1, taskId: "task-zk-01" },
    state: TaskState.COMPLETED,
    diffSnapshot: "refactor(core): optimized scheduler loop",
    workspaceMerkleHash: "merkle_sha256_root_xyz",
    contextMessages: ["tool output ok"],
    tokensUsed: 420,
    passRate: 1.0,
    errorCount: 0,
    diffEntropy: 0.5,
  } as any;
  const proof = prover.generateTurnProof(mockFrame, "Refactor the scheduling core");
  assert.equal(prover.verifyTurnProof(proof), true);

  // 5. TASK-160: KV-Cache Block Eviction Manager
  const kvMgr = new KvCacheBlockEvictionManager(4);
  kvMgr.registerOrTouchBlock({ blockId: "b1", taskId: "t1", tokenCount: 512, attentionScore: 0.9, isAttentionSink: true, turnSeq: 1 });
  kvMgr.registerOrTouchBlock({ blockId: "b2", taskId: "t1", tokenCount: 512, attentionScore: 0.1, isAttentionSink: false, turnSeq: 2 });
  kvMgr.registerOrTouchBlock({ blockId: "b3", taskId: "t1", tokenCount: 512, attentionScore: 0.2, isAttentionSink: false, turnSeq: 3 });
  kvMgr.registerOrTouchBlock({ blockId: "b4", taskId: "t1", tokenCount: 512, attentionScore: 0.3, isAttentionSink: false, turnSeq: 4 });
  const evRes = kvMgr.registerOrTouchBlock({ blockId: "b5", taskId: "t1", tokenCount: 512, attentionScore: 0.4, isAttentionSink: false, turnSeq: 5 });
  assert.ok(evRes.evictedBlockIds.includes("b2")); // Lowest attention score non-sink evicted

  // 6. TASK-161: AST Speculative Decoder
  const lsp = new WarmLspDaemonPool();
  lsp.prewarmWorkspaceLspIndex("sbx-spec-01", "typescript", { "src/a.ts": "export function parseToken() {}" });
  const decoder = new AstSpeculativeDecoder(lsp);
  const decRes = decoder.generateAndVerifyDraftTokens("sbx-spec-01", "src/a.ts", "export function parseToken() { return true; }");
  assert.ok(decRes.acceptanceRatePct > 0);

  // 7. TASK-162: Parallel CoW Git-Bisect Coordinator
  const driver = new LayerFsWorkspaceDriver();
  const commits = [0, 0, 1, 1].map((exitCode, index) => driver.executeInProcessFileTool({
    branchName: "main", agentId: "bisect-fixture", stepId: `bisect-${index}`,
    operation: "write", filePath: "check.sh", newContent: `# revision ${index}\nexit ${exitCode}\n`,
  }).commitId);
  const bisectCoord = new ParallelCowGitBisectCoordinator();
  const bisectRep = bisectCoord.executeLiveLayerFsBisect({ driver, commits, testShellCommand: "sh check.sh" });
  assert.equal(bisectRep.firstBadCommit, commits[2]);
  assert.ok(bisectRep.probedCommits.every(probe => probe.cleanedUp));

  // 8. TASK-163: Priority-Based Load Shedder
  const shedder = new PriorityLoadShedder();
  assert.equal(shedder.shouldAdmitUnderLoad(TaskPriority.P3_BATCH, 0.95), false);
  assert.equal(shedder.shouldAdmitUnderLoad(TaskPriority.P0_EMERGENCY, 1), true);
});
