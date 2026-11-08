/**
 * @fileoverview M24 & M25 (TASK-113 ~ TASK-120) Integration Verification Suite:
 * - `PrefixCacheAffinityRouter`: Joint Compute-LLM KV-Cache affinity routing, TTFT reduction (>50%),
 *   FinOps 80% cached-token discount, and overload spillover protection.
 * - `HierarchicalContextCompactor`: 3-tier memory pyramid (L1 Hot Window, L2 Episodic Summary,
 *   L3 S3 Cold Archive) with on-demand byte/char Range pagination.
 * - `SandboxProvenanceAttestor`: SLSA v1.0 provenance signing, tamper detection, and tainted-sandbox
 *   merge-gate rejection.
 * - `OnlineContinuousProfiler`: Watermark-triggered non-disruptive CPU/Heap flame-graph capture (<1% overhead).
 */

import test from "node:test";
import assert from "node:assert/strict";
import {
  PrefixCacheAffinityRouter,
  HierarchicalContextCompactor,
  SandboxProvenanceAttestor,
  ZeroTrustSecurityGuard,
  OnlineContinuousProfiler,
} from "../src/index";

test("test 74: [TASK-113 & TASK-114] PrefixCacheAffinityRouter achieves KV-Cache hit, reduces TTFT by 65%, applies 80% FinOps discount, and spills over when overloaded", () => {
  const router = new PrefixCacheAffinityRouter();
  router.registerBackendNode({
    backendId: "deepseek-gpu-node-01",
    modelPool: "DeepSeek",
    baseTtftMs: 400,
    inFlightRequests: 10,
    maxConcurrency: 100,
    healthy: true,
  });
  router.registerBackendNode({
    backendId: "deepseek-gpu-node-02",
    modelPool: "DeepSeek",
    baseTtftMs: 420,
    inFlightRequests: 5,
    maxConcurrency: 100,
    healthy: true,
  });

  // 1. First request warms the prefix cache on `deepseek-gpu-node-01`
  const r1 = router.routeWithJointPrefixAffinity({
    repoId: "repo-ansts-core",
    staticSystemContext: "ANSTS v2.5 system architecture context...",
    prefixTokenCount: 64000,
    preferredPool: "DeepSeek",
  });
  assert.equal(r1.selectedBackendId, "deepseek-gpu-node-01");
  assert.equal(r1.kvCacheHit, false);

  // 2. Second sub-agent with identical repo context hits warm KV-Cache on `deepseek-gpu-node-01`
  const r2 = router.routeWithJointPrefixAffinity({
    repoId: "repo-ansts-core",
    staticSystemContext: "ANSTS v2.5 system architecture context...",
    prefixTokenCount: 64000,
    preferredPool: "DeepSeek",
  });
  assert.equal(r2.selectedBackendId, "deepseek-gpu-node-01");
  assert.equal(r2.kvCacheHit, true);
  assert.equal(r2.estimatedTtftMs, 140); // 65% reduction from 400ms
  assert.equal(r2.finopsDiscountRatio, 0.80);

  // 3. Simulate overload on `deepseek-gpu-node-01` (90/100 >= 85% threshold) -> spills over to `deepseek-gpu-node-02`
  router.backends.get("deepseek-gpu-node-01")!.inFlightRequests = 90;
  const r3 = router.routeWithJointPrefixAffinity({
    repoId: "repo-ansts-core",
    staticSystemContext: "ANSTS v2.5 system architecture context...",
    prefixTokenCount: 64000,
    preferredPool: "DeepSeek",
  });
  assert.equal(r3.selectedBackendId, "deepseek-gpu-node-02");
  assert.equal(r3.fallbackDueToOverload, true);
});

test("test 75: [TASK-115 & TASK-116] HierarchicalContextCompactor folds old turns into L2 Episodic Summaries and paginates L3 S3 cold logs via Range slices", () => {
  const compactor = new HierarchicalContextCompactor("task-long-horizon-01", 2);

  const hugeLog = "PASS suite_1\n".repeat(200) + "FAIL assertion_error_line_999";
  compactor.appendTurnAndCompactIfNeeded({
    turnSeq: 1,
    epoch: 1,
    reasoningText: "Running test suite",
    toolName: "bash_test",
    rawToolOutput: hugeLog,
    modifiedFiles: ["src/scheduler.ts"],
    errorSignature: "ERR_ASSERTION_LINE_999",
  });
  compactor.appendTurnAndCompactIfNeeded({
    turnSeq: 2,
    epoch: 1,
    reasoningText: "Inspecting failure",
    toolName: "grep",
    rawToolOutput: "matched line 999",
    modifiedFiles: [],
  });

  // Adding Turn #3 exceeds hotWindowTurns (2) -> Turn #1 is compacted to L2 + offloaded to L3 S3
  const compactRes = compactor.appendTurnAndCompactIfNeeded({
    turnSeq: 3,
    epoch: 1,
    reasoningText: "Applying fix",
    toolName: "edit_file",
    rawToolOutput: "patched src/scheduler.ts",
    modifiedFiles: ["src/scheduler.ts"],
  });

  assert.equal(compactRes.compacted, true);
  assert.equal(compactor.l1HotTurns.length, 2);
  assert.equal(compactor.l2EpisodicSummaries.length, 1);
  assert.deepEqual(compactor.l2EpisodicSummaries[0].errorSignatures, ["ERR_ASSERTION_LINE_999"]);
  assert.ok(compactor.l2EpisodicSummaries[0].savedBytes > 2000);

  // Paginate L3 cold archive via Range read
  const s3Uri = compactor.l2EpisodicSummaries[0].coldArchiveS3Uris[0];
  const page = compactor.readColdArchiveRange(s3Uri, hugeLog.length - 29, 29);
  assert.equal(page.found, true);
  assert.equal(page.slice, "FAIL assertion_error_line_999");
  assert.equal(page.hasMore, false);
});

test("test 76: [TASK-117 & TASK-118] SandboxProvenanceAttestor signs SLSA v1.0 provenance and rejects tampered or security-tainted sandbox patches", () => {
  const guard = new ZeroTrustSecurityGuard();
  const attestor = new SandboxProvenanceAttestor();
  const patchCode = "+export function verifiedFeature() { return true; }";

  // 1. Clean sandbox generates valid SLSA v1.0 provenance
  const attClean = attestor.generateBranchAttestation({
    sandboxId: "sbx-clean-01",
    branchId: "brn-clean-01",
    artifactName: "patch-01.diff",
    patchContent: patchCode,
    baseCheckpointMerkleRoot: "merkle_root_abc123",
    turnWalDiffSnapshots: ["turn1_wal", "turn2_wal"],
    securityGuard: guard,
  });

  const verifyOk = attestor.verifyAttestationForMergeGate(attClean, patchCode);
  assert.equal(verifyOk.verified, true);

  // 2. Modified patch content fails digest verification
  const verifyTampered = attestor.verifyAttestationForMergeGate(attClean, patchCode + "// backdoor");
  assert.equal(verifyTampered.verified, false);
  assert.equal(verifyTampered.reason, "ARTIFACT_DIGEST_MISMATCH");

  // 3. Sandbox that triggered an unauthorized egress or eBPF freeze is marked tainted & rejected by merge gate
  guard.validateEgressDomain("sbx-tainted-02", "unauthorized-c2.org");
  const attTainted = attestor.generateBranchAttestation({
    sandboxId: "sbx-tainted-02",
    branchId: "brn-tainted-02",
    artifactName: "patch-02.diff",
    patchContent: patchCode,
    baseCheckpointMerkleRoot: "merkle_root_abc123",
    turnWalDiffSnapshots: ["turn1_wal"],
    securityGuard: guard,
  });
  const verifyTainted = attestor.verifyAttestationForMergeGate(attTainted, patchCode);
  assert.equal(verifyTainted.verified, false);
  assert.ok(verifyTainted.reason?.startsWith("SANDBOX_TAINTED_BY_SECURITY_VIOLATION"));
});

test("test 77: [TASK-119 & TASK-120] OnlineContinuousProfiler auto-captures collapsed flame-graph stacks on 75% heap watermark with <1% overhead", () => {
  const profiler = new OnlineContinuousProfiler();

  // Normal metrics -> does not trigger
  const normal = profiler.evaluateAndCaptureIfTriggered({
    isolateId: "celld-iso-01",
    heapUsedMb: 52,
    heapLimitMb: 128,
    eventLoopLagMs: 8,
    turnExecutionMs: 240,
  });
  assert.equal(normal.triggered, false);

  // Heap crosses 75% (102MB / 128MB = 79.7%) -> automatically captures collapsed stack profile
  const triggered = profiler.evaluateAndCaptureIfTriggered({
    isolateId: "celld-iso-01",
    heapUsedMb: 102,
    heapLimitMb: 128,
    eventLoopLagMs: 64,
    turnExecutionMs: 310,
  });
  assert.equal(triggered.triggered, true);
  assert.ok(triggered.profile!.samplingOverheadPct < 1.0);
  assert.ok(triggered.profile!.collapsedStacksText.includes("TurnWalEngine.writeWalFrameIfNoneMatch"));
  assert.equal(triggered.profile!.hotspots[0].sampleCount, 42);
});

test("test 79: [M27 Full De-Simplification] Real Saga fs_write/fs_read/fs_delete tool execution, L1 paged pread restore, dynamic CloudEvents traceparent, Cgroup phase cpu.max files, and WarmPool zero-fill", () => {
  // 1. SagaToolGateway executes real fs_write_file, fs_read_file, and LIFO compensation fs_delete_file on disk
  const leaseCoord = new LeaseCoordinator();
  const wsDir = `/tmp/ansts_saga_ws_${Date.now()}`;
  const sagaGw = new SagaToolGateway(leaseCoord, wsDir);
  const f1 = leaseCoord.acquireOrIncrementEpoch("task-saga-real", "node-1", "DeepSeek");

  const writeRes = sagaGw.executeTool({
    fence: f1,
    toolCallId: "tc-write-1",
    toolName: "fs_write_file",
    effectClass: ToolEffectClass.SAGA_COMPENSATABLE,
    compensationTool: "fs_delete_file",
    payload: JSON.stringify({ filePath: "config.json", content: "{\"port\":8080}" }),
  });
  assert.equal(writeRes.status, "OK");
  assert.equal(fs.existsSync(`${wsDir}/config.json`), true);

  const f2 = f1.nextTurn();
  const readRes = sagaGw.executeTool({
    fence: f2,
    toolCallId: "tc-read-1",
    toolName: "fs_read_file",
    effectClass: ToolEffectClass.PURE_READ,
    payload: JSON.stringify({ filePath: "config.json" }),
  });
  assert.equal(readRes.result, "{\"port\":8080}");

  // Rollback and compensate -> real fs_delete_file removes config.json from disk
  const f3 = f2.nextTurn();
  sagaGw.rollbackAndCompensate(f3, 0);
  assert.equal(fs.existsSync(`${wsDir}/config.json`), false);

  // 2. WalCompactorAndPagedRestorer writes L1 snapshot to disk and performs positional pread
  const compactor = new WalCompactorAndPagedRestorer(`/tmp/ansts_l1_snap_${Date.now()}`);
  const restoreInfo = compactor.pagedRestore("task-l1-real", [
    {
      taskId: "task-l1-real",
      fence: new FenceToken(1, 1, "task-l1-real"),
      tokensUsed: 500,
      workspaceMerkleHash: "merkle_999",
      contextMessages: ["msg1", "msg2"],
    },
  ]);
  assert.equal(restoreInfo.hotPagesReady, true);
  assert.ok(restoreInfo.hotBytesRead > 0);

  // 3. CloudEventsBus generates unique cryptographic W3C traceparents per event
  const ceBus = new CloudEventsBus();
  const ev1 = ceBus.emit("io.ansts.test", "t1", "ten1", {});
  const ev2 = ceBus.emit("io.ansts.test", "t1", "ten1", {});
  assert.notEqual(ev1.traceparent, ev2.traceparent);

  // 4. CgroupPhaseController writes real cpu.max and memory.high files on disk
  const cgPhase = new CgroupPhaseController(32, 4, `/tmp/ansts_phase_cg_${Date.now()}`);
  const dummyTask = new AgentTask({ taskId: "task-cg-real", tenantId: "t1", burstCpu: 8.0, execMemMb: 2048 });
  const execLim = cgPhase.enterExecutionPhase(dummyTask);
  assert.equal(fs.existsSync(`${execLim.cgroupDirPath}/cpu.max`), true);
  assert.equal(fs.readFileSync(`${execLim.cgroupDirPath}/cpu.max`, "utf8").trim(), "800000 100000");

  // 5. WarmSandboxPoolManager allocates real scratch block image and zero-fills + unlinks on teardown
  const warmPool = new WarmSandboxPoolManager(2, `/tmp/ansts_warm_real_${Date.now()}`);
  const sbx = warmPool.acquireWarmSandbox("pool-py312-ds", "firecracker");
  assert.equal(fs.existsSync(sbx.scratchFilePath!), true);
  warmPool.teardownAndZeroBlocks(sbx);
  assert.equal(fs.existsSync(sbx.scratchFilePath!), false);
});

test("test 80: [M28 Multi-Instance Cluster HTTP Mesh] Live HTTP Gateway -> Remote Brain Shard -> Remote Body Daemon distributed dispatch & CoW sandbox execution", async () => {
  const baseDir = `/tmp/ansts_m28_cluster_${Date.now()}`;

  // 1. Start Body Daemon container node on ephemeral port
  const bodyNode = new MultiNodeClusterDaemonServer({
    nodeId: "body-host-01",
    role: "body",
    listenPort: 0,
    peerBrainUrls: {},
    peerBodyUrls: {},
    storageDir: `${baseDir}/body-01`,
  });
  const bodyPort = await bodyNode.start();
  const bodyUrl = `http://127.0.0.1:${bodyPort}`;

  // 2. Start 3 Brain nodes on ephemeral ports
  const brain1 = new MultiNodeClusterDaemonServer({
    nodeId: "brain-node-01",
    role: "brain",
    listenPort: 0,
    peerBrainUrls: {},
    peerBodyUrls: { "body-host-01": bodyUrl },
    storageDir: `${baseDir}/brain-01`,
  });
  const brain2 = new MultiNodeClusterDaemonServer({
    nodeId: "brain-node-02",
    role: "brain",
    listenPort: 0,
    peerBrainUrls: {},
    peerBodyUrls: { "body-host-01": bodyUrl },
    storageDir: `${baseDir}/brain-02`,
  });
  const brain3 = new MultiNodeClusterDaemonServer({
    nodeId: "brain-node-03",
    role: "brain",
    listenPort: 0,
    peerBrainUrls: {},
    peerBodyUrls: { "body-host-01": bodyUrl },
    storageDir: `${baseDir}/brain-03`,
  });

  const p1 = await brain1.start();
  const p2 = await brain2.start();
  const p3 = await brain3.start();

  const peerBrains = {
    "brain-node-01": `http://127.0.0.1:${p1}`,
    "brain-node-02": `http://127.0.0.1:${p2}`,
    "brain-node-03": `http://127.0.0.1:${p3}`,
  };

  // 3. Start Gateway node wired to all 3 Brain nodes and Body node
  const gateway = new MultiNodeClusterDaemonServer({
    nodeId: "celld-gw-01",
    role: "gateway",
    listenPort: 0,
    peerBrainUrls: peerBrains,
    peerBodyUrls: { "body-host-01": bodyUrl },
    storageDir: `${baseDir}/gw-01`,
  });
  const gwPort = await gateway.start();

  try {
    // Dispatch task via Gateway HTTP endpoint -> routes over HTTP to owning Brain node -> executes on Body node
    const res = await fetch(`http://127.0.0.1:${gwPort}/v1beta1/tasks/dispatch`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        requestId: "req-dist-80",
        tenantId: "tenant-distributed-mesh",
        taskId: "task-dist-80",
        repoId: "repo-ansts",
        promptText: "test multi-instance dispatch",
        targetBodyHostId: "body-host-01",
      }),
    });
    assert.equal(res.status, 200);
    const data = (await res.json()) as Record<string, any>;

    assert.equal(data.status, "EXECUTED");
    assert.equal(data.routedByNodeId, "celld-gw-01");
    assert.equal(data.hopMode, "ONE_HOP_CONTAINER_HTTP_RPC");
    assert.ok(["brain-node-01", "brain-node-02", "brain-node-03"].includes(data.executedByBrainNodeId));
    assert.equal(data.bodyExecution.bodyNodeId, "body-host-01");
    assert.equal(data.bodyExecution.mergedFilesCount, 1);
  } finally {
    await gateway.stop();
    await brain1.stop();
    await brain2.stop();
    await brain3.stop();
    await bodyNode.stop();
  }
});

test("test 81: [M29/M30 TanStack Control Plane & Live Cluster BFF/Metrics HTTP Verification] TanStack Shard Matrix, Canary Rollback, and Prometheus /metrics over live HTTP", async () => {
  const baseDir = `/tmp/ansts_m30_tanstack_${Date.now()}`;
  const gateway = new MultiNodeClusterDaemonServer({
    nodeId: "celld-gw-01",
    role: "gateway",
    listenPort: 0,
    peerBrainUrls: {},
    peerBodyUrls: {},
    storageDir: `${baseDir}/gw-01`,
  });
  const port = await gateway.start();

  try {
    // 1. Fetch 256-slot & Shard Matrix from BFF endpoint for TanStack Table
    const shardsRes = await fetch(`http://127.0.0.1:${port}/v1beta1/console/shards`);
    assert.equal(shardsRes.status, 200);
    const shardsJson = (await shardsRes.json()) as {
      totalVirtualSlots: number;
      shards: ShardMatrixRow[];
    };
    assert.equal(shardsJson.totalVirtualSlots, 256);
    assert.equal(shardsJson.shards.length, 3);

    // Filter shards using ConsoleDataTableEngine
    const tableEngine = new ConsoleDataTableEngine(shardsJson.shards, "shard-01");
    const filtered = tableEngine.getFilteredData((r, q) => r.shardId.includes(q));
    assert.equal(filtered.length, 1);
    assert.equal(filtered[0].shardId, "scheduler-shard-01");

    // 2. Trigger Canary Advance & Instant Rollback via BFF mutation endpoint
    const advRes = await fetch(`http://127.0.0.1:${port}/v1beta1/console/canary/action`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action: "advance", targetPhase: "PHASE_2_CANARY_8_SLOTS" }),
    });
    const advData = (await advRes.json()) as Record<string, any>;
    assert.equal(advData.phase, "PHASE_2_CANARY_8_SLOTS");

    const rbRes = await fetch(`http://127.0.0.1:${port}/v1beta1/console/canary/action`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action: "rollback" }),
    });
    const rbData = (await rbRes.json()) as Record<string, any>;
    assert.equal(rbData.currentPhase, "ROLLED_BACK_TO_STABLE");

    // 3. Scrape live Prometheus /metrics endpoint
    const metricsRes = await fetch(`http://127.0.0.1:${port}/metrics`);
    assert.equal(metricsRes.status, 200);
    const metricsText = await metricsRes.text();
    assert.ok(metricsText.includes("ansts_celld_isolate_heap_used_megabytes"));
    assert.ok(metricsText.includes("ansts_scheduler_shard_inflight_requests"));
  } finally {
    await gateway.stop();
  }
});

test("test 82: [M31 Full TanStack Console Prototype BFF Integration] TanStackConsoleBffClient verifies all 5 Prototype tabs (Overview, Fleet, Harness, Tunnels, TurnWAL Catch-up) and live mutations over HTTP", async () => {
  const baseDir = `/tmp/ansts_m31_bff_${Date.now()}`;
  const gateway = new MultiNodeClusterDaemonServer({
    nodeId: "celld-gw-01",
    role: "gateway",
    listenPort: 0,
    peerBrainUrls: {},
    peerBodyUrls: {},
    storageDir: `${baseDir}/gw-01`,
  });
  const port = await gateway.start();
  const client = new TanStackConsoleBffClient(`http://127.0.0.1:${port}`);

  try {
    // 1. Tab 1: Overview (KPIs + Dual-Homed Body Hosts Table)
    const overview = await client.fetchOverview("us-west-2");
    assert.equal(overview.clusterHealthy, true);
    assert.equal(overview.rpoZeroGuaranteed, true);
    assert.ok(Array.isArray(overview.bodyHostRows));
    assert.ok(overview.bodyHostRows.length >= 2);
    assert.equal(overview.bodyHostRows[0].hostId, "body-host-edge-01");

    // 2. Tab 2: Body Fleet (Cgroups v2 8.0C allocator + Preflight Gate checks)
    const fleet = await client.fetchFleet();
    assert.ok(fleet.hostAllocator.cpuMaxBurstUs.includes("800,000"));
    assert.equal(fleet.preflightChecks.length, 4);
    assert.equal(fleet.preflightChecks.every((c: any) => c.passed), true);

    // 3. Tab 3: Agent Harness & 256-Slot Shards
    const harness = await client.fetchHarnessAndShards();
    assert.equal(harness.totalVirtualSlots, 256);
    assert.equal(harness.shards.length, 3);
    assert.ok(harness.activeHarnessCells.length >= 2);

    // 4. Tab 4: mTLS Dual-Homed Tunnels & Make-Before-Break Drain on `body-host-edge-01`
    const tunnelsBefore = await client.fetchTunnels();
    assert.ok(tunnelsBefore.gateways.length >= 3);

    const drainRes = await client.mutateDrainGatewayOrHost("body-host-edge-01", 15000);
    assert.equal(drainRes.drainingGatewayId, "celld-gw-01");
    assert.ok(drainRes.s3CasGeneration.startsWith("v1-gen-"));

    // 5. Tab 5: Real Disk-Backed TurnWAL Catch-up Replay (`turnSeq > 14` -> returns frames #15, #16, #17)
    const walReplay = await client.mutateWalCatchupReplay({
      clientId: "console-operator-client",
      taskId: "task-prod-101",
      lastReceivedTurnSeq: 14,
    });
    assert.equal(walReplay.replayedWalFrames.length, 3);
    assert.equal(walReplay.replayedWalFrames[0].fence.turnSeq, 15);
    assert.equal(walReplay.replayedWalFrames[2].fence.turnSeq, 17);
  } finally {
    await gateway.stop();
  }
});
