/**
 * M12 & M13 Production Readiness & Governance Test Suite (TASK-57 ~ TASK-64, Non-K8s Manual Scale-Out Edition)
 */

import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { test } from "node:test";
import * as yaml from "js-yaml";

import {
  AgentTask,
  DashboardBackendServer,
  FenceToken,
  FinOpsBudgetController,
  LeaseCoordinator,
  ManualFleetScaleController,
  McpDynamicToolRegistry,
  MultiRegionFederationRouter,
  OtelAndPrometheusExporter,
  RpoZeroDurabilityEngine,
  runCli,
  SagaToolGateway,
  StaleFenceError,
  TaskState,
  ToolEffectClass,
} from "../src/index";

test("M12 (TASK-57 & TASK-58): Non-K8s Bare-Metal ManualFleetScaleController, scale-fleet.sh, and RustFS 16-Drive EC:4 HA Cluster manifests", () => {
  const rootDir = path.join(__dirname, "..", "..");

  // 1. Verify Non-K8s Manual Fleet Topology YAML, Systemd service & scale-fleet.sh script
  const topoPath = path.join(rootDir, "deploy", "manual-fleet", "fleet-topology.yaml");
  const scaleScriptPath = path.join(rootDir, "deploy", "manual-fleet", "scale-fleet.sh");
  const daemonServicePath = path.join(rootDir, "deploy", "manual-fleet", "systemd", "ansts-node-daemon.service");

  const topo = yaml.load(fs.readFileSync(topoPath, "utf8")) as Record<string, any>;
  assert.equal(topo.kind, "ManualFleetTopology");
  assert.equal(topo.metadata.deployment_mode, "systemd-and-compose");
  assert.equal(topo.spec.celld_brain_nodes.nodes.length, 3);
  assert.equal(topo.spec.sandbox_body_hosts.hosts.length, 2);

  const scriptText = fs.readFileSync(scaleScriptPath, "utf8");
  assert.ok(scriptText.includes("add-brain-node"));
  assert.ok(scriptText.includes("remove-brain-node"));
  assert.ok(scriptText.includes("add-body-host"));
  assert.ok(scriptText.includes("scale-shards"));
  assert.equal(fs.existsSync(daemonServicePath), true);

  // 2. Verify ManualFleetScaleController blocks manual scale-out while restoring > 0, and scales out cleanly when restoring == 0
  const manualCtrl = new ManualFleetScaleController();
  manualCtrl.fleetController.registerOrUpdateNodeLoad({
    node_id: "celld-node-01",
    sampled_ms: Date.now(),
    owned_cells: 500,
    placement_weight: 100,
    resident_cells: 100,
    host_websockets: 10,
    rss_bytes: 256_000_000,
    in_use_bytes: 128_000_000,
    cpu_percent_x100: 3000,
    open_fds: 64,
    pressured: false,
    memory_headroom: true,
    shed_cells: 0,
    restoring: 3, // Active paged restore in progress -> must block manual topology change!
    draining: false,
  });

  const blockedScale = manualCtrl.executeManualScaleOut({
    addBrainNodes: [{ nodeId: "celld-node-02", advertiseAddress: "10.240.16.12:9001" }],
  });
  assert.equal(blockedScale.status, "BLOCKED_BY_RESTORING");

  // Once restoring finishes (restoring = 0), manual scale-out adds brain nodes, body hosts & scheduler shards and triggers 32-cell rebalance
  manualCtrl.fleetController.nodes.get("celld-node-01")!.restoring = 0;
  const okScale = manualCtrl.executeManualScaleOut(
    {
      addBrainNodes: [
        { nodeId: "celld-node-02", advertiseAddress: "10.240.16.12:9001", placementWeight: 100 },
        { nodeId: "celld-node-03", advertiseAddress: "10.240.16.13:9001", placementWeight: 100 },
      ],
      addBodyHosts: [
        { hostId: "body-host-03", ipAddress: "10.240.32.13", maxBurstSandboxes: 20 },
      ],
      addSchedulerShards: 4,
    },
    40,
  );
  assert.equal(okScale.status, "SCALED_OUT");
  assert.equal(okScale.totalBrainNodes, 3);
  assert.equal(okScale.totalBodyHosts, 3);
  assert.equal(okScale.totalSchedulerShards, 12);
  assert.equal(okScale.rebalanceBatch?.movedCells, 32);

  // Test manual scale-in (`preserve` vs `drain`) and `anstsctl fleet scale-out` CLI
  const preserveRes = manualCtrl.executeManualNodeDrain("celld-node-03", "preserve");
  assert.equal(preserveRes.allowed, true);
  const drainRes = manualCtrl.executeManualNodeDrain("celld-node-02", "drain");
  assert.equal(drainRes.allowed, true);
  assert.equal(drainRes.remainingBrainNodes, 2);

  const cliScaleOut = runCli(["fleet", "scale-out", "celld-node-05", "body-host-05"]);
  assert.equal(cliScaleOut.status, "SCALED_OUT");
  assert.ok(cliScaleOut.addedBrainNodeIds.includes("celld-node-05"));

  // 3. Verify RustFS 4-Node x 4-NVMe (16-Drive EC:4) HA Compose & setup script
  const rustfsHaPath = path.join(rootDir, "deploy", "rustfs-celld", "docker-compose.rustfs-ha.yml");
  const rustfsHa = yaml.load(fs.readFileSync(rustfsHaPath, "utf8")) as Record<string, any>;
  assert.equal(rustfsHa.services["rustfs-node-1"].environment.RUSTFS_ERASURE_SET_DRIVE_COUNT, "16");
  assert.equal(rustfsHa.services["rustfs-node-1"].environment.RUSTFS_STORAGE_CLASS_STANDARD, "EC:4");
  assert.ok(rustfsHa.services["rustfs-node-4"]);
});

test("M12 (TASK-59 & TASK-60): OTLP/Prometheus Exporter, Grafana Dashboard & Multi-Node Split-Brain Chaos Harness", () => {
  const rootDir = path.join(__dirname, "..", "..");
  const grafanaJson = JSON.parse(
    fs.readFileSync(path.join(rootDir, "deploy", "observability", "grafana-dashboard.json"), "utf8"),
  );
  assert.equal(grafanaJson.uid, "ansts-v2-celld-ha");
  assert.ok(grafanaJson.panels.length >= 4);

  // 1. Verify OtelAndPrometheusExporter renders standard OpenMetrics / Prometheus text
  const exporter = new OtelAndPrometheusExporter();
  exporter.recordOtelSpan({
    traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
    spanId: "00f067aa0ba902b7",
    name: "AgentTaskHarnessCell.turn",
    durationMs: 14.2,
    attributes: { "ansts.task_id": "tsk-01", "ansts.model": "DeepSeek" },
  });
  exporter.incrementRpoZeroCommit("fleet");
  exporter.incrementRpoZeroCommit("bucket");
  exporter.recordEpochGcDeletion(3);

  const promText = exporter.renderPrometheusMetrics({
    tenantDominantShares: { "fin-enterprise": 0.42, "codegen-pro": 0.28 },
    schedulerShards: [{ shardId: "scheduler-cell-shard-00", inFlightRequests: 12, ewmaLatencyMs: 1.8 }],
    activeQueueShards: 16,
    modelPools: {
      DeepSeek: { state: "CLOSED", ttftSec: 0.45 },
      "GLM-5.3": { state: "CLOSED", ttftSec: 0.18 },
      "Qwen-3.8": { state: "CLOSED", ttftSec: 0.12 },
    },
  });
  assert.ok(promText.includes('ansts_tenant_dominant_share_ratio{tenant_id="fin-enterprise"} 0.42'));
  assert.ok(promText.includes('ansts_celld_rpo_zero_commits_total{mode="fleet"} 1'));
  assert.ok(promText.includes("ansts_celld_epoch_gc_deleted_epochs_total 3"));

  // 2. TASK-60: Multi-Node Asymmetric Partition + Stale Leader GC Pause + Concurrent Takeover Chaos Test
  const fleetEngine = new RpoZeroDurabilityEngine("fleet", 10000);
  const cellScope = "chaos-cell-99";
  const t0 = 1_700_000_000_000;

  // Node A claims ownership at epoch 1 and commits turn 1 with peer fsync proof
  const ownA = fleetEngine.claimCellOwnershipCas(cellScope, "celld-node-A", t0);
  assert.equal(ownA.epoch, 1);
  const commit1 = fleetEngine.commitAndProveRpoZero({
    cellScope,
    nodeId: "celld-node-A",
    fence: new FenceToken(1, 1, cellScope),
    ltxPayload: "TURN_1_COMMITTED_DATA",
    peerFollowerIds: ["celld-node-B", "celld-node-C"],
  });
  assert.equal(commit1.acknowledged, true);

  // Inject GC pause + network partition on Node A beyond lease TTL (t0 + 12000ms)
  // Node B & Node C race simultaneously to take over `chaos-cell-99`:
  const ownB = fleetEngine.claimCellOwnershipCas(cellScope, "celld-node-B", t0 + 12000);
  assert.equal(ownB.epoch, 2);

  // Node C attempts concurrent takeover 50ms later while Node B holds valid lease -> rejected!
  assert.throws(
    () => fleetEngine.claimCellOwnershipCas(cellScope, "celld-node-C", t0 + 12050),
    (err: any) => err instanceof StaleFenceError && err.message.includes("CAS Lease Held"),
  );

  // Partitioned stale leader Node A wakes from GC pause and tries to commit turn 2 at epoch 1 -> SessionSealed!
  assert.throws(
    () =>
      fleetEngine.commitAndProveRpoZero({
        cellScope,
        nodeId: "celld-node-A",
        fence: new FenceToken(1, 2, cellScope),
        ltxPayload: "ZOMBIE_TURN_2_DATA",
        peerFollowerIds: ["celld-node-C"],
      }),
    (err: any) => err instanceof StaleFenceError && err.message.includes("SessionSealed"),
  );

  // Verify Turn 1 RPO=0 data remains 100% intact in bucket & peer fsync ledger
  assert.equal(fleetEngine.bucketObjects.get("cells/chaos-cell-99/ep_0001/000001.ltx"), "TURN_1_COMMITTED_DATA");
});

test("M13 (TASK-61 & TASK-62): MCP Dynamic Tool Registry Safety Downgrade & FinOps 80%/100% Budget Circuit Breaker", () => {
  const leaseCoord = new LeaseCoordinator();
  const sagaGw = new SagaToolGateway(leaseCoord);
  const mcpRegistry = new McpDynamicToolRegistry(sagaGw);

  // 1. Register 3 MCP tools: read-only, compensatable, and an uncompensated mutating tool
  const bound = mcpRegistry.registerMcpServerTools([
    {
      serverName: "github-mcp",
      name: "get_pull_request",
      description: "Reads PR metadata",
      inputSchema: { type: "object", required: ["pr_number"], properties: { pr_number: { type: "number" } } },
      annotations: { readOnlyHint: true },
    },
    {
      serverName: "k8s-mcp",
      name: "scale_deployment",
      description: "Scales deployment replicas",
      inputSchema: { type: "object", required: ["deployment"], properties: { deployment: { type: "string" } } },
      annotations: { compensationTool: "k8s-mcp/restore_replicas" },
    },
    {
      serverName: "stripe-mcp",
      name: "issue_wire_payout",
      description: "Mutating payout without declared inverse compensation tool",
      inputSchema: { type: "object", required: ["amount_usd"], properties: { amount_usd: { type: "number" } } },
      annotations: {}, // Missing compensationTool -> MUST auto-downgrade to STAGED_COMMIT!
    },
  ]);

  assert.equal(bound[0].assignedEffectClass, ToolEffectClass.PURE_READ);
  assert.equal(bound[1].assignedEffectClass, ToolEffectClass.SAGA_COMPENSATABLE);
  assert.equal(bound[2].assignedEffectClass, ToolEffectClass.STAGED_COMMIT);
  assert.equal(bound[2].downgradedToStagedCommit, true);

  const taskId = "tsk-mcp-finops-01";
  leaseCoord.acquireOrIncrementEpoch(taskId, "node-01", "DeepSeek");

  // Invoke uncompensated mutating tool -> automatically held in STAGED_COMMIT buffer
  const mcpExec = mcpRegistry.invokeMcpTool({
    qualifiedName: "stripe-mcp/issue_wire_payout",
    toolCallId: "call-payout-01",
    fence: new FenceToken(1, 1, taskId),
    args: { amount_usd: 5000 },
  });
  assert.equal(mcpExec.gatewayResult.status, "STAGED");

  // 2. TASK-62: FinOps Budget Controller (80% soft warning -> 100% hard-cap GRACEFUL_DRAINING)
  const finops = new FinOpsBudgetController();
  finops.registerTenantBudget("startup-tenant", 0.10, 0.80); // $0.10 daily cap
  const task = new AgentTask({ taskId, tenantId: "startup-tenant", state: TaskState.RUNNING });

  // Turn 1 spends ~$0.088 (>80% of $0.10) -> triggers SOFT_WARNING_80
  const spend1 = finops.recordTurnSpend({
    task,
    pool: "DeepSeek",
    inputTokens: 0,
    reasoningTokens: 20_000, // 20 * $0.00219 = $0.0438
    outputTokens: 20_000, // 20 * $0.00219 = $0.0438 -> total $0.0876
    activeCpuCoreSeconds: 10,
  });
  assert.equal(spend1.status, "SOFT_WARNING_80");
  assert.equal(task.state, TaskState.RUNNING);

  // Turn 2 exceeds $0.10 cap -> triggers HARD_CAP_DRAINING_100 and transitions task to GRACEFUL_DRAINING
  const spend2 = finops.recordTurnSpend({
    task,
    pool: "DeepSeek",
    inputTokens: 10_000,
    reasoningTokens: 10_000,
    outputTokens: 5_000,
    activeCpuCoreSeconds: 10,
  });
  assert.equal(spend2.status, "HARD_CAP_DRAINING_100");
  assert.equal(task.state, TaskState.GRACEFUL_DRAINING);
  assert.equal(finops.canAdmitTenantTask("startup-tenant").allowed, false);
});

test("M13 (TASK-63 & TASK-64): Dashboard Shard Heatmap, Online STAGED_COMMIT Approval & Multi-Region Federation Failover", () => {
  // 1. TASK-63: Dashboard Shard Heatmap & STAGED_COMMIT approval
  const dashboard = new DashboardBackendServer();
  const heatmap = dashboard.handleGetShardHeatmap();
  assert.equal(heatmap.totalShards, 8);

  const approvalRes = dashboard.handleStageCommitDecision({
    taskId: "tsk-dash-approve-01",
    epoch: 1,
    turnSeq: 1,
    toolCallId: "call-prod-db-01",
    toolName: "prod_db_drop_index",
    payload: "index=idx_legacy",
    approved: true,
  });
  assert.equal(approvalRes.decision, "APPROVED_AND_FLUSHED");
  assert.equal(approvalRes.flushedCount, 1);

  // 2. TASK-64: Multi-Region Active-Active Federation Router
  const fedRouter = new MultiRegionFederationRouter();

  // Strict GDPR task MUST route to `eu-central` even if `us-west` is preferred
  const gdprRoute = fedRouter.routeGlobalTask({
    preferredRegion: "us-west",
    requiredComplianceZone: "GDPR",
  });
  assert.equal(gdprRoute.selectedRegion, "eu-central");

  // Simulate `us-west` regional outage -> global task preferring `us-west` automatically fails over to `ap-east`
  fedRouter.updateRegionHealth("us-west", { healthy: false });
  const foRoute = fedRouter.routeGlobalTask({
    preferredRegion: "us-west",
    requiredComplianceZone: "GLOBAL",
  });
  assert.equal(foRoute.failoverTriggered, true);
  assert.equal(foRoute.selectedRegion, "ap-east");
  assert.ok(foRoute.failoverReason?.includes("failed over to 'ap-east' in <1s"));
});
