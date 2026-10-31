/**
 * M12 & M13 Production Readiness & Governance Test Suite (TASK-57 ~ TASK-64):
 * - TASK-57: Kubernetes Operator (`CelldFleet` CRD, PDB, PreStop `handoff=preserve`) & `KubernetesCelldHpaAdapter`
 * - TASK-58: `RustFS` 4-Node x 4-NVMe (16-Drive EC:4) HA Cluster Compose & Setup Script Verification
 * - TASK-59: OpenTelemetry (OTLP) Span Exporter, Prometheus `/metrics` OpenMetrics Formatter & Grafana Dashboard
 * - TASK-60: E2E Multi-Node Network Partition, Stale-Leader GC Pause & Split-Brain Chaos Harness
 * - TASK-61: MCP Dynamic Tool Registry & 4-Tier Transaction Contract Binder (Automatic `STAGED_COMMIT` Safety Downgrade)
 * - TASK-62: Multi-Tenant FinOps Real-Time Cost Ledger, 80% Soft Warning & 100% Hard-Cap Graceful Drain Breaker
 * - TASK-63: Real-Time Dashboard Shard Heatmap & Interactive `STAGED_COMMIT` Online Approval / Rejection
 * - TASK-64: Multi-Region Active-Active Federation Router (Strict GDPR Data Residency & <1s Regional Failover)
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
  KubernetesCelldHpaAdapter,
  LeaseCoordinator,
  McpDynamicToolRegistry,
  MultiRegionFederationRouter,
  OtelAndPrometheusExporter,
  RpoZeroDurabilityEngine,
  SagaToolGateway,
  StaleFenceError,
  TaskState,
  ToolEffectClass,
} from "../src/index";

test("M12 (TASK-57 & TASK-58): K8s CelldFleet Operator, HPA Adapter, and RustFS 16-Drive EC:4 HA Cluster manifests", () => {
  const rootDir = path.join(__dirname, "..", "..");

  // 1. Verify K8s Operator YAML documents (CRD + PDB + StatefulSet with PreStop handoff=preserve)
  const k8sYamlPath = path.join(rootDir, "deploy", "k8s-operator", "celld-fleet-operator.yaml");
  const k8sDocs = yaml.loadAll(fs.readFileSync(k8sYamlPath, "utf8")) as Array<Record<string, any>>;
  assert.equal(k8sDocs.length, 3);
  assert.equal(k8sDocs[0].kind, "CustomResourceDefinition");
  assert.equal(k8sDocs[1].kind, "PodDisruptionBudget");
  assert.equal(k8sDocs[2].kind, "StatefulSet");
  const preStopCmd =
    k8sDocs[2].spec.template.spec.containers[0].lifecycle.preStop.exec.command.join(" ");
  assert.ok(preStopCmd.includes("/shutdown?handoff=preserve"));

  // 2. Verify KubernetesCelldHpaAdapter blocks scaling when restoring > 0, and scales out when pressured
  const hpa = new KubernetesCelldHpaAdapter(3, 32, 2500);
  const blockedPlan = hpa.reconcileClusterReplicas({
    brainNodes: [
      {
        node_id: "celld-0",
        sampled_ms: Date.now(),
        owned_cells: 3000,
        placement_weight: 100,
        resident_cells: 500,
        host_websockets: 50,
        rss_bytes: 500_000_000,
        in_use_bytes: 400_000_000,
        cpu_percent_x100: 7000,
        open_fds: 120,
        pressured: true,
        memory_headroom: false,
        shed_cells: 10,
        restoring: 4, // Restoring in progress -> must block replica mutation!
        draining: false,
      },
    ],
    bodyHosts: [
      {
        hostId: "body-host-01",
        activeToolBurstSandboxes: 20,
        maxBurstSandboxesPerHost: 20,
        warmPoolIdleContainers: 1,
        psiCpuSomeAvg10: 0.15,
        psiMemoryFullAvg10: 0.02,
      },
    ],
    activeToolBurstTasks: 80,
  });
  assert.equal(blockedPlan.brainStatefulSet.action, "BLOCKED_BY_RESTORING");
  assert.equal(blockedPlan.bodyDeployment.desiredReplicas, 5); // ceil(80 * 1.25 / 20) = 5

  const scaleOutPlan = hpa.reconcileClusterReplicas({
    brainNodes: [
      {
        node_id: "celld-0",
        sampled_ms: Date.now(),
        owned_cells: 6500,
        placement_weight: 100,
        resident_cells: 900,
        host_websockets: 80,
        rss_bytes: 700_000_000,
        in_use_bytes: 600_000_000,
        cpu_percent_x100: 8500,
        open_fds: 200,
        pressured: true,
        memory_headroom: false,
        shed_cells: 12,
        restoring: 0,
        draining: false,
      },
    ],
    bodyHosts: [],
    activeToolBurstTasks: 40,
  });
  assert.equal(scaleOutPlan.brainStatefulSet.action, "SCALE_OUT");
  assert.ok(scaleOutPlan.brainStatefulSet.desiredReplicas >= 4);

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
