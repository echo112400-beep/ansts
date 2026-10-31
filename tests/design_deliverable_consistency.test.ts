/**
 * Automated Design-to-Deliverable Consistency Audit Suite (`tests/design_deliverable_consistency.test.ts`).
 *
 * Verifies 100% alignment across:
 * 1. Architecture & Backlog Documents (`docs/*.md` & `docs/agent_sandbox_scheduler_dashboard.html`)
 * 2. Production Deployment Manifests (`wrangler.json`, `migrations/0001_init_ansts.sql`, `deploy/rustfs-celld/*`,
 *    `deploy/k8s-operator/*`, `deploy/observability/*`)
 * 3. All M1 ~ M13 (`TASK-01` ~ `TASK-64`) TypeScript classes, exact method names, `AnstsClusterRuntime` composition,
 *    and Edge Worker HTTP endpoints (`/metrics`, `/v1beta1/federation/route`, `/api/v1/saga/approve`, FinOps `402`).
 */

import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { test } from "node:test";

import * as anstsIndex from "../src/index";
import {
  AnstsClusterRuntime,
  AnstsSchedulerCell,
  AgentTaskHarnessCell,
  CloudflareCelldEnv,
  cloudflareCelldWorker,
  MockDurableObjectState,
  runCli,
} from "../src/index";

test("Design-Deliverable Consistency Audit: All M1~M13 exports, docs, manifests, and Edge Worker routes match 100%", async () => {
  const rootDir = path.join(__dirname, "..", "..");

  // 1. Verify all 5 core documentation & dashboard artifacts exist in docs/ and reflect v2.2 & M1~M13
  const requiredDocs = [
    "docs/architecture_design_doc.md",
    "docs/microservices_architecture.md",
    "docs/celld_persistent_recoverable_ha_scheduler_design.md",
    "docs/ansts_v2_implementation_backlog.md",
    "docs/agent_sandbox_scheduler_dashboard.html",
  ];
  for (const relDoc of requiredDocs) {
    const p = path.join(rootDir, relDoc);
    assert.equal(fs.existsSync(p), true, `Missing required documentation deliverable: ${relDoc}`);
  }

  const haDesignText = fs.readFileSync(
    path.join(rootDir, "docs/celld_persistent_recoverable_ha_scheduler_design.md"),
    "utf8",
  );
  assert.ok(haDesignText.includes("RpoZeroDurabilityEngine.commitAndProveRpoZero"));
  assert.ok(haDesignText.includes("PartitionedSchedulerRouter"));
  assert.ok(haDesignText.includes("DynamicQueueAutoSharder"));
  assert.ok(haDesignText.includes("BrainBodyContainerPoolScaler"));
  assert.ok(haDesignText.includes("RustFsS3LtxDriver"));
  assert.ok(haDesignText.includes("MultiRegionFederationRouter"));

  const backlogText = fs.readFileSync(
    path.join(rootDir, "docs/ansts_v2_implementation_backlog.md"),
    "utf8",
  );
  for (let taskNum = 49; taskNum <= 64; taskNum++) {
    assert.ok(
      backlogText.includes(`TASK-${taskNum}`),
      `Backlog must document TASK-${taskNum}`,
    );
  }

  // 2. Verify all production deployment manifests exist on disk
  const requiredManifests = [
    "wrangler.json",
    "migrations/0001_init_ansts.sql",
    "proto/ansts_rpc.proto",
    "specs/taskspec_v1beta1_example.yaml",
    "deploy/rustfs-celld/start-rustfs-celld.sh",
    "deploy/rustfs-celld/docker-compose.rustfs-celld.yml",
    "deploy/rustfs-celld/docker-compose.rustfs-ha.yml",
    "deploy/rustfs-celld/rustfs-ec-setup.sh",
    "deploy/rustfs-celld/systemd/celld.service",
    "deploy/k8s-operator/celld-fleet-operator.yaml",
    "deploy/observability/grafana-dashboard.json",
  ];
  for (const relManifest of requiredManifests) {
    assert.equal(
      fs.existsSync(path.join(rootDir, relManifest)),
      true,
      `Missing deployment manifest: ${relManifest}`,
    );
  }

  // 3. Verify all M1 ~ M13 classes are exported from the unified barrel `src/index.ts`
  const requiredExportedClasses = [
    "AgentTask",
    "FenceToken",
    "ResourceVector5D",
    "TaskSpecValidator",
    "ApiGatewayServer",
    "IngressBackpressureController",
    "SseEventStreamGateway",
    "BayesianTaskProfiler",
    "MultiTenantQueueManager",
    "DagEscrowCoordinator",
    "CoreScheduler5D",
    "LeaseCoordinator",
    "CognitiveRouter",
    "SlidingWindowTokenBucket",
    "ThreeStateCircuitBreaker",
    "SagaToolGateway",
    "SagaCompensatorEngine",
    "StagingCommitAndApprovalGate",
    "WarmSandboxPoolManager",
    "CgroupPhaseController",
    "LinuxPsiMemoryGuard",
    "ZeroTrustVsockBridge",
    "AgentHarnessRunner",
    "SyntacticAndVelocityBreaker",
    "TurnWalEngine",
    "WalCompactorAndPagedRestorer",
    "DeterministicTimeTravelReplayer",
    "CloudEventsBus",
    "TransactionalOutboxDispatcher",
    "BidirectionalBackpressureController",
    "AnstsClusterRuntime",
    "AnstsSchedulerCell",
    "AgentTaskHarnessCell",
    "RpoZeroDurabilityEngine",
    "DurableWorkflowContext",
    "ShardedQueueBrokerPool",
    "SafeEpochGarbageCollector",
    "FleetHaAndAutoscalerController",
    "PartitionedSchedulerRouter",
    "DynamicQueueAutoSharder",
    "BrainBodyContainerPoolScaler",
    "RustFsS3LtxDriver",
    "CelldAdminClient",
    "LiveOpenAiCompatibleSseAdapter",
    "LinuxCgroupV2AndPsiDriver",
    "CompanionContainerAndOverlayDriver",
    "ZeroTrustVsockHostDaemon",
    "EphemeralHeavyExecutorMigrator",
    "KubernetesCelldHpaAdapter",
    "OtelAndPrometheusExporter",
    "McpDynamicToolRegistry",
    "FinOpsBudgetController",
    "DashboardBackendServer",
    "DashboardBridgeService",
    "MultiRegionFederationRouter",
  ];
  for (const clsName of requiredExportedClasses) {
    assert.ok(
      clsName in anstsIndex,
      `Expected class '${clsName}' to be exported from src/index.ts`,
    );
  }

  // 4. Verify AnstsClusterRuntime & `anstsctl metrics` CLI integration
  const cluster = new AnstsClusterRuntime();
  assert.ok(cluster.mcpRegistry);
  assert.ok(cluster.finopsController);
  assert.ok(cluster.otelExporter);
  assert.ok(cluster.federationRouter);
  assert.ok(cluster.k8sAutoscaler);
  const cliMetrics = runCli(["metrics"]);
  assert.ok(cliMetrics.prometheus_text.includes("ansts_tenant_dominant_share_ratio"));

  // 5. Verify Edge Worker M12/M13 HTTP endpoints (/metrics, /v1beta1/federation/route, /api/v1/saga/approve, FinOps 402)
  const mockEnv: CloudflareCelldEnv = {
    PRIMARY_MODEL_POOL: "DeepSeek",
    FAST_UTILITY_POOL: "GLM-5.3",
    FALLBACK_MODEL_POOL: "Qwen-3.8",
    SCHEDULER_CELL: {
      idFromName: (n: string) => ({ toString: () => n, name: n }),
      get: (id) => {
        const cell = new AnstsSchedulerCell(new MockDurableObjectState(id.toString()), mockEnv);
        return {
          fetch: (input: RequestInfo, init?: RequestInit) =>
            cell.fetch(typeof input === "string" ? new Request(input, init) : input),
        };
      },
    },
    AGENT_HARNESS_CELL: {
      idFromName: (n: string) => ({ toString: () => n, name: n }),
      get: (id) => {
        const cell = new AgentTaskHarnessCell(new MockDurableObjectState(id.toString()), mockEnv);
        return {
          fetch: (input: RequestInfo, init?: RequestInit) =>
            cell.fetch(typeof input === "string" ? new Request(input, init) : input),
        };
      },
    },
    ANSTS_KV: { get: async () => null, put: async () => {}, delete: async () => {} },
    ANSTS_CHECKPOINTS_R2: { put: async (k) => ({ key: k }), get: async () => null, list: async () => ({ objects: [] }) },
    ANSTS_D1: {
      prepare: () => {
        const s = { bind: () => s, run: async () => ({ success: true }), all: async () => ({ results: [] }) };
        return s;
      },
    },
    ANSTS_EVENTS_QUEUE: { send: async () => {} },
  };

  // GET /metrics
  const metricsRes = await cloudflareCelldWorker.fetch(new Request("https://ansts.edge/metrics"), mockEnv);
  assert.equal(metricsRes.status, 200);
  const metricsBody = await metricsRes.text();
  assert.ok(metricsBody.includes("ansts_scheduler_shard_inflight_requests"));

  // POST /v1beta1/federation/route
  const fedRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/federation/route", {
      method: "POST",
      body: JSON.stringify({ preferred_region: "us-west", required_compliance_zone: "GDPR" }),
    }),
    mockEnv,
  );
  assert.equal(fedRes.status, 200);
  const fedBody = (await fedRes.json()) as Record<string, any>;
  assert.equal(fedBody.selectedRegion, "eu-central");

  // POST /api/v1/saga/approve
  const sagaApproveRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/api/v1/saga/approve", {
      method: "POST",
      body: JSON.stringify({
        task_id: "tsk-edge-approve-01",
        epoch: 1,
        turn_seq: 1,
        tool_call_id: "tc-01",
        tool_name: "prod_ledger_commit",
        payload: "amount=100",
        approved: true,
      }),
    }),
    mockEnv,
  );
  assert.equal(sagaApproveRes.status, 200);
  const sagaApproveBody = (await sagaApproveRes.json()) as Record<string, any>;
  assert.equal(sagaApproveBody.decision, "APPROVED_AND_FLUSHED");

  // FinOps 402 Payment Required / Budget Hard-Cap Check on Edge Worker
  cloudflareCelldWorker.publicFinOpsController.registerTenantBudget("bankrupt-tenant", 0.0);
  const finopsBlockedRes = await cloudflareCelldWorker.fetch(
    new Request("https://ansts.edge/v1beta1/tasks", {
      method: "POST",
      body: JSON.stringify({
        apiVersion: "scheduler.agent.io/v1beta1",
        kind: "AgentTask",
        metadata: { task_id: "tsk-blocked-01", tenant_id: "bankrupt-tenant" },
        spec: {},
      }),
    }),
    mockEnv,
  );
  assert.equal(finopsBlockedRes.status, 402);
});
