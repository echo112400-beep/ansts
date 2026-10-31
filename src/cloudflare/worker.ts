/**
 * Unified Cloudflare Workers & `denoland/celld` Entrypoint (`src/cloudflare/worker.ts`).
 * Directly deployable via:
 *   - `celld dev` / `celld deploy . --bucket s3://my-celld-fleet`
 *   - `npx wrangler dev` / `npx wrangler deploy`
 *
 * Composes all M1 ~ M13 hot-path capabilities:
 * - TenantLedgerCell admission, idempotency and budget authority
 * - Two-Tier Partitioned Scheduler Management (`PartitionedSchedulerRouter`)
 * - Dynamic Queue Auto-Sharding (`DynamicQueueAutoSharder` + `ShardedQueueBrokerPool`)
 * - Multi-Region Active-Active Federation Routing (`MultiRegionFederationRouter`)
 * - OpenMetrics / Prometheus `/metrics` endpoint (`OtelAndPrometheusExporter`)
 * - Dashboard Shard Heatmap & `STAGED_COMMIT` Approval Gate (`DashboardBackendServer`)
 */

import * as crypto from "crypto";
import { ManualFleetScaleController } from "../cluster/manual_scale_controller";
import { DashboardBackendServer } from "../dashboard/server";
import { FinOpsBudgetController } from "../finops/budget_controller";
import { OtelAndPrometheusExporter } from "../telemetry/otel_prometheus";
import { ShardedQueueBrokerPool } from "./celld_ha_engine";
import { CloudflareCelldEnv, MessageBatch } from "./env";
import { FederationRegionId, MultiRegionFederationRouter } from "./federation_router";
import { AgentTaskHarnessCell } from "./harness_cell";
import { DynamicQueueAutoSharder, PartitionedSchedulerRouter } from "./horizontal_scaler";
import { AnstsSchedulerCell } from "./scheduler_cell";
import { persistenceCellStub } from "../persistence/registry";
import { TenantLedgerCell } from "../persistence/tenant";

export { AnstsSchedulerCell, AgentTaskHarnessCell, TenantLedgerCell };

const globalSchedulerRouter = new PartitionedSchedulerRouter({
  initialShardCount: 8,
  maxConcurrencyPerShard: 512,
  hotTenantThresholdInFlight: 32,
  maxSubShardsPerTenant: 8,
});

const globalQueueAutoSharder = new DynamicQueueAutoSharder({
  initialShards: 8,
  maxShards: 64,
  scaleUpUtilizationThreshold: 0.75,
});

const globalShardedQueuePool = new ShardedQueueBrokerPool("ansts-events-q", 8, 5);
const globalFederationRouter = new MultiRegionFederationRouter();
const globalFinOpsController = new FinOpsBudgetController();
const globalOtelExporter = new OtelAndPrometheusExporter();
const globalDashboardServer = new DashboardBackendServer();
const globalManualScaleController = new ManualFleetScaleController(undefined, globalSchedulerRouter, globalQueueAutoSharder);

export default {
  publicRouter: globalSchedulerRouter,
  publicQueueAutoSharder: globalQueueAutoSharder,
  publicShardedQueuePool: globalShardedQueuePool,
  publicFederationRouter: globalFederationRouter,
  publicFinOpsController: globalFinOpsController,
  publicOtelExporter: globalOtelExporter,
  publicManualScaleController: globalManualScaleController,

  async fetch(
    request: Request,
    env: CloudflareCelldEnv,
    _ctx?: { waitUntil(p: Promise<any>): void },
  ): Promise<Response> {
    const url = new URL(request.url);

    // Route 0: Prometheus / OpenMetrics Telemetry Endpoint (`GET /metrics` — TASK-59)
    if (request.method === "GET" && url.pathname === "/metrics") {
      const text = globalOtelExporter.renderPrometheusMetrics({
        tenantDominantShares: { "fin-enterprise": 0.45, "codegen-pro": 0.30, "research-lab": 0.15 },
        schedulerShards: globalSchedulerRouter.getShardStates(),
        activeQueueShards: globalShardedQueuePool.shardCount,
        modelPools: {
          DeepSeek: { state: "CLOSED", ttftSec: 0.42 },
          "GLM-5.3": { state: "CLOSED", ttftSec: 0.16 },
          "Qwen-3.8": { state: "CLOSED", ttftSec: 0.11 },
        },
      });
      return new Response(text, {
        status: 200,
        headers: { "content-type": "text/plain; version=0.0.4; charset=utf-8" },
      });
    }

    // Route 1: Persist the complete admission in the authoritative per-tenant ledger.
    if (request.method === "POST" && url.pathname === "/v1beta1/tasks") {
      const rawBody = await request.text();
      let rawSpec: Record<string, any>;
      try {
        rawSpec = JSON.parse(rawBody) as Record<string, any>;
      } catch {
        return new Response(JSON.stringify({ error: "INVALID_JSON", message: "TaskSpec must be valid JSON." }), {
          status: 400,
          headers: { "content-type": "application/json" },
        });
      }
      const tenantId = rawSpec?.metadata?.tenant_id;
      if (typeof tenantId !== "string" || tenantId.trim().length === 0) {
        return new Response(JSON.stringify({ error: "TENANT_REQUIRED", message: "metadata.tenant_id is required." }), {
          status: 400,
          headers: { "content-type": "application/json" },
        });
      }
      try {
        const tenantStub = persistenceCellStub(env, "tenant-ledger", tenantId);
        return await tenantStub.fetch("http://tenant-ledger.internal/admissions", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: rawBody,
        });
      } catch {
        return new Response(
          JSON.stringify({ error: "TENANT_LEDGER_UNAVAILABLE", outcome: "UNKNOWN", tenant_id: tenantId }),
          { status: 503, headers: { "content-type": "application/json" } },
        );
      }
    }

    // Queries use the same tenant owner as admission; no process-local or D1 copy is authoritative.
    const taskQueryMatch = url.pathname.match(/^\/v1beta1\/tasks\/([^/]+)$/);
    const admissionQueryMatch = url.pathname.match(/^\/v1beta1\/admissions\/by-key\/([^/]+)$/);
    if (request.method === "GET" && (taskQueryMatch || admissionQueryMatch)) {
      const tenantId = url.searchParams.get("tenant_id");
      if (!tenantId) {
        return new Response(JSON.stringify({ error: "TENANT_REQUIRED", message: "tenant_id is required." }), {
          status: 400,
          headers: { "content-type": "application/json" },
        });
      }
      try {
        const tenantStub = persistenceCellStub(env, "tenant-ledger", tenantId);
        const entityId = decodeURIComponent((taskQueryMatch ?? admissionQueryMatch)![1]);
        const internalPath = taskQueryMatch
          ? `/tasks/${encodeURIComponent(entityId)}?tenant_id=${encodeURIComponent(tenantId)}`
          : `/admissions/by-key/${encodeURIComponent(entityId)}?tenant_id=${encodeURIComponent(tenantId)}`;
        return await tenantStub.fetch(`http://tenant-ledger.internal${internalPath}`, { method: "GET" });
      } catch {
        return new Response(JSON.stringify({ error: "TENANT_LEDGER_UNAVAILABLE", outcome: "UNKNOWN", tenant_id: tenantId }), {
          status: 503,
          headers: { "content-type": "application/json" },
        });
      }
    }

    // Tenant budget commands and queries share the same per-tenant authority as admission.
    const budgetRouteMatch = url.pathname.match(/^\/v1beta1\/tenants\/([^/]+)(\/.*)$/);
    if (
      budgetRouteMatch &&
      /^\/(?:budget-periods|budget-grants|budget-accounts|budget-holds|allowance-transfers)(?:\/|$)/.test(budgetRouteMatch[2])
    ) {
      const tenantId = decodeURIComponent(budgetRouteMatch[1]);
      const requestedTenant = url.searchParams.get("tenant_id");
      if (requestedTenant && requestedTenant !== tenantId) {
        return new Response(JSON.stringify({ error: "TENANT_SCOPE_MISMATCH" }), {
          status: 404,
          headers: { "content-type": "application/json" },
        });
      }
      try {
        const tenantStub = persistenceCellStub(env, "tenant-ledger", tenantId);
        const internalUrl = new URL(`http://tenant-ledger.internal${budgetRouteMatch[2]}`);
        for (const [key, value] of url.searchParams) internalUrl.searchParams.set(key, value);
        internalUrl.searchParams.set("tenant_id", tenantId);
        const init: RequestInit = { method: request.method, headers: request.headers };
        if (request.method !== "GET" && request.method !== "HEAD") init.body = await request.arrayBuffer();
        return await tenantStub.fetch(internalUrl.toString(), init);
      } catch {
        return new Response(
          JSON.stringify({ error: "TENANT_LEDGER_UNAVAILABLE", outcome: "UNKNOWN", tenant_id: tenantId }),
          { status: 503, headers: { "content-type": "application/json" } },
        );
      }
    }

    // Route 1B: Inspect or Scale Out Scheduler & Queue Shards Online (TASK-49 & TASK-63)
    if (url.pathname === "/v1beta1/scheduler/shards" || url.pathname === "/api/v1/shards/heatmap") {
      if (request.method === "POST") {
        const body = (await request.json()) as { add_shards?: number };
        const added = globalSchedulerRouter.scaleOutSchedulerShards(body.add_shards ?? 4);
        return new Response(
          JSON.stringify({
            added_shards: added,
            total_shards: globalSchedulerRouter.getShardStates().length,
            shards: globalSchedulerRouter.getShardStates(),
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }
      return new Response(
        JSON.stringify({
          total_shards: globalSchedulerRouter.getShardStates().length,
          shards: globalSchedulerRouter.getShardStates(),
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    // Route 1C: Interactive STAGED_COMMIT Online Approval / Rejection (TASK-63)
    if (request.method === "POST" && url.pathname === "/api/v1/saga/approve") {
      const body = (await request.json()) as {
        task_id: string;
        epoch: number;
        turn_seq: number;
        tool_call_id: string;
        tool_name: string;
        payload: string;
        approved: boolean;
      };
      const decision = globalDashboardServer.handleStageCommitDecision({
        taskId: body.task_id,
        epoch: body.epoch,
        turnSeq: body.turn_seq,
        toolCallId: body.tool_call_id,
        toolName: body.tool_name,
        payload: body.payload,
        approved: body.approved,
      });
      return new Response(JSON.stringify(decision), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }

    // Route 1E: Operator-Initiated Manual Fleet Scale-Out / Scale-In (TASK-57 Non-K8s)
    if (request.method === "POST" && url.pathname === "/v1beta1/fleet/scale") {
      const body = (await request.json()) as Record<string, any>;
      if (body.action === "scale-in") {
        const drainRes = globalManualScaleController.executeManualNodeDrain(
          body.node_id ?? "celld-node-02",
          body.mode ?? "drain",
        );
        return new Response(JSON.stringify(drainRes), {
          status: drainRes.allowed ? 200 : 409,
          headers: { "content-type": "application/json" },
        });
      }
      const scaleRes = globalManualScaleController.executeManualScaleOut({
        addBrainNodes: body.add_brain_nodes,
        addBodyHosts: body.add_body_hosts,
        addSchedulerShards: body.add_scheduler_shards,
        targetQueueShards: body.target_queue_shards,
      });
      return new Response(JSON.stringify(scaleRes), {
        status: scaleRes.status === "SCALED_OUT" ? 200 : 409,
        headers: { "content-type": "application/json" },
      });
    }

    // Route 1D: Multi-Region Active-Active Federation Routing (TASK-64)
    if (request.method === "POST" && url.pathname === "/v1beta1/federation/route") {
      const body = (await request.json()) as {
        preferred_region?: FederationRegionId;
        required_compliance_zone?: string;
        required_model_pool?: string;
      };
      const decision = globalFederationRouter.routeGlobalTask({
        preferredRegion: body.preferred_region,
        requiredComplianceZone: body.required_compliance_zone,
        requiredModelPool: body.required_model_pool,
      });
      return new Response(JSON.stringify(decision), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }

    // Route 2: Trigger 5D W-DRF Scheduler Tick or Inspect Cluster Top
    if (
      url.pathname === "/v1beta1/scheduler/tick" ||
      url.pathname === "/v1beta1/cluster/top" ||
      url.pathname === "/v1beta1/dag/spawn-escrow"
    ) {
      const targetShard = url.searchParams.get("shard") ?? "global-5d-scheduler";
        const schedStub = persistenceCellStub(env, "scheduler-pool", targetShard);
      const internalPath = url.pathname.replace("/v1beta1", "");
      return schedStub.fetch(`http://scheduler.internal${internalPath}`, {
        method: request.method,
        body: request.method === "POST" ? await request.text() : undefined,
      });
    }

    // Route 3: Per-Task AgentTaskHarnessCell Operations (/v1beta1/tasks/:taskId/turn | /workflow | /replay | /failover)
    const taskMatch = url.pathname.match(/^\/v1beta1\/tasks\/([^/]+)\/(turn|workflow|replay|failover)$/);
    if (taskMatch) {
      const [, taskId, action] = taskMatch;
        const cellStub = persistenceCellStub(env, "task", taskId);
      const resp = await cellStub.fetch(`http://harness.internal/${action}`, {
        method: request.method,
        body: await request.text(),
      });
      if (action === "turn" && resp.status === 200) {
        globalOtelExporter.incrementRpoZeroCommit("bucket");
      }
      return resp;
    }

    return new Response(
      JSON.stringify({
        service: "ANSTS v2.2 (Cloudflare Workers & denoland/celld HA Runtime)",
        status: "READY",
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  },

  async queue(batch: MessageBatch<any>, env: CloudflareCelldEnv): Promise<void> {
    const secret = env.WEBHOOK_HMAC_SECRET ?? "whsec_ansts_v2_prod";
    for (const msg of batch.messages) {
      const body = msg.body;
      const payloadStr = JSON.stringify(body.payload ?? {});
      const signature = crypto.createHmac("sha256", secret).update(payloadStr, "utf8").digest("hex");

      await env.ANSTS_D1.prepare(
        "INSERT INTO task_audit_events (event_id, task_id, tenant_id, event_type, epoch, turn_seq, routed_model, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      )
        .bind(
          msg.id,
          body.task_id ?? "system",
          body.tenant_id ?? "system",
          body.event_type ?? "agent.event",
          body.epoch ?? 0,
          body.turn_seq ?? 0,
          body.routed_model ?? "DeepSeek",
          payloadStr,
          new Date().toISOString(),
        )
        .run();

      msg.ack();
      void signature;
    }
  },

  async scheduled(
    _controller: { scheduledTime: number; cron: string },
    env: CloudflareCelldEnv,
  ): Promise<void> {
    const schedStub = persistenceCellStub(env, "scheduler-pool", "global-5d-scheduler");
    await schedStub.fetch("http://scheduler.internal/scheduler/tick", { method: "POST" });
  },
};
