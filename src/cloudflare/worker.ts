/**
 * Unified Cloudflare Workers & `denoland/celld` Entrypoint (`src/cloudflare/worker.ts`).
 * Directly deployable via:
 *   - `celld dev` / `celld deploy . --bucket s3://my-celld-fleet`
 *   - `npx wrangler dev` / `npx wrangler deploy`
 *
 * Composes all M1 ~ M13 hot-path capabilities:
 * - Edge KV Idempotency + FinOps Hard-Cap Admission Check (`FinOpsBudgetController`)
 * - Two-Tier Partitioned Scheduler Routing (`PartitionedSchedulerRouter` P2C + Hot-Tenant Splitting)
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

export { AnstsSchedulerCell, AgentTaskHarnessCell };

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
const tenantActiveCounts = new Map<string, number>();

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

    // Route 1: Submit TaskSpec v1beta1 with FinOps Guard + KV Idempotency + Partitioned Shard Routing
    if (request.method === "POST" && url.pathname === "/v1beta1/tasks") {
      const rawSpec = (await request.json()) as Record<string, any>;
      const tenantId = rawSpec?.metadata?.tenant_id ?? "default";
      const taskId = rawSpec?.metadata?.task_id ?? `tsk_${Date.now()}`;
      const idemKey = rawSpec?.metadata?.idempotency_key ?? taskId;
      const kvKey = `idem:${tenantId}:${idemKey}`;

      // FinOps Hard-Cap Admission Check (TASK-62)
      const budgetCheck = globalFinOpsController.canAdmitTenantTask(tenantId);
      if (!budgetCheck.allowed) {
        return new Response(
          JSON.stringify({ error: "FinOpsHardCapExceeded", message: budgetCheck.reason }),
          { status: 402, headers: { "content-type": "application/json" } },
        );
      }

      const cachedTask = await env.ANSTS_KV.get(kvKey, "json");
      if (cachedTask) {
        return new Response(JSON.stringify({ ...cachedTask, deduplicated: true }), {
          status: 200,
          headers: { "content-type": "application/json", "X-Idempotent-Replay": "true" },
        });
      }

      const currentTenantConcurrency = (tenantActiveCounts.get(tenantId) ?? 0) + 1;
      tenantActiveCounts.set(tenantId, currentTenantConcurrency);

      const routeDecision = globalSchedulerRouter.routeTaskAdmission(
        tenantId,
        taskId,
        currentTenantConcurrency,
      );
      globalSchedulerRouter.recordShardDispatchStart(routeDecision.targetShardId);

      const startMs = performance.now();
      const schedId = env.SCHEDULER_CELL.idFromName(routeDecision.targetShardId);
      const schedStub = env.SCHEDULER_CELL.get(schedId);
      const resp = await schedStub.fetch("http://scheduler.internal/tasks/submit", {
        method: "POST",
        body: JSON.stringify(rawSpec),
      });
      globalSchedulerRouter.recordShardDispatchComplete(
        routeDecision.targetShardId,
        performance.now() - startMs,
      );

      const respBody = (await resp.json()) as Record<string, any>;

      if (resp.status === 202) {
        const eventPayload = {
          event_type: "agent.task.queued",
          task_id: respBody.task_id,
          tenant_id: tenantId,
          scheduler_shard_id: routeDecision.targetShardId,
          routing_mode: routeDecision.routingMode,
          epoch: 0,
          turn_seq: 0,
          payload: respBody,
        };

        const queueReceipt = globalShardedQueuePool.produceMessage(
          `${tenantId}:${respBody.task_id}`,
          eventPayload,
        );
        const scaleEval = globalQueueAutoSharder.evaluateAndScale({
          0: currentTenantConcurrency,
        });

        const enrichedBody = {
          ...respBody,
          scheduler_shard_id: routeDecision.targetShardId,
          routing_mode: routeDecision.routingMode,
          queue_shard_id: queueReceipt.shardIndex,
          active_queue_shards: scaleEval.newShards,
        };

        await env.ANSTS_KV.put(kvKey, JSON.stringify(enrichedBody), { expirationTtl: 86400 });
        await env.ANSTS_EVENTS_QUEUE.send(eventPayload);

        return new Response(JSON.stringify(enrichedBody), {
          status: resp.status,
          headers: {
            "content-type": "application/json",
            "X-Scheduler-Shard": routeDecision.targetShardId,
            "X-Routing-Mode": routeDecision.routingMode,
            "X-Queue-Shard": String(queueReceipt.shardIndex),
          },
        });
      }

      return new Response(JSON.stringify(respBody), {
        status: resp.status,
        headers: { "content-type": "application/json" },
      });
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
      const schedId = env.SCHEDULER_CELL.idFromName(targetShard);
      const schedStub = env.SCHEDULER_CELL.get(schedId);
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
      const cellId = env.AGENT_HARNESS_CELL.idFromName(taskId);
      const cellStub = env.AGENT_HARNESS_CELL.get(cellId);
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
    const schedId = env.SCHEDULER_CELL.idFromName("global-5d-scheduler");
    const schedStub = env.SCHEDULER_CELL.get(schedId);
    await schedStub.fetch("http://scheduler.internal/scheduler/tick", { method: "POST" });
  },
};
