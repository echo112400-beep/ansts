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
import { schedulerPoolCellKey } from "../persistence/scheduler-pool";
import { NodeCapacityCell } from "./node_capacity_cell";

export { AnstsSchedulerCell, AgentTaskHarnessCell, NodeCapacityCell, TenantLedgerCell };

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

    // SchedulerPool identity is derived from the candidate's region and resource pool.
    if (url.pathname === "/v1beta1/scheduler/tick" && request.method === "POST") {
      let body: Record<string, any>;
      try {
        body = await request.json() as Record<string, any>;
      } catch {
        return jsonResponse({ error: "INVALID_JSON" }, 400);
      }
      const candidates = Array.isArray(body.candidates) ? body.candidates : null;
      if (candidates === null || candidates.length === 0) {
        try {
          const poolKey = url.searchParams.get("shard") ?? schedulerPoolCellKey(body.region, body.resource_pool);
          return callSchedulerPool(env, poolKey, "/scheduler/tick", "POST", body);
        } catch (error) {
          return jsonResponse({ error: "INVALID_SCHEDULER_POOL", message: error instanceof Error ? error.message : String(error) }, 400);
        }
      }
      let groups: Map<string, any[]>;
      try {
        groups = groupSchedulerCandidates(candidates);
      } catch (error) {
        return jsonResponse({ error: "INVALID_SCHEDULER_POOL", message: error instanceof Error ? error.message : String(error) }, 400);
      }
      const poolResults = await Promise.all(Array.from(groups, async ([poolKey, poolCandidates]) => {
        const response = await callSchedulerPool(env, poolKey, "/scheduler/tick", "POST", { ...body, candidates: poolCandidates });
        let result: Record<string, any>;
        try { result = await response.json() as Record<string, any>; }
        catch { result = { error: "INVALID_SCHEDULER_RESPONSE" }; }
        return { pool_key: poolKey, status: response.status, body: result };
      }));
      const dispatches = poolResults.flatMap((result) => Array.isArray(result.body.dispatches) ? result.body.dispatches : []);
      if (poolResults.some((result) => result.status < 200 || result.status >= 300)) {
        if (poolResults.length === 1) return jsonResponse(poolResults[0].body, poolResults[0].status);
        return jsonResponse({ error: "SCHEDULER_POOL_PARTIAL_FAILURE", outcome: "UNKNOWN", pool_results: poolResults, dispatches }, 503);
      }
      return jsonResponse({
        dispatched_count: dispatches.filter((dispatch) => dispatch.stage === "STARTED").length,
        blocked_count: dispatches.filter((dispatch) => dispatch.status === "BLOCKED_CAPACITY").length,
        suspected_tasks: poolResults.flatMap((result) => Array.isArray(result.body.suspected_tasks) ? result.body.suspected_tasks : []),
        dispatches,
      });
    }

    if (url.pathname === "/v1beta1/scheduler/dispatch" && request.method === "POST") {
      let body: Record<string, any>;
      try { body = await request.json() as Record<string, any>; }
      catch { return jsonResponse({ error: "INVALID_JSON" }, 400); }
      const candidate = body.candidate ?? body;
      try {
        return callSchedulerPool(env, schedulerPoolCellKey(candidate.region, candidate.resource_pool), "/scheduler/dispatch", "POST", {
          ...candidate,
          nodes: body.nodes ?? [],
        });
      } catch (error) {
        return jsonResponse({ error: "INVALID_SCHEDULER_POOL", message: error instanceof Error ? error.message : String(error) }, 400);
      }
    }

    if (url.pathname === "/v1beta1/scheduler/started" && request.method === "POST") {
      let body: Record<string, any>;
      try { body = await request.json() as Record<string, any>; }
      catch { return jsonResponse({ error: "INVALID_JSON" }, 400); }
      try {
        return callSchedulerPool(
          env,
          schedulerPoolCellKey(body.region, body.resource_pool),
          "/scheduler/started",
          "POST",
          body,
        );
      } catch (error) {
        return jsonResponse({ error: "INVALID_SCHEDULER_POOL", message: error instanceof Error ? error.message : String(error) }, 400);
      }
    }

    if (url.pathname === "/v1beta1/dag/spawn-escrow" && request.method === "POST") {
      let body: Record<string, any>;
      try { body = await request.json() as Record<string, any>; }
      catch { return jsonResponse({ error: "INVALID_JSON" }, 400); }
      const candidates = Array.isArray(body.candidates) ? body.candidates : [];
      if (candidates.length === 0) {
        try {
          return callSchedulerPool(
            env,
            url.searchParams.get("shard") ?? schedulerPoolCellKey(body.region, body.resource_pool),
            "/dag/spawn-escrow",
            "POST",
            body,
          );
        } catch (error) {
          return jsonResponse({ error: "INVALID_SCHEDULER_POOL", message: error instanceof Error ? error.message : String(error) }, 400);
        }
      }
      let groups: Map<string, any[]>;
      try { groups = groupSchedulerCandidates(candidates); }
      catch (error) { return jsonResponse({ error: "INVALID_SCHEDULER_POOL", message: error instanceof Error ? error.message : String(error) }, 400); }
      const poolResults = await Promise.all(Array.from(groups, async ([poolKey, poolCandidates]) => {
        const response = await callSchedulerPool(env, poolKey, "/dag/spawn-escrow", "POST", { ...body, candidates: poolCandidates });
        let result: Record<string, any>;
        try { result = await response.json() as Record<string, any>; }
        catch { result = { error: "INVALID_SCHEDULER_RESPONSE" }; }
        return { pool_key: poolKey, status: response.status, body: result };
      }));
      const dispatches = poolResults.flatMap((result) => Array.isArray(result.body.dispatches) ? result.body.dispatches : []);
      if (poolResults.some((result) => result.status < 200 || result.status >= 300)) {
        if (poolResults.length === 1) return jsonResponse(poolResults[0].body, poolResults[0].status);
        return jsonResponse({ error: "SCHEDULER_POOL_PARTIAL_FAILURE", outcome: "UNKNOWN", pool_results: poolResults, dispatches }, 503);
      }
      return jsonResponse({
        dispatched_children: poolResults.flatMap((result) => Array.isArray(result.body.dispatched_children) ? result.body.dispatched_children : []),
        dispatches,
      });
    }

    if (url.pathname === "/v1beta1/cluster/top" && request.method === "GET") {
      let targetShard = url.searchParams.get("shard");
      if (!targetShard) {
        try {
          targetShard = schedulerPoolCellKey(url.searchParams.get("region") ?? undefined, url.searchParams.get("resource_pool") ?? undefined);
        } catch (error) {
          return jsonResponse({ error: "INVALID_SCHEDULER_POOL", message: error instanceof Error ? error.message : String(error) }, 400);
        }
      }
      return callSchedulerPool(env, targetShard, "/cluster/top", "GET");
    }

    // Route 3: Per-Task AgentTaskHarnessCell operations and pending start commands.
    const taskMatch = url.pathname.match(/^\/v1beta1\/tasks\/([^/]+)\/(turn|workflow|replay|failover|start-commands)$/);
    if (taskMatch) {
      const [, taskId, action] = taskMatch;
      if (action === "start-commands" && request.method !== "GET") return jsonResponse({ error: "MethodNotAllowed" }, 405);
      const cellStub = persistenceCellStub(env, "task", taskId);
      const internalPath = action === "start-commands" ? "/execution/start-commands" : `/${action}`;
      let resp: Response;
      try {
        resp = await cellStub.fetch(`http://harness.internal${internalPath}`, {
          method: action === "start-commands" ? "GET" : request.method,
          body: action === "start-commands" ? undefined : await request.text(),
        });
      } catch {
        return jsonResponse({ error: "TASK_CELL_UNAVAILABLE", outcome: "UNKNOWN", task_id: taskId }, 503);
      }
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

function groupSchedulerCandidates(candidates: unknown[]): Map<string, any[]> {
  const groups = new Map<string, any[]>();
  for (const candidate of candidates) {
    if (!candidate || typeof candidate !== "object" || Array.isArray(candidate)) {
      throw new TypeError("Each scheduler candidate must be an object.");
    }
    const item = candidate as Record<string, any>;
    const poolKey = schedulerPoolCellKey(item.region, item.resource_pool);
    const group = groups.get(poolKey) ?? [];
    group.push(item);
    groups.set(poolKey, group);
  }
  return groups;
}

async function callSchedulerPool(
  env: CloudflareCelldEnv,
  poolKey: string,
  path: string,
  method: string,
  body?: unknown,
): Promise<Response> {
  try {
    const stub = persistenceCellStub(env, "scheduler-pool", poolKey);
    const init: RequestInit = { method };
    if (body !== undefined) {
      init.headers = { "content-type": "application/json" };
      init.body = JSON.stringify(body);
    }
    return await stub.fetch(`http://scheduler.internal${path}`, init);
  } catch {
    return jsonResponse({ error: "SCHEDULER_POOL_UNAVAILABLE", outcome: "UNKNOWN", pool_key: poolKey }, 503);
  }
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
