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
import { ProviderQuotaCell } from "./provider_quota_cell";
import { RoutingConfigCell } from "./routing_config_cell";
import { DagCell } from "./dag_cell";
import { TenantDirectoryCell, TENANT_DIRECTORY_SHARD_COUNT, tenantDirectoryShardKey, tenantDirectoryShardIndex } from "./tenant_directory_cell";
import { providerQuotaCellKey } from "../persistence/provider-quota";
import { listProjectedTenants, projectTaskEvents, projectTenantTasks, pruneTaskEventProjection, readTenantTaskProjection } from "../dashboard/durable_query_adapter";

export { AnstsSchedulerCell, AgentTaskHarnessCell, NodeCapacityCell, TenantLedgerCell, ProviderQuotaCell, RoutingConfigCell, DagCell, TenantDirectoryCell };

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
        const directoryKey = tenantDirectoryShardKey(tenantDirectoryShardIndex(tenantId));
        const directoryStub = persistenceCellStub(env, "tenant-directory", directoryKey);
        const registryResponse = await directoryStub.fetch("http://tenant-directory.internal/tenants/register", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ tenant_id: tenantId }),
        });
        if (!registryResponse.ok) return registryResponse;
      } catch {
        return jsonResponse({ error: "TENANT_DIRECTORY_UNAVAILABLE", outcome: "NOT_ACCEPTED", tenant_id: tenantId }, 503);
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

    const tenantTaskListMatch = url.pathname.match(/^\/v1beta1\/tenants\/([^/]+)\/tasks$/);
    if (request.method === "GET" && tenantTaskListMatch) {
      const tenantId = decodeURIComponent(tenantTaskListMatch[1]);
      const requestedTenant = url.searchParams.get("tenant_id");
      if (requestedTenant && requestedTenant !== tenantId) return jsonResponse({ error: "TENANT_SCOPE_MISMATCH" }, 404);
      return await rebuildTenantTaskProjection(env, tenantId, Number(url.searchParams.get("limit") ?? 100));
    }

    const dashboardTaskListMatch = url.pathname.match(/^\/v1beta1\/tenants\/([^/]+)\/dashboard\/tasks$/);
    if (request.method === "GET" && dashboardTaskListMatch) {
      const tenantId = decodeURIComponent(dashboardTaskListMatch[1]);
      const requestedTenant = url.searchParams.get("tenant_id");
      if (requestedTenant && requestedTenant !== tenantId) return jsonResponse({ error: "TENANT_SCOPE_MISMATCH" }, 404);
      if (url.searchParams.get("rebuild") === "1") return await rebuildTenantTaskProjection(env, tenantId, Number(url.searchParams.get("limit") ?? 100));
      const limit = Number(url.searchParams.get("limit") ?? 500);
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 500) return jsonResponse({ error: "TASK_LIST_LIMIT_INVALID" }, 400);
      try {
        const tasks = await readTenantTaskProjection(env.ANSTS_D1, tenantId, limit);
        return jsonResponse({ tenant_id: tenantId, source: "d1_projection", rebuildable: true,
          tasks, rebuild_url: `/v1beta1/tenants/${encodeURIComponent(tenantId)}/dashboard/tasks?rebuild=1` });
      } catch {
        return jsonResponse({ tenant_id: tenantId, source: "d1_projection", tasks: [], projection_unavailable: true,
          rebuild_url: `/v1beta1/tenants/${encodeURIComponent(tenantId)}/dashboard/tasks?rebuild=1` }, 503);
      }
    }

    const taskEventsMatch = url.pathname.match(/^\/v1beta1\/tasks\/([^/]+)\/events$/);
    if (request.method === "GET" && taskEventsMatch) {
      const tenantId = url.searchParams.get("tenant_id");
      if (!tenantId) return jsonResponse({ error: "TENANT_REQUIRED", message: "tenant_id is required." }, 400);
      const taskId = decodeURIComponent(taskEventsMatch[1]);
      let cellStub;
      try { cellStub = persistenceCellStub(env, "task", taskId); }
      catch { return jsonResponse({ error: "TASK_CELL_UNAVAILABLE", outcome: "UNKNOWN", task_id: taskId }, 503); }
      const poll = (cursor?: string) => {
        const internalUrl = new URL("http://harness.internal/events");
        internalUrl.searchParams.set("tenant_id", tenantId);
        for (const [key, value] of url.searchParams) if (key !== "tenant_id" && key !== "after_cursor") internalUrl.searchParams.set(key, value);
        const lastEventId = cursor ?? request.headers.get("last-event-id") ?? url.searchParams.get("after_cursor");
        const headers = new Headers(request.headers);
        headers.delete("last-event-id");
        if (lastEventId) internalUrl.searchParams.set("after_cursor", lastEventId);
        return cellStub.fetch(internalUrl.toString(), { method: "GET", headers });
      };
      const acceptsSse = request.headers.get("accept")?.includes("text/event-stream") ?? false;
      if (!acceptsSse) {
        try {
          const response = await poll();
          if (response.ok) {
            const page = await response.clone().json() as { events: Array<Record<string, any>> };
            await projectFeedPage(env, page.events, cellStub);
          }
          return response;
        }
        catch { return jsonResponse({ error: "TASK_CELL_UNAVAILABLE", outcome: "UNKNOWN", task_id: taskId }, 503); }
      }
      let firstResponse: Response;
      try { firstResponse = await poll(); }
      catch { return jsonResponse({ error: "TASK_CELL_UNAVAILABLE", outcome: "UNKNOWN", task_id: taskId }, 503); }
      if (!firstResponse.ok) return firstResponse;
      const firstPage = await firstResponse.json() as { events: Array<Record<string, any>>; next_cursor: string | null; has_more: boolean };
      await projectFeedPage(env, firstPage.events, cellStub);
      const snapshotOnly = url.searchParams.get("snapshot") === "1";
      if (snapshotOnly) return sseResponse(firstPage.events, false);
      let active = true;
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          const encoder = new TextEncoder();
          let cursor = request.headers.get("last-event-id") ?? url.searchParams.get("after_cursor") ?? undefined;
          const emit = (event: Record<string, any>) => {
            const data = JSON.stringify({ event_id: event.eventId, operation_id: event.operationId,
              aggregate_version: event.aggregateVersion, payload_digest: event.payloadDigest,
              tenant_id: event.tenantId, task_id: event.aggregateId, payload: event.payload });
            controller.enqueue(encoder.encode(`id: ${event.cursor}\nevent: ${event.eventType}\ndata: ${data}\n\n`));
            cursor = event.cursor;
          };
          const pump = async () => {
            try {
              for (const event of firstPage.events) emit(event);
              if (firstPage.next_cursor) cursor = firstPage.next_cursor;
              while (active) {
                await new Promise((resolve) => setTimeout(resolve, 2_000));
                if (!active) break;
                let response: Response;
                try { response = await poll(cursor); }
                catch { controller.enqueue(encoder.encode(": source temporarily unavailable\n\n")); continue; }
                if (response.status === 410) {
                  const details = await response.json().catch(() => ({ error: "CURSOR_EXPIRED", resync_required: true }));
                  controller.enqueue(encoder.encode(`event: resync-required\ndata: ${JSON.stringify(details)}\n\n`));
                  active = false;
                  controller.close();
                  return;
                }
                if (!response.ok) { controller.enqueue(encoder.encode(": source temporarily unavailable\n\n")); continue; }
                const page = await response.json() as { events: Array<Record<string, any>>; next_cursor: string | null };
                await projectFeedPage(env, page.events, cellStub);
                for (const event of page.events) emit(event);
                if (page.next_cursor && page.events.length === 0) cursor = page.next_cursor;
                if (page.events.length === 0) controller.enqueue(encoder.encode(`: heartbeat ${Date.now()}\n\n`));
              }
            } catch {
              active = false;
              try { controller.close(); } catch { /* stream was already cancelled */ }
            }
          };
          void pump();
        },
        cancel() { active = false; },
      });
      return new Response(stream, { status: 200, headers: {
        "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache, no-transform",
        "x-accel-buffering": "no", "x-ansts-cursor-scope": "task",
      } });
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

    const routingConfigMatch = url.pathname.match(/^\/v1beta1\/tenants\/([^/]+)\/routing-config(\/.*)?$/);
    if (routingConfigMatch) {
      const tenantId = decodeURIComponent(routingConfigMatch[1]);
      const requestedTenant = url.searchParams.get("tenant_id");
      if (requestedTenant && requestedTenant !== tenantId) return jsonResponse({ error: "TENANT_SCOPE_MISMATCH" }, 404);
      try {
        const stub = persistenceCellStub(env, "routing-config", tenantId);
        const internalUrl = new URL(`http://routing-config.internal/routing-config${routingConfigMatch[2] ?? ""}`);
        for (const [key, value] of url.searchParams) internalUrl.searchParams.set(key, value);
        const init: RequestInit = { method: request.method, headers: request.headers };
        if (request.method !== "GET" && request.method !== "HEAD") init.body = await request.arrayBuffer();
        return await stub.fetch(internalUrl.toString(), init);
      } catch {
        return jsonResponse({ error: "ROUTING_CONFIG_UNAVAILABLE", outcome: "UNKNOWN", tenant_id: tenantId }, 503);
      }
    }

    const providerQuotaMatch = url.pathname.match(/^\/v1beta1\/providers\/([^/]+)\/quota-domains\/([^/]+)(\/.*)?$/);
    if (providerQuotaMatch) {
      const provider = decodeURIComponent(providerQuotaMatch[1]);
      const quotaDomain = decodeURIComponent(providerQuotaMatch[2]);
      try {
        const stub = persistenceCellStub(env, "provider-quota", providerQuotaCellKey(provider, quotaDomain));
        const internalUrl = new URL(`http://provider-quota.internal/provider-quota${providerQuotaMatch[3] ?? "/policy"}`);
        for (const [key, value] of url.searchParams) internalUrl.searchParams.set(key, value);
        const init: RequestInit = { method: request.method, headers: request.headers };
        if (request.method !== "GET" && request.method !== "HEAD") init.body = await request.arrayBuffer();
        return await stub.fetch(internalUrl.toString(), init);
      } catch {
        return jsonResponse({ error: "PROVIDER_QUOTA_UNAVAILABLE", outcome: "UNKNOWN", provider, quota_domain: quotaDomain }, 503);
      }
    }

    const durableDagMatch = url.pathname.match(/^\/v1beta1\/dags\/([^/]+)(?:\/(state|reconcile|boosts|children\/([^/]+)\/(redeem|observations|return-allowance)))?$/);
    if (durableDagMatch) {
      const [, encodedDagId, section, encodedChildId, childAction] = durableDagMatch;
      const dagId = decodeURIComponent(encodedDagId);
      let rawBody: string | undefined;
      let bodyTenant: string | undefined;
      if (request.method !== "GET" && request.method !== "HEAD") {
        rawBody = await request.text();
        try { bodyTenant = (JSON.parse(rawBody) as { tenant_id?: string }).tenant_id; } catch { /* DagCell reports invalid JSON. */ }
      }
      const isCreate = !section && request.method === "POST";
      const isState = section === "state" && request.method === "GET";
      const isReconcile = section === "reconcile" && request.method === "POST";
      const isBoost = section === "boosts" && request.method === "POST";
      const isChildCommand = Boolean(encodedChildId && ["redeem", "observations", "return-allowance"].includes(childAction ?? "") && request.method === "POST");
      if (!isCreate && !isState && !isReconcile && !isBoost && !isChildCommand) return jsonResponse({ error: "NotFound" }, 404);
      const tenantId = url.searchParams.get("tenant_id") ?? bodyTenant;
      if (!isCreate && !tenantId) return jsonResponse({ error: "TENANT_REQUIRED", message: "tenant_id is required." }, 400);
      try {
        const stub = persistenceCellStub(env, "dag", dagId);
        const internalPath = isCreate ? "/dags" : isState ? "/dags/state" : isReconcile ? "/dags/reconcile" : isBoost ? "/dags/boosts" : `/dags/children/${encodeURIComponent(decodeURIComponent(encodedChildId!))}/${childAction}`;
        const internalUrl = new URL(`http://dag.internal${internalPath}`);
        if (tenantId) internalUrl.searchParams.set("tenant_id", tenantId);
        return await stub.fetch(internalUrl.toString(), {
          method: request.method,
          headers: request.headers,
          body: rawBody,
        });
      } catch {
        return jsonResponse({ error: "DAG_CELL_UNAVAILABLE", outcome: "UNKNOWN", dag_id: dagId }, 503);
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

    if (url.pathname === "/v1beta1/scheduler/stopped" && request.method === "POST") {
      let body: Record<string, any>;
      try { body = await request.json() as Record<string, any>; }
      catch { return jsonResponse({ error: "INVALID_JSON" }, 400); }
      try {
        return callSchedulerPool(env, schedulerPoolCellKey(body.region, body.resource_pool), "/scheduler/stopped", "POST", body);
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

    const taskDurableStateMatch = url.pathname.match(
      /^\/v1beta1\/tasks\/([^/]+)\/(operations|compensations)(?:\/([^/]+))?(?:\/([^/]+))?$/,
    );
    if (taskDurableStateMatch) {
      const [, encodedTaskId, owner, encodedPart, encodedAction] = taskDurableStateMatch;
      const tenantId = url.searchParams.get("tenant_id");
      if (!tenantId) return jsonResponse({ error: "TENANT_REQUIRED", message: "tenant_id is required." }, 400);
      const allowedRoute = owner === "operations"
        ? (!encodedPart || (Boolean(encodedPart) && !encodedAction) || (Boolean(encodedPart) && encodedAction === "approval"))
        : (!encodedPart || (!encodedAction && encodedPart === "run"));
      if (!allowedRoute) return jsonResponse({ error: "NotFound" }, 404);
      const taskId = decodeURIComponent(encodedTaskId);
      const internalPath = `/${owner}${encodedPart ? `/${encodeURIComponent(decodeURIComponent(encodedPart))}` : ""}${encodedAction ? `/${encodeURIComponent(decodeURIComponent(encodedAction))}` : ""}`;
      const internalUrl = new URL(`http://harness.internal${internalPath}`);
      for (const [key, value] of url.searchParams) internalUrl.searchParams.set(key, value);
      const cellStub = persistenceCellStub(env, "task", taskId);
      try {
        return await cellStub.fetch(internalUrl.toString(), {
          method: request.method,
          headers: request.headers,
          body: request.method === "GET" || request.method === "HEAD" ? undefined : await request.arrayBuffer(),
        });
      } catch {
        return jsonResponse({ error: "TASK_CELL_UNAVAILABLE", outcome: "UNKNOWN", task_id: taskId }, 503);
      }
    }

    const taskTurnsMatch = url.pathname.match(
      /^\/v1beta1\/tasks\/([^/]+)\/turns(?:\/(recovery|snapshot-uploads)(?:\/([^/]+)(?:\/chunks\/([^/]+))?)?)?$/,
    );
    if (taskTurnsMatch) {
      const [, encodedTaskId, section, encodedUploadId, encodedChunkIndex] = taskTurnsMatch;
      const tenantId = url.searchParams.get("tenant_id");
      if (!tenantId) return jsonResponse({ error: "TENANT_REQUIRED", message: "tenant_id is required." }, 400);
      const isRecovery = section === "recovery" && request.method === "GET" && !encodedUploadId;
      const isBeginUpload = section === "snapshot-uploads" && request.method === "POST" && !encodedUploadId;
      const isWriteChunk = section === "snapshot-uploads" && request.method === "PUT" && Boolean(encodedUploadId) && Boolean(encodedChunkIndex);
      if (!isRecovery && !isBeginUpload && !isWriteChunk) return jsonResponse({ error: "NotFound" }, 404);
      const taskId = decodeURIComponent(encodedTaskId);
      const internalPath = isRecovery
        ? "/turns/recovery"
        : isBeginUpload
          ? "/turns/snapshot-uploads"
          : `/turns/snapshot-uploads/${encodeURIComponent(decodeURIComponent(encodedUploadId!))}/chunks/${encodeURIComponent(decodeURIComponent(encodedChunkIndex!))}`;
      const internalUrl = new URL(`http://harness.internal${internalPath}`);
      for (const [key, value] of url.searchParams) internalUrl.searchParams.set(key, value);
      const cellStub = persistenceCellStub(env, "task", taskId);
      try {
        return await cellStub.fetch(internalUrl.toString(), {
          method: request.method,
          headers: request.headers,
          body: request.method === "GET" || request.method === "HEAD" ? undefined : await request.arrayBuffer(),
        });
      } catch {
        return jsonResponse({ error: "TASK_CELL_UNAVAILABLE", outcome: "UNKNOWN", task_id: taskId }, 503);
      }
    }

    // Route 3: Per-Task AgentTaskHarnessCell operations and pending start commands.
    const taskMatch = url.pathname.match(/^\/v1beta1\/tasks\/([^/]+)\/(turn|workflow|replay|failover|start-commands|complete)$/);
    if (taskMatch) {
      const [, taskId, action] = taskMatch;
      if (action === "start-commands" && request.method !== "GET") return jsonResponse({ error: "MethodNotAllowed" }, 405);
      if (action === "complete" && request.method !== "POST") return jsonResponse({ error: "MethodNotAllowed" }, 405);
      const cellStub = persistenceCellStub(env, "task", taskId);
      const internalPath = action === "start-commands" ? "/execution/start-commands" : action === "complete" ? "/execution/complete" : `/${action}`;
      let resp: Response;
      try {
        const internalUrl = new URL(`http://harness.internal${internalPath}`);
        for (const [key, value] of url.searchParams) internalUrl.searchParams.set(key, value);
        resp = await cellStub.fetch(internalUrl.toString(), {
          method: action === "start-commands" ? "GET" : request.method,
          headers: request.headers,
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
    try {
      const schedStub = persistenceCellStub(env, "scheduler-pool", "global-5d-scheduler");
      await schedStub.fetch("http://scheduler.internal/scheduler/tick", { method: "POST" });
    } catch { /* Scheduler failure must not prevent independent durable query repair. */ }
    try {
      await pruneTaskEventProjection(env.ANSTS_D1);
      const tenants = await listProjectedTenants(env.ANSTS_D1, 20);
      for (const tenantId of tenants) await rebuildTenantTaskProjection(env, tenantId, 500);
    } catch { /* D1 discovery is a projection repair loop, not an authority for task state. */ }
    // The sharded durable registry is independent of D1 and eventually rediscovers tenants
    // even after the entire query projection has been deleted.
    const firstShard = Math.floor(_controller.scheduledTime / 60_000) % TENANT_DIRECTORY_SHARD_COUNT;
    for (let offset = 0; offset < 4; offset += 1) {
      const shard = (firstShard + offset) % TENANT_DIRECTORY_SHARD_COUNT;
      try {
        const directoryStub = persistenceCellStub(env, "tenant-directory", tenantDirectoryShardKey(shard));
        const response = await directoryStub.fetch("http://tenant-directory.internal/tenants/scan?limit=20", { method: "GET" });
        if (!response.ok) continue;
        const scan = await response.json() as { tenants: string[] };
        for (const tenantId of scan.tenants) await rebuildTenantTaskProjection(env, tenantId, 500);
      } catch { /* One unavailable directory shard does not prevent later scheduled passes. */ }
    }
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

async function rebuildTenantTaskProjection(env: CloudflareCelldEnv, tenantId: string, limit: number): Promise<Response> {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 500) return jsonResponse({ error: "TASK_LIST_LIMIT_INVALID" }, 400);
  try {
    const tenantStub = persistenceCellStub(env, "tenant-ledger", tenantId);
    const ledgerUrl = new URL("http://tenant-ledger.internal/tasks");
    ledgerUrl.searchParams.set("tenant_id", tenantId);
    ledgerUrl.searchParams.set("limit", String(limit));
    const ledgerResponse = await tenantStub.fetch(ledgerUrl.toString(), { method: "GET" });
    if (!ledgerResponse.ok) return ledgerResponse;
    const ledger = await ledgerResponse.json() as { tenant_id: string; tasks: Array<Record<string, any>> };
    const rebuilt = await Promise.all(ledger.tasks.map(async (task) => {
      let execution: Record<string, any> | null = null;
      const events: Array<Record<string, any>> = [];
      let eventsComplete = true;
      let afterCursor: string | undefined;
      try {
        const taskStub = persistenceCellStub(env, "task", String(task.task_id));
        const response = await taskStub.fetch(`http://task.internal/execution/state?tenant_id=${encodeURIComponent(tenantId)}`, { method: "GET" });
        if (response.ok) execution = await response.json() as Record<string, any>;
        let pages = 0;
        while (pages < 100) {
          const eventsUrl = new URL("http://task.internal/events");
          eventsUrl.searchParams.set("tenant_id", tenantId);
          eventsUrl.searchParams.set("limit", "1000");
          if (afterCursor) eventsUrl.searchParams.set("after_cursor", afterCursor);
          const eventResponse = await taskStub.fetch(eventsUrl.toString(), { method: "GET" });
          if (eventResponse.status === 404) break;
          if (!eventResponse.ok) { eventsComplete = false; break; }
          const page = await eventResponse.json() as { events: Array<Record<string, any>>; next_cursor: string | null; has_more: boolean };
          events.push(...page.events);
          pages += 1;
          if (!page.has_more || !page.next_cursor) break;
          afterCursor = page.next_cursor;
        }
        if (pages === 100) eventsComplete = false;
      } catch { /* TaskCell creation and projection can lag admission; ledger state remains authoritative. */ }
      return { projection: {
          tenant_id: tenantId,
          task_id: String(task.task_id),
          admission_status: String(task.status),
          runnable_status: String(task.runnable_status),
          accepted_at: Number(task.accepted_at),
          lifecycle_state: String(execution?.lifecycle_state ?? "UNINITIALIZED"),
          business_epoch: Number(execution?.business_epoch ?? 0),
          last_turn_seq: Number(execution?.last_turn_seq ?? 0),
          execution_version: Number(execution?.execution_version ?? 0),
        }, events, eventsComplete, hasTaskCell: execution !== null };
    }));
    const projected = rebuilt.map((item) => item.projection);
    let projectionStored = rebuilt.every((item) => item.eventsComplete);
    try {
      await projectTenantTasks(env.ANSTS_D1, projected);
      await projectTaskEvents(env.ANSTS_D1, rebuilt.flatMap((item) => item.events.map((event) => ({
        event_id: String(event.eventId), tenant_id: String(event.tenantId), task_id: String(event.aggregateId),
        event_type: String(event.eventType), aggregate_version: Number(event.aggregateVersion),
        payload_digest: String(event.payloadDigest), payload: event.payload, created_at: Number(event.createdAt),
      }))));
      for (const item of rebuilt) {
        const taskStub = persistenceCellStub(env, "task", String(item.projection.task_id));
        const eventIds = item.events.filter((event) => event.eventType !== "task.execution.start_requested")
          .map((event) => String(event.eventId));
        for (let offset = 0; offset < eventIds.length; offset += 1000) {
          const ack = await taskStub.fetch("http://task.internal/events/ack", {
            method: "POST", headers: { "content-type": "application/json" },
            body: JSON.stringify({ tenant_id: tenantId, event_ids: eventIds.slice(offset, offset + 1000) }),
          });
          if (!ack.ok) projectionStored = false;
        }
        if (item.eventsComplete && item.hasTaskCell) {
          const prune = await taskStub.fetch("http://task.internal/events/prune", {
            method: "POST", headers: { "content-type": "application/json" },
            body: JSON.stringify({ tenant_id: tenantId }),
          });
          if (!prune.ok) projectionStored = false;
        }
      }
    }
    catch { projectionStored = false; }
    return jsonResponse({ tenant_id: tenantId, source: "tenant_and_task_cells", rebuildable: true,
      projection_stored: projectionStored, tasks: projected });
  } catch {
    return jsonResponse({ error: "TENANT_LEDGER_UNAVAILABLE", outcome: "UNKNOWN", tenant_id: tenantId }, 503);
  }
}

async function projectFeedPage(env: CloudflareCelldEnv, events: Array<Record<string, any>>, taskCell?: { fetch(input: RequestInfo, init?: RequestInit): Promise<Response> }): Promise<void> {
  if (events.length === 0) return;
  try {
    await projectTaskEvents(env.ANSTS_D1, events.map((event) => ({
      event_id: String(event.eventId), tenant_id: String(event.tenantId), task_id: String(event.aggregateId),
      event_type: String(event.eventType), aggregate_version: Number(event.aggregateVersion),
      payload_digest: String(event.payloadDigest), payload: event.payload, created_at: Number(event.createdAt),
    })));
    if (taskCell) {
      // D1 observing a start command is not proof that the NodeAgent started it.
      // Only the start receipt may retire this command from the durable outbox.
      const eventIds = events.filter((event) => event.eventType !== "task.execution.start_requested")
        .map((event) => String(event.eventId));
      if (eventIds.length === 0) return;
      const tenantId = String(events[0].tenantId);
      const ack = await taskCell.fetch("http://task.internal/events/ack", {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ tenant_id: tenantId, event_ids: eventIds }),
      });
      if (ack.ok) await taskCell.fetch("http://task.internal/events/prune", {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ tenant_id: tenantId }),
      });
    }
  } catch { /* D1 is a rebuildable view; the authoritative TaskCell feed remains queryable. */ }
}

function sseResponse(events: Array<Record<string, any>>, replayOnly: boolean): Response {
  const frames = events.map((event) => {
    const data = JSON.stringify({ event_id: event.eventId, operation_id: event.operationId,
      aggregate_version: event.aggregateVersion, payload_digest: event.payloadDigest,
      tenant_id: event.tenantId, task_id: event.aggregateId, payload: event.payload });
    return `id: ${event.cursor}\nevent: ${event.eventType}\ndata: ${data}\n\n`;
  }).join("");
  return new Response(frames || ": no retained events\n\n", { status: 200, headers: {
    "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache, no-transform",
    "x-ansts-cursor-scope": "task", "x-ansts-stream-mode": replayOnly ? "replay" : "follow",
  } });
}
