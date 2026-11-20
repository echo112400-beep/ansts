import {handleRegionCatalog,regionInternalPath} from '../control/admin_region_api.js';
export {RegionCatalogCell} from './region_catalog_cell.js';
import {handleConfigVersionRequest} from './config_release_cell.js';
export {ConfigReleaseCell} from './config_release_cell.js';
import {handleBodyUsageIngress} from '../control/body_usage_ingress.js';
import {executionInventoryIngress,settlementCoverageIngress,bodyInventoryIngress} from '../control/execution_inventory_ingress.js';
import {handleCapacityIngress} from '../control/capacity_ingress.js';
export {CapacityInventoryCell} from './capacity_inventory_cell.js';
/**
 * @fileoverview M7 TASK-27B, M64-1 & M65-4: Cloudflare Workers / denoland/celld Default Fetch, Queue & Durable Object Class Entrypoint.
 * Routes `/v1/scheduler/*`, `/v1/harness/*`, and `/v1/tunnel/*` to their respective stateful cells,
 * and dispatches queued task messages in `queue(batch, env)` to `ANSTS_SCHEDULER_CELL` with retry-on-failure semantics.
 */
import { CloudflareCelldEnv, QueueBatchLike } from "./env.js";
import { authenticateBody, authenticateBodyOperator, authenticateGatewayService, authenticateBodyControl, equalBodyToken } from "./body_auth.js";
import {receiveBodyObservation,lookupBodyObservation} from '../control/body_observation_ingress.js';
import { handleBodyRequest } from "./body_worker_routes.js";

import { normalizeTaskStorageId } from "./task_identity.js";
export { AnstsSchedulerCell } from "./scheduler_cell.js";
export { AgentTaskHarnessCell } from "./harness_cell.js";
export { TunnelGatewayCell } from "./tunnel_gateway_cell.js";
export { TenantControlCell } from "./tenant_control_cell.js";
export { TenantDirectoryCell } from "./tenant_directory_cell.js";
export { AnstsGatewayCell } from "./gateway_cell.js";

async function checkPilotOccupancy(env:CloudflareCelldEnv,tenantId:string):Promise<Response> {
  const shardRoster=(env.ANSTS_ADMISSION_SCHEDULER_SHARDS??"").split(",").filter(Boolean);
  const token=env.ANSTS_DIRECTORY_SERVICE_TOKEN;
  if(!env.ANSTS_SCHEDULER_CELL||!token||token.length<32||!shardRoster.includes("global-shard-0")||shardRoster.length>32)
    return Response.json({code:"ADMISSION_FLEET_INVENTORY_UNAVAILABLE"},{status:503});
  const states=await Promise.all(shardRoster.map(async (name,index)=>{
    const unavailable=(reason:string,status?:number)=>{
      console.warn("admission_fleet_occupancy_probe_failed",{index,reference:name.startsWith("id:")?"id":"name",reason,status});
      return null;
    };
    let stage="construct-request";
    try {
      const target=new URL("https://scheduler.internal/pilot-occupancy");
      target.searchParams.set("tenantId",tenantId);
      const namespace=env.ANSTS_SCHEDULER_CELL!;
      stage="resolve-object-id";
      const objectId=name.startsWith("id:")?namespace.idFromString?.(name.slice(3)):namespace.idFromName(name);
      if(!objectId)return unavailable("unsupported-object-reference");
      stage="get-object-stub";
      const stub=namespace.get(objectId);
      stage="fetch-occupancy";
      const response=await stub.fetch(
        new Request(target,{headers:{authorization:"Bearer "+token},signal:AbortSignal.timeout(2000)}));
      if(!response.ok)return unavailable("http-error",response.status);
      stage="decode-occupancy";
      const {occupied}=await response.json() as {occupied:unknown};
      return typeof occupied==="boolean"?occupied:unavailable("invalid-response");
    } catch(error) {
      return unavailable(error instanceof Error && ["TimeoutError","AbortError"].includes(error.name)?stage+"-timeout":stage+"-error");
    }
  }));
  if(states.includes(null))return Response.json({code:"ADMISSION_FLEET_INVENTORY_UNAVAILABLE"},{status:503});
  if(states.includes(true))return Response.json({code:"ADMISSION_LEGACY_OCCUPANCY_PRESENT"},{status:409});
  return Response.json({healthy:true,checked:states.length});
}

/** Reads the scheduler cell's task list; used by the brain read contract and by single-task lookups. */
async function listCellTasks(env: CloudflareCelldEnv, shardName: string): Promise<Record<string, unknown>[]> {
  if (!env.ANSTS_SCHEDULER_CELL) return [];
  const stub = env.ANSTS_SCHEDULER_CELL.get(env.ANSTS_SCHEDULER_CELL.idFromName(shardName));
  const resp = await stub.fetch(new Request("https://cell.internal/v1/scheduler/tasks"));
  if (!resp.ok) return [];
  const body = (await resp.json()) as { tasks?: Record<string, unknown>[] };
  return Array.isArray(body.tasks) ? body.tasks : [];
}

/**
 * celld-served brain shard: answers the gateway's `/internal/v1beta1/brain/schedule` contract from the
 * scheduler cell, so a real denoland/celld fleet can own the brain layer without a separate brain process.
 */
async function dispatchBrainSchedule(request: Request, env: CloudflareCelldEnv): Promise<Response> {
  const jsonHeaders = { "content-type": "application/json" };
  if (!env.ANSTS_SCHEDULER_CELL) {
    return new Response(JSON.stringify({ error: "SCHEDULER_CELL_UNBOUND" }), { status: 503, headers: jsonHeaders });
  }
  const body = (await request.json()) as Record<string, unknown>;
  const taskId = String(body.taskId ?? body.task_id ?? `task-${Date.now()}`);
  const tenantId = String(body.tenantId ?? body.tenant_id ?? "default");
  const shardName = typeof body.shard === "string" ? body.shard : "global-shard-0";
  const stub = env.ANSTS_SCHEDULER_CELL.get(env.ANSTS_SCHEDULER_CELL.idFromName(shardName));
  const submitResp = await stub.fetch(
    new Request("https://cell.internal/v1/scheduler/submit", {
      method: "POST",
      headers: jsonHeaders,
      body: JSON.stringify({
        task_id: taskId,
        tenant_id: tenantId,
        priority: Number(body.priority ?? 1),
        prompt: String(body.promptText ?? body.prompt ?? ""),
        merge_duplicate: Boolean(body.mergeDuplicate),
      }),
    })
  );
  if (!submitResp.ok) {
    return new Response(await submitResp.text(), { status: submitResp.status, headers: jsonHeaders });
  }
  const submitted = (await submitResp.json()) as Record<string, unknown>;
  const tickResp = await stub.fetch(
    new Request("https://cell.internal/v1/scheduler/schedule-tick", { method: "POST", headers: jsonHeaders })
  );
  const tick = tickResp.ok ? ((await tickResp.json()) as { placements?: Array<[string, string, number]> }) : {};
  const placements = Array.isArray(tick.placements) ? tick.placements : [];
  const firstPlacement = placements[0];
  return new Response(
    JSON.stringify({
      scheduledByBrainId: env.CELLD_REGION ?? "celld-fleet",
      state: String(submitted.state ?? "LEASED"),
      fencingToken: firstPlacement && typeof firstPlacement[2] === "number" ? firstPlacement[2] : 1,
      deduplicated: Number(submitted.merged_duplicate_count ?? 0) > 0,
      placements,
    }),
    { status: 200, headers: jsonHeaders }
  );
}

export { AdmissionShadowMailboxCell } from './admission_shadow_mailbox_cell.js';
export { AnstsBodyCell } from "./body_cell.js";
export default {
  async fetch(request: Request, env: CloudflareCelldEnv): Promise<Response> {
    const url = new URL(request.url);
    if(url.pathname==='/internal/v1/execution-inventory')return executionInventoryIngress(request,env);
    if(url.pathname==='/internal/v1/execution-inventory/body')return bodyInventoryIngress(request,env);
    if(url.pathname==='/internal/v1/settlement-coverage')return settlementCoverageIngress(request,env);
    if(['/v1/scheduler/execution-inventory','/v1/scheduler/pilot-occupancy','/v1/scheduler/pilot-expiry-status'].includes(url.pathname))return Response.json({code:'INTERNAL_ROUTE'},{status:403});
    if(['/internal/v1/body-usage','/internal/v1/body-usage-binding'].includes(url.pathname))return handleBodyUsageIngress(request,env);
    if(url.pathname.startsWith('/internal/v1/config-target/scheduler/')){
      const token=env.ANSTS_CONFIG_TARGET_TOKEN;
      if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token)){
        return Response.json({code:'SERVICE_AUTH_REQUIRED'},{status:401});
      }
      const namespace=env.ANSTS_SCHEDULER_CELL;
      if(!namespace)return Response.json({code:'CONFIG_TARGET_UNAVAILABLE'},{status:503});
      const path=url.pathname.slice('/internal/v1/config-target/scheduler/'.length);
      return namespace.get(namespace.idFromName('global-shard-0')).fetch(new Request('https://scheduler.internal/config-target/'+path,request));
    }
    if(url.pathname.startsWith('/internal/v1/config-versions/'))return handleConfigVersionRequest(request,env);
    if(url.pathname.startsWith(regionInternalPath+'/'))return handleRegionCatalog(request,env);
    if(url.pathname.startsWith('/internal/v1/capacity/'))return handleCapacityIngress(request,env);
    if(url.pathname==='/internal/v1/body-observation-binding')return lookupBodyObservation(request,env);
    if(url.pathname==='/internal/v1/body-execution-evidence')return receiveBodyObservation(request,env);
    if(url.pathname.startsWith('/internal/v1/tenant-control/')) {
      const action=url.pathname.slice('/internal/v1/tenant-control/'.length);
      const admission=action.startsWith('admission-');
      const token=admission?env.ANSTS_ADMISSION_SERVICE_TOKEN:env.ANSTS_DIRECTORY_SERVICE_TOKEN;
      if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token))return Response.json({code:'SERVICE_AUTH_REQUIRED'},{status:401});
      if(!env.ANSTS_TENANT_CONTROL_CELL||!env.ANSTS_TENANT_DIRECTORY_CELL)return Response.json({code:'TENANT_CONTROL_UNAVAILABLE'},{status:503});
      if(request.method!=='POST'||!['policy-read','policy-write','admission-read','admission-cost-preview','admission-settle-cost','admission-prepare','admission-transition','admission-credential','admission-verify','admission-outbox','admission-dispatch','admission-delivery-status','admission-delivery-control','admission-observe-execution','admission-observation-binding','admission-usage-binding','admission-usage-bind','admission-usage-ingest','usage-read','reservation-summary','rate-read','rate-activate','cost-read','cost-summary','cost-export-create','cost-export-run','cost-export-status','cost-export-download','rate-activation-audit','finops-archive-status','finops-archive-create','finops-archive-read','period-preview','period-close','period-adjustment'].includes(action))return Response.json({code:'NOT_FOUND'},{status:404});
      let raw:string,tenantId:unknown;try{raw=await request.text();if(raw.length>65536)return Response.json({code:'PAYLOAD_TOO_LARGE'},{status:413});tenantId=JSON.parse(raw).tenantId;}catch{return Response.json({code:'INVALID_JSON'},{status:400});}
      if(typeof tenantId!=='string'||tenantId.length>128||!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]*$/.test(tenantId))return Response.json({code:'INVALID_TENANT_ID'},{status:422});
      // Authorize the existing tenant before materializing a per-tenant control object.
      const parsed=JSON.parse(raw);
      if(['policy-write','rate-activate'].includes(action)&&parsed.actor?.role!=='SRE_ADMIN')return Response.json({code:'CONTROL_FORBIDDEN'},{status:403});
      const directoryToken=env.ANSTS_DIRECTORY_SERVICE_TOKEN;if(!directoryToken||directoryToken.length<32)return Response.json({code:'TENANT_CONTROL_UNAVAILABLE'},{status:503});
      const directories=env.ANSTS_TENANT_DIRECTORY_CELL;
      const checked=await directories.get(directories.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/tenant',{method:'POST',headers:{authorization:'Bearer '+directoryToken,'content-type':'application/json'},body:JSON.stringify({actor:parsed.actor,tenantId})}));
      if(!checked.ok)return checked;
      await checked.body?.cancel();
      const ns=env.ANSTS_TENANT_CONTROL_CELL;
      return ns.get(ns.idFromName('tenant-control-v1:'+tenantId)).fetch(new Request('https://tenant.internal/'+action,{method:'POST',headers:request.headers,body:raw}));
    }
    if (url.pathname === "/internal/v1/admission/fleet-preflight" && request.method === "GET") {
      const token=env.ANSTS_DIRECTORY_SERVICE_TOKEN;
      if(!token||token.length<32||!equalBodyToken(request.headers.get("authorization")??"","Bearer "+token))
        return Response.json({code:"SERVICE_AUTH_REQUIRED"},{status:401});
      const tenantId=url.searchParams.get("tenantId");
      if(!tenantId||!(env.ANSTS_ADMISSION_PILOT_TENANTS??"").split(",").includes(tenantId))
        return Response.json({code:"PILOT_TENANT_REQUIRED"},{status:422});
      return checkPilotOccupancy(env,tenantId);
    }
    if (url.pathname.startsWith("/internal/v1/directory/")) {
      const token=env.ANSTS_DIRECTORY_SERVICE_TOKEN;
      if (!token || token.length<32 || !equalBodyToken(request.headers.get("authorization")??"", "Bearer "+token)) return Response.json({code:"SERVICE_AUTH_REQUIRED"},{status:401});
      if (!env.ANSTS_TENANT_DIRECTORY_CELL) return Response.json({code:"DIRECTORY_UNAVAILABLE"},{status:503});
      const action=url.pathname.slice("/internal/v1/directory/".length);
      if (request.method!=="POST" || !["list","audit","mutate","cost-centers","cost-center-mutate","operation","archive-list","archive-create","archive-read","rate-create","rate-list","rate-audit","finops-archive-status","finops-archive-create","finops-archive-read"].includes(action)) return Response.json({code:"NOT_FOUND"},{status:404});
      const ns=env.ANSTS_TENANT_DIRECTORY_CELL;
      return ns.get(ns.idFromName("tenant-directory-v1")).fetch(new Request("https://directory.internal/"+action,request));
    }
    if (url.pathname.startsWith("/internal/v1/gateway/")) {
      if (!authenticateGatewayService(request, env)) return Response.json({ error: "GATEWAY_SERVICE_REQUIRED" }, { status: 401 });
      if (!env.ANSTS_GATEWAY_CELL) return Response.json({ error: "GATEWAY_CELL_UNBOUND" }, { status: 503 });
      const cell = env.ANSTS_GATEWAY_CELL.get(env.ANSTS_GATEWAY_CELL.idFromName("gateway-control-plane"));
      return cell.fetch(new Request("https://cell" + url.pathname.slice("/internal".length), request));
    }
    if (
      request.method === "GET" &&
      (url.pathname === "/health" || url.pathname === "/healthz" || url.pathname === "/readyz")
    ) {
      const scheduler=url.pathname==='/readyz'?env.ANSTS_SCHEDULER_CELL:undefined;
      const readiness=scheduler?await scheduler.get(scheduler.idFromName('global-shard-0'))
        .fetch(new Request('https://scheduler.internal/config-readiness')):null;
      const health=readiness?.status===200||readiness?.status===503?await readiness.json() as {healthy?:boolean}:null;
      const healthy=health?.healthy??true;
      return new Response(
        JSON.stringify({
          status: healthy?"ok":"degraded",
          healthy,
          runtime: "denoland/celld-cloudflare-workers",
          region: env.CELLD_REGION ?? "local",
          controlPlane: "celld-scheduler",
          bodyAuthority: env.ANSTS_BODY_WORKER_URL ? "remote-worker" : "fleet",
          bodyNamespace: env.ANSTS_BODY_NAMESPACE_PREFIX ?? "",
          planes: { scheduler: "durable-object" },
          ...(health?{schedulerReadiness:health}:{}),
        }),
        { status: healthy?200:503, headers: { "content-type": "application/json" } }
      );
    }
    if (url.pathname === "/internal/v1/body-legacy/export") {
      if (!authenticateBodyOperator(request, env)) return Response.json({ error: "OPERATOR_AUTH_REQUIRED" }, { status: 401 });
      if (!env.ANSTS_BODY_CELL) return Response.json({ error: "LEGACY_BODY_CELL_UNBOUND" }, { status: 503 });
      const taskId = url.searchParams.get("taskId");
      const name = taskId ? `execution:${taskId}` : url.searchParams.get("hostId");
      if (!name || request.method !== "GET") return Response.json({ error: "INVALID_EXPORT" }, { status: 400 });
      return env.ANSTS_BODY_CELL.get(env.ANSTS_BODY_CELL.idFromName(name)).fetch(new Request("https://cell/v1/body/export", { headers: { "x-ansts-verified-operator": "development-migration" } }));
    }
    if (["/v1/scheduler", "/v1/harness", "/v1/tunnel", "/internal/v1beta1/brain/"].some(prefix => url.pathname.startsWith(prefix))) {
      const bodyHost = authenticateBody(request, env);
      const gatewayService = authenticateGatewayService(request, env);
      const bodyControl = authenticateBodyControl(request, env);
      if (bodyControl && !["/v1/scheduler/tasks", "/v1/scheduler/suspend-uncertain", "/v1/scheduler/resolve-uncertain"].includes(url.pathname)) return Response.json({ error: "BODY_CONTROL_SCOPE_FORBIDDEN" }, { status: 403 });
      if (!bodyHost && !gatewayService && !bodyControl) return Response.json({ error: "SERVICE_AUTH_REQUIRED" }, { status: 401 });
      if (bodyHost && (url.pathname === "/v1/scheduler/submit" || url.pathname === "/internal/v1beta1/brain/schedule")) return Response.json({ error: "GATEWAY_SERVICE_REQUIRED" }, { status: 403 });
    }
    if (url.pathname === "/internal/v1beta1/brain/schedule" && request.method === "POST") {
      return dispatchBrainSchedule(request, env);
    }
    if (url.pathname === "/internal/v1beta1/brain/tasks" && request.method === "GET") {
      const tasks = await listCellTasks(env, "global-shard-0");
      return new Response(JSON.stringify({ brainNodeId: env.CELLD_REGION ?? "celld", totalTasks: tasks.length, tasks }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    if (url.pathname.startsWith("/internal/v1beta1/brain/tasks/") && request.method === "GET") {
      const taskId = decodeURIComponent(url.pathname.slice("/internal/v1beta1/brain/tasks/".length));
      const tasks = await listCellTasks(env, "global-shard-0");
      // The cell hands back raw `TaskSpec` objects keyed `task_id`; older callers used `taskId`. Accept both
      // shapes, otherwise every single-task lookup 404s even though the task is in the list.
      const found = tasks.find((task) => {
        const shaped = task as { taskId?: unknown; task_id?: unknown };
        return String(shaped.task_id ?? shaped.taskId ?? "") === taskId;
      });
      if (!found) {
        return new Response(JSON.stringify({ error: "TASK_NOT_FOUND", task_id: taskId }), {
          status: 404,
          headers: { "content-type": "application/json" },
        });
      }
      return new Response(JSON.stringify(found), { status: 200, headers: { "content-type": "application/json" } });
    }
    if (url.pathname.startsWith("/v1/body/")) {
      if (env.ANSTS_BODY_WORKER_URL) {
        try {
          return await fetch(new URL(url.pathname + url.search, env.ANSTS_BODY_WORKER_URL), { method: request.method, headers: request.headers, body: request.method === "GET" ? undefined : await request.arrayBuffer(), redirect: "error", signal: AbortSignal.timeout(10000) });
        } catch { return Response.json({ error: "BODY_WORKER_UNREACHABLE" }, { status: 503 }); }
      }
      return handleBodyRequest(request, env);
    }
    if (url.pathname.startsWith("/v1/scheduler")) {
      if (url.pathname === "/v1/scheduler/cancel" && !authenticateGatewayService(request, env)) return Response.json({ error: "GATEWAY_SERVICE_REQUIRED" }, { status: 403 });
      if (request.method === "POST" && ["/v1/scheduler/register-node", "/v1/scheduler/lease-renew", "/v1/scheduler/complete", "/v1/scheduler/schedule-tick"].includes(url.pathname)) {
        const host = authenticateBody(request, env);
        const gateway = authenticateGatewayService(request, env);
        if (!host && !gateway) return Response.json({ error: "SERVICE_AUTH_REQUIRED" }, { status: 401 });
        if (host && url.pathname !== "/v1/scheduler/schedule-tick") {
          const body = await request.clone().json() as Record<string, unknown>;
          if (body.node_id !== host) return Response.json({ error: "BODY_SCOPE_FORBIDDEN" }, { status: 403 });
          if (url.pathname !== "/v1/scheduler/register-node") {
            const scheduler = env.ANSTS_SCHEDULER_CELL.get(env.ANSTS_SCHEDULER_CELL.idFromName(url.searchParams.get("shard") ?? "global-shard-0"));
            const tasks = (await (await scheduler.fetch(new Request("https://cell/v1/scheduler/tasks"))).json() as { tasks: Record<string, unknown>[] }).tasks;
            const task = tasks.find(task => task.task_id === body.task_id);
            if (!task || task.assigned_node_id !== host || (body.tenant_id && task.tenant_id !== body.tenant_id)) return Response.json({ error: "BODY_SCOPE_FORBIDDEN" }, { status: 403 });
          }
        }
      }
      if (url.pathname === "/v1/scheduler/resolve-uncertain" && !authenticateBodyControl(request, env)) return Response.json({ error: "OPERATOR_ROUTE_REQUIRED" }, { status: 403 });
      if (url.pathname === "/v1/scheduler/suspend-uncertain" && !authenticateBodyControl(request, env)) {
        const host = authenticateBody(request, env);
        if (!host || (!env.ANSTS_BODY_CELL && !env.ANSTS_BODY_WORKER_URL)) return Response.json({ error: "BODY_AUTH_REQUIRED" }, { status: 401 });
        const body = await request.clone().json() as Record<string, unknown>;
        if (body.node_id !== host) return Response.json({ error: "BODY_SCOPE_FORBIDDEN" }, { status: 403 });
        const state = env.ANSTS_BODY_WORKER_URL
          ? await fetch(new URL(`/v1/body/state?taskId=${encodeURIComponent(String(body.task_id))}`, env.ANSTS_BODY_WORKER_URL), { headers: { authorization: request.headers.get("authorization") ?? "", "x-ansts-body-id": host }, redirect: "error", signal: AbortSignal.timeout(10000) })
          : await env.ANSTS_BODY_CELL!.get(env.ANSTS_BODY_CELL!.idFromName(`${env.ANSTS_BODY_NAMESPACE_PREFIX ?? ""}execution:${String(body.task_id)}`)).fetch(new Request("https://cell/v1/body/state"));
        const execution = (await state.json() as { execution?: Record<string, unknown> }).execution;
        if (!execution || execution.phase !== "UNCERTAIN" || execution.tenant_id !== body.tenant_id || execution.task_id !== body.task_id) return Response.json({ error: "UNCERTAIN_EXECUTION_REQUIRED" }, { status: 409 });
      }
      const shardName = url.searchParams.get("shard") ?? "global-shard-0";
      const id = env.ANSTS_SCHEDULER_CELL.idFromName(shardName);
      const stub = env.ANSTS_SCHEDULER_CELL.get(id);
      if (url.pathname === "/v1/scheduler/submit" && request.method === "POST") {
        const body = await request.json() as Record<string, unknown>;
        const tenantId=String(body.tenant_id??"");
        if(env.ANSTS_ADMISSION_MODE==="enforce" &&
           (env.ANSTS_ADMISSION_PILOT_TENANTS??"").split(",").some(id=>id.trim()===tenantId)){
          if(shardName!=="global-shard-0")return Response.json({code:"ADMISSION_PILOT_WRONG_SHARD"},{status:409});
          const occupancy=await checkPilotOccupancy(env,tenantId);
          if(!occupancy.ok)return occupancy;
        }
        const tasks = (await (await stub.fetch(new Request("https://cell/v1/scheduler/tasks"))).json() as { tasks: Record<string, unknown>[] }).tasks;
        const legacy = tasks.find(task => task.task_id === body.task_id && task.tenant_id === body.tenant_id);
        if (!legacy) body.task_id = normalizeTaskStorageId(String(body.tenant_id ?? ""), String(body.task_id ?? ""));
        return stub.fetch(new Request(request.url, { method: "POST", headers: request.headers, body: JSON.stringify(body) }));
      }
      return stub.fetch(request);
    }
    if (url.pathname.startsWith("/v1/harness") && env.ANSTS_HARNESS_CELL) {
      const taskId = url.searchParams.get("taskId") ?? "task-default";
      const id = env.ANSTS_HARNESS_CELL.idFromName(taskId);
      const stub = env.ANSTS_HARNESS_CELL.get(id);
      return stub.fetch(request);
    }
    if (url.pathname.startsWith("/v1/tunnel") && env.ANSTS_TUNNEL_CELL) {
      const hostId = url.searchParams.get("hostId") ?? "body-host-01";
      const id = env.ANSTS_TUNNEL_CELL.idFromName(hostId);
      const stub = env.ANSTS_TUNNEL_CELL.get(id);
      return stub.fetch(request);
    }
    return new Response(JSON.stringify({ error: "NOT_FOUND" }), { status: 404 });
  },

  async queue(batch: QueueBatchLike<Record<string, unknown>>, env?: CloudflareCelldEnv): Promise<void> {
    for (const msg of batch.messages) {
      try {
        const body = msg.body ?? {};
        if (env?.ANSTS_SCHEDULER_CELL && (body.task_id || body.prompt)) {
          const shardName = typeof body.shard === "string" ? body.shard : "global-shard-0";
          const id = env.ANSTS_SCHEDULER_CELL.idFromName(shardName);
          const stub = env.ANSTS_SCHEDULER_CELL.get(id);
          const resp = await stub.fetch(
            new Request("https://cell.internal/v1/scheduler/submit", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ ...body, task_id: normalizeTaskStorageId(String(body.tenant_id ?? ""), String(body.task_id ?? "")) }),
            })
          );
          if (!resp.ok && resp.status >= 500) {
            msg.retry();
            continue;
          }
        }
        msg.ack();
      } catch {
        msg.retry();
      }
    }
  },
};
