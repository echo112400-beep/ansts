import {rejectUnavailableRegionPlacement} from '../control/region_placement_gate.js';
import {handleRegionCatalog,regionAdminPath,regionSimulationPath,regionProbePath,regionPlacementTrialPath,type RegionApiEnv} from '../control/admin_region_api.js';
import {handleConfigPreflight,isConfigPreflightPath,type ConfigPreflightEnv} from './config_preflight_api.js';
import {handleFinopsApi,isFinopsPath} from '../control/admin_finops_api.js';
import {handleCapacityApi,type CapacityApiEnv} from '../control/admin_capacity_api.js';
/**
 * @fileoverview Unified control-plane gateway on denoland/celld.
 * AnstsGatewayCell stores dispatch, lease, and operator-session rows in its SQLite Durable Object.
 * Gateway HTTP routes in this module record session and lease state in this cell, then call the
 * scheduler worker over HTTP at ANSTS_SCHEDULER_URL. Sandbox execution stays on a Body process.
 */
import { handleDirectoryApi, DirectoryApiEnv } from "../control/admin_directory_api.js";
import { ConsoleRbacAuthGuard, ConsoleRole } from "../dashboard/console_rbac_guard.js";
import { TaskSpecValidationError, TaskSpecValidator } from "../gateway/validator.js";
import { DurableObjectNamespaceLike, DurableObjectStateLike } from "./env.js";
import { publicTaskId, taskStorageId } from "./task_identity.js";
import { gatewayPrincipal, gatewayAuthorizationError, tenantAllowed } from "./gateway_authorization.js";
import { controlPlaneFetch } from "./control_plane_fetch.js";
import { oidcPrincipal, OidcConfiguration } from "./oidc_identity.js";
import { collectSystemTopology } from "./system_topology.js";
import { githubConfigured } from "./github_identity.js";
import { githubProfileForSubject } from "./github_profile.js";
import { applicationSession, githubAuthRequest, GithubConfiguration } from "./github_identity.js";

export interface UnifiedGatewayEnv extends OidcConfiguration, GithubConfiguration, DirectoryApiEnv, CapacityApiEnv, ConfigPreflightEnv, RegionApiEnv {
  ANSTS_GATEWAY_CELL?: DurableObjectNamespaceLike;
  /** Base URL of the separate scheduler celld worker. Empty or unset fail-closes cross-worker calls. */
  ANSTS_SCHEDULER_URL?: string;
  ANSTS_SCHEDULER_URLS?: string;
  ANSTS_GATEWAY_STATE_MODE?: string;
  ANSTS_AGENT_ENABLED?: string;
  ANSTS_SYSTEM_TOPOLOGY_JSON?: string;
  ANSTS_SANDBOX_IMAGE?: string;
  ANSTS_SCHEDULER_SERVICE_TOKEN?: string;
  ANSTS_USER_AUTH_SECRET?: string;
  ANSTS_USER_AUTH_PREVIOUS_SECRET?: string;
  ANSTS_USER_AUTH_PREVIOUS_UNTIL?: string;
  ANSTS_CONSOLE_RBAC_SECRET?: string;
  ANSTS_MONITORING_TOKEN?: string;
  CELLD_REGION?: string;
}

const JSON_HEADERS = { "content-type": "application/json" };

function json(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });
}

export class AnstsGatewayCell {
  private readonly ctx: DurableObjectStateLike;

  constructor(ctx: DurableObjectStateLike) {
    this.ctx = ctx;
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS gateway_dispatches (task_id TEXT PRIMARY KEY, tenant_id TEXT, prompt TEXT, state TEXT, assigned_node_id TEXT, fencing_token INTEGER, payload_json TEXT, updated_at INTEGER)"
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS gateway_leases (task_id TEXT PRIMARY KEY, holder_id TEXT, node_id TEXT, fencing_token INTEGER, status TEXT, updated_at INTEGER)"
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS gateway_sessions (session_id TEXT PRIMARY KEY, subject TEXT, role TEXT, expires_at INTEGER, payload_json TEXT)"
    );
  }

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname.replace(/^\/v1\/gateway(?=\/|$)/, "") || "/";
    if (request.method === "POST" && path === "/dispatch") {
      const body = (await request.json()) as Record<string, unknown>;
      const taskId = String(body.task_id ?? "");
      if (!taskId) return json({ error: "TASK_ID_REQUIRED" }, 400);
      const now = Date.now();
      const assigned = String(body.assigned_node_id ?? "");
      const fencing = Number(body.fencing_token ?? 0);
      this.ctx.storage.sql.exec(
        "INSERT OR REPLACE INTO gateway_dispatches (task_id, tenant_id, prompt, state, assigned_node_id, fencing_token, payload_json, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        taskId,
        String(body.tenant_id ?? ""),
        String(body.prompt ?? ""),
        String(body.state ?? "QUEUED"),
        assigned,
        fencing,
        JSON.stringify(body),
        now
      );
      this.ctx.storage.sql.exec(
        "INSERT OR REPLACE INTO gateway_leases (task_id, holder_id, node_id, fencing_token, status, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        taskId,
        String(body.holder_id ?? "celld-gateway"),
        assigned,
        fencing,
        assigned ? "HELD" : "PENDING",
        now
      );
      return json({ recorded: true, cellId: this.ctx.id.toString(), task_id: taskId }, 202);
    }
    if (request.method === "POST" && path === "/session") {
      const body = (await request.json()) as Record<string, unknown>;
      const sessionId = String(body.session_id ?? "");
      if (!sessionId) return json({ error: "SESSION_ID_REQUIRED" }, 400);
      this.ctx.storage.sql.exec(
        "INSERT OR REPLACE INTO gateway_sessions (session_id, subject, role, expires_at, payload_json) VALUES (?, ?, ?, ?, ?)",
        sessionId,
        String(body.subject ?? ""),
        String(body.role ?? ""),
        Number(body.expires_at ?? 0),
        JSON.stringify({ subject: body.subject, role: body.role })
      );
      return json({ recorded: true, session_id: sessionId }, 200);
    }
    if (request.method === "POST" && path === "/release") {
      const body = (await request.json()) as Record<string, unknown>;
      if (body.confirm !== true) {
        return json({ error: "CONFIRMATION_REQUIRED", message: "confirm must be true" }, 400);
      }
      const taskId = String(body.task_id ?? "");
      if (!taskId) return json({ error: "TASK_ID_REQUIRED" }, 400);
      const existing = this.ctx.storage.sql
        .exec<Record<string, unknown>>("SELECT task_id, status FROM gateway_leases WHERE task_id = ?", taskId)
        .toArray()[0];
      if (!existing) return json({ error: "LEASE_NOT_FOUND", task_id: taskId }, 404);
      this.ctx.storage.sql.exec(
        "UPDATE gateway_leases SET status = ?, updated_at = ? WHERE task_id = ?",
        "RELEASED",
        Date.now(),
        taskId
      );
      return json({ released: true, task_id: taskId, status: "RELEASED", cellId: this.ctx.id.toString() }, 200);
    }
    if (request.method === "GET" && path === "/state") {
      return json(this.snapshot(), 200);
    }
    return json({ error: "NOT_FOUND" }, 404);
  }

  private snapshot(): {
    cellId: string;
    dispatches: Record<string, unknown>[];
    leases: Record<string, unknown>[];
    sessions: Record<string, unknown>[];
    activeLeases: number;
  } {
    const dispatches = this.ctx.storage.sql
      .exec<Record<string, unknown>>("SELECT * FROM gateway_dispatches")
      .toArray();
    const leases = this.ctx.storage.sql.exec<Record<string, unknown>>("SELECT * FROM gateway_leases").toArray();
    const sessions = this.ctx.storage.sql.exec<Record<string, unknown>>("SELECT * FROM gateway_sessions").toArray();
    return {
      cellId: this.ctx.id.toString(),
      dispatches,
      leases,
      sessions,
      activeLeases: leases.filter((row) => String(row.status) === "HELD").length,
    };
  }
}

function gatewayStub(env: UnifiedGatewayEnv) {
  if (env.ANSTS_GATEWAY_STATE_MODE === "remote") return {
    async fetch(request: Request): Promise<Response> {
      const url = new URL(request.url);
      return schedulerFetch(env, "/internal" + url.pathname + url.search, { method: request.method, headers: JSON_HEADERS, body: request.method === "GET" ? undefined : await request.arrayBuffer() });
    },
  };
  const ns = env.ANSTS_GATEWAY_CELL;
  if (!ns) return null;
  return ns.get(ns.idFromName("gateway-control-plane"));
}

function schedulerBase(env: UnifiedGatewayEnv): string | null {
  const raw = env.ANSTS_SCHEDULER_URL?.trim() ?? "";
  if (!raw) return null;
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
  } catch {
    return null;
  }
  return raw.replace(/\/+$/, "");
}

function schedulerClientError(body: Record<string, unknown>, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...JSON_HEADERS, "x-ansts-scheduler-client": "gateway" },
  });
}

/** Calls the scheduler worker. A missing URL or a refused connection is a local 503, not an in-process fallback. */
async function schedulerFetch(env: UnifiedGatewayEnv, path: string, init?: RequestInit): Promise<Response> {
  const base = schedulerBase(env);
  if (!base) {
    return schedulerClientError(
      {
        error: "SCHEDULER_URL_UNCONFIGURED",
        message: "Set ANSTS_SCHEDULER_URL to the scheduler worker. The gateway process does not embed the scheduler.",
      },
      503
    );
  }
  try {
    const headers = new Headers(init?.headers);
    if (env.ANSTS_SCHEDULER_SERVICE_TOKEN) headers.set("authorization", `Gateway ${env.ANSTS_SCHEDULER_SERVICE_TOKEN}`);
    return await controlPlaneFetch(env.ANSTS_SCHEDULER_URLS || base, path, { ...init, headers });
  } catch (err) {
    return schedulerClientError(
      {
        error: "SCHEDULER_UNREACHABLE",
        schedulerUrl: base,
        message: (err as Error).message,
      },
      503
    );
  }
}

async function readGatewayState(env: UnifiedGatewayEnv): Promise<Record<string, unknown> | null> {
  const stub = gatewayStub(env);
  if (!stub) return null;
  const resp = await stub.fetch(new Request("https://cell.internal/v1/gateway/state"));
  if (!resp.ok) return null;
  const state = (await resp.json()) as Record<string, unknown>;
  const listed = await listSchedulerTasks(env);
  if (!listed.failure && Array.isArray(state.leases)) {
    for (const lease of state.leases as Record<string, unknown>[]) {
      const task = listed.tasks.find(task => task.task_id === lease.task_id);
      if (task && ["COMPLETED", "SUCCEEDED", "FAILED", "CANCELLED", "DEAD_LETTER"].includes(String(task.state))) lease.status = "RELEASED";
    }
    state.activeLeases = (state.leases as Record<string, unknown>[]).filter(lease => lease.status === "HELD").length;
  }
  return state;
}

async function listSchedulerTasks(env: UnifiedGatewayEnv): Promise<{ tasks: Record<string, unknown>[]; failure: Response | null }> {
  const resp = await schedulerFetch(env, "/v1/scheduler/tasks");
  if (resp.headers.get("x-ansts-scheduler-client") === "gateway" || !resp.ok) {
    return { tasks: [], failure: resp };
  }
  const body = (await resp.json()) as { tasks?: Record<string, unknown>[] };
  return { tasks: Array.isArray(body.tasks) ? body.tasks : [], failure: null };
}

function isGatewayPath(method: string, pathname: string): boolean {
  if((pathname===regionAdminPath||pathname===regionSimulationPath||pathname===regionProbePath||pathname===regionPlacementTrialPath))return true;
  if(isFinopsPath(pathname)||isConfigPreflightPath(pathname))return true;
  if(/^\/v1\/admin\/tenants\/[^/]+\/(usage|reservation-summary)$/.test(pathname))return true;
  if(pathname.startsWith("/v1/admin/capacity/"))return true;
  if (/^\/v1\/admin\/tenants\/[^/]+\/quota$/.test(pathname) || pathname === "/v1/admin/archives" || /^\/v1\/admin\/archives\/[^/]+$/.test(pathname) || /^\/v1\/admin\/operations\/[^/]+$/.test(pathname) || pathname === "/v1/admin/cost-centers" || /^\/v1\/admin\/cost-centers\/[^/]+$/.test(pathname) || pathname === "/v1/admin/tenants" || pathname === "/v1/admin/audit" || /^\/v1\/admin\/tenants\/[^/]+$/.test(pathname)) return true;
  if (method === "GET" && (pathname === "/v1/auth/me" || pathname === "/v1/auth/profile")) return true;
  if (method === "GET" && pathname === "/v1/console/system") return true;
  if (method === "GET" && pathname === "/v1/console/management") return true;
  if (method === "GET" && pathname === "/v1/console/tasks") return true;
  if (method === "GET" && (pathname === "/health" || pathname === "/healthz" || pathname === "/readyz")) return true;
  if (method === "POST" && pathname === "/v1/tasks") return true;
  if (method === "POST" && /^\/v1\/tasks\/[^/]+\/(cancel|reconcile)$/.test(pathname)) return true;
  if (method === "GET" && pathname.startsWith("/v1/tasks/") && pathname.length > "/v1/tasks/".length) return true;
  if (method === "POST" && pathname === "/v1beta1/tasks/dispatch") return true;
  if (method === "GET" && (pathname === "/v1beta1/gateway/state" || pathname === "/v1beta1/console/gateway")) return true;
  if (method === "POST" && pathname === "/v1beta1/gateway/leases/release") return true;
  return false;
}

/**
 * Gateway half of the celld worker fetch. Returns null when the path belongs to the scheduler,
 * harness, or tunnel routes.
 */
export async function handleUnifiedGatewayRequest(request: Request, env: UnifiedGatewayEnv, background?:(work:Promise<void>)=>void): Promise<Response | null> {
  const url = new URL(request.url);
  const github = await githubAuthRequest(request, env);
  if (github) return github;
  const session = applicationSession(request, env);
  if (session && !request.headers.get("authorization")) {
    if (request.method !== "GET" && request.headers.get("origin") !== env.ANSTS_PUBLIC_ORIGIN) return json({ error: "CSRF_REJECTED" }, 403);
    const headers = new Headers(request.headers); headers.set("authorization", `Bearer ${session}`); request = new Request(request, { headers });
  }
  if (!isGatewayPath(request.method, url.pathname)) return null;
  const publicHealth = request.method === "GET" && ["/health", "/healthz", "/readyz"].includes(url.pathname);
  const principal = publicHealth ? null : env.ANSTS_OIDC_ISSUER ? await oidcPrincipal(request, env) : gatewayPrincipal(request, env.ANSTS_USER_AUTH_SECRET, env.ANSTS_USER_AUTH_PREVIOUS_SECRET, Number(env.ANSTS_USER_AUTH_PREVIOUS_UNTIL));
  if (!publicHealth) {
    if (!principal) return json({ error: "USER_AUTH_REQUIRED" }, 401);
    const denied = gatewayAuthorizationError(request, principal);
    if (denied) return denied;
    const capacityTenantRoute = url.pathname === "/v1/admin/capacity/requests" ||
      url.pathname === "/v1/admin/capacity/catalog";
    if (capacityTenantRoute) {
      const capacityDenied = gatewayAuthorizationError(request, principal, url.searchParams.get("tenantId"));
      if (capacityDenied) return capacityDenied;
    }
    if (request.method === "POST" && !capacityTenantRoute && url.pathname !== "/v1beta1/gateway/leases/release") {
      let body: Record<string, unknown>;
      try {
        const raw = await request.clone().text();
        if (raw.length > 65536) return json({ error: "PAYLOAD_TOO_LARGE" }, 413);
        body = JSON.parse(raw) as Record<string, unknown>;
        if (!body || typeof body !== "object" || Array.isArray(body)) return json({ error: "INVALID_JSON" }, 400);
      }
      catch { return json({ error: "INVALID_JSON" }, 400); }
      const tenantDenied = gatewayAuthorizationError(request, principal, body.tenant_id ?? body.tenantId);
      if (tenantDenied) return tenantDenied;
    }
  }
  if((url.pathname===regionAdminPath||url.pathname===regionSimulationPath||url.pathname===regionProbePath||url.pathname===regionPlacementTrialPath)&&principal)return handleRegionCatalog(request,env,principal);
  if(isConfigPreflightPath(url.pathname)&&principal)return handleConfigPreflight(request,env,principal);
  if(isFinopsPath(url.pathname)&&principal)return handleFinopsApi(request,env,principal,background);
  if(url.pathname.startsWith("/v1/admin/capacity/")&&principal)return handleCapacityApi(request,env,principal);
  if (url.pathname.startsWith("/v1/admin/") && principal) return handleDirectoryApi(request, env, principal);
  if (request.method === "GET" && url.pathname === "/v1/auth/me" && principal) {
    return Response.json({ authenticated: true, user: { sub: principal.sub, role: principal.role, scopeTenant: principal.scopeTenant, scopeRegion: principal.scopeRegion, canWrite: !principal.readOnly }, expiresAt: principal.exp * 1000 }, { headers: { "cache-control": "no-store" } });
  }
  if (request.method === "GET" && url.pathname === "/v1/auth/profile" && principal) {
    try { return Response.json({ profile: await githubProfileForSubject(principal.sub, principal.profile) }, { headers: { "cache-control": "no-store" } }); }
    catch { return Response.json({ error: "GITHUB_PROFILE_UNAVAILABLE" }, { status: 503, headers: { "cache-control": "no-store" } }); }
  }
  if (request.method === "GET" && url.pathname === "/v1/console/system" && principal) {
    if (principal.role !== "SRE_ADMIN") return Response.json({ error: "SYSTEM_ADMIN_REQUIRED" }, { status: 403, headers: { "cache-control": "no-store" } });
    try { return Response.json(await collectSystemTopology(env, githubConfigured(env)), { headers: { "cache-control": "no-store" } }); }
    catch { return Response.json({ error: "SYSTEM_TOPOLOGY_UNAVAILABLE" }, { status: 503, headers: { "cache-control": "no-store" } }); }
  }
  if (request.method === "GET" && url.pathname === "/v1/console/management" && principal) {
    const listed = await listSchedulerTasks(env);
    if (listed.failure) return listed.failure;
    const tenantId=url.searchParams.get("tenantId");
    const region=url.searchParams.get("region");
    if(tenantId!==null&&!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(tenantId))return json({error:"INVALID_TENANT_ID"},422);
    if(region!==null&&(!region.trim()||region.length>128))return json({error:"INVALID_REGION"},422);
    let allowedTenants:Set<string>|undefined;
    if(region!==null||principal.scopeRegion!=="all"){
      allowedTenants=new Set();
      let cursor:string|null=null;
      do {
        const directory=new URL("/v1/admin/tenants",url);
        directory.searchParams.set("limit","100");
        if(region!==null)directory.searchParams.set("region",region);
        if(tenantId!==null)directory.searchParams.set("tenantId",tenantId);
        if(cursor)directory.searchParams.set("cursor",cursor);
        const page=await handleDirectoryApi(new Request(directory),env,principal);
        if(!page.ok)return page;
        const data=await page.json() as {items:{id:string}[];nextCursor:string|null};
        for(const record of data.items)allowedTenants.add(record.id);
        cursor=data.nextCursor;
      } while(cursor);
    }
    const tasks = listed.tasks
      .filter(t=>tenantAllowed(principal,t.tenant_id)&&(!tenantId||t.tenant_id===tenantId)&&(!allowedTenants||allowedTenants.has(String(t.tenant_id))))
      .sort((a,b)=>Number(b.created_at??0)-Number(a.created_at??0));
    const category=(t:Record<string,unknown>)=>t.execution_outcome==="CANCELLED"?"cancelled":t.state==="SUSPENDED_RECONCILIATION"?"uncertain":["COMPLETED","SUCCEEDED"].includes(String(t.state))?"completed":["FAILED","DEAD_LETTER"].includes(String(t.state))?"failed":["SUBMITTED","PENDING","QUEUED","CREATED"].includes(String(t.state))?"queued":"active";
    const counts:Record<string,number>={total:tasks.length,active:0,queued:0,completed:0,failed:0,cancelled:0,uncertain:0};
    const tenants:Record<string,{tenant:string;total:number;active:number;failed:number;uncertain:number}>=Object.create(null);
    for(const t of tasks){const c=category(t);counts[c]++;const id=String(t.tenant_id);const row=tenants[id]??(tenants[id]={tenant:id,total:0,active:0,failed:0,uncertain:0});row.total++;if(c==="active"||c==="failed"||c==="uncertain")row[c]++;}
    return Response.json({counts,tenants:Object.values(tenants),total:tasks.length,truncated:tasks.length>1000,tasks:tasks.slice(0,1000).map(t=>({task_id:publicTaskId(String(t.task_id))?.taskId??t.task_id,tenant_id:t.tenant_id,state:t.state,category:category(t),execution_kind:(t.execution as {kind?:string}|undefined)?.kind??"trace",execution_outcome:t.execution_outcome??null,assigned_node_id:t.assigned_node_id??null,created_at:t.created_at??null,priority:typeof t.priority==="number"?t.priority:null,parent_task_id:typeof t.parent_task_id==="string"&&(publicTaskId(t.parent_task_id)?.tenantId??t.tenant_id)===t.tenant_id?(publicTaskId(t.parent_task_id)?.taskId??t.parent_task_id):null,dependency_count:Array.isArray(t.dependencies)?t.dependencies.length:null,prompt_preview:String(t.prompt??"").slice(0,160),cancel_requested:t.cancel_requested?{actor:(t.cancel_requested as Record<string,unknown>).actor,reason:(t.cancel_requested as Record<string,unknown>).reason}:null})),configuration:{sandboxImage:env.ANSTS_SANDBOX_IMAGE??null,agentEnabled:env.ANSTS_AGENT_ENABLED==="true",githubEnabled:githubConfigured(env),identityProvider:env.ANSTS_OIDC_ISSUER?"OIDC":"Application JWT / GitHub",gatewayState:env.ANSTS_GATEWAY_STATE_MODE??"binding",runtimeProfile:env.ANSTS_RUNTIME_PROFILE??"unknown"},refreshedAt:Date.now()}, {headers:{"cache-control":"no-store"}});
  }
  if (request.method === "GET" && url.pathname === "/v1/console/tasks" && principal) {
    const tenant = url.searchParams.get("tenantId");
    if (tenant && !tenantAllowed(principal, tenant)) return json({ error: "TENANT_FORBIDDEN" }, 403);
    const listed = await listSchedulerTasks(env);
    if (listed.failure) return listed.failure;
    const tasks = listed.tasks.filter(task => tenantAllowed(principal, task.tenant_id) && (!tenant || task.tenant_id === tenant)).sort((a, b) => Number(b.created_at ?? 0) - Number(a.created_at ?? 0));
    const category = (task: Record<string, unknown>) => task.execution_outcome === "CANCELLED" ? "cancelled" : task.state === "SUSPENDED_RECONCILIATION" ? "uncertain" : ["COMPLETED", "SUCCEEDED"].includes(String(task.state)) ? "completed" : ["FAILED", "DEAD_LETTER"].includes(String(task.state)) ? "failed" : ["SUBMITTED", "PENDING", "QUEUED", "CREATED"].includes(String(task.state)) ? "queued" : "active";
    const counts: Record<string, number> = { total: tasks.length, active: 0, queued: 0, completed: 0, failed: 0, cancelled: 0, uncertain: 0 };
    for (const task of tasks) counts[category(task)]++;
    const query = (url.searchParams.get("q") ?? "").toLowerCase().slice(0, 128);
    const status = url.searchParams.get("status") ?? "all";
    const filtered = tasks.filter(task => (status === "all" || category(task) === status) && `${publicTaskId(String(task.task_id))?.taskId ?? task.task_id} ${task.tenant_id}`.toLowerCase().includes(query));
    const limit = Math.floor(Math.min(50, Math.max(1, Number(url.searchParams.get("limit")) || 50)));
    const page = Math.max(1, Math.min(1000000, Math.floor(Number(url.searchParams.get("page")) || 1)));
    return Response.json({ total: filtered.length, counts, page, limit, attention: tasks.filter(task => category(task) === "uncertain").slice(0, 3).map(task => ({ task_id: publicTaskId(String(task.task_id))?.taskId ?? task.task_id, tenant_id: task.tenant_id, state: task.state, category: "uncertain", execution_kind: (task.execution as { kind?: string } | undefined)?.kind ?? "trace", assigned_node_id: task.assigned_node_id ?? null, created_at: task.created_at ?? null })), capabilities: { sandboxImage: env.ANSTS_SANDBOX_IMAGE ?? null, agentEnabled: env.ANSTS_AGENT_ENABLED === "true" }, tasks: filtered.slice((page - 1) * limit, page * limit).map(task => ({ task_id: publicTaskId(String(task.task_id))?.taskId ?? task.task_id, tenant_id: task.tenant_id, state: task.state, category: category(task), execution_outcome: task.execution_outcome ?? null, execution_kind: (task.execution as { kind?: string } | undefined)?.kind ?? "trace", assigned_node_id: task.assigned_node_id ?? null, created_at: task.created_at ?? null })), refreshedAt: Date.now() }, { headers: { "cache-control": "no-store" } });
  }
  if (!env.ANSTS_GATEWAY_CELL && env.ANSTS_GATEWAY_STATE_MODE !== "remote") {
    return json({ error: "GATEWAY_CELL_UNBOUND", controlPlane: "celld-gateway" }, 503);
  }
  if (request.method === "GET" && (url.pathname === "/health" || url.pathname === "/healthz" || url.pathname === "/readyz")) {
    const gateway = await readGatewayState(env);
    const peer = await schedulerFetch(env, "/healthz");
    const peerIsLocal = peer.headers.get("x-ansts-scheduler-client") === "gateway";
    let peerPlane: string | null = null;
    if (!peerIsLocal && peer.ok) {
      const peerBody = (await peer.json().catch(() => ({}))) as { controlPlane?: string };
      peerPlane = peerBody.controlPlane ?? null;
    }
    return json(
      {
        status: "ok",
        healthy: !!gateway && !peerIsLocal && peer.ok && peerPlane === "celld-scheduler",
        runtime: "denoland/celld-cloudflare-workers",
        region: env.CELLD_REGION ?? "local",
        controlPlane: "celld-gateway",
        planes: {
          gateway: "durable-object",
          scheduler: "remote-http",
          bodyExecution: "external-node-process",
        },
        gateway: {
          cellId: gateway?.cellId ?? null,
          dispatches: Array.isArray(gateway?.dispatches) ? gateway.dispatches.length : 0,
          activeLeases: Number(gateway?.activeLeases ?? 0),
        },
        scheduler: {
          url: schedulerBase(env),
          reachable: !peerIsLocal && peer.ok && peerPlane === "celld-scheduler",
        },
      },
      url.pathname === "/readyz" && (!gateway || peerIsLocal || !peer.ok || peerPlane !== "celld-scheduler") ? 503 : 200
    );
  }
  if (request.method === "GET" && url.pathname === "/v1beta1/gateway/state") {
    const gateway = await readGatewayState(env);
    if (gateway) {
      if (principal?.scopeTenant !== "all") {
        for (const key of ["leases", "dispatches"]) if (Array.isArray(gateway[key])) gateway[key] = (gateway[key] as Record<string, unknown>[]).filter(row => tenantAllowed(principal!, row.tenant_id ?? publicTaskId(String(row.task_id))?.tenantId));
        gateway.sessions = [];
        gateway.activeLeases = (gateway.leases as Record<string, unknown>[] ?? []).filter(row => row.status === "HELD").length;
      }
      for (const key of ["leases", "dispatches"]) {
        if (Array.isArray(gateway[key])) gateway[key] = (gateway[key] as Record<string, unknown>[]).map(row => ({ ...row, executionTaskId: row.task_id, task_id: publicTaskId(String(row.task_id))?.taskId ?? row.task_id }));
      }
    }
    return json(gateway ?? { error: "GATEWAY_STATE_UNAVAILABLE" }, gateway ? 200 : 503);
  }
  if (request.method === "GET" && url.pathname === "/v1beta1/console/gateway") {
    const gateway = await readGatewayState(env);
    const listed = await listSchedulerTasks(env);
    if (listed.failure) return listed.failure;
    const tasks = listed.tasks.filter(task => tenantAllowed(principal!, task.tenant_id));
    return json(
      {
        controlPlane: "celld-gateway",
        totalTasks: tasks.length,
        gatewayDispatches: Array.isArray(gateway?.dispatches) ? (gateway.dispatches as Record<string, unknown>[]).filter(row => tenantAllowed(principal!, row.tenant_id)).length : 0,
        activeLeases: Array.isArray(gateway?.leases) ? (gateway.leases as Record<string, unknown>[]).filter(row => row.status === "HELD" && tenantAllowed(principal!, row.tenant_id ?? publicTaskId(String(row.task_id))?.tenantId)).length : 0,
        bodyExecution: "external-node-process",
        gatewayCellId: gateway?.cellId ?? null,
      },
      200
    );
  }
  if ((request.method === "GET" || request.method === "POST") && url.pathname.startsWith("/v1/tasks/")) {
    const cancelling = request.method === "POST" && url.pathname.endsWith("/cancel");
    const reconciling = request.method === "POST" && url.pathname.endsWith("/reconcile");
    const taskId = decodeURIComponent(url.pathname.slice("/v1/tasks/".length, cancelling ? -7 : reconciling ? -10 : undefined));
    const listed = await listSchedulerTasks(env);
    if (listed.failure) return listed.failure;
    const tenantId = url.searchParams.get("tenantId");
    if (tenantId && !tenantAllowed(principal!, tenantId)) return json({ error: "TENANT_FORBIDDEN" }, 403);
    const candidates = listed.tasks.filter(task => {
      const identity = publicTaskId(String(task.task_id ?? task.taskId ?? ""));
      return (identity?.taskId ?? String(task.task_id ?? task.taskId ?? "")) === taskId && tenantAllowed(principal!, identity?.tenantId ?? task.tenant_id) && (!tenantId || (identity?.tenantId ?? task.tenant_id) === tenantId);
    });
    if (candidates.length > 1) return json({ error: "TENANT_REQUIRED" }, 400);
    const found = candidates[0];
    if (!found) return json({ error: "TASK_NOT_FOUND", task_id: taskId }, 404);
    if (reconciling) {
      if (principal!.role !== "SRE_ADMIN") return json({ error: "ADMIN_REQUIRED" }, 403);
      const body = await request.json() as Record<string, unknown>;
      if (body.action === "resume_from_checkpoint" && (found.execution as { kind?: string } | undefined)?.kind !== "agent") return json({ error: "AGENT_CHECKPOINT_REQUIRED" }, 400);
      return schedulerFetch(env, `/v1/body/reconcile?taskId=${encodeURIComponent(String(found.task_id))}`, { method: "POST", headers: { ...JSON_HEADERS, "x-ansts-operator-sub": principal!.sub }, body: JSON.stringify({ ...body, actor: principal!.sub, task_id: found.task_id, tenant_id: found.tenant_id }) });
    }
    if (cancelling) {
      const body = await request.json() as Record<string, unknown>;
      if ((body.tenant_id ?? body.tenantId) !== found.tenant_id || typeof body.reason !== "string" || !body.reason.trim() || body.reason.length > 1024) return json({ error: "INVALID_CANCEL_REQUEST" }, 400);
      return schedulerFetch(env, "/v1/scheduler/cancel", { method: "POST", headers: JSON_HEADERS, body: JSON.stringify({ task_id: found.task_id, tenant_id: found.tenant_id, actor: principal!.sub, reason: body.reason }) });
    }
    if (url.searchParams.get("includeResult") === "1") {
      return schedulerFetch(env, `/v1/body/result?taskId=${encodeURIComponent(String(found.task_id))}&tenantId=${encodeURIComponent(String(found.tenant_id))}`);
    }
    const gateway = await readGatewayState(env);
    const leases = Array.isArray(gateway?.leases) ? (gateway.leases as Array<Record<string, unknown>>) : [];
    const lease = leases.find((row) => row.task_id === found.task_id) ?? null;
    return json({ ...found, executionTaskId: found.task_id, task_id: publicTaskId(String(found.task_id))?.taskId ?? found.task_id, controlPlane: "celld-gateway", gatewayLease: lease }, 200);
  }
  if (request.method === "POST" && (url.pathname === "/v1/tasks" || url.pathname === "/v1beta1/tasks/dispatch")) {
    return submitThroughCells(request, env, url.pathname === "/v1beta1/tasks/dispatch");
  }
  if (request.method === "POST" && url.pathname === "/v1beta1/gateway/leases/release") {
    return releaseLease(request, env);
  }
  return null;
}

async function submitThroughCells(request: Request, env: UnifiedGatewayEnv, dispatchShape: boolean): Promise<Response> {
  let body: Record<string, unknown>;
  try {
    body = (await request.json()) as Record<string, unknown>;
  } catch {
    return json({ error: "VALIDATION_ERROR", message: "JSON body required" }, 400);
  }
  const taskId = String(body.task_id ?? body.taskId ?? "");
  const tenantId = String(body.tenant_id ?? body.tenantId ?? "");
  const prompt = String(body.prompt ?? body.promptText ?? "");
  let spec;
  try {
    spec = TaskSpecValidator.validateAndNormalize({
      ...body,
      task_id: taskId,
      tenant_id: tenantId,
      prompt,
      priority: body.priority,
    });
  } catch (err) {
    if (err instanceof TaskSpecValidationError) {
      return json({ error: "VALIDATION_ERROR", field: err.field, message: err.message }, 400);
    }
    throw err;
  }
  // A constrained request must never fall back to the local execution path.
  const regionalDenial=rejectUnavailableRegionPlacement(body);
  if(regionalDenial)return regionalDenial;
  spec.task_id = taskStorageId(tenantId, taskId);
  const existing = await listSchedulerTasks(env);
  if (existing.failure) return existing.failure;
  const legacy = existing.tasks.find(task => task.task_id === taskId && task.tenant_id === tenantId);
  if (legacy) spec.task_id = String(legacy.task_id);
  const gateway = gatewayStub(env);
  if (!gateway) return json({ error: "GATEWAY_CELL_UNBOUND" }, 503);
  if (spec.execution && spec.execution.image !== env.ANSTS_SANDBOX_IMAGE) return json({ error: "SANDBOX_IMAGE_NOT_ENABLED" }, 503);
  if (spec.execution?.kind === "agent" && env.ANSTS_AGENT_ENABLED !== "true") return json({ error: "AGENT_EXECUTION_NOT_ENABLED" }, 503);
  const capacityReservationId = body.capacity_reservation_id;
  if (capacityReservationId !== undefined &&
      (typeof capacityReservationId !== "string" || !capacityReservationId.trim() || capacityReservationId.length > 128)) {
    return json({error:"INVALID_CAPACITY_INPUT"},422);
  }
  const submitResp = await schedulerFetch(env, "/v1/scheduler/submit", {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify({
      task_id: spec.task_id,
      capacity_reservation_id: capacityReservationId,
      tenant_id: spec.tenant_id,
      priority: spec.priority,
      execution: spec.execution,
      prompt: spec.prompt,
      max_turns: spec.max_turns,
      timeout_seconds: body.timeout_seconds==null?undefined:spec.timeout_seconds,
      resource_profile: spec.resource_profile,
    }),
  });
  if (submitResp.headers.get("x-ansts-scheduler-client") === "gateway") return submitResp;
  if (!submitResp.ok) {
    if (capacityReservationId !== undefined || submitResp.headers.get("content-type")?.startsWith("application/json")) return submitResp;
    return json({ error: "SCHEDULER_REJECTED", status: submitResp.status }, submitResp.status);
  }
  const submitted = (await submitResp.json()) as Record<string, unknown>;
  const tickResp = await schedulerFetch(env, "/v1/scheduler/schedule-tick", { method: "POST", headers: JSON_HEADERS });
  if (tickResp.headers.get("x-ansts-scheduler-client") === "gateway") return tickResp;
  const tick = tickResp.ok
    ? ((await tickResp.json()) as { placements?: Array<[string, string, number]> })
    : { placements: [] };
  const placements = Array.isArray(tick.placements) ? tick.placements : [];
  const placement = placements.find((row) => row[0] === spec.task_id) ?? null;
  const state = placement ? "LEASED" : String(submitted.state ?? "QUEUED");
  const assigned = placement ? placement[1] : "";
  const fencing = placement ? Number(placement[2]) : Number(submitted.lease_fencing_token ?? 0);
  const recordResp = await gateway.fetch(
    new Request("https://cell.internal/v1/gateway/dispatch", {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify({
        task_id: spec.task_id,
        tenant_id: spec.tenant_id,
        execution: spec.execution,
      prompt: spec.prompt,
        state,
        assigned_node_id: assigned,
        fencing_token: fencing,
        holder_id: "celld-gateway",
      }),
    })
  );
  if (!recordResp.ok) return json({ error: "GATEWAY_RECORD_FAILED" }, 503);
  const recorded = (await recordResp.json()) as { cellId?: string };
  if (dispatchShape) {
    return json(
      {
        dispatched: true,
        controlPlane: "celld-gateway",
        gatewayNodeId: env.CELLD_REGION ?? "celld-gateway",
        gatewayCellId: recorded.cellId ?? null,
        taskId,
        executionTaskId: spec.task_id,
        tenantId: spec.tenant_id,
        state,
        fencingToken: fencing,
        targetBodyHostId: assigned || null,
        bodyExecution: null,
        executionPlane: "external-body",
        deferred: "BODY_EXECUTION_IS_EXTERNAL",
        placements,
      },
      202
    );
  }
  return json(
    {
      task_id: taskId,
      executionTaskId: spec.task_id,
      state,
      assigned_node_id: assigned || null,
      lease_fencing_token: fencing,
      controlPlane: "celld-gateway",
      gatewayCellId: recorded.cellId ?? null,
      executionPlane: "external-body",
      bodyExecution: null,
      placements,
    },
    202
  );
}

async function releaseLease(request: Request, env: UnifiedGatewayEnv): Promise<Response> {
  const secret = env.ANSTS_CONSOLE_RBAC_SECRET?.trim() ?? "";
  if (secret.length < 16) {
    return json(
      {
        error: "AUTH_UNCONFIGURED",
        message: "Set ANSTS_CONSOLE_RBAC_SECRET before operator writes. No default secret is used.",
      },
      503
    );
  }
  let body: Record<string, unknown> = {};
  try {
    body = (await request.json()) as Record<string, unknown>;
  } catch {
    body = {};
  }
  const guard = new ConsoleRbacAuthGuard(secret);
  const verified = guard.verifyAuthorizationHeader(request.headers.get("authorization") ?? undefined);
  if (!verified.authenticated || !verified.claims) {
    return json({ error: verified.errorCode ?? "UNAUTHORIZED", message: verified.message }, 401);
  }
  const permission = guard.checkPermission({ role: verified.claims.role as ConsoleRole, isWriteMutation: true });
  if (!permission.allowed) {
    return json({ error: permission.error, message: permission.message }, permission.status);
  }
  const gateway = gatewayStub(env);
  if (!gateway) return json({ error: "GATEWAY_CELL_UNBOUND" }, 503);
  const sessionId = `${verified.claims.sub}:${verified.claims.iat}`;
  await gateway.fetch(
    new Request("https://cell.internal/v1/gateway/session", {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify({
        session_id: sessionId,
        subject: verified.claims.sub,
        role: verified.claims.role,
        expires_at: verified.claims.exp,
      }),
    })
  );
  if (body.confirm !== true) {
    return json({ error: "CONFIRMATION_REQUIRED", message: "confirm must be true" }, 400);
  }
  const taskId = String(body.task_id ?? body.taskId ?? "");
  const state = await readGatewayState(env);
  const leases = Array.isArray(state?.leases) ? (state.leases as Array<Record<string, unknown>>) : [];
  const tenantId = String(body.tenant_id ?? body.tenantId ?? (verified.claims.scopeTenant === "all" ? "" : verified.claims.scopeTenant));
  const matches = leases.filter(row => {
    const identity = publicTaskId(String(row.task_id));
    return (identity?.taskId ?? row.task_id) === taskId && (!tenantId || identity?.tenantId === tenantId);
  });
  if (matches.length > 1) return json({ error: "TENANT_REQUIRED" }, 400);
  const lease = matches[0];
  if (!lease) return json({ error: "LEASE_NOT_FOUND" }, 404);
  const identity = publicTaskId(String(lease.task_id));
  if (identity && verified.claims.scopeTenant !== "all" && verified.claims.scopeTenant !== identity.tenantId) return json({ error: "TENANT_FORBIDDEN" }, 403);
  const storageTaskId = String(lease.task_id);
  const fencing = Number(lease?.fencing_token ?? 0);
  const nodeId = String(lease?.node_id ?? "");
  if (fencing > 0 && nodeId) {
    const completeResp = await schedulerFetch(env, "/v1/scheduler/complete", {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify({ task_id: storageTaskId, node_id: nodeId, fencing_token: fencing }),
    });
    if (completeResp.headers.get("x-ansts-scheduler-client") === "gateway") return completeResp;
    if (!completeResp.ok) {
      const detail = completeResp ? await completeResp.text() : "scheduler missing";
      return json({ error: "SCHEDULER_COMPLETE_REJECTED", message: detail }, completeResp?.status ?? 503);
    }
  }
  const releaseResp = await gateway.fetch(
    new Request("https://cell.internal/v1/gateway/release", {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify({ task_id: storageTaskId, confirm: true }),
    })
  );
  const releaseBody = (await releaseResp.json()) as Record<string, unknown>;
  return json({ ...releaseBody, subject: verified.claims.sub, schedulerCompleted: fencing > 0 && nodeId }, releaseResp.status);
}


