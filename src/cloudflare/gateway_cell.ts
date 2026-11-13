/**
 * @fileoverview Unified control-plane gateway on denoland/celld.
 * AnstsGatewayCell stores dispatch, lease, and operator-session rows in its SQLite Durable Object.
 * Gateway HTTP routes in this module record session and lease state in this cell, then call the
 * scheduler worker over HTTP at ANSTS_SCHEDULER_URL. Sandbox execution stays on a Body process.
 */
import { ConsoleRbacAuthGuard, ConsoleRole } from "../dashboard/console_rbac_guard.js";
import { TaskSpecValidationError, TaskSpecValidator } from "../gateway/validator.js";
import { DurableObjectNamespaceLike, DurableObjectStateLike } from "./env.js";
import { publicTaskId, taskStorageId } from "./task_identity.js";

export interface UnifiedGatewayEnv {
  ANSTS_GATEWAY_CELL?: DurableObjectNamespaceLike;
  /** Base URL of the separate scheduler celld worker. Empty or unset fail-closes cross-worker calls. */
  ANSTS_SCHEDULER_URL?: string;
  ANSTS_SCHEDULER_SERVICE_TOKEN?: string;
  ANSTS_CONSOLE_RBAC_SECRET?: string;
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
    return await fetch(`${base}${path}`, { ...init, headers });
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
  if (method === "GET" && (pathname === "/health" || pathname === "/healthz" || pathname === "/readyz")) return true;
  if (method === "POST" && pathname === "/v1/tasks") return true;
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
export async function handleUnifiedGatewayRequest(request: Request, env: UnifiedGatewayEnv): Promise<Response | null> {
  const url = new URL(request.url);
  if (!isGatewayPath(request.method, url.pathname)) return null;
  if (!env.ANSTS_GATEWAY_CELL) {
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
        healthy: true,
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
      200
    );
  }
  if (request.method === "GET" && url.pathname === "/v1beta1/gateway/state") {
    const gateway = await readGatewayState(env);
    if (gateway) {
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
    const tasks = listed.tasks;
    return json(
      {
        controlPlane: "celld-gateway",
        totalTasks: tasks.length,
        gatewayDispatches: Array.isArray(gateway?.dispatches) ? gateway.dispatches.length : 0,
        activeLeases: Number(gateway?.activeLeases ?? 0),
        bodyExecution: "external-node-process",
        gatewayCellId: gateway?.cellId ?? null,
      },
      200
    );
  }
  if (request.method === "GET" && url.pathname.startsWith("/v1/tasks/")) {
    const taskId = decodeURIComponent(url.pathname.slice("/v1/tasks/".length));
    const listed = await listSchedulerTasks(env);
    if (listed.failure) return listed.failure;
    const tenantId = url.searchParams.get("tenantId");
    const candidates = listed.tasks.filter(task => {
      const identity = publicTaskId(String(task.task_id ?? task.taskId ?? ""));
      return (identity?.taskId ?? String(task.task_id ?? task.taskId ?? "")) === taskId && (!tenantId || (identity?.tenantId ?? task.tenant_id) === tenantId);
    });
    if (candidates.length > 1) return json({ error: "TENANT_REQUIRED" }, 400);
    const found = candidates[0];
    if (!found) return json({ error: "TASK_NOT_FOUND", task_id: taskId }, 404);
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
  spec.task_id = taskStorageId(tenantId, taskId);
  const existing = await listSchedulerTasks(env);
  if (existing.failure) return existing.failure;
  const legacy = existing.tasks.find(task => task.task_id === taskId && task.tenant_id === tenantId);
  if (legacy) spec.task_id = String(legacy.task_id);
  const gateway = gatewayStub(env);
  if (!gateway) return json({ error: "GATEWAY_CELL_UNBOUND" }, 503);
  const submitResp = await schedulerFetch(env, "/v1/scheduler/submit", {
    method: "POST",
    headers: JSON_HEADERS,
    body: JSON.stringify({
      task_id: spec.task_id,
      tenant_id: spec.tenant_id,
      priority: spec.priority,
      prompt: spec.prompt,
      max_turns: spec.max_turns,
      timeout_seconds: spec.timeout_seconds,
      resource_profile: spec.resource_profile,
    }),
  });
  if (submitResp.headers.get("x-ansts-scheduler-client") === "gateway") return submitResp;
  if (!submitResp.ok) {
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
