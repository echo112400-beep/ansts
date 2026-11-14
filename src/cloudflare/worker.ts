/**
 * @fileoverview M7 TASK-27B, M64-1 & M65-4: Cloudflare Workers / denoland/celld Default Fetch, Queue & Durable Object Class Entrypoint.
 * Routes `/v1/scheduler/*`, `/v1/harness/*`, and `/v1/tunnel/*` to their respective stateful cells,
 * and dispatches queued task messages in `queue(batch, env)` to `ANSTS_SCHEDULER_CELL` with retry-on-failure semantics.
 */
import { CloudflareCelldEnv, QueueBatchLike } from "./env.js";
import { authenticateBody, authenticateBodyOperator, authenticateGatewayService, authenticateBodyControl } from "./body_auth.js";
import { handleBodyRequest } from "./body_worker_routes.js";

import { normalizeTaskStorageId } from "./task_identity.js";
export { AnstsSchedulerCell } from "./scheduler_cell.js";
export { AgentTaskHarnessCell } from "./harness_cell.js";
export { TunnelGatewayCell } from "./tunnel_gateway_cell.js";

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

export { AnstsBodyCell } from "./body_cell.js";
export default {
  async fetch(request: Request, env: CloudflareCelldEnv): Promise<Response> {
    const url = new URL(request.url);
    if (
      request.method === "GET" &&
      (url.pathname === "/health" || url.pathname === "/healthz" || url.pathname === "/readyz")
    ) {
      return new Response(
        JSON.stringify({
          status: "ok",
          healthy: true,
          runtime: "denoland/celld-cloudflare-workers",
          region: env.CELLD_REGION ?? "local",
          controlPlane: "celld-scheduler",
          planes: { scheduler: "durable-object" },
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }
    if (url.pathname === "/internal/v1/body-legacy/export") {
      if (!authenticateBodyOperator(request, env.ANSTS_BODY_AUTH_SECRET)) return Response.json({ error: "OPERATOR_AUTH_REQUIRED" }, { status: 401 });
      if (!env.ANSTS_BODY_CELL) return Response.json({ error: "LEGACY_BODY_CELL_UNBOUND" }, { status: 503 });
      const taskId = url.searchParams.get("taskId");
      const name = taskId ? `execution:${taskId}` : url.searchParams.get("hostId");
      if (!name || request.method !== "GET") return Response.json({ error: "INVALID_EXPORT" }, { status: 400 });
      return env.ANSTS_BODY_CELL.get(env.ANSTS_BODY_CELL.idFromName(name)).fetch(new Request("https://cell/v1/body/export", { headers: { "x-ansts-verified-operator": "development-migration" } }));
    }
    if (["/v1/scheduler", "/v1/harness", "/v1/tunnel", "/internal/v1beta1/brain/"].some(prefix => url.pathname.startsWith(prefix))) {
      const bodyHost = authenticateBody(request, env.ANSTS_BODY_AUTH_SECRET);
      const gatewayService = authenticateGatewayService(request, env.ANSTS_BODY_AUTH_SECRET);
      const bodyControl = authenticateBodyControl(request, env.ANSTS_BODY_AUTH_SECRET);
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
      if (request.method === "POST" && ["/v1/scheduler/register-node", "/v1/scheduler/lease-renew", "/v1/scheduler/complete", "/v1/scheduler/schedule-tick"].includes(url.pathname)) {
        const host = authenticateBody(request, env.ANSTS_BODY_AUTH_SECRET);
        const gateway = authenticateGatewayService(request, env.ANSTS_BODY_AUTH_SECRET);
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
      if (url.pathname === "/v1/scheduler/resolve-uncertain" && !authenticateBodyControl(request, env.ANSTS_BODY_AUTH_SECRET)) return Response.json({ error: "OPERATOR_ROUTE_REQUIRED" }, { status: 403 });
      if (url.pathname === "/v1/scheduler/suspend-uncertain" && !authenticateBodyControl(request, env.ANSTS_BODY_AUTH_SECRET)) {
        const host = authenticateBody(request, env.ANSTS_BODY_AUTH_SECRET);
        if (!host || (!env.ANSTS_BODY_CELL && !env.ANSTS_BODY_WORKER_URL)) return Response.json({ error: "BODY_AUTH_REQUIRED" }, { status: 401 });
        const body = await request.clone().json() as Record<string, unknown>;
        if (body.node_id !== host) return Response.json({ error: "BODY_SCOPE_FORBIDDEN" }, { status: 403 });
        const state = env.ANSTS_BODY_WORKER_URL
          ? await fetch(new URL(`/v1/body/state?taskId=${encodeURIComponent(String(body.task_id))}`, env.ANSTS_BODY_WORKER_URL), { headers: { authorization: request.headers.get("authorization") ?? "", "x-ansts-body-id": host }, redirect: "error", signal: AbortSignal.timeout(10000) })
          : await env.ANSTS_BODY_CELL!.get(env.ANSTS_BODY_CELL!.idFromName(`execution:${String(body.task_id)}`)).fetch(new Request("https://cell/v1/body/state"));
        const execution = (await state.json() as { execution?: Record<string, unknown> }).execution;
        if (!execution || execution.phase !== "UNCERTAIN" || execution.tenant_id !== body.tenant_id || execution.task_id !== body.task_id) return Response.json({ error: "UNCERTAIN_EXECUTION_REQUIRED" }, { status: 409 });
      }
      const shardName = url.searchParams.get("shard") ?? "global-shard-0";
      const id = env.ANSTS_SCHEDULER_CELL.idFromName(shardName);
      const stub = env.ANSTS_SCHEDULER_CELL.get(id);
      if (url.pathname === "/v1/scheduler/submit" && request.method === "POST") {
        const body = await request.json() as Record<string, unknown>;
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
