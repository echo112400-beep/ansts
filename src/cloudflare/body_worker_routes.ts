/** @fileoverview Body control routes shared by the standalone Worker and legacy migration host. */
import { DurableObjectNamespaceLike } from "./env.js";
import { authenticateBody, authenticateBodyOperator, bodyServiceToken } from "./body_auth.js";
import { issueExecutionGrant, verifyExecutionGrant } from "./execution_token.js";
import { ExecutionAttempt, sameOwner } from "./execution_attempt.js";

export interface BodyWorkerEnv {
  ANSTS_BODY_CELL?: DurableObjectNamespaceLike;
  ANSTS_BODY_AUTH_SECRET?: string;
  ANSTS_SCHEDULER_CELL?: DurableObjectNamespaceLike;
  ANSTS_SCHEDULER_URL?: string;
}
export function bodySchedulerClient(env: BodyWorkerEnv): { fetch(request: Request): Promise<Response> } {
  if (env.ANSTS_SCHEDULER_CELL) return env.ANSTS_SCHEDULER_CELL.get(env.ANSTS_SCHEDULER_CELL.idFromName("global-shard-0"));
  return { async fetch(request: Request): Promise<Response> {
    if (!env.ANSTS_SCHEDULER_URL || !env.ANSTS_BODY_AUTH_SECRET) return Response.json({ error: "SCHEDULER_UNCONFIGURED" }, { status: 503 });
    const source = new URL(request.url);
    const headers = new Headers(request.headers);
    headers.set("authorization", `BodyControl ${bodyServiceToken(env.ANSTS_BODY_AUTH_SECRET, "body:control")}`);
    return fetch(new URL(source.pathname + source.search, env.ANSTS_SCHEDULER_URL), { method: request.method, headers, body: request.method === "GET" ? undefined : await request.arrayBuffer(), redirect: "error", signal: AbortSignal.timeout(10000) });
  } };
}
export async function handleBodyRequest(request: Request, env: BodyWorkerEnv): Promise<Response> {
  const url = new URL(request.url);
    if (url.pathname.startsWith("/v1/body/") && env.ANSTS_BODY_CELL) {
      if (url.pathname === "/v1/body/consume-grant") return Response.json({ error: "INTERNAL_ROUTE" }, { status: 403 });
      if (["/v1/body/export", "/v1/body/import"].includes(url.pathname)) {
        if (!authenticateBodyOperator(request, env.ANSTS_BODY_AUTH_SECRET)) return Response.json({ error: "OPERATOR_AUTH_REQUIRED" }, { status: 401 });
        const name = url.searchParams.get("taskId") ? `execution:${url.searchParams.get("taskId")}` : url.searchParams.get("hostId");
        if (!name) return Response.json({ error: "CELL_IDENTITY_REQUIRED" }, { status: 400 });
        const headers = new Headers(request.headers); headers.set("x-ansts-verified-operator", "development-migration");
        return env.ANSTS_BODY_CELL.get(env.ANSTS_BODY_CELL.idFromName(name)).fetch(new Request(request, { headers }));
      }
      if (url.pathname === "/v1/body/reconcile") {
        if (!authenticateBodyOperator(request, env.ANSTS_BODY_AUTH_SECRET)) return Response.json({ error: "OPERATOR_AUTH_REQUIRED" }, { status: 401 });
        const taskId = url.searchParams.get("taskId");
        if (!taskId || request.method !== "POST") return Response.json({ error: "INVALID_RECONCILIATION_ROUTE" }, { status: 400 });
        const headers = new Headers(request.headers);
        headers.set("x-ansts-verified-operator", "development-operator");
        const id = env.ANSTS_BODY_CELL.idFromName(`execution:${taskId}`);
        let review: Record<string, unknown>;
        try { review = await request.clone().json() as Record<string, unknown>; }
        catch { return Response.json({ error: "INVALID_JSON" }, { status: 400 }); }
        const reconciled = await env.ANSTS_BODY_CELL.get(id).fetch(new Request(request, { headers }));
        if (!reconciled.ok || review.action === "retain_block") return reconciled;
        const scheduler = bodySchedulerClient(env);
        // State transitions across cells are retried with the same reconciliation request digest.
        const listed = await scheduler.fetch(new Request("https://cell/v1/scheduler/tasks"));
        const tasks = (await listed.json() as { tasks?: Record<string, unknown>[] }).tasks ?? [];
        const task = tasks.find(task => task.task_id === review.task_id && task.tenant_id === review.tenant_id);
        if (!task) return Response.json({ error: "SCHEDULER_RECONCILIATION_PENDING", retry_same_request: true }, { status: 503 });
        const current = { ...review, node_id: task.assigned_node_id, fencing_token: task.lease_fencing_token };
        await scheduler.fetch(new Request("https://cell/v1/scheduler/suspend-uncertain", { method: "POST", body: JSON.stringify(current) }));
        const resolved = await scheduler.fetch(new Request("https://cell/v1/scheduler/resolve-uncertain", {
          method: "POST", body: JSON.stringify({ ...current, retry: review.action === "confirm_not_started" }),
        }));
        if (!resolved.ok) return Response.json({ error: "SCHEDULER_RECONCILIATION_PENDING", retry_same_request: true }, { status: 503 });
        if (review.action === "confirm_completed") {
          const decision = await reconciled.clone().json() as { execution: Record<string, unknown> };
          const acknowledged = await env.ANSTS_BODY_CELL.get(id).fetch(new Request("https://cell/v1/body/acknowledge", {
            method: "POST", body: JSON.stringify({ ...decision.execution, expected_version: decision.execution.version }),
          }));
          if (!acknowledged.ok) return Response.json({ error: "SCHEDULER_RECONCILIATION_PENDING", retry_same_request: true }, { status: 503 });
          return Response.json({ reconciled: true, ...(await acknowledged.json() as Record<string, unknown>) });
        }
        return reconciled;
      }
      const host = authenticateBody(request, env.ANSTS_BODY_AUTH_SECRET);
      if (!host) return Response.json({ error: "BODY_AUTH_REQUIRED" }, { status: 401 });
      const taskId = url.searchParams.get("taskId");
      if (url.searchParams.has("hostId") && url.searchParams.get("hostId") !== host) return Response.json({ error: "BODY_SCOPE_FORBIDDEN" }, { status: 403 });
      if (request.method === "POST") {
        try {
          const raw = await request.clone().text();
          if (raw.length > 65536) return Response.json({ error: "PAYLOAD_TOO_LARGE" }, { status: 413 });
          const body = JSON.parse(raw);
          if (body.node_id !== host || (taskId && body.task_id !== taskId)) return Response.json({ error: "BODY_SCOPE_FORBIDDEN" }, { status: 403 });
        } catch { return Response.json({ error: "INVALID_JSON" }, { status: 400 }); }
      }
      const id = env.ANSTS_BODY_CELL.idFromName(taskId ? `execution:${taskId}` : url.searchParams.get("hostId") ?? "body-host-01");
      const cell = env.ANSTS_BODY_CELL.get(id);
      if (url.pathname === "/v1/body/verify-execution-token" && request.method === "POST") {
        const body = await request.json() as Record<string, unknown>;
        const snapshot = await cell.fetch(new Request("https://cell/v1/body/state"));
        const execution = (await snapshot.json() as { execution: ExecutionAttempt }).execution;
        if (!execution || execution.phase !== "RUNNING" || !sameOwner(execution, body)) return Response.json({ error: "EXECUTION_ADMISSION_REQUIRED" }, { status: 409 });
        const grant = verifyExecutionGrant(env.ANSTS_BODY_AUTH_SECRET ?? "", String(body.execution_token ?? ""), execution);
        if (!grant) return Response.json({ error: "INVALID_EXECUTION_GRANT" }, { status: 403 });
        return cell.fetch(new Request("https://cell/v1/body/consume-grant", { method: "POST", headers: { "x-ansts-verified-grant": "true" }, body: JSON.stringify({ ...body, nonce: grant.nonce }) }));
      }
      const response = await cell.fetch(request);
      if (url.pathname === "/v1/body/start" && response.ok) {
        const body = await response.json() as { execution: ExecutionAttempt };
        return Response.json({ ...body, execution_token: issueExecutionGrant(env.ANSTS_BODY_AUTH_SECRET ?? "", body.execution) });
      }
      return response;
    }

  return Response.json({ error: 'BODY_CELL_UNBOUND' }, { status: 503 });
}
