/** @fileoverview Independent celld Body control Worker; native execution stays in the host Daemon. */
import { authenticateBody } from "./body_auth.js";
import { BodyWorkerEnv, bodySchedulerClient, handleBodyRequest } from "./body_worker_routes.js";
import { controlPlaneFetch } from "./control_plane_fetch.js";
import {bodyInventoryIngress} from '../control/execution_inventory_ingress.js';
export { AnstsBodyCell } from "./body_cell.js";

const SCHEDULER_ROUTES = new Set(["/v1/scheduler/register-node", "/v1/scheduler/schedule-tick", "/v1/scheduler/tasks", "/v1/scheduler/lease-renew", "/v1/scheduler/complete", "/v1/scheduler/suspend-uncertain", "/internal/v1/body-usage-binding"]);
export default {
  async fetch(request: Request, env: BodyWorkerEnv): Promise<Response> {
    const url = new URL(request.url);
    try {
      if(url.pathname==='/internal/v1/execution-inventory/body')return await bodyInventoryIngress(request,env);
      if (request.method === "GET" && ["/health", "/healthz", "/readyz"].includes(url.pathname)) {
        let ready = true;
        if (url.pathname === "/readyz") {
          const response = await bodySchedulerClient(env).fetch(new Request("https://scheduler/healthz"));
          ready = response.ok && (await response.json() as { controlPlane?: string }).controlPlane === "celld-scheduler";
        }
        return Response.json({ controlPlane: "celld-body", runtime: "denoland/celld", healthy: ready, planes: { body: "durable-object", scheduler: "remote-http", execution: "native-daemon" } }, { status: ready ? 200 : 503 });
      }
      if (url.pathname.startsWith("/v1/body/")) {
        if (env.ANSTS_BODY_PROXY_URLS) return controlPlaneFetch(env.ANSTS_BODY_PROXY_URLS, url.pathname + url.search, { method: request.method, headers: request.headers, body: request.method === "GET" ? undefined : await request.arrayBuffer(), signal: AbortSignal.timeout(10000) });
        return await handleBodyRequest(request, env);
      }
      if (SCHEDULER_ROUTES.has(url.pathname)) {
        if (!authenticateBody(request, env)) return Response.json({ error: "BODY_AUTH_REQUIRED" }, { status: 401 });
        if (!env.ANSTS_SCHEDULER_URL) return Response.json({ error: "SCHEDULER_UNCONFIGURED" }, { status: 503 });
        return await controlPlaneFetch(env.ANSTS_SCHEDULER_URLS || env.ANSTS_SCHEDULER_URL, url.pathname + url.search, { method: request.method, headers: request.headers, body: request.method === "GET" ? undefined : await request.arrayBuffer(), signal: AbortSignal.timeout(10000) });
      }
      return Response.json({ error: "NOT_FOUND" }, { status: 404 });
    } catch { return Response.json({ error: "BODY_CONTROL_DEPENDENCY_UNAVAILABLE" }, { status: 503 }); }
  },
};
