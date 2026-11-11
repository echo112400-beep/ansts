/**
 * @fileoverview M7 TASK-27B & M64-1: Cloudflare Workers / denoland/celld Default Fetch, Queue & Durable Object Class Entrypoint.
 * Routes `/v1/scheduler/*`, `/v1/harness/*`, and `/v1/tunnel/*` to their respective stateful cells,
 * and dispatches queued task messages in `queue(batch, env)` to `ANSTS_SCHEDULER_CELL` with retry-on-failure semantics.
 */
import { CloudflareCelldEnv, QueueBatchLike } from "./env.js";
export { AnstsSchedulerCell } from "./scheduler_cell.js";
export { AgentTaskHarnessCell } from "./harness_cell.js";
export { TunnelGatewayCell } from "./tunnel_gateway_cell.js";

export default {
  async fetch(request: Request, env: CloudflareCelldEnv): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/healthz") {
      return new Response(
        JSON.stringify({
          status: "ok",
          runtime: "denoland/celld-cloudflare-workers",
          region: env.CELLD_REGION ?? "local",
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }
    if (url.pathname.startsWith("/v1/scheduler")) {
      const shardName = url.searchParams.get("shard") ?? "global-shard-0";
      const id = env.ANSTS_SCHEDULER_CELL.idFromName(shardName);
      const stub = env.ANSTS_SCHEDULER_CELL.get(id);
      return stub.fetch(request);
    }
    if (url.pathname.startsWith("/v1/harness") && env.ANSTS_HARNESS_CELL) {
      const taskId = url.searchParams.get("taskId") ?? "task-default";
      const id = env.ANSTS_HARNESS_CELL.idFromName(taskId);
      const stub = env.ANSTS_HARNESS_CELL.get(id);
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
              body: JSON.stringify(body),
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
