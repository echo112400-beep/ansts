/**
 * @fileoverview M7 TASK-27B: Cloudflare Workers / denoland/celld Default Fetch & Queue Entrypoint.
 */
import { CloudflareCelldEnv, QueueBatchLike } from "./env.js";

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
    if (url.pathname.startsWith("/v1beta1/")) {
      const namespace = env.SCHEDULER_CELL ?? env.ANSTS_SCHEDULER_CELL;
      const clusterId = env.SCHEDULER_CLUSTER_ID || "global-5d-scheduler";
      const id = namespace.idFromName(clusterId);
      return namespace.get(id).fetch(request);
    }
    if (url.pathname.startsWith("/v1/scheduler")) {
      const id = env.ANSTS_SCHEDULER_CELL.idFromName("global-shard-0");
      const stub = env.ANSTS_SCHEDULER_CELL.get(id);
      return stub.fetch(request);
    }
    return new Response(JSON.stringify({ error: "NOT_FOUND" }), { status: 404 });
  },

  async queue(batch: QueueBatchLike<Record<string, unknown>>): Promise<void> {
    for (const msg of batch.messages) {
      msg.ack();
    }
  },
};
