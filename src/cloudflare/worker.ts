/**
 * @fileoverview M7 TASK-27B: Cloudflare Workers / denoland/celld Default Fetch & Queue Entrypoint.
 *
 * Both `/v1/scheduler` and `/v1beta1` resolve the scheduler Durable Object from
 * `SCHEDULER_CELL` when that binding is present, otherwise `ANSTS_SCHEDULER_CELL`.
 * Neither path invents a binding. A missing binding is a 500 on HTTP and a retry
 * (no ack) on the queue.
 *
 * Historical cell names stay in place unless `SCHEDULER_CLUSTER_ID` is set:
 * `/v1beta1` defaults to `global-5d-scheduler`, `/v1/scheduler` defaults to
 * `global-shard-0`. Setting the env var points both routes at one cell.
 */
import { CloudflareCelldEnv, DurableObjectNamespaceLike, QueueBatchLike, QueueMessageLike } from "./env.js";

export const SCHEDULER_BINDING_ERROR =
  "scheduler binding missing: set SCHEDULER_CELL or ANSTS_SCHEDULER_CELL";

export function resolveSchedulerNamespace(env: CloudflareCelldEnv): {
  namespace: DurableObjectNamespaceLike;
  binding: "SCHEDULER_CELL" | "ANSTS_SCHEDULER_CELL";
} {
  if (env.SCHEDULER_CELL?.idFromName && env.SCHEDULER_CELL?.get) {
    return { namespace: env.SCHEDULER_CELL, binding: "SCHEDULER_CELL" };
  }
  if (env.ANSTS_SCHEDULER_CELL?.idFromName && env.ANSTS_SCHEDULER_CELL?.get) {
    return { namespace: env.ANSTS_SCHEDULER_CELL, binding: "ANSTS_SCHEDULER_CELL" };
  }
  throw new Error(SCHEDULER_BINDING_ERROR);
}

export function schedulerClusterName(env: CloudflareCelldEnv, route: "v1" | "v1beta1"): string {
  if (env.SCHEDULER_CLUSTER_ID) return env.SCHEDULER_CLUSTER_ID;
  return route === "v1beta1" ? "global-5d-scheduler" : "global-shard-0";
}

function json(status: number, body: Record<string, unknown>): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

async function deliverQueueMessage(
  message: QueueMessageLike<Record<string, unknown>>,
  env: CloudflareCelldEnv,
): Promise<void> {
  const { namespace } = resolveSchedulerNamespace(env);
  const id = namespace.idFromName(schedulerClusterName(env, "v1beta1"));
  const response = await namespace.get(id).fetch(
    new Request("https://scheduler/internal/events/consume", {
      method: "POST",
      body: JSON.stringify(message.body ?? {}),
      headers: { "content-type": "application/json" },
    }),
  );
  if (!response.ok) {
    throw new Error(`event consume failed: ${response.status}`);
  }
  message.ack();
}

export default {
  async fetch(request: Request, env: CloudflareCelldEnv): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/healthz") {
      return json(200, {
        status: "ok",
        runtime: "denoland/celld-cloudflare-workers",
        region: env.CELLD_REGION ?? "local",
      });
    }
    const route = url.pathname.startsWith("/v1beta1/")
      ? "v1beta1"
      : url.pathname.startsWith("/v1/scheduler")
        ? "v1"
        : undefined;
    if (!route) return json(404, { error: "NOT_FOUND" });
    try {
      const { namespace } = resolveSchedulerNamespace(env);
      const id = namespace.idFromName(schedulerClusterName(env, route));
      return namespace.get(id).fetch(request);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return json(500, { error: message });
    }
  },

  async queue(batch: QueueBatchLike<Record<string, unknown>>, env: CloudflareCelldEnv): Promise<void> {
    const failures: string[] = [];
    for (const message of batch.messages) {
      try {
        await deliverQueueMessage(message, env);
      } catch (error) {
        failures.push(error instanceof Error ? error.message : String(error));
      }
    }
    if (failures.length > 0) {
      throw new Error(`queue delivery failed without ack: ${failures.join("; ")}`);
    }
  },
};
