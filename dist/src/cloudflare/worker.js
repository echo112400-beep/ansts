"use strict";Object.defineProperty(exports, "__esModule", { value: true });Object.defineProperty(exports, "AgentTaskHarnessCell", { enumerable: true, get: function () {return _harness_cell.AgentTaskHarnessCell;} });Object.defineProperty(exports, "AnstsSchedulerCell", { enumerable: true, get: function () {return _scheduler_cell.AnstsSchedulerCell;} });Object.defineProperty(exports, "TunnelGatewayCell", { enumerable: true, get: function () {return _tunnel_gateway_cell.TunnelGatewayCell;} });exports.default = void 0;





var _scheduler_cell = require("./scheduler_cell.js");
var _harness_cell = require("./harness_cell.js");
var _tunnel_gateway_cell = require("./tunnel_gateway_cell.js"); /**
 * @fileoverview M7 TASK-27B, M64-1 & M65-4: Cloudflare Workers / denoland/celld Default Fetch, Queue & Durable Object Class Entrypoint.
 * Routes `/v1/scheduler/*`, `/v1/harness/*`, and `/v1/tunnel/*` to their respective stateful cells,
 * and dispatches queued task messages in `queue(batch, env)` to `ANSTS_SCHEDULER_CELL` with retry-on-failure semantics.
 */var _default = { async fetch(request, env) {const url = new URL(request.url);
    if (url.pathname === "/healthz") {
      return new Response(
      JSON.stringify({
        status: "ok",
        runtime: "denoland/celld-cloudflare-workers",
        region: env.CELLD_REGION ?? "local"
      }),
      { status: 200, headers: { "content-type": "application/json" } });

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
    if (url.pathname.startsWith("/v1/tunnel") && env.ANSTS_TUNNEL_CELL) {
      const hostId = url.searchParams.get("hostId") ?? "body-host-01";
      const id = env.ANSTS_TUNNEL_CELL.idFromName(hostId);
      const stub = env.ANSTS_TUNNEL_CELL.get(id);
      return stub.fetch(request);
    }
    return new Response(JSON.stringify({ error: "NOT_FOUND" }), { status: 404 });
  },

  async queue(batch, env) {
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
            body: JSON.stringify(body)
          }));

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
  }
};exports.default = _default;