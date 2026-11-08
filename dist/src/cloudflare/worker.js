"use strict";Object.defineProperty(exports, "__esModule", { value: true });Object.defineProperty(exports, "AgentTaskHarnessCell", { enumerable: true, get: function () {return _harness_cell.AgentTaskHarnessCell;} });Object.defineProperty(exports, "AnstsSchedulerCell", { enumerable: true, get: function () {return _scheduler_cell.AnstsSchedulerCell;} });exports.default = void 0;



var _scheduler_cell = require("./scheduler_cell.js");
var _harness_cell = require("./harness_cell.js"); /**
 * @fileoverview M7 TASK-27B: Cloudflare Workers / denoland/celld Default Fetch, Queue & Durable Object Class Entrypoint.
 */var _default = {
  async fetch(request, env) {
    const url = new URL(request.url);
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
      const id = env.ANSTS_SCHEDULER_CELL.idFromName("global-shard-0");
      const stub = env.ANSTS_SCHEDULER_CELL.get(id);
      return stub.fetch(request);
    }
    return new Response(JSON.stringify({ error: "NOT_FOUND" }), { status: 404 });
  },

  async queue(batch) {
    for (const msg of batch.messages) {
      msg.ack();
    }
  }
};exports.default = _default;