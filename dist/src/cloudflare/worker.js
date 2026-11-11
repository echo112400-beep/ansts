"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TunnelGatewayCell = exports.AgentTaskHarnessCell = exports.AnstsSchedulerCell = void 0;
var scheduler_cell_js_1 = require("./scheduler_cell.js");
Object.defineProperty(exports, "AnstsSchedulerCell", { enumerable: true, get: function () { return scheduler_cell_js_1.AnstsSchedulerCell; } });
var harness_cell_js_1 = require("./harness_cell.js");
Object.defineProperty(exports, "AgentTaskHarnessCell", { enumerable: true, get: function () { return harness_cell_js_1.AgentTaskHarnessCell; } });
var tunnel_gateway_cell_js_1 = require("./tunnel_gateway_cell.js");
Object.defineProperty(exports, "TunnelGatewayCell", { enumerable: true, get: function () { return tunnel_gateway_cell_js_1.TunnelGatewayCell; } });
exports.default = {
    async fetch(request, env) {
        const url = new URL(request.url);
        if (url.pathname === "/healthz") {
            return new Response(JSON.stringify({
                status: "ok",
                runtime: "denoland/celld-cloudflare-workers",
                region: env.CELLD_REGION ?? "local",
            }), { status: 200, headers: { "content-type": "application/json" } });
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
                    const resp = await stub.fetch(new Request("https://cell.internal/v1/scheduler/submit", {
                        method: "POST",
                        headers: { "content-type": "application/json" },
                        body: JSON.stringify(body),
                    }));
                    if (!resp.ok && resp.status >= 500) {
                        msg.retry();
                        continue;
                    }
                }
                msg.ack();
            }
            catch {
                msg.retry();
            }
        }
    },
};
