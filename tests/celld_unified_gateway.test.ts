/**
 * @fileoverview Unified celld control plane: the gateway HTTP API and the scheduler share one
 * worker, and gateway dispatch/lease/session rows live in AnstsGatewayCell.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as http from "node:http";
import { AnstsGatewayCell } from "../src/cloudflare/gateway_cell.js";
import { handleUnifiedGatewayRequest } from "../src/cloudflare/gateway_cell.js";
import { MockDurableObjectState } from "../src/cloudflare/env.js";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import schedulerWorker from "../src/cloudflare/worker.js";
import gatewayWorker from "../src/cloudflare/gateway_worker.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";

const SECRET = "unified-gateway-test-secret-32b";

function gatewayEnv(gateway: AnstsGatewayCell, schedulerUrl?: string, secret?: string) {
  return {
    CELLD_REGION: "celld-gateway",
    ANSTS_CONSOLE_RBAC_SECRET: secret,
    ANSTS_SCHEDULER_URL: schedulerUrl,
    ANSTS_GATEWAY_CELL: {
      idFromName: (name: string) => ({ toString: () => name, name }),
      get: () => ({ id: { toString: () => "gateway-control-plane" }, fetch: (req: Request) => gateway.fetch(req) }),
    },
  };
}

describe("unified celld gateway and scheduler", () => {
  it("keeps gateway dispatch, lease, and session rows in the Durable Object across a new instance", async () => {
    const state = new MockDurableObjectState("gateway-control-plane");
    const first = new AnstsGatewayCell(state);
    const recorded = await first.fetch(
      new Request("https://cell.internal/v1/gateway/dispatch", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task_id: "task-unified-1",
          tenant_id: "probe",
          prompt: "persist the gateway",
          state: "LEASED",
          assigned_node_id: "body-host-01",
          fencing_token: 4,
          holder_id: "celld-worker",
        }),
      })
    );
    assert.equal(recorded.status, 202);
    const session = await first.fetch(
      new Request("https://cell.internal/v1/gateway/session", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ session_id: "sess-1", subject: "lin", role: "OPERATOR", expires_at: 9_000_000_000 }),
      })
    );
    assert.equal(session.status, 200);

    const revived = new AnstsGatewayCell(state);
    const body = (await (await revived.fetch(new Request("https://cell.internal/v1/gateway/state"))).json()) as {
      cellId: string;
      dispatches: Array<{ task_id: string; state: string; fencing_token: number }>;
      leases: Array<{ task_id: string; status: string; fencing_token: number }>;
      sessions: Array<{ subject: string; role: string }>;
    };
    assert.equal(body.cellId, "gateway-control-plane");
    assert.equal(body.dispatches[0].task_id, "task-unified-1");
    assert.equal(body.dispatches[0].state, "LEASED");
    assert.equal(body.leases[0].status, "HELD");
    assert.equal(body.leases[0].fencing_token, 4);
    assert.equal(body.sessions[0].subject, "lin");

    const refused = await revived.fetch(
      new Request("https://cell.internal/v1/gateway/release", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "task-unified-1", confirm: false }),
      })
    );
    assert.equal(refused.status, 400);
    const released = await revived.fetch(
      new Request("https://cell.internal/v1/gateway/release", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "task-unified-1", confirm: true }),
      })
    );
    assert.equal(released.status, 200);
    const after = (await (await revived.fetch(new Request("https://cell.internal/v1/gateway/state"))).json()) as {
      leases: Array<{ status: string }>;
    };
    assert.equal(after.leases[0].status, "RELEASED");
  });

  it("serves gateway submit, dispatch, and health on the worker and fail-closes lease release", async () => {
    const gateway = new AnstsGatewayCell(new MockDurableObjectState("gateway-control-plane"));
    const scheduler = new AnstsSchedulerCell(new MockDurableObjectState("global-shard-0"));
    const schedulerEnv = {
      CELLD_REGION: "celld-scheduler",
      ANSTS_SCHEDULER_CELL: {
        idFromName: (name: string) => ({ toString: () => name, name }),
        get: () => ({ id: { toString: () => "global-shard-0" }, fetch: (req: Request) => scheduler.fetch(req) }),
      },
    };
    const schedulerServer = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      const body = Buffer.concat(chunks);
      const upstream = await schedulerWorker.fetch(
        new Request(`http://scheduler.local${req.url}`, {
          method: req.method,
          headers: { "content-type": "application/json" },
          body: req.method === "GET" || req.method === "HEAD" ? undefined : body,
        }),
        schedulerEnv as never
      );
      const payload = Buffer.from(await upstream.arrayBuffer());
      res.writeHead(upstream.status, { "content-type": upstream.headers.get("content-type") ?? "application/json" });
      res.end(payload);
    });
    await new Promise<void>((resolve) => schedulerServer.listen(0, "127.0.0.1", () => resolve()));
    const schedulerAddress = schedulerServer.address();
    const schedulerPort = typeof schedulerAddress === "object" && schedulerAddress ? schedulerAddress.port : 0;
    const schedulerUrl = `http://127.0.0.1:${schedulerPort}`;
    const registered = await fetch(`${schedulerUrl}/v1/scheduler/register-node`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ node_id: "body-host-01", total_cpu_cores: 4, free_cpu_cores: 4 }),
    });
    assert.equal(registered.status, 200);
    const env = gatewayEnv(gateway, schedulerUrl, SECRET);
    try {
    const health = await gatewayWorker.fetch(new Request("https://edge.ansts.io/health"), env as never);
    assert.equal(health.status, 200);
    const healthBody = (await health.json()) as {
      controlPlane: string;
      planes: { gateway: string; scheduler: string; bodyExecution: string };
    };
    assert.equal(healthBody.controlPlane, "celld-gateway");
    assert.equal(healthBody.planes.gateway, "durable-object");
    assert.equal(healthBody.planes.scheduler, "remote-http");
    assert.equal(healthBody.planes.bodyExecution, "external-node-process");

    const submit = await gatewayWorker.fetch(
      new Request("https://edge.ansts.io/v1/tasks", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "task-gw-submit", tenant_id: "probe", priority: 1, prompt: "from gateway" }),
      }),
      env as never
    );
    assert.equal(submit.status, 202);
    const submitBody = (await submit.json()) as {
      task_id: string;
      controlPlane: string;
      executionPlane: string;
      bodyExecution: unknown;
      state: string;
    };
    assert.equal(submitBody.task_id, "task-gw-submit");
    assert.equal(submitBody.controlPlane, "celld-gateway");
    assert.equal(submitBody.executionPlane, "external-body");
    assert.equal(submitBody.bodyExecution, null);
    assert.equal(submitBody.state, "LEASED");

    const listed = await gatewayWorker.fetch(new Request("https://edge.ansts.io/v1/tasks/task-gw-submit"), env as never);
    assert.equal(listed.status, 200);

    const dispatch = await gatewayWorker.fetch(
      new Request("https://edge.ansts.io/v1beta1/tasks/dispatch", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ taskId: "task-gw-dispatch", tenantId: "probe", priority: 1, promptText: "dispatch" }),
      }),
      env as never
    );
    assert.equal(dispatch.status, 202);
    const dispatchBody = (await dispatch.json()) as { dispatched: boolean; bodyExecution: unknown; deferred: string };
    assert.equal(dispatchBody.dispatched, true);
    assert.equal(dispatchBody.bodyExecution, null);
    assert.equal(dispatchBody.deferred, "BODY_EXECUTION_IS_EXTERNAL");

    const overview = await gatewayWorker.fetch(new Request("https://edge.ansts.io/v1beta1/console/gateway"), env as never);
    assert.equal(overview.status, 200);
    const overviewBody = (await overview.json()) as { totalTasks: number; controlPlane: string };
    assert.equal(overviewBody.controlPlane, "celld-gateway");
    assert.ok(overviewBody.totalTasks >= 2);

    const missingAuth = await gatewayWorker.fetch(
      new Request("https://edge.ansts.io/v1beta1/gateway/leases/release", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "task-gw-submit", confirm: true }),
      }),
      env as never
    );
    assert.equal(missingAuth.status, 401);

    const unconfirmed = await gatewayWorker.fetch(
      new Request("https://edge.ansts.io/v1beta1/gateway/leases/release", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${new ConsoleRbacAuthGuard(SECRET).issueToken({ sub: "lin", role: "OPERATOR" })}`,
        },
        body: JSON.stringify({ task_id: "task-gw-submit" }),
      }),
      env as never
    );
    assert.equal(unconfirmed.status, 400);

    const viewer = await gatewayWorker.fetch(
      new Request("https://edge.ansts.io/v1beta1/gateway/leases/release", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${new ConsoleRbacAuthGuard(SECRET).issueToken({ sub: "xu", role: "VIEWER" })}`,
        },
        body: JSON.stringify({ task_id: "task-gw-submit", confirm: true }),
      }),
      env as never
    );
    assert.equal(viewer.status, 403);

    const unbound = await gatewayWorker.fetch(
      new Request("https://edge.ansts.io/v1beta1/gateway/leases/release", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${new ConsoleRbacAuthGuard(SECRET).issueToken({ sub: "lin", role: "OPERATOR" })}`,
        },
        body: JSON.stringify({ task_id: "task-gw-submit", confirm: true }),
      }),
      gatewayEnv(gateway, schedulerUrl, undefined) as never
    );
    assert.equal(unbound.status, 503);

    const released = await gatewayWorker.fetch(
      new Request("https://edge.ansts.io/v1beta1/gateway/leases/release", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${new ConsoleRbacAuthGuard(SECRET).issueToken({ sub: "lin", role: "OPERATOR" })}`,
        },
        body: JSON.stringify({ task_id: "task-gw-submit", confirm: true }),
      }),
      env as never
    );
    assert.equal(released.status, 200);
    const state = (await (await gateway.fetch(new Request("https://cell.internal/v1/gateway/state"))).json()) as {
      sessions: Array<{ subject: string }>;
      leases: Array<{ task_id: string; status: string }>;
    };
    assert.equal(state.sessions.some((row) => row.subject === "lin"), true);
    assert.equal(state.leases.find((row) => row.task_id === "task-gw-submit")?.status, "RELEASED");
    assert.equal(await handleUnifiedGatewayRequest(new Request("https://edge.ansts.io/v1/scheduler/tasks"), env as never), null);
    const schedulerOnly = (await (await fetch(`${schedulerUrl}/healthz`)).json()) as { controlPlane?: string };
    assert.equal(schedulerOnly.controlPlane, "celld-scheduler");
    assert.equal((await fetch(`${schedulerUrl}/v1/tasks`)).status, 404);
    const missingUrl = await gatewayWorker.fetch(
      new Request("https://edge.ansts.io/v1/tasks", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "task-no-url", tenant_id: "probe", priority: 1, prompt: "x" }),
      }),
      gatewayEnv(gateway, "", SECRET) as never
    );
    assert.equal(missingUrl.status, 503);
    assert.equal(((await missingUrl.json()) as { error?: string }).error, "SCHEDULER_URL_UNCONFIGURED");
    } finally {
      await new Promise<void>((resolve) => schedulerServer.close(() => resolve()));
    }
    const unreachable = await gatewayWorker.fetch(
      new Request("https://edge.ansts.io/v1/tasks", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "task-down", tenant_id: "probe", priority: 1, prompt: "x" }),
      }),
      env as never
    );
    assert.equal(unreachable.status, 503);
    assert.equal(((await unreachable.json()) as { error?: string }).error, "SCHEDULER_UNREACHABLE");
  });

  it("proxies control-plane routes from the host process to the celld worker and does not schedule them locally", async () => {
    const upstream = http.createServer((req, res) => {
      const url = req.url ?? "/";
      if (req.method === "POST" && url === "/v1/tasks") {
        res.writeHead(202, { "content-type": "application/json" });
        res.end(JSON.stringify({ task_id: "proxied-task", state: "QUEUED", controlPlane: "celld-gateway" }));
        return;
      }
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "NOT_FOUND" }));
    });
    await new Promise<void>((resolve) => upstream.listen(0, "127.0.0.1", () => resolve()));
    const address = upstream.address();
    const port = typeof address === "object" && address ? address.port : 0;
    const host = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      workerNodeCount: 0,
      tickIntervalMs: 60_000,
      celldWorkerUrl: `http://127.0.0.1:${port}`,
    });
    const started = await host.start(0, 0);
    try {
      const submit = await fetch(`http://127.0.0.1:${started.apiPort}/v1/tasks`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "proxied-task", tenant_id: "probe", priority: 1, prompt: "via adapter" }),
      });
      assert.equal(submit.status, 202);
      const body = (await submit.json()) as { controlPlane: string; task_id: string };
      assert.equal(body.controlPlane, "celld-gateway");
      assert.equal(body.task_id, "proxied-task");
      assert.equal(host.scheduler.allTasks.has("proxied-task"), false);
      const health = (await (await fetch(`http://127.0.0.1:${started.apiPort}/healthz`)).json()) as {
        controlPlane: string;
        celldWorkerUrl: string;
      };
      assert.equal(health.controlPlane, "legacy-node-host");
      assert.equal(health.celldWorkerUrl, `http://127.0.0.1:${port}`);
    } finally {
      await host.stop();
      await new Promise<void>((resolve) => upstream.close(() => resolve()));
    }
  });
});
