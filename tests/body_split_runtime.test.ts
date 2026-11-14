/** @fileoverview Two independent control Workers and the native-only host execute, recover and stop sandbox work. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as http from "node:http";
import brainWorker from "../src/cloudflare/worker.js";
import bodyWorker from "../src/cloudflare/body_worker.js";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell.js";
import { AnstsBodyCell } from "../src/cloudflare/body_cell.js";
import { MockDurableObjectState, DurableObjectNamespaceLike, CloudflareCelldEnv } from "../src/cloudflare/env.js";
import { BodyWorkerEnv } from "../src/cloudflare/body_worker_routes.js";
import { BodyDaemonServer } from "../src/runtime/body_daemon_server.js";
import { bodyServiceToken } from "../src/cloudflare/body_auth.js";
import { executionDigest } from "../src/cloudflare/execution_attempt.js";
import { taskStorageId } from "../src/cloudflare/task_identity.js";
import { NodeDaemon } from "../src/node_daemon.js";

function namespace(factory: (name: string) => { fetch(request: Request): Promise<Response> }): DurableObjectNamespaceLike {
  const cells = new Map<string, ReturnType<typeof factory>>();
  return { idFromName: name => ({ toString: () => name }), get: id => ({ id, fetch: async (input, init) => {
    const name = id.toString(); if (!cells.has(name)) cells.set(name, factory(name));
    return cells.get(name)!.fetch(new Request(input, init));
  } }) };
}
async function serve(handler: (request: Request) => Promise<Response>) {
  const server = http.createServer(async (request, response) => {
    const chunks: Buffer[] = []; for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const headers = new Headers(); for (const [key, value] of Object.entries(request.headers)) if (value !== undefined) headers.set(key, String(value));
    const result = await handler(new Request(`http://worker${request.url}`, { method: request.method, headers, body: request.method === "GET" ? undefined : Buffer.concat(chunks) }));
    response.writeHead(result.status, { "content-type": "application/json" }); response.end(Buffer.from(await result.arrayBuffer()));
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  return { url: `http://127.0.0.1:${port}`, close: async () => { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); } };
}
it("runs Body control independently, preserves native results on restart, and stops sandbox processes", async () => {
  const secret = "f".repeat(64), nodeId = "body-host-test";
  const token = bodyServiceToken(secret, nodeId);
  const bodyHeaders = { authorization: `Body ${token}`, "x-ansts-body-id": nodeId, "content-type": "application/json" };
  const schedulerNs = namespace(name => new AnstsSchedulerCell(new MockDurableObjectState(name)));
  const bodyNs = namespace(name => new AnstsBodyCell(new MockDurableObjectState(name)));
  const brainEnv = { ANSTS_BODY_AUTH_SECRET: secret, ANSTS_SCHEDULER_CELL: schedulerNs } as CloudflareCelldEnv;
  const brain = await serve(request => brainWorker.fetch(request, brainEnv));
  const bodyEnv: BodyWorkerEnv = { ANSTS_BODY_AUTH_SECRET: secret, ANSTS_BODY_CELL: bodyNs, ANSTS_SCHEDULER_URL: brain.url };
  const control = await serve(request => bodyWorker.fetch(request, bodyEnv));
  brainEnv.ANSTS_BODY_WORKER_URL = control.url;
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "body-split-"));
  const options = { nodeId, token, bodyWorkerUrl: control.url, storageDir: directory, host: "127.0.0.1", port: 0 };
  let daemon = new BodyDaemonServer(options);
  let address = await daemon.start();
  const rpc = (base: string, route: string, payload?: unknown, headers = bodyHeaders) => fetch(base + route, { method: payload === undefined ? "GET" : "POST", headers, body: payload === undefined ? undefined : JSON.stringify(payload) });
  const daemonUrl = () => `http://127.0.0.1:${address.port}`;
  async function prepare(publicId: string) {
    await rpc(control.url, "/v1/scheduler/register-node", new NodeDaemon(nodeId).emitHeartbeat());
    await rpc(brain.url, "/v1/scheduler/submit", { tenant_id: "tenant", task_id: publicId, prompt: "work" }, { ...bodyHeaders, authorization: `Gateway ${bodyServiceToken(secret, "gateway:control")}` });
    const tick = await (await rpc(control.url, "/v1/scheduler/schedule-tick", {})).json();
    const taskId = taskStorageId("tenant", publicId);
    const fence = tick.placements.find((row: unknown[]) => row[0] === taskId)[2];
    const lease = { task_id: taskId, tenant_id: "tenant", node_id: nodeId, attempt_id: `${taskId}:${fence}`, fencing_token: fence, input_digest: executionDigest({ tenant_id: "tenant", task_id: taskId, prompt: "work" }) };
    await rpc(control.url, `/v1/body/acquire?taskId=${encodeURIComponent(taskId)}`, lease);
    const started = await (await rpc(control.url, `/v1/body/start?taskId=${encodeURIComponent(taskId)}`, { ...lease, expected_version: 1 })).json();
    return { lease, payload: { taskId, tenantId: "tenant", attemptId: lease.attempt_id, fencingToken: fence, promptText: "work", executionToken: started.execution_token } };
  }
  try {
    assert.equal((await (await rpc(daemonUrl(), "/health")).json()).runtime, "native-body-daemon");
    assert.equal((await rpc(daemonUrl(), "/v1beta1/management/state")).status, 404);
    assert.equal((await rpc(daemonUrl(), "/v1/scheduler/tasks")).status, 404);
    const first = await prepare("first");
    const executed = await rpc(daemonUrl(), "/v1/sandboxes/execute", first.payload);
    assert.equal(executed.status, 200);
    const result = await executed.json();
    await daemon.stop(); daemon = new BodyDaemonServer(options); address = await daemon.start();
    const reissued = await (await rpc(control.url, `/v1/body/start?taskId=${encodeURIComponent(first.lease.task_id)}`, { ...first.lease, expected_version: 2 })).json();
    const recovered = await rpc(daemonUrl(), "/v1/sandboxes/execute", { ...first.payload, executionToken: reissued.execution_token });
    assert.deepEqual(await recovered.json(), result);
    await rpc(control.url, "/v1/scheduler/complete", first.lease);
    await daemon.stop(); daemon = new BodyDaemonServer({ ...options, executionDelayMs: 30000 }); address = await daemon.start();
    const second = await prepare("second");
    const running = rpc(daemonUrl(), "/v1/sandboxes/execute", second.payload);
    for (let i = 0; i < 100; i++) {
      const listed = await (await rpc(daemonUrl(), "/v1/sandboxes")).json();
      if (listed.sandboxes.length && fs.readdirSync(path.join(directory, "execution-journal")).length >= 2) break;
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    const stopped = await rpc(daemonUrl(), "/v1/sandboxes/stop", second.payload);
    assert.equal(stopped.status, 200); assert.equal((await stopped.json()).stopped, true);
    assert.equal((await running).status, 503);
    assert.equal((await (await rpc(daemonUrl(), "/v1/sandboxes")).json()).sandboxes.length, 0);
    assert.equal(fs.readdirSync(path.join(directory, "workspaces")).filter(name => name.endsWith(".json")).length, 1);
    assert.equal((await rpc(control.url, `/v1/body/uncertain?taskId=${encodeURIComponent(second.lease.task_id)}`, { ...second.lease, expected_version: 2, reason: "test stopped execution" })).status, 200);
    // Real HTTP POST carries content-length; the remote state GET must not inherit it.
    assert.equal((await rpc(control.url, "/v1/scheduler/suspend-uncertain", second.lease)).status, 200);
    const suspended = await (await rpc(control.url, "/v1/scheduler/tasks")).json();
    assert.equal(suspended.tasks.find((task: { task_id: string }) => task.task_id === second.lease.task_id).state, "SUSPENDED_RECONCILIATION");
    await control.close();
    assert.equal((await rpc(daemonUrl(), "/readyz")).status, 503);
    assert.equal((await fetch(brain.url + "/healthz")).status, 200);
    assert.equal((await fetch(brain.url + "/v1/body/state?taskId=missing", { headers: bodyHeaders })).status, 503);
  } finally { await daemon.stop(); await control.close(); await brain.close(); fs.rmSync(directory, { recursive: true, force: true }); }
});
