/**
 * @fileoverview M18 Local Host & Docker Compose Unified Service Runtime E2E Suite (4 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/index.js";

describe("M18 Local Host & Docker Compose Unified Service Runtime Suite", () => {
  it("1. Starts AnstsLocalAndComposeServer locally and serves /healthz, /.well-known/celld/health, /state, /metrics & Dashboard /api/overview", async () => {
    const server = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      workerNodeCount: 3,
      tickIntervalMs: 200,
    });
    const { apiPort, dashboardPort } = await server.start(0, 0);
    try {
      const healthRes = await fetch(`http://127.0.0.1:${apiPort}/healthz`);
      assert.equal(healthRes.status, 200);
      const healthBody = (await healthRes.json()) as { status: string; activeNodes: number };
      assert.equal(healthBody.status, "ok");
      assert.equal(healthBody.activeNodes, 3);

      const celldHealth = await fetch(`http://127.0.0.1:${apiPort}/.well-known/celld/health`);
      assert.equal(celldHealth.status, 200);

      const stateRes = await fetch(`http://127.0.0.1:${apiPort}/state`);
      assert.equal(stateRes.status, 200);
      const stateBody = (await stateRes.json()) as { node_load: { memory_headroom: boolean } };
      assert.equal(stateBody.node_load.memory_headroom, true);

      const dashRes = await fetch(`http://127.0.0.1:${dashboardPort}/api/overview`);
      assert.equal(dashRes.status, 200);
      const dashBody = (await dashRes.json()) as { nodes: unknown[] };
      assert.equal(dashBody.nodes.length, 3);
    } finally {
      await server.stop();
    }
  });

  it("2. Submits tasks via POST /v1/tasks, schedules onto local worker nodes, queries GET /v1/tasks/:id & exports Prometheus /metrics", async () => {
    const server = new AnstsLocalAndComposeServer({ host: "127.0.0.1", workerNodeCount: 2 });
    const { apiPort } = await server.start(0, 0);
    try {
      const submitRes = await fetch(`http://127.0.0.1:${apiPort}/v1/tasks`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task_id: "local-http-task-1",
          tenant_id: "tenant-local",
          priority: 1,
          prompt: "Verify local HTTP task submission and placement",
        }),
      });
      assert.equal(submitRes.status, 202);
      const submitBody = (await submitRes.json()) as {
        task_id: string;
        state: string;
        assigned_node_id: string;
        lease_fencing_token: number;
      };
      assert.equal(submitBody.task_id, "local-http-task-1");
      assert.equal(submitBody.state, "LEASED");
      assert.ok(submitBody.assigned_node_id.startsWith("ansts-worker-"));
      assert.equal(submitBody.lease_fencing_token, 1);

      const getRes = await fetch(`http://127.0.0.1:${apiPort}/v1/tasks/local-http-task-1`);
      assert.equal(getRes.status, 200);

      const metricsRes = await fetch(`http://127.0.0.1:${apiPort}/metrics`);
      const metricsText = await metricsRes.text();
      assert.ok(metricsText.includes("ansts_tasks_submitted_total 1"));
    } finally {
      await server.stop();
    }
  });

  it("3. Executes real LayerFS workspace commands over HTTP POST /v1/layerfs/exec and transitions to HTTP 503 on POST /shutdown", async () => {
    const server = new AnstsLocalAndComposeServer({ host: "127.0.0.1", workerNodeCount: 1 });
    const { apiPort } = await server.start(0, 0);
    try {
      const lfsRes = await fetch(`http://127.0.0.1:${apiPort}/v1/layerfs/exec`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          branch: "feature-http-branch",
          agent_id: "local-agent",
          step_id: "http-step-1",
          command: "node -e \"const r=require('react'); require('fs').writeFileSync('http_out.txt', r.version);\"",
        }),
      });
      assert.equal(lfsRes.status, 200);
      const lfsBody = (await lfsRes.json()) as { committedNewSnapshot: boolean; nodeModulesPreMounted: boolean };
      assert.equal(lfsBody.committedNewSnapshot, true);
      assert.equal(lfsBody.nodeModulesPreMounted, true);

      const shutRes = await fetch(`http://127.0.0.1:${apiPort}/shutdown?handoff=preserve`, { method: "POST" });
      assert.equal(shutRes.status, 200);

      const drainHealth = await fetch(`http://127.0.0.1:${apiPort}/.well-known/celld/health`);
      assert.equal(drainHealth.status, 503);
    } finally {
      await server.stop();
    }
  });

  it("4. Validates root Dockerfile, docker-compose.yml, and scripts/run-local.sh for local and container deployment", () => {
    const dockerfile = fs.readFileSync(path.join(process.cwd(), "Dockerfile"), "utf-8");
    assert.ok(dockerfile.includes("EXPOSE 8080 8090"));
    assert.ok(dockerfile.includes("HEALTHCHECK"));
    assert.ok(dockerfile.includes('CMD ["node", "dist/src/server_main.js"]'));

    const compose = fs.readFileSync(path.join(process.cwd(), "docker-compose.yml"), "utf-8");
    assert.ok(compose.includes("ansts-control-plane:"));
    assert.ok(compose.includes("ansts-worker-node-1:"));
    assert.ok(!compose.includes('version: "3.9"'));

    const runLocal = fs.readFileSync(path.join(process.cwd(), "scripts/run-local.sh"), "utf-8");
    assert.ok(runLocal.includes("exec node dist/src/server_main.js"));

    const devClusterCompose = fs.readFileSync(
      path.join(process.cwd(), "deploy/dev-cluster/docker-compose.dev-cluster.yml"),
      "utf-8"
    );
    assert.ok(devClusterCompose.includes("celld-brain-01:"));
    assert.ok(devClusterCompose.includes("body-daemon-01:"));
    assert.ok(devClusterCompose.includes("ansts-gateway-01:"));
    assert.ok(devClusterCompose.includes("tanstack-console:"));
    assert.ok(devClusterCompose.includes('"http://celld-brain-02:8082"'));
  });

  it("5. Verifies v2.5 dev-cluster Dual-Homed Tunnel Registration, Distributed Task Dispatch & TanStack Console BFF endpoints", async () => {
    const server = new AnstsLocalAndComposeServer({ host: "127.0.0.1", workerNodeCount: 2 });
    const { apiPort } = await server.start(0, 0);
    try {
      const tunRes = await fetch(`http://127.0.0.1:${apiPort}/internal/v1beta1/tunnel/register`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ hostId: "body-host-01", gatewayId: "celld-gw-01", role: "PRIMARY", epoch: 1 }),
      });
      assert.equal(tunRes.status, 200);

      const dispRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/tasks/dispatch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          tenantId: "tenant-alpha",
          taskId: "task-distributed-001",
          promptText: "verify distributed celld dispatch",
          targetBodyHostId: "body-host-01",
        }),
      });
      assert.equal(dispRes.status, 202);

      const ovRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/console/overview?region=us-west-2`);
      assert.equal(ovRes.status, 200);

      const shRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/console/shards`);
      assert.equal(shRes.status, 200);
    } finally {
      await server.stop();
    }
  });
});
