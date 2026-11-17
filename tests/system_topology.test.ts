import { it } from "node:test";
import * as assert from "node:assert/strict";
import gatewayWorker from "../src/cloudflare/gateway_worker.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import {
  collectSystemTopology,
  parseTopologyConfig,
} from "../src/cloudflare/system_topology.js";
function configuration(scenario: string) {
  const node = (id: string, kind: string, plane: string) => ({
    id,
    label: id,
    kind,
    plane,
    description: scenario,
    url: `https://${id}.test`,
    healthPath: "/healthz",
    ...(kind === "fleet"
      ? { telemetryUrl: `https://${id}.test`, telemetryPath: "/state" }
      : {}),
  });
  return JSON.stringify({
    schemaVersion: 1,
    nodes: [
      node("gw", "gateway", "gateway"),
      node("fleet", "fleet", "control"),
      node("body", "daemon", "execution"),
      node("storage", "storage", "storage"),
      node("prom", "prometheus", "observability"),
      node("grafana", "grafana", "observability"),
      {
        id: "github",
        label: "GitHub",
        kind: "external",
        plane: "external",
        description: "unprobed",
      },
      {
        id: "engine",
        label: "Engine",
        kind: "logical",
        plane: "execution",
        description: "unprobed",
      },
    ],
    edges: [
      { source: "gw", target: "fleet", label: "control", kind: "state" },
      { source: "body", target: "fleet", label: "result", kind: "execution" },
    ],
  });
}
function healthy(input: URL | string | Request) {
  const url = new URL(String(input));
  switch (url.hostname) {
    case "gw.test":
      return Response.json({
        healthy: true,
        controlPlane: "celld-gateway",
        gateway: { cellId: "shared-gateway" },
        scheduler: { reachable: true },
        secret: "never-return-this",
      });
    case "fleet.test":
      return url.pathname === "/state"
        ? Response.json({
            owned_cells: 4,
            residents: [{ task_id: "tenant-secret" }],
            rss_bytes: 1024,
            token: "never-return-this",
          })
        : Response.json({
            healthy: true,
            controlPlane: "celld-scheduler",
            bodyAuthority: "fleet",
            bodyNamespace: "body-ha-v1:",
          });
    case "body.test":
      return Response.json({
        healthy: true,
        draining: false,
        recoveryRequired: false,
        activeSandboxes: 0,
        capacity: { slots: 1, reserved: 0, available: 1 },
        resources: {
          total: { cpu: 2, memoryMb: 1024 },
          reserved: { cpu: 0, memoryMb: 0 },
          available: { cpu: 2, memoryMb: 1024 },
        },
        journal: { taskId: "tenant-secret" },
      });
    case "storage.test":
      return Response.json({ ready: true });
    case "prom.test":
      return Response.json({
        status: "success",
        data: {
          activeTargets: [
            {
              health: "up",
              scrapeUrl: "http://body.test/metrics",
              labels: { job: "body" },
              lastScrape: "2026-11-15T00:00:00Z",
            },
          ],
        },
      });
    case "grafana.test":
      return Response.json({ database: "ok", version: "11.0.0" });
    default:
      throw new Error("unexpected URL");
  }
}
it("authorizes infrastructure before probing; query parameters cannot select another target", async () => {
  const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async (input) => {
    calls++;
    return healthy(input);
  };
  const secret = "system-topology-user-key-at-least-32-characters";
  const env = {
    ANSTS_USER_AUTH_SECRET: secret,
    ANSTS_SYSTEM_TOPOLOGY_JSON: configuration("authorization"),
  } as never;
  const guard = new ConsoleRbacAuthGuard(secret);
  const get = (role?: "VIEWER" | "OPERATOR" | "SRE_ADMIN") =>
    gatewayWorker.fetch(
      new Request(
        "https://console/v1/console/system?url=http://attacker.test",
        {
          headers: role
            ? {
                authorization: `Bearer ${guard.issueToken({ sub: "system-test", role, scopeTenant: "a", ttlSec: 900 })}`,
              }
            : {},
        },
      ),
      env,
    );
  try {
    assert.equal((await get()).status, 401);
    for (const role of ["VIEWER", "OPERATOR"] as const)
      assert.equal((await get(role)).status, 403);
    assert.equal(calls, 0);
    const response = await get("SRE_ADMIN");
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("cache-control"), "no-store");
    const data = await response.json();
    assert.equal(data.status, "healthy");
    assert.ok(!JSON.stringify(data).includes("tenant-secret"));
    assert.ok(!JSON.stringify(data).includes("never-return-this"));
    assert.equal(data.summary.unverified, 2);
    assert.equal(data.summary.availableSlots, 1);
  } finally {
    globalThis.fetch = original;
  }
});
it("reports functional scrape failures separately from HTTP availability and retains daemon recovery evidence", async () => {
  const original = globalThis.fetch;
  globalThis.fetch = async (input) => {
    const url = new URL(String(input));
    if (url.hostname === "prom.test")
      return Response.json({
        status: "success",
        data: {
          activeTargets: [
            {
              health: "down",
              scrapeUrl:
                "http://user:private-password@body.test/metrics?token=secret-query",
              labels: { job: "body" },
              lastError: "server returned HTTP status 401 Unauthorized",
            },
          ],
        },
      });
    if (url.hostname === "body.test")
      return Response.json(
        {
          healthy: false,
          draining: true,
          recoveryRequired: true,
          capacity: { slots: 1, reserved: 1, available: 0 },
          resources: {
            total: { cpu: 2, memoryMb: 1024 },
            reserved: { cpu: 1, memoryMb: 512 },
          },
        },
        { status: 503 },
      );
    return healthy(input);
  };
  try {
    const result = await collectSystemTopology({
      ANSTS_SYSTEM_TOPOLOGY_JSON: configuration("functional"),
    });
    assert.equal(result.status, "degraded");
    const prom = result.nodes.find((n) => n.id === "prom")!;
    assert.equal(prom.httpStatus, 200);
    assert.equal(prom.status, "degraded");
    assert.equal(prom.targets![0].error, "HTTP 401");
    assert.equal(prom.targets![0].endpoint, "body.test/metrics");
    assert.ok(!JSON.stringify(result).includes("private-password"));
    assert.ok(!JSON.stringify(result).includes("secret-query"));
    const body = result.nodes.find((n) => n.id === "body")!;
    assert.equal(body.status, "degraded");
    assert.match(body.summary, /恢复/);
    assert.equal(result.summary.availableSlots, 0);
    assert.equal(
      result.nodes.find((n) => n.id === "engine")!.status,
      "unverified",
    );
  } finally {
    globalThis.fetch = original;
  }
});
it("returns partial observations when a critical node times out, and coalesces simultaneous reads", async () => {
  const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async (input) => {
    calls++;
    if (String(input).includes("storage.test"))
      throw new DOMException("timeout", "TimeoutError");
    return healthy(input);
  };
  try {
    const env = { ANSTS_SYSTEM_TOPOLOGY_JSON: configuration("partial") };
    const [first, second] = await Promise.all([
      collectSystemTopology(env),
      collectSystemTopology(env),
    ]);
    assert.equal(first, second);
    assert.equal(calls, 7);
    assert.equal(first.status, "unavailable");
    assert.equal(
      first.nodes.find((n) => n.id === "storage")!.summary,
      "探测超时",
    );
    assert.equal(first.nodes.find((n) => n.id === "body")!.status, "healthy");
    assert.equal(first.summary.probed, 6);
  } finally {
    globalThis.fetch = original;
  }
});
it("does not invent capacity or memory when upstream fields are missing", async () => {
  const original = globalThis.fetch;
  globalThis.fetch = async (input) =>
    String(input).includes("body.test")
      ? Response.json({ healthy: true })
      : healthy(input);
  try {
    const result = await collectSystemTopology({
      ANSTS_SYSTEM_TOPOLOGY_JSON: configuration("missing"),
    });
    assert.equal(result.summary.slots, null);
    assert.equal(result.summary.availableSlots, null);
    assert.equal(result.summary.capacityReporting, 0);
    assert.equal(
      result.nodes
        .find((n) => n.id === "body")!
        .metrics.find((m) => m.key === "cpuTotal")!.value,
      null,
    );
  } finally {
    globalThis.fetch = original;
  }
});
it("rejects unsafe configuration and response bodies above the bound", async () => {
  const raw = JSON.parse(configuration("validation"));
  raw.nodes[0].url = "http://user:password@gw.test";
  assert.throws(() => parseTopologyConfig(JSON.stringify(raw)));
  raw.nodes[0].url = "https://gw.test";
  raw.edges[0].target = "unknown";
  assert.throws(() => parseTopologyConfig(JSON.stringify(raw)));
  const original = globalThis.fetch;
  globalThis.fetch = async (input) =>
    String(input).includes("gw.test")
      ? Response.json({ padding: "x".repeat(262145) })
      : healthy(input);
  try {
    const result = await collectSystemTopology({
      ANSTS_SYSTEM_TOPOLOGY_JSON: configuration("large"),
    });
    assert.notEqual(result.nodes.find((n) => n.id === "gw")!.status, "healthy");
    assert.ok(!JSON.stringify(result).includes("padding"));
  } finally {
    globalThis.fetch = original;
  }
});
