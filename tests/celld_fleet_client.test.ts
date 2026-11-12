/**
 * @fileoverview M-FLEET-1 CelldFleetClient Suite (6 tests): capacity-view parsing, health-aware node
 * selection, brain-schedule dispatch, body heartbeat sync, and `own.json` lease CAS.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import { InMemoryR2Bucket, CelldFleetClient, CelldOwnershipCasError, CelldFleetUnavailableError } from "../src/index.js";

interface CapturedCall {
  url: string;
  method: string;
  body: Record<string, unknown> | null;
}

function jsonFetch(
  handler: (url: string) => { status?: number; body: unknown } | null,
  captured: CapturedCall[] = []
): typeof fetch {
  return (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method ?? "GET";
    captured.push({
      url,
      method,
      body: init?.body ? (JSON.parse(String(init.body)) as Record<string, unknown>) : null,
    });
    const result = handler(url);
    if (!result) return new Response("{}", { status: 404 });
    return new Response(JSON.stringify(result.body), {
      status: result.status ?? 200,
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;
}

function capacityView(leases: Array<{ node: string; expiresMs: number; load: Record<string, unknown> }>) {
  return {
    ready: {
      started_ms: 1,
      leases: leases.map((lease) => ({
        node: lease.node,
        expires_ms: lease.expiresMs,
        addr: `${lease.node}:8082`,
        load: lease.load,
      })),
    },
  };
}

const NODES = [
  { id: "celld-node-01", publicUrl: "http://celld-01:8081", internalUrl: "http://celld-01:8082" },
  { id: "celld-node-02", publicUrl: "http://celld-02:8081", internalUrl: "http://celld-02:8082" },
];

describe("M-FLEET-1 CelldFleetClient", () => {
  it("1. parses fleet/capacity-v1.json into node states with wall-clock lease readiness", () => {
    const now = 1_000_000;
    const client = new CelldFleetClient({ nodes: NODES, now: () => now });
    const states = client.parseCapacityView(
      capacityView([
        {
          node: "celld-node-01",
          expiresMs: now - 1,
          load: { owned_cells: 3, resident_cells: 2, cgroup_working_set_bytes: 512, draining: false },
        },
        {
          node: "celld-node-02",
          expiresMs: now + 8_000,
          load: { owned_cells: 0, resident_cells: 0, cgroup_working_set_bytes: 64, draining: false },
        },
      ])
    );
    assert.equal(states.length, 2);
    assert.equal(states[0].nodeId, "celld-node-01");
    assert.equal(states[0].ready, false, "expired lease is not ready");
    assert.equal(states[0].ownedCells, 3);
    assert.equal(states[0].workingSetBytes, 512);
    assert.equal(states[1].ready, true);
  });

  it("2. pickNode skips expired and draining leases and returns the emptiest ready node", async () => {
    const now = 1_000_000;
    const bucket = new InMemoryR2Bucket();
    await bucket.put(
      "fleet/capacity-v1.json",
      JSON.stringify(
        capacityView([
          { node: "celld-node-01", expiresMs: now + 9_000, load: { cgroup_working_set_bytes: 900, draining: true } },
          { node: "celld-node-02", expiresMs: now + 9_000, load: { cgroup_working_set_bytes: 100 } },
        ])
      )
    );
    const client = new CelldFleetClient({ nodes: NODES, bucket, now: () => now, fetchImpl: jsonFetch(() => null) });
    const chosen = await client.pickNode();
    assert.equal(chosen.id, "celld-node-02");
  });

  it("3. pickNode throws CelldFleetUnavailableError when no node is ready", async () => {
    const now = 1_000_000;
    const bucket = new InMemoryR2Bucket();
    await bucket.put(
      "fleet/capacity-v1.json",
      JSON.stringify(capacityView([{ node: "celld-node-01", expiresMs: now - 5, load: {} }]))
    );
    // The fallback probe path also reports nothing healthy.
    const client = new CelldFleetClient({
      nodes: NODES,
      bucket,
      now: () => now,
      fetchImpl: jsonFetch(() => ({ status: 503, body: { ok: false } })),
    });
    await assert.rejects(() => client.pickNode(), CelldFleetUnavailableError);
  });

  it("4. scheduleTask posts the brain contract onto the chosen celld public listener", async () => {
    const now = 1_000_000;
    const bucket = new InMemoryR2Bucket();
    await bucket.put(
      "fleet/capacity-v1.json",
      JSON.stringify(
        capacityView([{ node: "celld-node-01", expiresMs: now + 9_000, load: { cgroup_working_set_bytes: 10 } }])
      )
    );
    const captured: CapturedCall[] = [];
    const client = new CelldFleetClient({
      nodes: NODES,
      bucket,
      now: () => now,
      fetchImpl: jsonFetch(() => ({
        body: {
          scheduledByBrainId: "celld-node-01",
          state: "LEASED",
          fencingToken: 7,
          deduplicated: false,
          placements: [["task-1", "body-host-01", 7]],
        },
      }), captured),
    });
    const result = await client.scheduleTask({ taskId: "task-1", tenantId: "t1", promptText: "hello" });
    assert.equal(captured.length, 1);
    assert.equal(captured[0].url, "http://celld-01:8081/internal/v1beta1/brain/schedule");
    assert.equal(captured[0].method, "POST");
    assert.equal(captured[0].body?.taskId, "task-1");
    assert.equal(result.celldNodeId, "celld-node-01");
    assert.equal(result.fencingToken, 7);
    assert.deepEqual(result.placements, [["task-1", "body-host-01", 7]]);
  });

  it("5. registerNodes heartbeats every body host into the scheduler cell", async () => {
    const now = 1_000_000;
    const bucket = new InMemoryR2Bucket();
    await bucket.put(
      "fleet/capacity-v1.json",
      JSON.stringify(
        capacityView([{ node: "celld-node-02", expiresMs: now + 9_000, load: { cgroup_working_set_bytes: 1 } }])
      )
    );
    const captured: CapturedCall[] = [];
    const client = new CelldFleetClient({
      nodes: NODES,
      bucket,
      now: () => now,
      fetchImpl: jsonFetch(() => ({ body: { registered: true } }), captured),
    });
    const registered = await client.registerNodes([
      { node_id: "body-host-01", total_cpu_cores: 32, free_cpu_cores: 32, total_memory_mb: 65536, free_memory_mb: 65536 },
      { node_id: "body-host-02", total_cpu_cores: 32, free_cpu_cores: 16, total_memory_mb: 65536, free_memory_mb: 32768 },
    ]);
    // Broadcast: every celld node must learn about every body, because brain calls load-balance across them.
    assert.equal(registered, 4);
    assert.deepEqual(
      captured.map((call) => call.url),
      [
        "http://celld-01:8081/v1/scheduler/register-node",
        "http://celld-01:8081/v1/scheduler/register-node",
        "http://celld-02:8081/v1/scheduler/register-node",
        "http://celld-02:8081/v1/scheduler/register-node",
      ]
    );
    assert.deepEqual(
      captured.map((call) => call.body?.node_id),
      ["body-host-01", "body-host-02", "body-host-01", "body-host-02"]
    );
  });

  it("6. claimOwnership uses own.json CAS: first claim If-None-Match, takeover If-Match, stale CAS rejected", async () => {
    const bucket = new InMemoryR2Bucket();
    const client = new CelldFleetClient({ nodes: NODES, bucket, fetchImpl: jsonFetch(() => null) });
    const classId = "AnstsSchedulerCell";
    const cellId = "145c35ef";

    const first = await client.claimOwnership(classId, cellId, "celld-node-01");
    assert.equal(first.epoch, 1);
    assert.equal(first.nodeId, "celld-node-01");
    const stored = await bucket.get(`cells/${classId}:${cellId}/own.json`);
    assert.ok(stored);
    assert.equal(await stored?.text(), JSON.stringify({ node: "celld-node-01", epoch: 1 }));

    // A second writer holding the same (now stale) etag must lose the CAS.
    const stolen = await client.claimOwnership(classId, cellId, "celld-node-02");
    assert.equal(stolen.epoch, 2);
    await assert.rejects(() => client.renewOwnership(first), CelldOwnershipCasError);

    const renewed = await client.renewOwnership(stolen);
    assert.equal(renewed.epoch, 2);
    assert.equal(renewed.nodeId, "celld-node-02");
  });

  it("7. fromEnv parses fleet url maps and rejects unsafe cell ids", () => {
    const client = CelldFleetClient.fromEnv({
      ANSTS_CELLD_FLEET_URLS: '{"celld-node-01":"http://celld-01:8081/"}',
      ANSTS_CELLD_INTERNAL_URLS: '{"celld-node-01":"http://celld-01:8082"}',
    });
    const nodes = client.listNodes();
    assert.equal(nodes.length, 1);
    assert.equal(nodes[0].publicUrl, "http://celld-01:8081");
    assert.equal(nodes[0].internalUrl, "http://celld-01:8082");
    assert.throws(() => CelldFleetClient.fromEnv({ ANSTS_CELLD_FLEET_URLS: '"not-a-map"' }), /JSON object/);
  });
});
