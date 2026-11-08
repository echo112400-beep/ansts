/**
 * @fileoverview Canonical scheduler path, queue ack policy, binding names, and API version alias.
 * These are in-process tests. They do not start celld, S3, or a Cloudflare queue.
 */
import { test } from "node:test";
import { strict as assert } from "assert";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell";
import { CloudflareCelldEnv, MockDurableObjectState } from "../src/cloudflare/env";
import cloudflareCelldWorker, { SCHEDULER_BINDING_ERROR } from "../src/cloudflare/worker";
import { CANONICAL_TASKSPEC_API_VERSION, TaskSpecValidator } from "../src/gateway/validator";
import { TaskPriority, TaskSpec } from "../src/models";

const { DatabaseSync } = require("node:sqlite");

function sqliteCell() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-authority-"));
  const filename = path.join(dir, "scheduler.sqlite");
  let db: any = new DatabaseSync(filename);
  const env = { ANSTS_EVENTS_QUEUE: { send: async () => undefined } } as CloudflareCelldEnv;
  let state: MockDurableObjectState;
  const open = () => {
    state = new MockDurableObjectState("authority-cell");
    state.storage.sql = {
      exec: (query: string, ...bindings: any[]) => {
        const rows = db.prepare(query).all(...bindings);
        return { toArray: () => rows, one: () => rows[0] ?? null };
      },
    };
    state.storage.transactionSync = (callback: () => unknown) => {
      db.exec("BEGIN IMMEDIATE");
      try {
        const result = callback();
        db.exec("COMMIT");
        return result;
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    };
    return new AnstsSchedulerCell(state, env);
  };
  return {
    cell: open(),
    reopen: () => {
      db.close();
      db = new DatabaseSync(filename);
      return open();
    },
    sql(query: string) { return db.prepare(query); },
    close() { db.close(); fs.rmSync(dir, { recursive: true, force: true }); },
  };
}

test("legacy /submit and /tasks/submit share one canonical task and keep the ansts_tasks projection", async () => {
  const fx = sqliteCell();
  try {
    const created = await fx.cell.fetch(new Request("https://cell/submit", {
      method: "POST",
      body: JSON.stringify({ task_id: "shared", tenant_id: "fin-enterprise", idempotency_key: "shared", prompt: "keep" }),
    }));
    assert.equal(created.status, 202);
    const again = await fx.cell.fetch(new Request("https://cell/v1beta1/tasks/submit", {
      method: "POST",
      body: JSON.stringify({ apiVersion: "scheduler.agent.io/v1beta1", kind: "AgentTask", metadata: { task_id: "other", tenant_id: "fin-enterprise", idempotency_key: "shared" }, spec: {} }),
    }));
    assert.equal(again.status, 200);
    assert.equal((await again.json() as any).task_id, "shared");
    const cell = fx.reopen();
    assert.equal(cell.cluster.scheduler.tasks.size, 1);
    assert.equal(fx.sql("SELECT COUNT(*) AS n FROM cell_tasks").get().n, 1);
    assert.equal(fx.sql("SELECT COUNT(*) AS n FROM ansts_tasks").get().n, 1);
    assert.equal(fx.sql("SELECT payload_json FROM ansts_tasks").get().payload_json.includes("keep"), true);
  } finally { fx.close(); }
});

test("ansts_tasks rows are copied into v2.5 without being deleted, and a second open does not duplicate them", async () => {
  const fx = sqliteCell();
  try {
    fx.sql("INSERT INTO ansts_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, payload_json) VALUES ('old', 'tenant-a', 1, 'QUEUED', 0, '', '{\"prompt\":\"from-legacy\"}')").run();
    const imported = fx.reopen();
    assert.equal(imported.cluster.scheduler.tasks.get("old")?.state, "QUEUED");
    assert.equal(fx.sql("SELECT COUNT(*) AS n FROM ansts_tasks").get().n, 1);
    assert.equal(fx.sql("SELECT copied_count FROM scheduler_migration").get().copied_count, 1);
    const again = fx.reopen();
    assert.equal(again.cluster.scheduler.tasks.size, 1);
    assert.equal(fx.sql("SELECT COUNT(*) AS n FROM cell_tasks").get().n, 1);
    assert.equal(fx.sql("SELECT COUNT(*) AS n FROM ansts_tasks").get().n, 1);
  } finally { fx.close(); }
});

test("disagreeing legacy and canonical rows fail closed and leave both rows in place", async () => {
  const fx = sqliteCell();
  try {
    await fx.cell.fetch(new Request("https://cell/submit", {
      method: "POST",
      body: JSON.stringify({ task_id: "conflict", tenant_id: "tenant-a", idempotency_key: "conflict" }),
    }));
    fx.sql("UPDATE ansts_tasks SET state = 'FAILED' WHERE task_id = 'conflict'").run();
    assert.throws(() => fx.reopen(), /disagrees with canonical/);
    assert.equal(fx.sql("SELECT state FROM ansts_tasks").get().state, "FAILED");
    assert.equal(fx.sql("SELECT state FROM cell_tasks").get().state, "QUEUED");
  } finally { fx.close(); }
});

test("queue consumer acks only after a durable receipt and dedupes a repeated event", async () => {
  const fx = sqliteCell();
  const acked: string[] = [];
  const env = {
    SCHEDULER_CELL: {
      idFromName: () => ({ toString: () => "cluster" }),
      get: () => ({ fetch: (input: Request) => fx.cell.fetch(input) }),
    },
  } as unknown as CloudflareCelldEnv;
  const message = (id: string) => ({
    id,
    timestamp: new Date(),
    attempts: 1,
    body: { event_id: "evt_shared_queued", event_type: "agent.task.queued", task_id: "shared" },
    ack: () => { acked.push(id); },
    retry: () => { throw new Error("retry should not be required for a committed receipt"); },
  });
  try {
    await cloudflareCelldWorker.queue({ queue: "ansts-cloudevents-outbox", messages: [message("m1")] }, env);
    await cloudflareCelldWorker.queue({ queue: "ansts-cloudevents-outbox", messages: [message("m2")] }, env);
    assert.deepEqual(acked, ["m1", "m2"]);
    assert.equal(fx.sql("SELECT COUNT(*) AS n FROM cell_event_receipts").get().n, 1);
  } finally { fx.close(); }
});

test("queue consumer does not ack when the cell rejects the delivery", async () => {
  const acked: string[] = [];
  const env = {
    ANSTS_SCHEDULER_CELL: {
      idFromName: () => ({ toString: () => "cluster" }),
      get: () => ({ fetch: async () => new Response("no", { status: 503 }) }),
    },
  } as unknown as CloudflareCelldEnv;
  await assert.rejects(
    cloudflareCelldWorker.queue({
      queue: "ansts-cloudevents-outbox",
      messages: [{
        id: "m",
        timestamp: new Date(),
        attempts: 1,
        body: { event_id: "evt_x", event_type: "agent.task.queued", task_id: "x" },
        ack: () => { acked.push("m"); },
        retry: () => undefined,
      }],
    }, env),
    /without ack/,
  );
  assert.deepEqual(acked, []);
});

test("missing scheduler binding fails HTTP and the queue without acking", async () => {
  const acked: string[] = [];
  const missing = {} as CloudflareCelldEnv;
  const response = await cloudflareCelldWorker.fetch(new Request("https://worker/v1/scheduler/tick", { method: "POST", body: "{}" }), missing);
  assert.equal(response.status, 500);
  assert.match(await response.text(), new RegExp(SCHEDULER_BINDING_ERROR));
  await assert.rejects(
    cloudflareCelldWorker.queue({
      queue: "q",
      messages: [{ id: "m", timestamp: new Date(), attempts: 1, body: { event_id: "evt" }, ack: () => { acked.push("m"); }, retry: () => undefined }],
    }, missing),
    /scheduler binding missing/,
  );
  assert.deepEqual(acked, []);
});

test("either binding name routes both scheduler prefixes, and SCHEDULER_CELL wins when both are set", async () => {
  const names: string[] = [];
  const namespace = (label: string) => ({
    idFromName(name: string) { names.push(`${label}:${name}`); return { toString: () => name }; },
    get: () => ({ fetch: async () => new Response("{}") }),
  });
  const onlyLegacy = { ANSTS_SCHEDULER_CELL: namespace("legacy") } as unknown as CloudflareCelldEnv;
  await cloudflareCelldWorker.fetch(new Request("https://worker/v1/scheduler/tick", { method: "POST", body: "{}" }), onlyLegacy);
  await cloudflareCelldWorker.fetch(new Request("https://worker/v1beta1/scheduler/tick", { method: "POST", body: "{}" }), onlyLegacy);
  assert.deepEqual(names, ["legacy:global-shard-0", "legacy:global-5d-scheduler"]);

  names.length = 0;
  const both = {
    SCHEDULER_CLUSTER_ID: "cluster-east",
    SCHEDULER_CELL: namespace("wrangler"),
    ANSTS_SCHEDULER_CELL: namespace("legacy"),
  } as unknown as CloudflareCelldEnv;
  await cloudflareCelldWorker.fetch(new Request("https://worker/v1/scheduler/tick", { method: "POST", body: "{}" }), both);
  await cloudflareCelldWorker.fetch(new Request("https://worker/v1beta1/scheduler/tick", { method: "POST", body: "{}" }), both);
  assert.deepEqual(names, ["wrangler:cluster-east", "wrangler:cluster-east"]);
});

test("gateway accepts the canonical TaskSpec version and the historical alias", () => {
  const canonical = TaskSpecValidator.validateAndNormalize({
    apiVersion: CANONICAL_TASKSPEC_API_VERSION,
    task_id: "task-canonical",
    tenant_id: "tenant-a",
  });
  const alias = TaskSpecValidator.validateAndNormalize({
    apiVersion: "agent.scheduler.io/v1beta1",
    task_id: "task-alias",
    tenant_id: "tenant-a",
  });
  assert.equal(canonical.task_id, "task-canonical");
  assert.equal(alias.task_id, "task-alias");
  assert.equal(CANONICAL_TASKSPEC_API_VERSION, "scheduler.agent.io/v1beta1");
  assert.throws(() => TaskSpecValidator.validateAndNormalize({
    apiVersion: "scheduler.example/v9",
    task_id: "task-nope",
    tenant_id: "tenant-a",
  }), /Unsupported apiVersion/);
});

test("deploy files do not embed credential defaults or mask install and cgroup failures", () => {
  const root = process.cwd();
  const read = (rel: string) => fs.readFileSync(path.join(root, rel), "utf8");
  for (const rel of [
    "deploy/dev-cluster/docker-compose.dev-cluster.yml",
    "deploy/rustfs-celld/docker-compose.rustfs-ha.yml",
    "deploy/manual-fleet/production-deploy.sh",
  ]) {
    const text = read(rel);
    assert.equal(text.includes(":-ansts_"), false, rel);
    assert.equal(text.includes(":-rustfs"), false, rel);
  }
  assert.equal(read("Dockerfile.dev").includes("|| true"), false);
  assert.equal(read("deploy/manual-fleet/production-deploy.sh").includes("cgroup.subtree_control\" || true"), false);
  assert.match(read("deploy/manual-fleet/production-deploy.sh"), /must be set/);
  const task = new TaskSpec({ task_id: "priority-still-constructs", tenant_id: "t", priority: TaskPriority.P1_HIGH });
  assert.equal(task.priority, TaskPriority.P1_HIGH);
});
