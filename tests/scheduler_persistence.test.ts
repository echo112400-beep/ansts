import { strict as assert } from "assert";
import { test } from "node:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell";
import { CloudflareCelldEnv, MockDurableObjectState } from "../src/cloudflare/env";
import { cloudflareCelldWorker } from "../src/index";
import { FenceToken, TaskState } from "../src/models";

// Real SQLite transactions and disk reopen: the regex SQL mock cannot prove durability.
const { DatabaseSync } = require("node:sqlite");
function fixture() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-scheduler-"));
  const filename = path.join(dir, "scheduler.sqlite");
  let db: any;
  let failQuery = "";
  let queueDown = true;
  const sent: any[] = [];
  const env = { ANSTS_EVENTS_QUEUE: { send: async (event: any) => {
    if (queueDown) throw new Error("queue offline");
    sent.push(event);
  } } } as CloudflareCelldEnv;
  let state: MockDurableObjectState;
  function open() {
    db = new DatabaseSync(filename);
    state = new MockDurableObjectState("persistent-cluster");
    state.storage.sql = { exec: (query: string, ...bindings: any[]) => {
      if (failQuery && query.includes(failQuery)) throw new Error("injected disk failure");
      const rows = db.prepare(query).all(...bindings);
      return { toArray: () => rows, one: () => rows[0] ?? null };
    } };
    state.storage.transactionSync = (callback) => {
      db.exec("BEGIN IMMEDIATE");
      try { const result = callback(); db.exec("COMMIT"); return result; }
      catch (error) { db.exec("ROLLBACK"); throw error; }
    };
    return new AnstsSchedulerCell(state, env);
  }
  let cell = open();
  return {
    get cell() { return cell; }, get state() { return state; }, env, sent,
    fail(query: string) { failQuery = query; }, queueUp() { queueDown = false; },
    reopen() { db.close(); cell = open(); return cell; },
    sql(query: string) { return db.prepare(query); },
    close() { db.close(); fs.rmSync(dir, { recursive: true, force: true }); },
  };
}
const spec = (id: string, idem = id) => ({ apiVersion: "scheduler.agent.io/v1beta1", kind: "AgentTask",
  metadata: { task_id: id, tenant_id: "fin-enterprise", idempotency_key: idem }, spec: {} });
const post = (cell: AnstsSchedulerCell, endpoint: string, body: unknown = {}) => cell.fetch(new Request(`https://cell${endpoint}`, {
  method: "POST", body: JSON.stringify(body),
}));

test("accepted queues, profiles and durable idempotency survive SQLite close/reopen", async () => {
  const f = fixture();
  try {
    assert.equal((await post(f.cell, "/tasks/submit", spec("one"))).status, 202);
    f.reopen();
    const task = f.cell.cluster.scheduler.tasks.get("one")!;
    assert.equal(task.state, TaskState.QUEUED);
    assert.equal(f.cell.cluster.queueMgr.tenantQueues.get(task.tenantId)![0], task);
    assert.ok(task.fence instanceof FenceToken);
    assert.equal(typeof task.demandVector().steadyCpu, "number");
    assert.equal(f.cell.cluster.profiler.snapshot().length, 1);
    const retry = await post(f.cell, "/tasks/submit", spec("different-id", "one"));
    assert.equal(retry.status, 200);
    assert.equal((await retry.json() as Record<string, any>).task_id, "one");
    assert.equal(f.cell.cluster.scheduler.tasks.size, 1);
    assert.equal((await post(f.cell, "/tasks/submit", spec("one", "new-key"))).status, 409);
  } finally { f.close(); }
});

test("concurrent duplicate submissions produce one task and one durable event", async () => {
  const f = fixture();
  try {
    const responses = await Promise.all(Array.from({ length: 10 }, () => post(f.cell, "/tasks/submit", spec("one"))));
    assert.equal(responses.filter(r => r.status === 202).length, 1);
    assert.equal(f.cell.cluster.queueMgr.tenantQueues.get("fin-enterprise")!.length, 1);
    f.reopen();
    f.queueUp();
    await f.cell.alarm();
    assert.equal(f.sent.length, 1);
    assert.equal(f.sent[0].event_type, "agent.task.queued");
  } finally { f.close(); }
});

test("failed transaction rolls back checkpoint, lease projection and runtime cache", async () => {
  const f = fixture();
  try {
    await post(f.cell, "/tasks/submit", spec("one"));
    const before = f.sql("SELECT payload FROM scheduler_checkpoint").get().payload;
    f.fail("INSERT INTO cell_leases");
    assert.equal((await post(f.cell, "/scheduler/tick")).status, 503);
    assert.equal(f.cell.cluster.scheduler.tasks.get("one")!.state, TaskState.QUEUED);
    assert.equal(f.sql("SELECT payload FROM scheduler_checkpoint").get().payload, before);
    assert.equal(f.sql("SELECT * FROM cell_leases").all().length, 0);
    f.fail("");
    f.reopen();
    assert.equal((await (await post(f.cell, "/scheduler/tick")).json() as Record<string, any>).dispatched_count, 1);
    f.reopen();
    assert.equal((await (await post(f.cell, "/scheduler/tick")).json() as Record<string, any>).dispatched_count, 0);
    assert.equal(f.cell.cluster.leaseCoord.getLease("one")!.fence.epoch, 1);
  } finally { f.close(); }
});

test("outbox retries the same event ID after a send/ack crash and keeps concurrent submissions", async () => {
  const f = fixture();
  try {
    await post(f.cell, "/tasks/submit", spec("one"));
    await assert.rejects(f.cell.alarm(), /queue offline/);
    f.reopen();
    f.queueUp();
    f.fail("INSERT INTO scheduler_checkpoint");
    // Fail after the queue accepts the event, rather than during alarm's initial checkpoint.
    const send = f.env.ANSTS_EVENTS_QUEUE.send;
    f.fail("");
    f.env.ANSTS_EVENTS_QUEUE.send = async event => { await send(event); f.fail("INSERT INTO scheduler_checkpoint"); };
    await assert.rejects(f.cell.alarm(), /disk failure/);
    f.fail("");
    f.reopen();
    f.env.ANSTS_EVENTS_QUEUE.send = async event => {
      await send(event);
      await post(f.cell, "/tasks/submit", spec("two"));
    };
    await f.cell.alarm();
    assert.equal(f.sent[0].event_id, f.sent[1].event_id);
    f.env.ANSTS_EVENTS_QUEUE.send = send;
    await f.cell.alarm();
    assert.equal(f.sent.filter(e => e.task_id === "two").length, 1);
    f.reopen();
    assert.equal(f.cell.cluster.scheduler.tasks.size, 2);
    assert.equal((await (await f.cell.fetch(new Request("https://cell/cluster/top"))).json() as Record<string, any>).pending_outbox_events, 0);
  } finally { f.close(); }
});

test("DAG identity and pending escrow children survive all nodes being offline", async () => {
  const f = fixture();
  try {
    await post(f.cell, "/tasks/submit", spec("parent"));
    await post(f.cell, "/scheduler/tick");
    for (const n of f.cell.cluster.scheduler.nodes.values()) n.alive = false;
    const body = { dag_id: "dag", parent_task_id: "parent", children_ids: ["a", "b"] };
    await post(f.cell, "/dag/spawn-escrow", body);
    f.reopen();
    const c = f.cell.cluster;
    const dag = c.queueMgr.activeDags.get("dag")!;
    assert.equal(c.queueMgr.escrowFastQueue.length, 2);
    assert.equal(c.queueMgr.issuedVouchers.size, 2);
    assert.ok(c.queueMgr.issuedVouchers.get("a")!.parentFence instanceof FenceToken);
    assert.equal(dag.parentTask, c.scheduler.tasks.get("parent"));
    assert.equal(c.queueMgr.escrowFastQueue[0].child, dag.children[0]);
    assert.equal((await (await post(f.cell, "/dag/spawn-escrow", body)).json() as Record<string, any>).deduplicated, true);
    await post(f.cell, "/nodes/heartbeat", { node_id: "worker-us-west-02" });
    assert.equal((await (await post(f.cell, "/scheduler/tick")).json() as Record<string, any>).dispatched_count, 2);
    f.reopen();
    assert.equal(f.cell.cluster.queueMgr.activeDags.get("dag")!.children[0], f.cell.cluster.scheduler.tasks.get("a"));
  } finally { f.close(); }
});

test("dead-node recovery is repeatable and invalidates the previous fence across restart", async () => {
  const f = fixture();
  const realNow = Date.now;
  try {
    await post(f.cell, "/tasks/submit", spec("one"));
    await post(f.cell, "/scheduler/tick");
    const lease = f.cell.cluster.leaseCoord.getLease("one")!;
    const old = { task_id: "one", node_id: lease.nodeId, epoch: lease.fence.epoch, turn_seq: lease.fence.turnSeq };
    Date.now = () => realNow() + 20000;
    f.reopen();
    assert.deepEqual((await (await post(f.cell, "/scheduler/recover")).json() as Record<string, any>).recovered_tasks, ["one"]);
    assert.equal((await (await post(f.cell, "/leases/validate", old)).json() as Record<string, any>).valid, false);
    assert.deepEqual((await (await post(f.cell, "/scheduler/recover")).json() as Record<string, any>).recovered_tasks, []);
    await post(f.cell, "/scheduler/tick");
    f.reopen();
    assert.ok(f.cell.cluster.leaseCoord.getLease("one")!.fence.epoch > old.epoch);
    assert.notEqual(f.cell.cluster.leaseCoord.getLease("one")!.nodeId, old.node_id);
  } finally { Date.now = realNow; f.close(); }
});

test("corrupt or legacy state fails closed instead of starting an empty scheduler", async () => {
  const f = fixture();
  try {
    await post(f.cell, "/tasks/submit", spec("one"));
    f.sql("UPDATE scheduler_checkpoint SET checksum = 'bad'").run();
    assert.throws(() => new AnstsSchedulerCell(f.state, f.env), /checksum mismatch/);
    f.sql("DELETE FROM scheduler_checkpoint").run();
    f.sql("INSERT INTO cell_leases (task_id) VALUES ('legacy')").run();
    assert.throws(() => new AnstsSchedulerCell(f.state, f.env), /operator migration/);
  } finally { f.close(); }
});

test("Worker routes all scheduler operations to the configured stable cluster identity", async () => {
  const names: string[] = [];
  const env = { SCHEDULER_CLUSTER_ID: "cluster-east", SCHEDULER_CELL: {
    idFromName(name: string) { names.push(name); return { name, toString: () => name }; },
    get() { return { fetch: async () => new Response("{}") }; },
  } } as unknown as CloudflareCelldEnv;
  for (const endpoint of ["scheduler/tick", "scheduler/recover", "nodes/heartbeat", "leases/validate", "dag/spawn-escrow"]) {
    await cloudflareCelldWorker.fetch(new Request(`https://worker/v1beta1/${endpoint}`, { method: "POST", body: "{}" }), env);
  }
  await cloudflareCelldWorker.fetch(new Request("https://worker/v1beta1/tasks", { method: "POST", body: JSON.stringify(spec("one")) }), env);
  assert.deepEqual(names, Array(6).fill("cluster-east"));
});

test("completed tasks remain terminal after restart and reject obsolete leases", async () => {
  const f = fixture();
  const realNow = Date.now;
  try {
    await post(f.cell, "/tasks/submit", spec("done"));
    const dispatch = await (await post(f.cell, "/scheduler/tick")).json() as any;
    const t = dispatch.dispatched_tasks[0];
    const completion = { task_id: "done", node_id: t.node_id, epoch: t.fence.epoch, turn_seq: 7 };
    assert.equal((await post(f.cell, "/tasks/complete", { ...completion, epoch: 0 })).status, 409);
    assert.equal((await post(f.cell, "/tasks/complete", completion)).status, 200);
    f.reopen();
    assert.equal(f.cell.cluster.scheduler.tasks.get("done")!.state, TaskState.SUCCEEDED);
    assert.equal((await (await post(f.cell, "/tasks/complete", completion)).json() as any).deduplicated, true);
    assert.equal((await (await post(f.cell, "/leases/validate", completion)).json() as any).valid, false);
    Date.now = () => realNow() + 20000;
    await f.cell.alarm().catch(error => { assert.match(String(error), /queue offline/); });
    assert.equal(f.cell.cluster.scheduler.tasks.get("done")!.state, TaskState.SUCCEEDED);
    assert.equal((await (await post(f.cell, "/scheduler/tick")).json() as any).dispatched_count, 0);
  } finally { Date.now = realNow; f.close(); }
});

test("new traffic preserves an earlier alarm; ambiguous sync failure is safe to retry", async () => {
  const f = fixture();
  try {
    const due = Date.now() + 1000;
    await f.state.storage.setAlarm(due);
    f.state.storage.sync = async () => { throw new Error("sync failed"); };
    assert.equal((await post(f.cell, "/tasks/submit", spec("one"))).status, 503);
    assert.equal(await f.state.storage.getAlarm(), due);
    f.reopen();
    assert.equal((await post(f.cell, "/tasks/submit", spec("one"))).status, 200);
    assert.equal(f.cell.cluster.queueMgr.tenantQueues.get("fin-enterprise")!.length, 1);
  } finally { f.close(); }
});
