import { strict as assert } from "node:assert";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { test } from "node:test";
import { MockDurableObjectState } from "../src/cloudflare/env";
import { TenantDirectoryCell, tenantDirectoryShardIndex } from "../src/cloudflare/tenant_directory_cell";
import { projectTaskEvents, projectTenantTasks, readTenantTaskProjection } from "../src/dashboard/durable_query_adapter";
import { TenantLedgerCell } from "../src/persistence/tenant";
import worker from "../src/cloudflare/worker";
import * as yaml from "js-yaml";
import { TaskSpecValidator } from "../src/gateway/validator";

// Real SQLite semantics and file reopen; storage.sync remains a test binding, not a Celld bucket proof.
let DatabaseSync: any;
try { ({ DatabaseSync } = require("node:sqlite")); }
catch (error) { if ((error as NodeJS.ErrnoException).code !== "ERR_UNKNOWN_BUILTIN_MODULE") throw error; }
const sqliteUnavailable = !DatabaseSync && "Real SQLite verification requires Node with node:sqlite (verified on Node 24).";

function sqliteStorage(db: any): any {
  return {
    exec(query: string, ...bindings: unknown[]) {
      const statement = db.prepare(query);
      const rows = statement.columns().length ? statement.all(...bindings) : (statement.run(...bindings), []);
      return { toArray: () => rows, one: () => rows[0] ?? null };
    },
    transactionSync(callback: () => unknown) {
      db.exec("BEGIN IMMEDIATE");
      try { const result = callback(); db.exec("COMMIT"); return result; }
      catch (error) { db.exec("ROLLBACK"); throw error; }
    },
  };
}

function d1Binding(db: any, beforeRun: (query: string) => void = () => {}): any {
  return { prepare(query: string) {
    let values: unknown[] = [];
    const statement = {
      bind(...args: unknown[]) { values = args; return statement; },
      async run() { beforeRun(query); db.prepare(query).run(...values); return { success: true }; },
      async all() { return { results: db.prepare(query).all(...values) }; },
    };
    return statement;
  } };
}

function projectionDatabase(): any {
  const db = new DatabaseSync(":memory:");
  db.exec(readFileSync(path.join(__dirname, "../../migrations/0002_rebuildable_task_projections.sql"), "utf8"));
  return db;
}

const event = { event_id: "sqlite-event", tenant_id: "tenant", task_id: "task", event_type: "task.turn.committed",
  aggregate_version: 2, payload_digest: "a".repeat(64), payload: { answer: "first" }, created_at: Date.now() };

test("public admission survives lost sync acknowledgment and reopening every owner SQLite file", { skip: sqliteUnavailable }, async () => {
  const folder = mkdtempSync(path.join(tmpdir(), "ansts-admission-sqlite-"));
  const opened = new Map<string, { db: any; cell: any }>();
  let loseLedgerSync = true;
  const env: any = {};
  function namespace(kind: string, Cell: any) {
    return { idFromName: (name: string) => ({ name, toString: () => name }), get(id: any) {
      const key = `${kind}-${id.name}`;
      if (!opened.has(key)) {
        const db = new DatabaseSync(path.join(folder, `${encodeURIComponent(key)}.sqlite`));
        db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
        const state = new MockDurableObjectState(id.name);
        state.storage.sql = sqliteStorage(db);
        state.storage.sync = async () => {
          if (kind === "ledger" && loseLedgerSync) {
            loseLedgerSync = false; throw new Error("confirmation acknowledgment lost");
          }
        };
        opened.set(key, { db, cell: new Cell(state, env) });
      }
      return { fetch: (input: RequestInfo, init?: RequestInit) => opened.get(key)!.cell.fetch(new Request(input, init)) };
    } };
  }
  env.TENANT_DIRECTORY_CELL = namespace("directory", TenantDirectoryCell);
  env.TENANT_LEDGER_CELL = namespace("ledger", TenantLedgerCell);
  const spec = yaml.load(readFileSync(path.join(__dirname, "../../specs/taskspec_v1beta1_example.yaml"), "utf8")) as any;
  const submit = () => worker.fetch(new Request("https://ansts/v1beta1/tasks", { method: "POST", body: JSON.stringify(spec) }), env);
  try {
    const lost = await submit();
    assert.equal(lost.status, 503);
    assert.equal(((await lost.json()) as any).outcome, "UNKNOWN");
    for (const { db } of opened.values()) db.close();
    opened.clear();
    const byKey = await worker.fetch(new Request(`https://ansts/v1beta1/admissions/by-key/${encodeURIComponent(spec.metadata.idempotency_key)}?tenant_id=${encodeURIComponent(spec.metadata.tenant_id)}`), env);
    assert.equal(byKey.status, 200);
    const found = await byKey.json() as any;
    assert.equal(found.outcome, "ACCEPTED");
    const task = await worker.fetch(new Request(`https://ansts/v1beta1/tasks/${encodeURIComponent(found.task_id)}?tenant_id=${encodeURIComponent(spec.metadata.tenant_id)}`), env);
    assert.deepEqual(((await task.json()) as any).task_spec, new TaskSpecValidator().validateAndNormalize(spec));
    const retry = await submit();
    assert.equal(retry.status, 200);
    assert.equal(((await retry.json()) as any).task_id, found.task_id);
    assert.equal(opened.get(`ledger-${spec.metadata.tenant_id}`)!.db.prepare("SELECT COUNT(*) AS count FROM tenant_admissions").get().count, 1);
  } finally {
    for (const { db } of opened.values()) db.close();
    rmSync(folder, { recursive: true, force: true });
  }
});

test("SQLite directory deduplicates registration and resumes its scan cursor after file reopen", { skip: sqliteUnavailable }, async () => {
  const folder = mkdtempSync(path.join(tmpdir(), "ansts-directory-sqlite-"));
  const file = path.join(folder, "directory.sqlite");
  const tenants: string[] = [];
  for (let index = 0; tenants.length < 45; index += 1) {
    const tenant = `tenant-${String(index).padStart(5, "0")}`;
    if (tenantDirectoryShardIndex(tenant) === 0) tenants.push(tenant);
  }
  let db: any;
  function activate() {
    db = new DatabaseSync(file);
    db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;");
    const state = new MockDurableObjectState("0");
    state.storage.sql = sqliteStorage(db);
    return new TenantDirectoryCell(state, {} as any);
  }
  try {
    let cell = activate();
    for (const tenant_id of [...tenants, ...tenants]) {
      const result = await cell.fetch(new Request("http://directory/tenants/register", {
        method: "POST", body: JSON.stringify({ tenant_id }),
      }));
      assert.equal(result.status, 200);
    }
    const discovered: string[] = [];
    for (let page = 0; page < 3; page += 1) {
      const result = await cell.fetch(new Request("http://directory/tenants/scan?limit=20"));
      discovered.push(...((await result.json()) as any).tenants);
      db.close(); cell = activate();
    }
    assert.deepEqual(discovered, tenants);
    const wrapped = await cell.fetch(new Request("http://directory/tenants/scan?limit=20"));
    const next = await wrapped.json() as any;
    assert.equal(next.wrapped, true);
    assert.deepEqual(next.tenants, tenants.slice(0, 20));
  } finally { db?.close(); rmSync(folder, { recursive: true, force: true }); }
});

test("SQLite event projection rejects concurrent conflicting reuse of an event identity", { skip: sqliteUnavailable }, async () => {
  const db = projectionDatabase();
  try {
    const binding = d1Binding(db);
    const results = await Promise.allSettled([
      projectTaskEvents(binding, [event]),
      projectTaskEvents(binding, [{ ...event, payload_digest: "b".repeat(64), payload: { answer: "changed" } }]),
    ]);
    assert.equal(results.filter((result) => result.status === "fulfilled").length, 1);
    assert.equal(results.filter((result) => result.status === "rejected").length, 1);
  } finally { db.close(); }
});

test("SQLite projection replay repairs an offset write lost after event insertion and rejects stale task state", { skip: sqliteUnavailable }, async () => {
  const db = projectionDatabase();
  let failOffset = true;
  const binding = d1Binding(db, (query) => {
    if (failOffset && query.includes("INSERT INTO task_projection_offsets")) {
      failOffset = false; throw new Error("offset write interrupted");
    }
  });
  try {
    await assert.rejects(projectTaskEvents(binding, [event]), /interrupted/);
    await projectTaskEvents(binding, [event]);
    assert.equal(db.prepare("SELECT latest_aggregate_version FROM task_projection_offsets").get()?.latest_aggregate_version, 2);
    const task = { tenant_id: "tenant", task_id: "task", admission_status: "ACCEPTED", runnable_status: "READY",
      accepted_at: 1, lifecycle_state: "SUCCEEDED", business_epoch: 1, last_turn_seq: 2, execution_version: 3 };
    await projectTenantTasks(binding, [task, { ...task, lifecycle_state: "RUNNING", execution_version: 2 }]);
    assert.equal((await readTenantTaskProjection(binding, "tenant"))[0].lifecycle_state, "SUCCEEDED");
  } finally { db.close(); }
});
