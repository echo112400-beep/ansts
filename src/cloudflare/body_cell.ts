/** @fileoverview Durable Body registration, heartbeat and fenced execution receipts. */
import { DurableObjectStateLike } from "./env.js";
import { ExecutionAttempt, executionDigest, migrateAttempt, newAttempt, sameExecution, sameOwner } from "./execution_attempt.js";

export class AnstsBodyCell {
  constructor(private readonly ctx: DurableObjectStateLike) {
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS body_state (id TEXT PRIMARY KEY, payload TEXT)");
  }
  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    const route = path.slice("/v1/body/".length);
    const allowed = ["acquire", "start", "finish", "acknowledge", "uncertain", "heartbeat", "receipt", "state"];
    if (!path.startsWith("/v1/body/") || !allowed.includes(route)) return Response.json({ error: "NOT_FOUND" }, { status: 404 });
    if ((route === "state" && request.method !== "GET") || (route !== "state" && request.method !== "POST")) {
      return Response.json({ error: "METHOD_NOT_ALLOWED" }, { status: 405 });
    }
    if (request.method === "POST") {
      let body: Record<string, unknown>;
      try {
        const raw = await request.text();
        if (raw.length > 65536) return Response.json({ error: "PAYLOAD_TOO_LARGE" }, { status: 413 });
        body = JSON.parse(raw);
        if (!body || typeof body !== "object" || Array.isArray(body)) throw new Error("object required");
      } catch { return Response.json({ error: "INVALID_JSON" }, { status: 400 }); }
      if (["acquire", "start", "finish", "acknowledge", "uncertain"].includes(route)) return this.transition(route, body);
      const id = path.endsWith("/heartbeat") ? "heartbeat" : `receipt:${String(body.task_id)}:${Number(body.fencing_token)}`;
      if (id !== "heartbeat" && (!body.task_id || !Number.isInteger(body.fencing_token))) {
        return Response.json({ error: "INVALID_RECEIPT" }, { status: 400 });
      }
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", id, JSON.stringify({ ...body, updated_at: Date.now() }));
      return Response.json({ recorded: true });
    }
    const rows = this.ctx.storage.sql.exec<{ id: string; payload: string }>("SELECT * FROM body_state").toArray();
    const heartbeat = rows.find(row => row.id === "heartbeat");
    const hb = heartbeat ? JSON.parse(heartbeat.payload) : null;
    const execution = this.loadAttempt();
    return Response.json({ controlPlane: "celld-body", heartbeat: hb, online: !!hb && Date.now() - hb.updated_at < 15000, execution, receipts: rows.filter(row => row.id.startsWith("receipt:")).map(row => JSON.parse(row.payload)) });
  }
  private saveAttempt(attempt: ExecutionAttempt): void {
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", "execution", JSON.stringify(attempt));
  }
  private loadAttempt(): ExecutionAttempt | null {
    const row = this.ctx.storage.sql.exec<{ payload: string }>("SELECT * FROM body_state WHERE id = ?", "execution").toArray()[0];
    if (!row) return null;
    const raw = JSON.parse(row.payload);
    const attempt = migrateAttempt(raw);
    if (raw.schema_version !== 1) this.saveAttempt(attempt);
    return attempt;
  }
  private transition(route: string, body: Record<string, unknown>): Response {
    const proposed = newAttempt(body);
    if (!proposed) return Response.json({ error: "INVALID_EXECUTION_IDENTITY" }, { status: 400 });
    const saved = this.loadAttempt();
    if (route === "acquire" && !saved) {
      this.saveAttempt(proposed);
      return Response.json({ acquired: true, execution: proposed });
    }
    if (!saved || !sameExecution(saved, body)) return Response.json({ error: "EXECUTION_IDENTITY_CONFLICT", execution: saved }, { status: 409 });
    if (route === "acquire") {
      const completed = saved.phase === "RESULT_PENDING" || saved.phase === "COMPLETED";
      const acquired = sameOwner(saved, body) && ["ADMITTED", "RUNNING"].includes(saved.phase);
      return Response.json({ acquired, execution: saved, ...(completed ? { result: saved.result } : {}) }, { status: acquired || completed ? 200 : 409 });
    }
    if (!sameOwner(saved, body)) return Response.json({ error: "STALE_EXECUTION_OWNER" }, { status: 409 });
    if (route === "finish" && saved.result) {
      if (executionDigest(body.result) !== saved.result_digest) return Response.json({ error: "RESULT_CONFLICT", execution: saved }, { status: 409 });
      return Response.json({ recorded: true, execution: saved });
    }
    const target = route === "start" ? "RUNNING" : route === "finish" ? "RESULT_PENDING" : route === "acknowledge" ? "COMPLETED" : "UNCERTAIN";
    if (saved.phase === target && route !== "finish") return Response.json({ recorded: true, execution: saved });
    if (body.expected_version !== saved.version) return Response.json({ error: "VERSION_CONFLICT", execution: saved }, { status: 409 });
    const valid = (route === "start" && saved.phase === "ADMITTED") ||
      (route === "finish" && saved.phase === "RUNNING" && body.result && typeof body.result === "object" && !Array.isArray(body.result)) ||
      (route === "acknowledge" && saved.phase === "RESULT_PENDING") ||
      (route === "uncertain" && ["ADMITTED", "RUNNING"].includes(saved.phase));
    if (!valid) return Response.json({ error: "INVALID_EXECUTION_TRANSITION", execution: saved }, { status: 409 });
    const next: ExecutionAttempt = { ...saved, phase: target, version: saved.version + 1, updated_at: Date.now() };
    if (route === "finish") { next.result = body.result as Record<string, unknown>; next.result_digest = executionDigest(body.result); }
    if (route === "uncertain") next.reason = String(body.reason ?? "EXECUTION_OUTCOME_UNCERTAIN").slice(0, 512);
    this.saveAttempt(next);
    return Response.json({ recorded: true, execution: next });
  }
}
