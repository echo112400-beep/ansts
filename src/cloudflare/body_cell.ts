/** @fileoverview Durable Body registration, heartbeat and fenced execution receipts. */
import { DurableObjectStateLike } from "./env.js";
import { ExecutionAttempt, executionDigest, migrateAttempt, newAttempt, sameExecution, sameOwner } from "./execution_attempt.js";

export class AnstsBodyCell {
  constructor(private readonly ctx: DurableObjectStateLike) {
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS body_state (id TEXT PRIMARY KEY, payload TEXT)");
    this.finishMigration();
  }
  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    const route = path.slice("/v1/body/".length);
    const allowed = ["acquire", "start", "finish", "acknowledge", "uncertain", "reconcile", "heartbeat", "receipt", "state", "consume-grant", "export", "import"];
    if (!path.startsWith("/v1/body/") || !allowed.includes(route)) return Response.json({ error: "NOT_FOUND" }, { status: 404 });
    if ((["state", "export"].includes(route) && request.method !== "GET") || (!["state", "export"].includes(route) && request.method !== "POST")) {
      return Response.json({ error: "METHOD_NOT_ALLOWED" }, { status: 405 });
    }
    if (request.method === "POST") {
      let body: Record<string, unknown>;
      try {
        const raw = await request.text();
        if (raw.length > (route === "import" ? 1048576 : 65536)) return Response.json({ error: "PAYLOAD_TOO_LARGE" }, { status: 413 });
        body = JSON.parse(raw);
        if (!body || typeof body !== "object" || Array.isArray(body)) throw new Error("object required");
      } catch { return Response.json({ error: "INVALID_JSON" }, { status: 400 }); }
      if (route === "import") {
        if (!request.headers.get("x-ansts-verified-operator")) return Response.json({ error: "OPERATOR_AUTH_REQUIRED" }, { status: 401 });
        if (body.schema_version !== 1 || !Array.isArray(body.rows) || body.rows.length > 10000) return Response.json({ error: "INVALID_MIGRATION" }, { status: 400 });
        for (const row of body.rows) {
          if (!row || typeof row.id !== "string" || row.id.length > 1024 || !/^(execution$|heartbeat$|receipt:|grant:)/.test(row.id) || typeof row.payload !== "string") return Response.json({ error: "INVALID_MIGRATION" }, { status: 400 });
          try { JSON.parse(row.payload); } catch { return Response.json({ error: "INVALID_MIGRATION" }, { status: 400 }); }
        }
        if (this.ctx.storage.sql.exec("SELECT * FROM body_state").toArray().length) return Response.json({ imported: false, already_initialized: true });
        this.ctx.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", "__migration_pending", JSON.stringify(body.rows));
        this.finishMigration();
        return Response.json({ imported: true, rows: body.rows.length });
      }
      if (route === "consume-grant") {
        const execution = this.loadAttempt();
        if (request.headers.get("x-ansts-verified-grant") !== "true" || !execution || execution.phase !== "RUNNING" || !sameOwner(execution, body) || typeof body.nonce !== "string") return Response.json({ error: "EXECUTION_GRANT_REQUIRED" }, { status: 403 });
        const id = `grant:${body.nonce}`;
        if (this.ctx.storage.sql.exec("SELECT * FROM body_state WHERE id = ?", id).toArray().length) return Response.json({ error: "EXECUTION_GRANT_REPLAY" }, { status: 409 });
        this.ctx.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", id, JSON.stringify({ consumed_at: Date.now() }));
        return Response.json({ authorized: true });
      }
      if (route === "reconcile") return this.reconcile(body, request.headers.get("x-ansts-verified-operator"));
      if (["acquire", "start", "finish", "acknowledge", "uncertain"].includes(route)) return this.transition(route, body);
      const id = path.endsWith("/heartbeat") ? "heartbeat" : `receipt:${String(body.task_id)}:${Number(body.fencing_token)}`;
      if (id !== "heartbeat" && (!body.task_id || !Number.isInteger(body.fencing_token))) {
        return Response.json({ error: "INVALID_RECEIPT" }, { status: 400 });
      }
      this.ctx.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", id, JSON.stringify({ ...body, updated_at: Date.now() }));
      return Response.json({ recorded: true });
    }
    const rows = this.ctx.storage.sql.exec<{ id: string; payload: string }>("SELECT * FROM body_state").toArray();
    if (route === "export") {
      if (!request.headers.get("x-ansts-verified-operator")) return Response.json({ error: "OPERATOR_AUTH_REQUIRED" }, { status: 401 });
      return Response.json({ schema_version: 1, rows });
    }
    const heartbeat = rows.find(row => row.id === "heartbeat");
    const hb = heartbeat ? JSON.parse(heartbeat.payload) : null;
    const execution = this.loadAttempt();
    return Response.json({ controlPlane: "celld-body", heartbeat: hb, online: !!hb && Date.now() - hb.updated_at < 15000, execution, receipts: rows.filter(row => row.id.startsWith("receipt:")).map(row => JSON.parse(row.payload)) });
  }
  private finishMigration(): void {
    const pending = this.ctx.storage.sql.exec<{ payload: string }>("SELECT * FROM body_state WHERE id = ?", "__migration_pending").toArray()[0];
    if (!pending) return;
    for (const row of JSON.parse(pending.payload) as Array<{ id: string; payload: string }>) this.ctx.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", row.id, row.payload);
    this.ctx.storage.sql.exec("DELETE FROM body_state WHERE id = ?", "__migration_pending");
  }
  private saveAttempt(attempt: ExecutionAttempt): void {
    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", "execution", JSON.stringify(attempt));
  }
  private reconcile(body: Record<string, unknown>, actor: string | null): Response {
    if (!actor) return Response.json({ error: "OPERATOR_AUTH_REQUIRED" }, { status: 401 });
    const saved = this.loadAttempt();
    if (saved?.reconciliation_digest === executionDigest(body)) return Response.json({ reconciled: true, execution: saved });
    if (!saved || saved.task_id !== body.task_id || saved.tenant_id !== body.tenant_id || saved.version !== body.expected_version) {
      return Response.json({ error: "RECONCILIATION_VERSION_CONFLICT" }, { status: 409 });
    }
    if (typeof body.evidence !== "string" || !body.evidence.trim() || body.evidence.length > 4096 || typeof body.reason !== "string" || !body.reason.trim() || body.reason.length > 512) {
      return Response.json({ error: "RECONCILIATION_EVIDENCE_REQUIRED" }, { status: 400 });
    }
    if (saved.phase !== "UNCERTAIN") return Response.json({ error: "EXECUTION_NOT_UNCERTAIN" }, { status: 409 });
    if (body.action !== "retain_block" && (body.fencing_token !== saved.fencing_token || body.node_id !== saved.node_id)) return Response.json({ error: "RECONCILIATION_OWNER_CONFLICT" }, { status: 409 });
    let next = { ...saved, version: saved.version + 1, updated_at: Date.now() };
    if (body.action === "confirm_completed") {
      if (!body.result || typeof body.result !== "object" || Array.isArray(body.result)) return Response.json({ error: "RESULT_REQUIRED" }, { status: 400 });
      const digest = executionDigest(body.result);
      if (saved.result_digest && saved.result_digest !== digest) return Response.json({ error: "RESULT_CONFLICT" }, { status: 409 });
      next = { ...next, phase: "RESULT_PENDING", result: body.result as Record<string, unknown>, result_digest: digest };
    } else if (body.action === "confirm_not_started") {
      const replacement = body.replacement && typeof body.replacement === "object" ? newAttempt(body.replacement as Record<string, unknown>) : null;
      if (!replacement || !sameExecution(saved, replacement as unknown as Record<string, unknown>) || replacement.attempt_id === saved.attempt_id || replacement.fencing_token <= saved.fencing_token) {
        return Response.json({ error: "INVALID_REPLACEMENT_ATTEMPT" }, { status: 400 });
      }
      next = { ...replacement, version: saved.version + 1, reason: "RECONCILED_NOT_STARTED" };
    } else if (body.action !== "retain_block") return Response.json({ error: "INVALID_RECONCILIATION_ACTION" }, { status: 400 });
    const audit = saved.audit ?? [];
    const entry = { version: next.version, actor, action: String(body.action), evidence: body.evidence, reason: body.reason, timestamp: Date.now(), previous_hash: audit.at(-1)?.hash ?? "" };
    next.audit = [...audit, { ...entry, hash: executionDigest(entry) }];
    next.reconciliation_digest = executionDigest(body);
    this.saveAttempt(next);
    return Response.json({ reconciled: true, execution: next });
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
      if (saved.phase === "ADMITTED" && saved.reason === "RECONCILED_NOT_STARTED" && proposed.fencing_token >= saved.fencing_token) {
        const bound = { ...proposed, version: saved.version + 1, audit: saved.audit };
        this.saveAttempt(bound);
        return Response.json({ acquired: true, execution: bound });
      }
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
