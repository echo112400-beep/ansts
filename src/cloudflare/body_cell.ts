/** @fileoverview Durable Body registration, heartbeat and fenced execution receipts. */
import { DurableObjectStateLike } from "./env.js";

export class AnstsBodyCell {
  constructor(private readonly ctx: DurableObjectStateLike) {
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS body_state (id TEXT PRIMARY KEY, payload TEXT)");
  }
  async fetch(request: Request): Promise<Response> {
    const path = new URL(request.url).pathname;
    if (request.method === "POST") {
      const body = await request.json() as Record<string, unknown>;
      if (path.endsWith("/acquire")) {
        if (!body.task_id || !body.node_id || !Number.isInteger(body.fencing_token) || Number(body.fencing_token) < 1) {
          return Response.json({ error: "INVALID_EXECUTION_LEASE" }, { status: 400 });
        }
        const previous = this.ctx.storage.sql.exec<{ payload: string }>("SELECT * FROM body_state WHERE id = ?", "execution").toArray()[0];
        if (previous) {
          const saved = JSON.parse(previous.payload);
          if (!saved.result && saved.node_id === body.node_id && saved.fencing_token === body.fencing_token) {
            return Response.json({ acquired: true });
          }
          return Response.json({ acquired: false, ...saved }, { status: saved.result ? 200 : 409 });
        }
        this.ctx.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", "execution", JSON.stringify(body));
        return Response.json({ acquired: true });
      }
      if (path.endsWith("/finish")) {
        const previous = this.ctx.storage.sql.exec<{ payload: string }>("SELECT * FROM body_state WHERE id = ?", "execution").toArray()[0];
        const saved = previous ? JSON.parse(previous.payload) : null;
        if (!saved || saved.node_id !== body.node_id || saved.fencing_token !== body.fencing_token || !body.result) {
          return Response.json({ error: "STALE_EXECUTION_OWNER" }, { status: 409 });
        }
        this.ctx.storage.sql.exec("INSERT OR REPLACE INTO body_state (id, payload) VALUES (?, ?)", "execution", JSON.stringify({ ...saved, result: body.result }));
        return Response.json({ recorded: true });
      }
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
    return Response.json({ controlPlane: "celld-body", heartbeat: hb, online: !!hb && Date.now() - hb.updated_at < 15000, receipts: rows.filter(row => row.id !== "heartbeat").map(row => JSON.parse(row.payload)) });
  }
}
