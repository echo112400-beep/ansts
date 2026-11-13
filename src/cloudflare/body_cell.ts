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
