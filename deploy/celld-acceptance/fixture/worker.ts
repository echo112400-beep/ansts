interface DurableObjectId {
  toString(): string;
}

interface DurableObjectNamespace {
  idFromName(name: string): DurableObjectId;
  get(id: DurableObjectId): DurableObjectStub;
}

interface DurableObjectStub {
  fetch(request: Request): Promise<Response>;
}

interface SqlCursor<Row> {
  toArray(): Row[];
}

interface DurableObjectSqlStorage {
  exec<Row = Record<string, unknown>>(query: string, ...bindings: unknown[]): SqlCursor<Row>;
}

interface DurableObjectState {
  storage: { sql: DurableObjectSqlStorage; sync(): Promise<void> };
}

interface Env {
  PROBE: DurableObjectNamespace;
}

interface ProbeRow {
  probe_key: string;
  probe_value: string;
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/health") return new Response("ok");
    if (url.pathname !== "/probe") return new Response("not found", { status: 404 });

    let write: { key?: unknown; value?: unknown } | undefined;
    if (request.method === "POST") {
      try {
        write = await request.json() as { key?: unknown; value?: unknown };
      } catch {
        return json({ error: "body must be valid JSON" }, 400);
      }
    }
    const key = request.method === "POST"
      ? String(write?.key ?? "")
      : String(url.searchParams.get("key") ?? "");
    if (!/^[a-zA-Z0-9._-]{1,120}$/.test(key)) {
      return json({ error: "key must contain 1-120 letters, digits, dot, underscore, or hyphen" }, 400);
    }

    const id = env.PROBE.idFromName(key);
    const stub = env.PROBE.get(id);
    if (request.method === "POST") {
      const value = String(write?.value ?? "");
      if (value.length < 1 || value.length > 240) {
        return json({ error: "value must contain 1-240 characters" }, 400);
      }
      return stub.fetch(new Request("https://probe/write", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ key, value }),
      }));
    }
    if (request.method === "GET") {
      return stub.fetch(new Request(`https://probe/read?key=${encodeURIComponent(key)}`));
    }
    return new Response("method not allowed", { status: 405, headers: { allow: "GET, POST" } });
  },
};

export class DurabilityProbeCell {
  constructor(
    private readonly state: DurableObjectState,
    _env: Env,
  ) {
    this.state.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS acceptance_probe (probe_key TEXT PRIMARY KEY, probe_value TEXT NOT NULL)",
    );
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/write") {
      const { key, value } = await request.json() as { key: string; value: string };
      this.state.storage.sql.exec(
        "INSERT INTO acceptance_probe (probe_key, probe_value) VALUES (?, ?) " +
          "ON CONFLICT(probe_key) DO UPDATE SET probe_value = excluded.probe_value",
        key,
        value,
      ).toArray();
      await this.state.storage.sync();
      return json({ ok: true, key, value });
    }

    if (request.method === "GET" && url.pathname === "/read") {
      const key = url.searchParams.get("key") ?? "";
      const row = this.state.storage.sql.exec<ProbeRow>(
        "SELECT probe_key, probe_value FROM acceptance_probe WHERE probe_key = ?",
        key,
      ).toArray()[0];
      if (!row) return json({ error: "not found", key }, 404);
      return json({ key: row.probe_key, value: row.probe_value });
    }

    return new Response("not found", { status: 404 });
  }
}
