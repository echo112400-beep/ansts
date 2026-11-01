/** Durable, sharded tenant discovery index used to rebuild disposable query projections. */

import { CloudflareCelldEnv, DurableObjectState } from "./env";
import { PersistenceSqlStorage } from "../persistence/contracts";
import { confirmCellWrite, withCellTransaction } from "../persistence/transactions";
import { initializeCellProtocol } from "../persistence/schema";

export const TENANT_DIRECTORY_SHARD_COUNT = 16;

export function tenantDirectoryShardIndex(tenantId: string): number {
  if (typeof tenantId !== "string" || tenantId.trim().length === 0 || tenantId.length > 512) {
    throw new TypeError("tenant_id must be non-empty and at most 512 characters.");
  }
  let hash = 0x811c9dc5;
  for (let index = 0; index < tenantId.length; index += 1) {
    hash = Math.imul(hash ^ tenantId.charCodeAt(index), 0x01000193) >>> 0;
  }
  return hash % TENANT_DIRECTORY_SHARD_COUNT;
}

export function tenantDirectoryShardKey(index: number): string {
  if (!Number.isSafeInteger(index) || index < 0 || index >= TENANT_DIRECTORY_SHARD_COUNT) {
    throw new RangeError(`Tenant directory shard must be in [0, ${TENANT_DIRECTORY_SHARD_COUNT - 1}].`);
  }
  return String(index);
}

const tenantDirectoryMigration = {
  version: 2,
  description: "durable tenant discovery for rebuildable projections",
  up(sql: PersistenceSqlStorage): void {
    sql.exec(`CREATE TABLE IF NOT EXISTS tenant_directory (
      tenant_id TEXT PRIMARY KEY,
      first_seen_at INTEGER NOT NULL,
      last_seen_at INTEGER NOT NULL
    )`);
    sql.exec(`CREATE TABLE IF NOT EXISTS tenant_directory_scan (
      singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
      scan_after_tenant_id TEXT NOT NULL
    )`);
    sql.exec("INSERT OR IGNORE INTO tenant_directory_scan (singleton, scan_after_tenant_id) VALUES (?, ?)", 1, "");
  },
};

interface DirectoryRow {
  tenant_id: string;
}

export class TenantDirectoryCell {
  private readonly shardKey: string;
  private readonly shardIndex: number;

  constructor(public readonly ctx: DurableObjectState, public readonly env: CloudflareCelldEnv) {
    this.shardKey = ctx.id.name ?? ctx.id.toString();
    const index = Number(this.shardKey);
    if (!Number.isSafeInteger(index) || tenantDirectoryShardKey(index) !== this.shardKey) {
      throw new TypeError("TenantDirectoryCell requires a canonical numeric shard key.");
    }
    this.shardIndex = index;
    initializeCellProtocol(ctx.storage, { type: "tenant-directory", key: this.shardKey }, [tenantDirectoryMigration]);
  }

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/tenants/register") return this.register(request);
    if (request.method === "GET" && url.pathname === "/tenants/scan") return this.scan(url.searchParams.get("limit"));
    return json({ error: "NotFound", path: url.pathname }, 404);
  }

  private async register(request: Request): Promise<Response> {
    let body: { tenant_id?: unknown };
    try { body = await request.json() as { tenant_id?: unknown }; }
    catch { return json({ error: "INVALID_JSON" }, 400); }
    const tenantId = body.tenant_id;
    if (typeof tenantId !== "string" || tenantId.trim().length === 0 || tenantId.length > 512) {
      return json({ error: "TENANT_REQUIRED" }, 400);
    }
    if (tenantDirectoryShardIndex(tenantId) !== this.shardIndex) return json({ error: "TENANT_DIRECTORY_SHARD_MISMATCH" }, 409);

    const now = Date.now();
    withCellTransaction(this.ctx.storage, () => {
      this.ctx.storage.sql.exec(`INSERT INTO tenant_directory (tenant_id, first_seen_at, last_seen_at)
        VALUES (?, ?, ?)
        ON CONFLICT(tenant_id) DO UPDATE SET last_seen_at = MAX(tenant_directory.last_seen_at, excluded.last_seen_at)`,
      tenantId, now, now);
    });
    await confirmCellWrite(this.ctx.storage);
    return json({ registered: true, tenant_id: tenantId, shard: this.shardIndex });
  }

  private async scan(rawLimit: string | null): Promise<Response> {
    const limit = Number(rawLimit ?? 20);
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) return json({ error: "TENANT_SCAN_LIMIT_INVALID" }, 400);

    const result = withCellTransaction(this.ctx.storage, () => {
      const state = this.ctx.storage.sql.exec<{ scan_after_tenant_id: string }>(
        "SELECT scan_after_tenant_id FROM tenant_directory_scan WHERE singleton = ?", 1,
      ).toArray()[0];
      const cursor = state?.scan_after_tenant_id ?? "";
      let rows = this.ctx.storage.sql.exec<DirectoryRow>(`SELECT tenant_id FROM tenant_directory
        WHERE tenant_id > ? ORDER BY tenant_id LIMIT ?`, cursor, limit).toArray();
      let wrapped = false;
      if (rows.length === 0 && cursor) {
        rows = this.ctx.storage.sql.exec<DirectoryRow>("SELECT tenant_id FROM tenant_directory ORDER BY tenant_id LIMIT ?", limit).toArray();
        wrapped = true;
      }
      if (rows.length === 0) {
        if (cursor) this.ctx.storage.sql.exec("UPDATE tenant_directory_scan SET scan_after_tenant_id = ? WHERE singleton = ?", "", 1);
        return { tenants: [] as string[], changed: Boolean(cursor), wrapped };
      }
      const nextCursor = rows[rows.length - 1].tenant_id;
      this.ctx.storage.sql.exec("UPDATE tenant_directory_scan SET scan_after_tenant_id = ? WHERE singleton = ?", nextCursor, 1);
      return { tenants: rows.map((row) => row.tenant_id), changed: true, wrapped };
    });
    if (result.changed) await confirmCellWrite(this.ctx.storage);
    return json({ shard: this.shardIndex, tenants: result.tenants, wrapped: result.wrapped });
  }
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}
