/**
 * @fileoverview v2.3, M37 & M63-3: Geo-Sovereignty (GDPR/CCPA/PIPL) & Latency-Weighted Anycast Router
 * with Finite Non-Negative Latency Sanitization, Deterministic Region Tie-Breaking & SQLite Persistence.
 */
import { SqlStorageLike } from "../cloudflare/env.js";

export interface GeoRegionNode {
  regionId: string;
  jurisdiction: "GDPR_EU" | "US_FED" | "APAC_SG";
  latencyMs: number;
  capacityAvailable: boolean;
}

export class GeoSovereigntyAndLatencyRouter {
  private readonly regions: GeoRegionNode[] = [];
  private sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M63-3] DFX-Compliance & Operability: Binds SQLite storage so sovereign region topology
   * and capacity availability flags survive Gateway restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): number {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS geo_sovereignty_regions (region_id TEXT PRIMARY KEY, jurisdiction TEXT, latency_ms REAL, capacity_available INTEGER)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM geo_sovereignty_regions")
      .toArray();
    let restored = 0;
    for (const r of rows) {
      const node: GeoRegionNode = {
        regionId: String(r.region_id),
        jurisdiction: String(r.jurisdiction) as GeoRegionNode["jurisdiction"],
        latencyMs: Number(r.latency_ms ?? 50),
        capacityAvailable: Boolean(Number(r.capacity_available ?? 1)),
      };
      const existingIdx = this.regions.findIndex((x) => x.regionId === node.regionId);
      if (existingIdx >= 0) {
        this.regions[existingIdx] = node;
      } else {
        this.regions.push(node);
      }
      restored += 1;
    }
    return restored;
  }

  private persistRegion(node: GeoRegionNode): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO geo_sovereignty_regions (region_id, jurisdiction, latency_ms, capacity_available) VALUES (?, ?, ?, ?)",
      node.regionId,
      node.jurisdiction,
      node.latencyMs,
      node.capacityAvailable ? 1 : 0
    );
  }

  private static sanitizeLatencyMs(latencyMs: number): number {
    if (!Number.isFinite(latencyMs) || latencyMs < 0) {
      return 999_999;
    }
    return latencyMs;
  }

  public registerGeoRegion(node: GeoRegionNode): void {
    const sanitized: GeoRegionNode = {
      ...node,
      latencyMs: GeoSovereigntyAndLatencyRouter.sanitizeLatencyMs(node.latencyMs),
    };
    const existingIdx = this.regions.findIndex((r) => r.regionId === sanitized.regionId);
    if (existingIdx >= 0) {
      this.regions[existingIdx] = sanitized;
    } else {
      this.regions.push(sanitized);
    }
    this.persistRegion(sanitized);
  }

  public setRegionAvailability(regionId: string, capacityAvailable: boolean, latencyMs?: number): boolean {
    const found = this.regions.find((r) => r.regionId === regionId);
    if (!found) return false;
    found.capacityAvailable = capacityAvailable;
    if (latencyMs !== undefined) {
      found.latencyMs = GeoSovereigntyAndLatencyRouter.sanitizeLatencyMs(latencyMs);
    }
    this.persistRegion(found);
    return true;
  }

  public listRegions(): GeoRegionNode[] {
    return this.regions.map((r) => ({ ...r }));
  }

  public routeByJurisdiction(requiredJurisdiction: "GDPR_EU" | "US_FED" | "APAC_SG"): GeoRegionNode {
    const eligible = this.regions.filter(
      (r) => r.jurisdiction === requiredJurisdiction && r.capacityAvailable
    );
    if (eligible.length === 0) {
      throw new Error(`Data sovereignty lock: no healthy region in jurisdiction '${requiredJurisdiction}'`);
    }
    eligible.sort((a, b) => {
      if (a.latencyMs !== b.latencyMs) {
        return a.latencyMs - b.latencyMs;
      }
      return a.regionId.localeCompare(b.regionId);
    });
    return { ...eligible[0] };
  }
}
