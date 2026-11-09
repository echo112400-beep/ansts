/**
 * @fileoverview v2.3 & M37 Geo-Sovereignty (GDPR/CCPA/PIPL) & Latency-Weighted Anycast Router.
 */
export interface GeoRegionNode {
  regionId: string;
  jurisdiction: "GDPR_EU" | "US_FED" | "APAC_SG";
  latencyMs: number;
  capacityAvailable: boolean;
}

export class GeoSovereigntyAndLatencyRouter {
  private readonly regions: GeoRegionNode[] = [];

  public registerGeoRegion(node: GeoRegionNode): void {
    const existingIdx = this.regions.findIndex((r) => r.regionId === node.regionId);
    if (existingIdx >= 0) {
      this.regions[existingIdx] = { ...node };
    } else {
      this.regions.push({ ...node });
    }
  }

  public setRegionAvailability(regionId: string, capacityAvailable: boolean, latencyMs?: number): boolean {
    const found = this.regions.find((r) => r.regionId === regionId);
    if (!found) return false;
    found.capacityAvailable = capacityAvailable;
    if (latencyMs !== undefined) found.latencyMs = latencyMs;
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
    eligible.sort((a, b) => a.latencyMs - b.latencyMs);
    return eligible[0];
  }
}
