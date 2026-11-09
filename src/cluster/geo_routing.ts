/**
 * @fileoverview v2.3 Geo-Sovereignty (GDPR/CCPA/PIPL) & Latency-Weighted Anycast Router.
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
    this.regions.push(node);
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
