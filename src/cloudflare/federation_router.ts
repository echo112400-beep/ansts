/**
 * @fileoverview M12 TASK-45: Multi-Region Geo-Latency Aware Cell Federation Router.
 */
export interface FederatedRegionEndpoint {
  regionCode: string;
  baseUrl: string;
  healthy: boolean;
  rttEwmaMs: number;
  sovereigntyZone: "US" | "EU" | "APAC";
}

export class MultiRegionFederationRouter {
  private readonly regions: Map<string, FederatedRegionEndpoint> = new Map();

  public registerRegion(ep: FederatedRegionEndpoint): void {
    this.regions.set(ep.regionCode, ep);
  }

  public selectOptimalRegion(params: {
    requiredSovereigntyZone?: "US" | "EU" | "APAC";
    preferredRegionCode?: string;
  }): FederatedRegionEndpoint {
    const candidates = Array.from(this.regions.values()).filter(
      (r) =>
        r.healthy &&
        (!params.requiredSovereigntyZone || r.sovereigntyZone === params.requiredSovereigntyZone)
    );
    if (candidates.length === 0) {
      throw new Error("No healthy federated region matches sovereignty constraints");
    }
    if (params.preferredRegionCode) {
      const pref = candidates.find((c) => c.regionCode === params.preferredRegionCode);
      if (pref && pref.rttEwmaMs <= 150) return pref;
    }
    candidates.sort((a, b) => a.rttEwmaMs - b.rttEwmaMs);
    return candidates[0];
  }
}
