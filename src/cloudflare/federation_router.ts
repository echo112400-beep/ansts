/**
 * @fileoverview M12 TASK-45 & M58-6: Multi-Region Geo-Latency Aware Cell Federation Router
 * with Non-Negative Finite RTT Sanitization & Immutable Copy Return.
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
    const safeRtt = Number.isFinite(ep.rttEwmaMs) && ep.rttEwmaMs >= 0 ? ep.rttEwmaMs : 9999;
    this.regions.set(ep.regionCode, { ...ep, rttEwmaMs: safeRtt });
  }

  public listRegions(): FederatedRegionEndpoint[] {
    return Array.from(this.regions.values()).map((r) => ({ ...r }));
  }

  public selectOptimalRegion(params: {
    requiredSovereigntyZone?: "US" | "EU" | "APAC";
    preferredRegionCode?: string;
  }): FederatedRegionEndpoint {
    const candidates = Array.from(this.regions.values()).filter(
      (r) =>
        r.healthy &&
        Number.isFinite(r.rttEwmaMs) &&
        r.rttEwmaMs >= 0 &&
        (!params.requiredSovereigntyZone || r.sovereigntyZone === params.requiredSovereigntyZone)
    );
    if (candidates.length === 0) {
      throw new Error("No healthy federated region matches sovereignty constraints");
    }
    if (params.preferredRegionCode) {
      const pref = candidates.find((c) => c.regionCode === params.preferredRegionCode);
      if (pref && pref.rttEwmaMs <= 150) return { ...pref };
    }
    candidates.sort((a, b) => a.rttEwmaMs - b.rttEwmaMs);
    return { ...candidates[0] };
  }
}
