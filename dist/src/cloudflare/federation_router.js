"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.MultiRegionFederationRouter = void 0; /**
 * @fileoverview M12 TASK-45: Multi-Region Geo-Latency Aware Cell Federation Router.
 */








class MultiRegionFederationRouter {
  regions = new Map();

  registerRegion(ep) {
    this.regions.set(ep.regionCode, ep);
  }

  selectOptimalRegion(params)


  {
    const candidates = Array.from(this.regions.values()).filter(
    (r) =>
    r.healthy && (
    !params.requiredSovereigntyZone || r.sovereigntyZone === params.requiredSovereigntyZone));

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
}exports.MultiRegionFederationRouter = MultiRegionFederationRouter;