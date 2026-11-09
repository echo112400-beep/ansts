"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.GeoSovereigntyAndLatencyRouter = void 0; /**
 * @fileoverview v2.3 & M37 Geo-Sovereignty (GDPR/CCPA/PIPL) & Latency-Weighted Anycast Router.
 */







class GeoSovereigntyAndLatencyRouter {
  regions = [];

  registerGeoRegion(node) {
    const existingIdx = this.regions.findIndex((r) => r.regionId === node.regionId);
    if (existingIdx >= 0) {
      this.regions[existingIdx] = { ...node };
    } else {
      this.regions.push({ ...node });
    }
  }

  setRegionAvailability(regionId, capacityAvailable, latencyMs) {
    const found = this.regions.find((r) => r.regionId === regionId);
    if (!found) return false;
    found.capacityAvailable = capacityAvailable;
    if (latencyMs !== undefined) found.latencyMs = latencyMs;
    return true;
  }

  listRegions() {
    return this.regions.map((r) => ({ ...r }));
  }

  routeByJurisdiction(requiredJurisdiction) {
    const eligible = this.regions.filter(
    (r) => r.jurisdiction === requiredJurisdiction && r.capacityAvailable);

    if (eligible.length === 0) {
      throw new Error(`Data sovereignty lock: no healthy region in jurisdiction '${requiredJurisdiction}'`);
    }
    eligible.sort((a, b) => a.latencyMs - b.latencyMs);
    return eligible[0];
  }
}exports.GeoSovereigntyAndLatencyRouter = GeoSovereigntyAndLatencyRouter;