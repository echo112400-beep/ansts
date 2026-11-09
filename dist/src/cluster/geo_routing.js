"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.GeoSovereigntyAndLatencyRouter = void 0; /**
 * @fileoverview v2.3 Geo-Sovereignty (GDPR/CCPA/PIPL) & Latency-Weighted Anycast Router.
 */







class GeoSovereigntyAndLatencyRouter {
  regions = [];

  registerGeoRegion(node) {
    this.regions.push(node);
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