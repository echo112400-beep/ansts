"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.MultiRegionFederationRouter = void 0;
class MultiRegionFederationRouter {
    regions = new Map();
    registerRegion(ep) {
        const safeRtt = Number.isFinite(ep.rttEwmaMs) && ep.rttEwmaMs >= 0 ? ep.rttEwmaMs : 9999;
        this.regions.set(ep.regionCode, { ...ep, rttEwmaMs: safeRtt });
    }
    selectOptimalRegion(params) {
        const candidates = Array.from(this.regions.values()).filter((r) => r.healthy &&
            Number.isFinite(r.rttEwmaMs) &&
            r.rttEwmaMs >= 0 &&
            (!params.requiredSovereigntyZone || r.sovereigntyZone === params.requiredSovereigntyZone));
        if (candidates.length === 0) {
            throw new Error("No healthy federated region matches sovereignty constraints");
        }
        if (params.preferredRegionCode) {
            const pref = candidates.find((c) => c.regionCode === params.preferredRegionCode);
            if (pref && pref.rttEwmaMs <= 150)
                return { ...pref };
        }
        candidates.sort((a, b) => a.rttEwmaMs - b.rttEwmaMs);
        return { ...candidates[0] };
    }
}
exports.MultiRegionFederationRouter = MultiRegionFederationRouter;
