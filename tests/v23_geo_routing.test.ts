/**
 * @fileoverview feat/celld-v25 Geo-Sovereignty (GDPR/US_FED/APAC) Routing Suite.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import { GeoSovereigntyAndLatencyRouter } from "../src/index.js";

describe("feat/celld-v25 Geo-Sovereignty Routing Suite", () => {
  it("Enforces strict GDPR data residency lock and selects lowest-latency region within jurisdiction", () => {
    const router = new GeoSovereigntyAndLatencyRouter();
    router.registerGeoRegion({ regionId: "eu-frankfurt", jurisdiction: "GDPR_EU", latencyMs: 18, capacityAvailable: true });
    router.registerGeoRegion({ regionId: "eu-dublin", jurisdiction: "GDPR_EU", latencyMs: 34, capacityAvailable: true });
    router.registerGeoRegion({ regionId: "us-virginia", jurisdiction: "US_FED", latencyMs: 8, capacityAvailable: true });

    const chosen = router.routeByJurisdiction("GDPR_EU");
    assert.equal(chosen.regionId, "eu-frankfurt");
    assert.throws(() => router.routeByJurisdiction("APAC_SG"), /Data sovereignty lock/);
  });
});
