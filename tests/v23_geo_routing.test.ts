/**
 * @fileoverview ANSTS v2.3 Integration Test Suite for Multi-Region Geo-Routing.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { MultiRegionGeoRouter } from "../src/index";

test("test 60: [ANSTS v2.3] MultiRegionGeoRouter automatic geo-failover under high latency", () => {
  const router = new MultiRegionGeoRouter("us-west-2");
  router.registerRegion({ regionId: "us-west-2", gatewayEndpoint: "us.gateway.ansts", pingEwmaMs: 45, healthy: true, activeSessions: 100 });
  router.registerRegion({ regionId: "eu-central-1", gatewayEndpoint: "eu.gateway.ansts", pingEwmaMs: 180, healthy: true, activeSessions: 50 });

  const r1 = router.routeRequest("us-west-2");
  assert.equal(r1.selectedRegion, "us-west-2");
  assert.equal(r1.isFailover, false);

  // Simulate US West degradation
  router.updateRegionMetrics("us-west-2", 600, true);
  const r2 = router.routeRequest("us-west-2");
  assert.equal(r2.selectedRegion, "eu-central-1");
  assert.equal(r2.isFailover, true);
});
