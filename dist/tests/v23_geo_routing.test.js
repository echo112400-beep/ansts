"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview feat/celld-v25 Geo-Sovereignty (GDPR/US_FED/APAC) Routing Suite.
 */(0, _nodeTest.describe)("feat/celld-v25 Geo-Sovereignty Routing Suite", () => {
  (0, _nodeTest.it)("Enforces strict GDPR data residency lock and selects lowest-latency region within jurisdiction", () => {
    const router = new _index.GeoSovereigntyAndLatencyRouter();
    router.registerGeoRegion({ regionId: "eu-frankfurt", jurisdiction: "GDPR_EU", latencyMs: 18, capacityAvailable: true });
    router.registerGeoRegion({ regionId: "eu-dublin", jurisdiction: "GDPR_EU", latencyMs: 34, capacityAvailable: true });
    router.registerGeoRegion({ regionId: "us-virginia", jurisdiction: "US_FED", latencyMs: 8, capacityAvailable: true });

    const chosen = router.routeByJurisdiction("GDPR_EU");
    assert.equal(chosen.regionId, "eu-frankfurt");
    assert.throws(() => router.routeByJurisdiction("APAC_SG"), /Data sovereignty lock/);
  });
});