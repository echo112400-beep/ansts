"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview feat/celld-v25 Geo-Sovereignty (GDPR/US_FED/APAC) Routing Suite.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("feat/celld-v25 Geo-Sovereignty Routing Suite", () => {
    (0, node_test_1.it)("Enforces strict GDPR data residency lock and selects lowest-latency region within jurisdiction", () => {
        const router = new index_js_1.GeoSovereigntyAndLatencyRouter();
        router.registerGeoRegion({ regionId: "eu-frankfurt", jurisdiction: "GDPR_EU", latencyMs: 18, capacityAvailable: true });
        router.registerGeoRegion({ regionId: "eu-dublin", jurisdiction: "GDPR_EU", latencyMs: 34, capacityAvailable: true });
        router.registerGeoRegion({ regionId: "us-virginia", jurisdiction: "US_FED", latencyMs: 8, capacityAvailable: true });
        const chosen = router.routeByJurisdiction("GDPR_EU");
        assert.equal(chosen.regionId, "eu-frankfurt");
        assert.throws(() => router.routeByJurisdiction("APAC_SG"), /Data sovereignty lock/);
    });
});
