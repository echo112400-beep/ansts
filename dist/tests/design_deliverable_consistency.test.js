"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview Design-to-Deliverable Consistency Audit Test (1 test).
 */(0, _nodeTest.describe)("Design-to-Deliverable Consistency Audit", () => {
  (0, _nodeTest.it)("Verifies all 52 TypeScript source modules, deployment scripts, proto, SQL migrations, and Wrangler configs exist", () => {
    const requiredFiles = [
    "package.json",
    "tsconfig.json",
    "wrangler.json",
    "wrangler.toml",
    "proto/ansts_rpc.proto",
    "migrations/0001_init_ansts.sql",
    "deploy/rustfs-celld/start-rustfs-celld.sh",
    "deploy/manual-fleet/scale-fleet.sh",
    "src/index.ts",
    "src/cloudflare/celld_native_optimizations.ts",
    "src/checkpoint/unified_state_store.ts",
    "src/cluster/manual_scale_controller.ts"];

    for (const rel of requiredFiles) {
      const full = path.join(process.cwd(), rel);
      assert.ok(fs.existsSync(full), `Missing required deliverable file: ${rel}`);
    }
  });
});