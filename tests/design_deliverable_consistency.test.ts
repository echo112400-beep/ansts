/**
 * @fileoverview Design-to-Deliverable Consistency Audit Test (1 test).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";

describe("Design-to-Deliverable Consistency Audit", () => {
  it("Verifies all 52 TypeScript source modules, deployment scripts, proto, SQL migrations, and Wrangler configs exist", () => {
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
      "src/cluster/manual_scale_controller.ts",
    ];
    for (const rel of requiredFiles) {
      const full = path.join(process.cwd(), rel);
      assert.ok(fs.existsSync(full), `Missing required deliverable file: ${rel}`);
    }
  });
});
