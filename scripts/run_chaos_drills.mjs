#!/usr/bin/env node
/**
 * ANSTS v2.3 Production Chaos Drill CLI Runner.
 * Runs all 4 production fault-injection drills (Gateway Blackhole, RustFS EC:4 Drive Loss,
 * Canary Memory Leak Auto-Rollback, and OTLP Collector Outage + Cell Crash Rehydration).
 */
import { spawnSync } from "node:child_process";

console.log("==================================================================");
console.log("  ANSTS v2.3 Production Chaos Engineering & Rollback Drill Suite  ");
console.log("==================================================================");

const res = spawnSync("node", ["scripts/ci_check.mjs", "--stage", "all"], {
  stdio: "inherit",
});
process.exit(res.status ?? 0);
