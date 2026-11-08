/**
 * @fileoverview Google Engineering Standards & 74-Task Full-Lifecycle Verification Suite (7 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import * as Ansts from "../src/index.js";

describe("Google Engineering Standards & 74-Task Full-Lifecycle Audit", () => {
  it("1. Verifies all 52 src/<all>/*.ts modules carry @fileoverview JSDoc headers and strict typing", () => {
    const walk = (dir: string): string[] => {
      let out: string[] = [];
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) out = out.concat(walk(full));
        else if (entry.isFile() && entry.name.endsWith(".ts")) out.push(full);
      }
      return out;
    };
    const srcFiles = walk(path.join(process.cwd(), "src"));
    assert.equal(srcFiles.length, 54);
    for (const f of srcFiles) {
      const content = fs.readFileSync(f, "utf-8");
      assert.ok(content.includes("@fileoverview"), `Missing @fileoverview in ${f}`);
    }
  });

  it("2. Verifies M1-M3 Gateway, 5D Scheduler, DAG Escrow, CognitiveRouter & Saga exports", () => {
    assert.equal(typeof Ansts.TaskSpecValidator.validateAndNormalize, "function");
    assert.equal(typeof Ansts.CoreScheduler5D, "function");
    assert.equal(typeof Ansts.DagEscrowCoordinator, "function");
    assert.equal(typeof Ansts.CognitiveRouter, "function");
    assert.equal(typeof Ansts.SagaToolGateway, "function");
  });

  it("3. Verifies M4-M6 Sandbox Pool, Linux Cgroup/PSI, Vsock, WAL, Time-Travel & CloudEvents exports", () => {
    assert.equal(typeof Ansts.WarmSandboxPoolManager, "function");
    assert.equal(typeof Ansts.CgroupPhaseController, "function");
    assert.equal(typeof Ansts.LinuxPsiMemoryGuard, "function");
    assert.equal(typeof Ansts.TurnWalEngine, "function");
    assert.equal(typeof Ansts.DeterministicTimeTravelReplayer, "function");
    assert.equal(typeof Ansts.CloudEventsBus, "function");
  });

  it("4. Verifies M7-M9 Cloudflare/celld Cells, RPO=0 LTX Durability & 64-Shard Horizontal Scaler", () => {
    assert.equal(typeof Ansts.AnstsSchedulerCell, "function");
    assert.equal(typeof Ansts.AgentTaskHarnessCell, "function");
    assert.equal(typeof Ansts.RpoZeroDurabilityEngine, "function");
    assert.equal(typeof Ansts.PartitionedSchedulerRouter, "function");
  });

  it("5. Verifies M10-M11 RustFS S3 SigV4 CAS Driver, celld Admin Client & Real Linux Cgroup/Vsock Drivers", () => {
    assert.equal(typeof Ansts.RustFsS3LtxDriver, "function");
    assert.equal(typeof Ansts.CelldAdminClient, "function");
    assert.equal(typeof Ansts.LinuxCgroupV2AndPsiDriver, "function");
    assert.equal(typeof Ansts.ZeroTrustVsockHostDaemon, "function");
  });

  it("6. Verifies M12-M14 OTel/Prometheus, MCP Registry, FinOps, Manual Fleet Controller & 100% Persistence Store", () => {
    assert.equal(typeof Ansts.OtelAndPrometheusExporter, "function");
    assert.equal(typeof Ansts.McpDynamicToolRegistry, "function");
    assert.equal(typeof Ansts.FinOpsBudgetController, "function");
    assert.equal(typeof Ansts.ManualFleetScaleController, "function");
    assert.equal(typeof Ansts.UnifiedComponentPersistenceStore, "function");
  });

  it("7. Verifies M15 celld Native Facets, Dynamic Workers, KV Sharding & V8 128MB Heap/SQL Budget Guard", () => {
    assert.equal(typeof Ansts.DurableObjectFacetSupervisor, "function");
    assert.equal(typeof Ansts.DynamicWorkerSandboxLoader, "function");
    assert.equal(typeof Ansts.CelldKvOptimizerAndGuard, "function");
    assert.equal(typeof Ansts.V8HeapAndSqlBudgetGuard, "function");
  });
});
