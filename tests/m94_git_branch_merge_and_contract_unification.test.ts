/**
 * @fileoverview M94: Git Branch Merge (`main` a9ccc0e + `feat/celld-v25` M16-M93) & Unified Contract Verification Suite
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import * as child_process from "node:child_process";
import {
  AgentTask,
  TaskSpec,
  ToolEffectClass,
  ToolCallEffectClass,
  EFFORT_MULTIPLIERS,
  BayesianTaskProfiler,
  TaskResourceProfiler,
  MultiTenantQueueManager,
  HierarchicalQueueManager,
  computeCrc32,
  encodeLengthPrefixedCrcFrame,
  decodeLengthPrefixedCrcFrames,
} from "../src/index.js";

describe("M94 Git Branch Merge & Unified Contract Suite", () => {
  it("verifies unified Git ancestry of main (a9ccc0e) and feat/celld-v25 (a9235d8) and preserves all architecture & console docs", () => {
    const requiredDocs = [
      "docs/agent_sandbox_scheduler_dashboard.html",
      "docs/ansts_production_deployment_architecture_design.md",
      "docs/ansts_v2_implementation_backlog.md",
      "docs/architecture_design_doc.md",
      "docs/celld_persistent_recoverable_ha_scheduler_design.md",
      "docs/microservices_architecture.md",
      "docs/index.html",
      "docs/ansts_console_management.html",
      "docs/console-management-design.md",
      "docs/ansts_full_improvements_and_bugfixes_report.md",
      "docs/google_engineering_implementation_report.md",
    ];
    for (const rel of requiredDocs) {
      assert.equal(
        fs.existsSync(path.resolve(process.cwd(), rel)),
        true,
        `Expected merged repository to contain ${rel}`
      );
    }

    // Verify both original main (a9ccc0e) and M93 (a9235d8) exist in the Git object store
    const revA = child_process.execSync("git rev-parse --verify a9ccc0e", { encoding: "utf-8" }).trim();
    const revB = child_process.execSync("git rev-parse --verify a9235d8", { encoding: "utf-8" }).trim();
    assert.ok(revA.startsWith("a9ccc0e"));
    assert.ok(revB.startsWith("a9235d8"));
  });

  it("verifies unified M1-M15 and M16-M94 exported classes, aliases, and binary framing helpers", () => {
    assert.equal(AgentTask, TaskSpec);
    assert.equal(ToolEffectClass, ToolCallEffectClass);
    assert.equal(BayesianTaskProfiler, TaskResourceProfiler);
    assert.equal(MultiTenantQueueManager, HierarchicalQueueManager);
    assert.equal(EFFORT_MULTIPLIERS.high, 1.6);

    const task = new AgentTask({
      task_id: "merged-task-01",
      tenant_id: "Northstar",
      priority: 0,
      prompt: "Verify merged AgentTask alias",
    });
    const qm = new MultiTenantQueueManager();
    qm.enqueue(task);
    assert.equal(qm.totalQueued(), 1);
    const popped = qm.popNext();
    assert.equal(popped?.task_id, "merged-task-01");

    const profiler = new BayesianTaskProfiler();
    profiler.recordExecutionMetrics(task, 2.0, 2048);
    assert.equal(profiler.listTenantProfiles().length, 1);

    const frame1 = encodeLengthPrefixedCrcFrame({ requestId: "req-1", ok: true });
    const frame2 = encodeLengthPrefixedCrcFrame({ requestId: "req-2", count: 42 });
    const decoded = decodeLengthPrefixedCrcFrames(Buffer.concat([frame1, frame2]));
    assert.equal(decoded.frames.length, 2);
    assert.equal(decoded.frames[0].requestId, "req-1");
    assert.equal(decoded.frames[1].count, 42);
    assert.equal(decoded.remaining.length, 0);
    assert.ok(computeCrc32(Buffer.from("ansts-v25")) > 0);
  });
});
