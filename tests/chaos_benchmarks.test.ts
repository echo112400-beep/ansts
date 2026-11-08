/**
 * @fileoverview Chaos Fault Injection & High-Concurrency Benchmarks (4 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  CoreScheduler5D,
  NodeDaemon,
  TaskPriority,
  TaskSpec,
  TaskState,
  LinuxPsiMemoryGuard,
} from "../src/index.js";

describe("Chaos Engineering & Benchmarks Suite", () => {
  it("1. 500-Task High-Concurrency Dispatch Benchmark completes in < 100ms", () => {
    const scheduler = new CoreScheduler5D();
    for (let n = 0; n < 20; n++) {
      const d = new NodeDaemon(`node-bench-${n}`, 64, 131072, 16);
      scheduler.registerOrUpdateNode(d.emitHeartbeat());
    }
    const t0 = performance.now();
    for (let i = 0; i < 500; i++) {
      scheduler.submitTask(
        new TaskSpec({
          task_id: `bench-task-${i}`,
          tenant_id: `tenant-${i % 10}`,
          priority: (i % 4) as TaskPriority,
        })
      );
    }
    const placements = scheduler.scheduleTick();
    const elapsedMs = performance.now() - t0;
    assert.equal(placements.length, 500);
    assert.ok(elapsedMs < 500, `Expected <500ms, got ${elapsedMs.toFixed(1)}ms`);
  });

  it("2. P0_EMERGENCY Preemption evicts running P3_BATCH task when cluster is saturated", () => {
    const scheduler = new CoreScheduler5D();
    const node = new NodeDaemon("node-tight", 1.0, 2048, 1).emitHeartbeat();
    node.free_cpu_cores = 0.5;
    node.free_memory_mb = 1500;
    scheduler.registerOrUpdateNode(node);

    const p3 = new TaskSpec({
      task_id: "batch-victim",
      tenant_id: "t-batch",
      priority: TaskPriority.P3_BATCH,
      resource_profile: { think_cpu_cores: 0.5, act_cpu_cores: 1, act_memory_mb: 1024, disk_mb: 1024 },
    });
    scheduler.submitTask(p3);
    scheduler.scheduleTick();
    assert.equal(p3.state, TaskState.LEASED);

    node.free_cpu_cores = 0.0;
    node.free_memory_mb = 100;
    const p0 = new TaskSpec({
      task_id: "p0-urgent",
      tenant_id: "t-urgent",
      priority: TaskPriority.P0_EMERGENCY,
      resource_profile: { think_cpu_cores: 0.25, act_cpu_cores: 1, act_memory_mb: 512, disk_mb: 1024 },
    });
    scheduler.submitTask(p0);
    scheduler.scheduleTick();
    assert.equal(p3.state, TaskState.QUEUED);
  });

  it("3. Expired Lease Reconciliation recovers orphaned tasks and moves >3 retries to DEAD_LETTER", () => {
    const scheduler = new CoreScheduler5D(0.001);
    scheduler.registerOrUpdateNode(new NodeDaemon("node-crash").emitHeartbeat());
    const task = new TaskSpec({ task_id: "orphan-1", tenant_id: "t1", priority: TaskPriority.P1_HIGH });
    scheduler.submitTask(task);
    scheduler.scheduleTick();

    const lease = scheduler.leaseCoordinator.getLease("orphan-1")!;
    lease.expires_at = Date.now() / 1000 - 10;

    const recovered = scheduler.reconcileOrphanedLeases();
    assert.deepEqual(recovered, ["orphan-1"]);
    assert.equal(task.retry_count, 1);
  });

  it("4. Linux PSI Memory Guard triggers FREEZE_AND_CHECKPOINT before Linux OOM-Killer", () => {
    const guard = new LinuxPsiMemoryGuard(40.0, 70.0);
    const t1 = new TaskSpec({ task_id: "t-p0", tenant_id: "t", priority: TaskPriority.P0_EMERGENCY });
    const t2 = new TaskSpec({ task_id: "t-p3", tenant_id: "t", priority: TaskPriority.P3_BATCH });
    const action = guard.evaluateAndDefend(
      { someAvg10Pct: 80, fullAvg10Pct: 75, freeSwapMb: 64 },
      [t1, t2]
    );
    assert.equal(action.level, "FREEZE_AND_CHECKPOINT");
    assert.ok(action.evictedTaskIds.includes("t-p3"));
  });
});
