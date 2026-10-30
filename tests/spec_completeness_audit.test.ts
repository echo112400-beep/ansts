/**
 * 100% Spec-to-Code Completeness Verification Suite.
 * Verifies every granular feature in the Google Design Doc & TaskSpec v1beta1.
 */

import { strict as assert } from "assert";
import { test } from "node:test";

import {
  AgentHarnessRunner,
  AgentTask,
  AnstsClusterRuntime,
  SyntacticAndVelocityBreaker,
  TaskState,
} from "../src/index";

test("Spec Completeness #1: A -> B -> A -> B Cyclic Tool Oscillation & Consecutive Tool Error (>=5) Breakers", () => {
  const breaker = new SyntacticAndVelocityBreaker(3, 5);
  const task = new AgentTask({ taskId: "tsk_osc_01", tenantId: "codegen-pro", state: TaskState.RUNNING });

  // A -> B -> A -> B cyclic oscillation triggers REFLEXION_INTERVENTION on 1st cycle, CIRCUIT_BROKEN on 2nd
  assert.equal(breaker.recordAndCheckToolCall(task.taskId, "read_file", "path=a.ts"), "OK");
  assert.equal(breaker.recordAndCheckToolCall(task.taskId, "edit_file", "path=a.ts"), "OK");
  assert.equal(breaker.recordAndCheckToolCall(task.taskId, "read_file", "path=a.ts"), "OK");
  assert.equal(
    breaker.recordAndCheckToolCall(task.taskId, "edit_file", "path=a.ts"),
    "REFLEXION_INTERVENTION",
  );
  assert.equal(breaker.recordAndCheckToolCall(task.taskId, "read_file", "path=a.ts"), "CIRCUIT_BROKEN");

  // Consecutive tool error breaker (>=5 non-zero exits)
  for (let i = 0; i < 4; i++) {
    assert.equal(breaker.recordToolExitStatus(task, 1), "OK");
  }
  assert.equal(breaker.recordToolExitStatus(task, 127), "CIRCUIT_BROKEN");
  assert.equal(task.state, TaskState.CIRCUIT_BROKEN);
});

test("Spec Completeness #2: Token Burn-Rate Spike (>35%) & Degenerate Repetition Breaker", () => {
  const breaker = new SyntacticAndVelocityBreaker();
  const task = new AgentTask({
    taskId: "tsk_spike_01",
    tenantId: "fin-enterprise",
    maxTotalTokens: 100000,
    temperature: 0.4,
    fallbackModels: ["Qwen-3.8", "GLM-5.3"],
  });

  // Single turn burns 40,000 tokens (40% > 35% threshold) -> halves temperature & switches to Qwen-3.8
  const spikeRes = breaker.evaluateBurnRateSpikeAndRepetition(task, 40000, "normal output");
  assert.equal(spikeRes.tripped, true);
  assert.equal(spikeRes.newModel, "Qwen-3.8");
  assert.equal(task.temperature, 0.2);

  // Degenerate character repetition detection
  const repRes = breaker.evaluateBurnRateSpikeAndRepetition(
    task,
    1000,
    "REPEATED_TOKEN_BLOCK_".repeat(8),
  );
  assert.equal(repRes.tripped, true);
});

test("Spec Completeness #3: Sandbox Abuse Breaker (deny_commands & max_pids -> Cgroup Freeze)", () => {
  const breaker = new SyntacticAndVelocityBreaker();
  const task = new AgentTask({
    taskId: "tsk_abuse_01",
    tenantId: "codegen-pro",
    maxPids: 256,
    denyCommands: ["rm -rf /", "mkfs", "dd", "shutdown"],
  });

  const cmdCheck = breaker.enforceSandboxAbuseAndCommandGuard(task, "sudo rm -rf / --no-preserve-root", 12);
  assert.equal(cmdCheck.allowed, false);
  assert.equal(cmdCheck.cgroupFrozen, true);
  assert.equal(task.cgroupFrozen, true);
  assert.equal(task.state, TaskState.CIRCUIT_BROKEN);

  const task2 = new AgentTask({ taskId: "tsk_forkbomb", tenantId: "codegen-pro", maxPids: 256 });
  const pidCheck = breaker.enforceSandboxAbuseAndCommandGuard(task2, "python3 worker.py", 300);
  assert.equal(pidCheck.allowed, false);
  assert.equal(pidCheck.cgroupFrozen, true);
});

test("Spec Completeness #4: Ephemeral Heavy Executor Offloading & Live Turn Migration (<50ms)", () => {
  const cluster = new AnstsClusterRuntime();
  const task = new AgentTask({ taskId: "tsk_heavy_01", tenantId: "research-lab" });
  cluster.scheduler.submitTask(task);
  cluster.scheduler.scheduleTick(1.0);

  // Attach ephemeral heavy executor when playwright_browser tool is invoked
  const handle = cluster.nodeDaemon.attachEphemeralHeavyExecutorIfNeeded(task, "playwright_browser");
  assert.ok(handle !== null);
  assert.equal(handle!.executorPool, "pool-headless-chromium-gpu");
  assert.equal(handle!.attached, true);
  assert.equal(cluster.nodeDaemon.detachEphemeralHeavyExecutor(task.taskId), true);

  // Section 6.2 Guardrail 3: Live Turn Migration after 3 consecutive saturated turns
  assert.equal(
    cluster.nodeDaemon.checkAndLiveMigrateAtTurnBoundary(task, true, "worker-us-west-02").migrated,
    false,
  );
  assert.equal(
    cluster.nodeDaemon.checkAndLiveMigrateAtTurnBoundary(task, true, "worker-us-west-02").migrated,
    false,
  );
  const mig = cluster.nodeDaemon.checkAndLiveMigrateAtTurnBoundary(task, true, "worker-us-west-02");
  assert.equal(mig.migrated, true);
  assert.ok((mig.migrationLatencyMs ?? 100) < 50);
  assert.equal(task.assignedNodeId, "worker-us-west-02");
});

test("Spec Completeness #5: Cumulative maxTotalSessions Enforcement in AgentHarnessRunner", () => {
  const runner = new AgentHarnessRunner();
  const task = new AgentTask({
    taskId: "tsk_sess_cap",
    tenantId: "fin-enterprise",
    maxSessions: 2,
    maxTotalSessions: 3,
    activeSessions: 1,
    totalSpawnedSessions: 1,
  });

  // Spawn 2nd session -> allowed (active=2, total=2)
  assert.equal(runner.tryAcquireSubSession(task), true);
  // 3rd concurrent session -> blocked by maxSessions=2
  assert.equal(runner.tryAcquireSubSession(task), false);
  // Release one session -> active=1, spawn 3rd cumulative session -> allowed (active=2, total=3)
  runner.releaseSubSession(task);
  assert.equal(runner.tryAcquireSubSession(task), true);
  // Release one session -> active=1, try to spawn 4th cumulative session -> blocked by maxTotalSessions=3!
  runner.releaseSubSession(task);
  assert.equal(runner.tryAcquireSubSession(task), false);
});
