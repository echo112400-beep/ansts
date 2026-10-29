/**
 * End-to-End Multi-Service Integration Test Suite in TypeScript.
 */

import { strict as assert } from "assert";
import * as fs from "fs";
import * as path from "path";
import { test } from "node:test";
import * as yaml from "js-yaml";

import {
  AgentTask,
  AnstsClusterRuntime,
  FenceToken,
  SandboxPhase,
  StaleFenceError,
  TaskState,
  ToolEffectClass,
} from "../src/index";

test("01. TaskSpec v1beta1 loading, P50 Bayesian soft reservation, and 5D W-DRF scheduling", () => {
  const cluster = new AnstsClusterRuntime();
  const specPath = path.join(__dirname, "..", "..", "specs", "taskspec_v1beta1_example.yaml");
  const raw = yaml.load(fs.readFileSync(specPath, "utf8")) as Record<string, any>;

  assert.equal(raw.apiVersion, "scheduler.agent.io/v1beta1");
  assert.equal(raw.spec.cognitive_policy.primary_model, "DeepSeek");
  assert.equal(raw.spec.cognitive_policy.fast_utility_model, "GLM-5.3");
  assert.ok(raw.spec.cognitive_policy.fallback_models.includes("Qwen-3.8"));

  const t1 = new AgentTask({
    taskId: "tsk_fin_01",
    tenantId: "fin-enterprise",
    priorityClass: "P0",
    baseScore: 950,
    primaryModel: "DeepSeek",
    effort: "high",
    maxTotalTokens: 600000,
  });
  const t2 = new AgentTask({
    taskId: "tsk_code_02",
    tenantId: "codegen-pro",
    priorityClass: "P1",
    baseScore: 820,
    primaryModel: "GLM-5.3",
    effort: "medium",
    maxTotalTokens: 300000,
  });

  const soft1 = cluster.scheduler.submitTask(t1);
  cluster.scheduler.submitTask(t2);
  assert.ok(soft1 <= t1.maxTotalTokens);

  const dispatched = cluster.scheduler.scheduleTick(10.0);
  assert.equal(dispatched.length, 2);
  assert.equal(t1.state, TaskState.RUNNING);
  assert.equal(t1.fence.epoch, 1);
  assert.equal(t1.fence.turnSeq, 1);
});

test("02. Bi-Modal Phase-Elastic Cgroups (0.1C <-> 8.0C) & Node Burst Semaphore", () => {
  const cluster = new AnstsClusterRuntime();
  const task = new AgentTask({ taskId: "tsk_phase_01", tenantId: "fin-enterprise" });
  cluster.scheduler.submitTask(task);
  cluster.scheduler.scheduleTick(1.0);

  const cogCpu = cluster.nodeDaemon.transitionToCognitivePhase(task);
  assert.equal(cogCpu, 0.1);
  assert.equal(task.phase, SandboxPhase.COGNITIVE);

  const burstRes = cluster.nodeDaemon.transitionToExecutionPhase(task);
  assert.equal(burstRes.allocatedCpu, 8.0);
  assert.equal(burstRes.microQueued, false);

  cluster.nodeDaemon.activeBurstCount = 4;
  const cappedRes = cluster.nodeDaemon.transitionToExecutionPhase(task);
  assert.equal(cappedRes.allocatedCpu, 2.0);
  assert.equal(cappedRes.microQueued, true);
});

test("03. Turn-level Cognitive Routing (DeepSeek / GLM-5.3 / Qwen-3.8) & 429 Failover", () => {
  const cluster = new AnstsClusterRuntime();
  const task = new AgentTask({
    taskId: "tsk_route_01",
    tenantId: "fin-enterprise",
    primaryModel: "DeepSeek",
    fastUtilityModel: "GLM-5.3",
    fallbackModels: ["Qwen-3.8", "GLM-5.3"],
    effort: "high",
  });
  cluster.scheduler.submitTask(task);
  cluster.scheduler.scheduleTick(1.0);

  const r1 = cluster.cogRouter.routeTurn(task, task.fence, false, 0);
  assert.equal(r1.routedModel, "DeepSeek");
  assert.equal(r1.effectiveEffort, "high");

  const r2 = cluster.cogRouter.routeTurn(task, task.fence, true, 0);
  assert.equal(r2.routedModel, "GLM-5.3");
  assert.equal(r2.effectiveEffort, "minimal");

  cluster.cogRouter.tripPoolBreaker("DeepSeek", 0.48);
  const r3 = cluster.cogRouter.routeTurn(task, task.fence, false, 0);
  assert.equal(r3.routedModel, "Qwen-3.8");
  assert.equal(r3.effectiveEffort, "medium");
});

test("04. Multi-Agent DAG Quota Escrow Vouchers & Straggler Acceleration", () => {
  const cluster = new AnstsClusterRuntime();
  const parent = new AgentTask({ taskId: "tsk_parent", tenantId: "fin-enterprise", reservedTps: 180 });
  cluster.scheduler.submitTask(parent);
  cluster.scheduler.scheduleTick(1.0);

  const children = [0, 1, 2, 3].map(
    (i) => new AgentTask({ taskId: `tsk_child_${i}`, tenantId: "fin-enterprise" }),
  );
  const vouchers = cluster.queueMgr.spawnDagWithEscrow("dag_01", parent, children, false);
  assert.equal(parent.state, TaskState.HIBERNATED_YIELD);
  assert.equal(vouchers.length, 4);

  const dispatched = cluster.scheduler.scheduleTick(2.0);
  assert.equal(dispatched.length, 4);

  children.slice(0, 3).forEach((c) => {
    c.state = TaskState.SUCCEEDED;
  });
  const oldTps = children[3].reservedTps;
  const actions = cluster.queueMgr.evaluateDagBarriers();
  assert.ok(actions.includes(`STRAGGLER_BOOSTED:${children[3].taskId}`));
  assert.ok(children[3].reservedTps > oldTps);
});

test("05. Progress Velocity (V_k < 0.10) Tree Backtrack & 4-Tier Saga Compensation", () => {
  const cluster = new AnstsClusterRuntime();
  const task = new AgentTask({ taskId: "tsk_saga_01", tenantId: "codegen-pro" });
  cluster.scheduler.submitTask(task);
  cluster.scheduler.scheduleTick(1.0);

  const res1 = cluster.nodeDaemon.recordTurnAndAuditVelocity({
    task,
    routedModel: "DeepSeek",
    effectiveEffort: "high",
    tokensUsed: 1200,
    workspaceMerkleHash: "hash_t1",
    passRate: 0.5,
    errorCount: 4,
    diffEntropy: 0.6,
  });
  assert.equal(res1.action, "CONTINUE");

  cluster.sagaGw.executeTool({
    fence: task.fence,
    toolCallId: "call_git_1",
    toolName: "git_branch_push",
    effectClass: ToolEffectClass.SAGA_COMPENSATABLE,
    payload: "branch=fix-bug-1",
    compensationTool: "git_branch_delete",
  });
  const stagedResp = cluster.sagaGw.executeTool({
    fence: task.fence,
    toolCallId: "call_ledger_1",
    toolName: "settlement_ledger_post",
    effectClass: ToolEffectClass.STAGED_COMMIT,
    payload: "amount=50000",
  });
  assert.equal(stagedResp.staged, true);

  let outcome: Record<string, any> = {};
  for (const h of ["hash_t1", "hash_t1", "hash_t1"]) {
    outcome = cluster.nodeDaemon.recordTurnAndAuditVelocity({
      task,
      routedModel: "DeepSeek",
      effectiveEffort: "high",
      tokensUsed: 1200,
      workspaceMerkleHash: h,
      passRate: 0.5,
      errorCount: 4,
      diffEntropy: 0.0,
    });
  }
  assert.equal(outcome.action, "BACKTRACKED");
  assert.ok(outcome.compensatedTools.includes("git_branch_delete(branch=fix-bug-1)"));
  assert.equal((cluster.sagaGw.stagingBuffer.get(task.taskId) ?? []).length, 0);
  assert.equal(task.effort, "max");
});

test("06. Host Crash Epoch+1 Takeover, TurnWAL Resume & Dual-Tuple Fencing Split-Brain Guard", () => {
  const cluster = new AnstsClusterRuntime();
  const task = new AgentTask({ taskId: "tsk_ha_01", tenantId: "fin-enterprise" });
  cluster.scheduler.submitTask(task);
  cluster.scheduler.scheduleTick(1.0);

  cluster.nodeDaemon.recordTurnAndAuditVelocity({
    task,
    routedModel: "DeepSeek",
    effectiveEffort: "high",
    tokensUsed: 1500,
    workspaceMerkleHash: "hash_ok_1",
    passRate: 0.7,
    errorCount: 1,
    diffEntropy: 0.5,
  });
  const staleZombieFence = new FenceToken(1, 2, task.taskId);

  const newFence = cluster.nodeDaemon.failoverTaskToNode(task, "worker-us-west-02");
  assert.equal(newFence.epoch, 2);
  assert.equal(task.assignedNodeId, "worker-us-west-02");

  assert.throws(
    () =>
      cluster.sagaGw.executeTool({
        fence: staleZombieFence,
        toolCallId: "zombie_call_99",
        toolName: "settlement_ledger_post",
        effectClass: ToolEffectClass.IDEMPOTENT_WRITE,
        payload: "duplicate_write",
      }),
    StaleFenceError,
  );
});
