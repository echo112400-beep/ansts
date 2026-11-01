/**
 * Verification Suite for the 6 Architectural Audit & Production Hardening Improvements.
 */

import { strict as assert } from "assert";
import { test } from "node:test";

import {
  AgentTask,
  AgentTaskHarnessCell,
  AnstsSchedulerCell,
  AnstsClusterRuntime,
  BayesianTaskProfiler,
  CloudflareCelldEnv,
  FenceToken,
  MockDurableObjectState,
  TaskState,
} from "../src/index";
import { NodeCapacityCell } from "../src/cloudflare/node_capacity_cell";

test("Audit Fix #1: celld / Durable Object Cold-Start & Hibernation Wakeup SQLite Rehydration", async () => {
  const sharedState = new MockDurableObjectState("tsk_rehydrate_01");
  const mockEnv = {
    TEST_SIMULATOR_MODE: true,
    PRIMARY_MODEL_POOL: "DeepSeek",
    FAST_UTILITY_POOL: "GLM-5.3",
    FALLBACK_MODEL_POOL: "Qwen-3.8",
    ANSTS_CHECKPOINTS_R2: {
      put: async (key: string) => ({ key }),
      get: async () => null,
      list: async () => ({ objects: [] }),
      delete: async () => {},
    },
  } as CloudflareCelldEnv;

  const nodeCells = new Map<string, NodeCapacityCell>();
  let activeHarness: AgentTaskHarnessCell;
  mockEnv.AGENT_HARNESS_CELL = {
    idFromName: (name: string) => ({ toString: () => name, name }),
    get: () => ({ fetch: (input: RequestInfo, init?: RequestInit) => {
      const req = typeof input === "string" ? new Request(input, init) : input;
      return activeHarness.fetch(req);
    } }),
  };
  mockEnv.NODE_CAPACITY_CELL = {
    idFromName: (name: string) => ({ toString: () => name, name }),
    get: (id: { toString(): string; name?: string }) => {
      const nodeId = id.name ?? id.toString();
      if (!nodeCells.has(nodeId)) nodeCells.set(nodeId, new NodeCapacityCell(new MockDurableObjectState(nodeId), mockEnv));
      const cell = nodeCells.get(nodeId)!;
      return { fetch: (input: RequestInfo, init?: RequestInit) => {
        const req = typeof input === "string" ? new Request(input, init) : input;
        return cell.fetch(req);
      } };
    },
  };
  const scheduler = new AnstsSchedulerCell(new MockDurableObjectState("global-5d-scheduler"), mockEnv);
  const dispatch = async (kind: "ordinary" | "recovery", nodeId: string, incarnation: string, dispatchId: string) => {
    const nodeCapacity = { cpu: 8, memory_mb: 4096, burst_cpu: 4, sessions: 8, tps: 100 };
    const response = await scheduler.fetch(new Request("http://scheduler.internal/scheduler/dispatch", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        task_id: "tsk_rehydrate_01",
        tenant_id: "fin-enterprise",
        kind,
        dispatch_id: dispatchId,
        node_id: nodeId,
        node_incarnation: incarnation,
        nodes: [{ node_id: nodeId, incarnation, capacity: nodeCapacity }],
        task_spec: { metadata: { task_id: "tsk_rehydrate_01", tenant_id: "fin-enterprise" }, spec: {} },
        resources: { cpu: 1, memory_mb: 512, burst_cpu: 1, sessions: 1, tps: 10 },
      }),
    }));
    const result = await response.json() as any;
    assert.equal(result.stage, "AUTHORIZED");
    const started = await scheduler.fetch(new Request("http://scheduler.internal/scheduler/started", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        dispatch_id: result.dispatch_id,
        node_id: result.node_id,
        node_incarnation: result.node_incarnation,
        authorization_token: result.authorization_token,
        business_epoch: result.business_epoch,
        start_receipt: `test-node-running:${dispatchId}`,
      }),
    }));
    assert.equal(started.status, 200);
    return result;
  };

  // First activation: execute Turn 1 and Turn 2, then hibernate and destroy V8 Isolate
  const cellInstance1 = new AgentTaskHarnessCell(sharedState, mockEnv);
  activeHarness = cellInstance1;
  const firstDispatch = await dispatch("ordinary", "rehydrate-node-1", "rehydrate-boot-1", "rehydrate-initial");
  assert.equal(cellInstance1.taskCore.getDispatch(firstDispatch.dispatch_id)!.stage, "STARTED");
  const firstTurn = await cellInstance1.fetch(
    new Request("http://harness.internal/turn", {
      method: "POST",
      body: JSON.stringify({
        epoch: 1,
        turn_seq: 1,
        workspace_merkle_hash: "hash_turn_1",
      }),
    }),
  );
  assert.equal(firstTurn.status, 200, await firstTurn.clone().text());
  const secondTurn = await cellInstance1.fetch(
    new Request("http://harness.internal/turn", {
      method: "POST",
      body: JSON.stringify({
        epoch: 1,
        turn_seq: 2,
        is_read_only_turn: true,
        workspace_merkle_hash: "hash_turn_2",
      }),
    }),
  );
  assert.equal(secondTurn.status, 200, await secondTurn.clone().text());
  await cellInstance1.alarm();
  assert.equal(cellInstance1.task.state, TaskState.HIBERNATED);

  // Second activation (Cold-Start Wakeup on same SQLite storage): constructor must rehydrate Turn 1 & 2!
  const cellInstance2 = new AgentTaskHarnessCell(sharedState, mockEnv);
  activeHarness = cellInstance2;
  assert.equal(cellInstance2.task.fence.epoch, 1);
  assert.equal(cellInstance2.task.fence.turnSeq, 3);
  assert.ok(cellInstance2.task.usedTokens > 0);
  assert.equal((cellInstance2.nodeDaemon.walStore.get("tsk_rehydrate_01") ?? []).length, 2);

  // Recovery restarts the suspended execution through TaskCore and the same capacity boundary.
  const recovery = await dispatch("recovery", "rehydrate-node-2", "rehydrate-boot-2", "rehydrate-recovery-2");
  assert.equal(recovery.business_epoch, 2);
  assert.equal(cellInstance2.taskCore.getDispatch(recovery.dispatch_id)!.stage, "STARTED");
  assert.equal(cellInstance2.task.fence.epoch, 2);

  // Replay to Turn 2 works seamlessly on the newly rehydrated Cell instance
  const replayRes = await cellInstance2.fetch(
    new Request("http://harness.internal/replay", {
      method: "POST",
      body: JSON.stringify({ target_turn_seq: 2 }),
    }),
  );
  assert.equal(replayRes.status, 200);
  const replayBody = (await replayRes.json()) as Record<string, any>;
  assert.equal(replayBody.replayed_up_to_turn, 2);
  assert.equal(replayBody.restored_workspace_hash, "hash_turn_2");
});

test("Audit Fix #2: Preempted Sandboxes Automatically Resume from Exact Turn When Node Headroom Returns", () => {
  const cluster = new AnstsClusterRuntime();
  const node1 = cluster.scheduler.nodes.get("worker-us-west-01")!;
  node1.cpuCap = 2.0;
  cluster.scheduler.nodes.get("worker-us-west-02")!.alive = false;
  cluster.scheduler.nodes.get("worker-us-west-03")!.alive = false;

  // 1. P4 batch task starts on worker-us-west-01 and completes 3 turns
  const p4Task = new AgentTask({
    taskId: "tsk_p4_batch",
    tenantId: "research-lab",
    priorityClass: "P4",
    baseScore: 200,
    execCpu: 2.0,
  });
  cluster.scheduler.submitTask(p4Task);
  cluster.scheduler.scheduleTick(1.0);
  p4Task.fence = new FenceToken(1, 4, p4Task.taskId);
  cluster.leaseCoord.verifyAndAdvanceFence(p4Task.fence);

  // 2. P0 urgent task arrives and preempts P4 task to HIBERNATED
  const p0Task = new AgentTask({
    taskId: "tsk_p0_crit",
    tenantId: "fin-enterprise",
    priorityClass: "P0",
    baseScore: 950,
    execCpu: 2.0,
  });
  cluster.scheduler.submitTask(p0Task);
  cluster.scheduler.scheduleTick(2.0);
  assert.equal(p4Task.state, TaskState.HIBERNATED);
  assert.equal(p4Task.hibernationReason, "PREEMPTED");
  assert.equal(p0Task.state, TaskState.RUNNING);

  // 3. P0 task finishes and frees node CPU headroom -> next scheduleTick automatically wakes P4 at turnSeq=4, epoch=2!
  p0Task.state = TaskState.SUCCEEDED;
  const resumed = cluster.scheduler.scheduleTick(3.0);
  assert.equal(resumed.length, 1);
  assert.equal(p4Task.state, TaskState.RUNNING);
  assert.equal(p4Task.fence.epoch, 2);
  assert.equal(p4Task.fence.turnSeq, 4);
});

test("Audit Fix #3: Unified CognitiveRouter Composes 3-State Breaker, TokenBucket Pacing & PoolClient Metering", () => {
  const cluster = new AnstsClusterRuntime();
  const task = new AgentTask({
    taskId: "tsk_unified_router",
    tenantId: "fin-enterprise",
    reservedTps: 200,
    effort: "max",
  });
  cluster.scheduler.submitTask(task);
  cluster.scheduler.scheduleTick(1.0);

  const r1 = cluster.cogRouter.routeTurn(task, task.fence, false, 0, 10.0);
  assert.equal(r1.routedModel, "DeepSeek");
  assert.equal(r1.chunks.length, 2);
  assert.ok(r1.metering.thinkingTokens > 0);

  // Trip DeepSeek breaker at t=10s -> routes to Qwen-3.8 at t=12s -> cooldown expires at t=21s (HALF_OPEN)
  cluster.cogRouter.tripPoolBreaker("DeepSeek", 0.50, 10.0);
  const r2 = cluster.cogRouter.routeTurn(task, task.fence, false, 0, 12.0);
  assert.equal(r2.routedModel, "Qwen-3.8");

  const r3 = cluster.cogRouter.routeTurn(task, task.fence, false, 0, 22.0);
  assert.equal(r3.routedModel, "DeepSeek");
  assert.equal(cluster.cogRouter.pools.DeepSeek.breakerState, "HALF_OPEN");
});

test("Audit Fix #4: Dual-Clock Budget Accounting (Active Compute vs Wall Clock) & Graceful Drain", () => {
  const task = new AgentTask({
    taskId: "tsk_dual_clock",
    tenantId: "codegen-pro",
    state: TaskState.HIBERNATED,
    maxWallClockSec: 1800,
    maxActiveComputeSec: 60,
  });

  // Spending 500s in HIBERNATED advances wallClockSec but 0s of activeComputeSec
  const drained1 = task.advanceClocks(500.0, 45.0);
  assert.equal(drained1, false);
  assert.equal(task.wallClockSec, 500.0);
  assert.equal(task.activeComputeSec, 0.0);

  // Transition to RUNNING and consume 65s of active compute -> triggers GRACEFUL_DRAINING
  task.state = TaskState.RUNNING;
  const drained2 = task.advanceClocks(70.0, 65.0);
  assert.equal(drained2, true);
  assert.equal(task.state, TaskState.GRACEFUL_DRAINING);
  assert.ok(task.drainSummary?.includes("Graceful drain triggered"));
});

test("Audit Fix #5: V8 Isolate Portable JSON Serialization in BayesianTaskProfiler", () => {
  const p1 = new BayesianTaskProfiler(3);
  const task = new AgentTask({ taskId: "t_v8", tenantId: "fin", maxTotalTokens: 400000, usedTokens: 95000 });
  for (let i = 0; i < 4; i++) {
    p1.recordCompletion(task, 0.82);
  }
  const jsonPayload = p1.toJSONString();

  const p2 = new BayesianTaskProfiler(3);
  p2.loadFromJSONString(jsonPayload);
  const soft = p2.assignInitialP50Reservation(task);
  assert.ok(soft >= 90000 && soft <= 130000);
});
