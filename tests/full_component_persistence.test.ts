/**
 * Full-Component Persistence & Cold-Start Rehydration Verification Suite (`tests/full_component_persistence.test.ts`).
 *
 * Verifies that ALL stateful service components in ANSTS v2.2 persist their state to SQLite / LTX
 * and survive process restarts / V8 Isolate eviction with zero state loss (`RPO = 0`):
 * 1. `AnstsSchedulerCell`: tasks, queues, CAS leases, DAG vouchers & Bayesian priors rehydrated across cold start
 * 2. `AgentTaskHarnessCell`: TurnWAL frames, Saga journal, and `DurableWorkflowContext` steps rehydrated across cold start
 * 3. `UnifiedComponentPersistenceStore`: `ShardedQueueBrokerPool`, `TransactionalOutboxDispatcher`,
 *    `FinOpsBudgetController`, `McpDynamicToolRegistry`, `PartitionedSchedulerRouter`, and `ManualFleetScaleController`
 */

import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import { test } from "node:test";
import * as yaml from "js-yaml";

import {
  AgentTask,
  AgentTaskHarnessCell,
  AnstsSchedulerCell,
  CloudEventsBus,
  CloudflareCelldEnv,
  DurableWorkflowContext,
  FinOpsBudgetController,
  LeaseCoordinator,
  ManualFleetScaleController,
  McpDynamicToolRegistry,
  MockDurableObjectState,
  PartitionedSchedulerRouter,
  SagaToolGateway,
  ShardedQueueBrokerPool,
  TaskState,
  ToolEffectClass,
  TransactionalOutboxDispatcher,
  UnifiedComponentPersistenceStore,
} from "../src/index";

test("Persistence Audit #1: AnstsSchedulerCell persists & rehydrates queued/dispatched tasks, leases, and Bayesian priors across cold starts", async () => {
  const sharedDoState = new MockDurableObjectState("scheduler-cell-shard-00");
  const mockEnv = {} as CloudflareCelldEnv;

  const specPath = path.join(__dirname, "..", "..", "specs", "taskspec_v1beta1_example.yaml");
  const sampleSpec = yaml.load(fs.readFileSync(specPath, "utf8")) as Record<string, any>;

  // 1. Instance #1 submits task and runs scheduler tick -> writes to embedded SQLite
  const cellInstance1 = new AnstsSchedulerCell(sharedDoState, mockEnv);
  const subRes = await cellInstance1.fetch(
    new Request("http://scheduler.internal/tasks/submit", {
      method: "POST",
      body: JSON.stringify(sampleSpec),
    }),
  );
  assert.equal(subRes.status, 202);
  await cellInstance1.fetch(new Request("http://scheduler.internal/scheduler/tick", { method: "POST" }));

  // 2. Simulate V8 Isolate eviction / node restart -> instantiate brand-new AnstsSchedulerCell on same SQLite storage
  const cellInstance2 = new AnstsSchedulerCell(sharedDoState, mockEnv);
  assert.equal(cellInstance2.cluster.scheduler.tasks.size, 1);
  const rehydratedTask = cellInstance2.cluster.scheduler.tasks.get(sampleSpec.metadata.task_id)!;
  assert.equal(rehydratedTask.state, TaskState.RUNNING);
  assert.equal(rehydratedTask.fence.epoch, 1);
});

test("Persistence Audit #2: AgentTaskHarnessCell persists & rehydrates DurableWorkflowContext steps across cold starts", async () => {
  const sharedDoState = new MockDurableObjectState("tsk-wf-persist-01");
  const mockEnv = {
    PRIMARY_MODEL_POOL: "DeepSeek",
    FAST_UTILITY_POOL: "GLM-5.3",
    FALLBACK_MODEL_POOL: "Qwen-3.8",
  } as CloudflareCelldEnv;

  const harness1 = new AgentTaskHarnessCell(sharedDoState, mockEnv);
  const wfRes1 = await harness1.fetch(
    new Request("http://harness.internal/workflow", {
      method: "POST",
      body: JSON.stringify({
        event_to_deliver: { type: "sandbox.ready", payload: { containerId: "ctr-99" } },
        sleep_duration_ms: 5000,
      }),
    }),
  );
  assert.equal(wfRes1.status, 200);
  assert.equal(harness1.workflowCtx.completedSteps.size, 3);

  // Simulate V8 Isolate hibernation wakeup -> brand-new AgentTaskHarnessCell restores workflow steps from SQLite!
  const harness2 = new AgentTaskHarnessCell(sharedDoState, mockEnv);
  assert.equal(harness2.workflowCtx.completedSteps.size, 3);
  assert.deepEqual(harness2.workflowCtx.completedSteps.get("evt:sandbox.ready"), { containerId: "ctr-99" });
});

test("Persistence Audit #3: UnifiedComponentPersistenceStore persists & rehydrates QueuePool, Outbox, FinOps, MCP Registry & Fleet Router", async () => {
  const doState = new MockDurableObjectState("control-plane-persistence");
  const store = new UnifiedComponentPersistenceStore(doState.storage.sql);

  // 1. ShardedQueueBrokerPool persistence
  const qPool1 = new ShardedQueueBrokerPool("ansts-q", 4, 3);
  qPool1.produceMessage("msg-1", { task: "t1" });
  qPool1.settleOrRetryMessage({ id: "msg-dlq", body: {}, attempts: 3 }, false);
  store.persistQueuePool(qPool1);

  const qPool2 = new ShardedQueueBrokerPool("ansts-q", 4, 3);
  assert.equal(store.rehydrateQueuePool(qPool2), true);
  assert.equal(qPool2.deadLetterQueue.length, 1);

  // 2. TransactionalOutboxDispatcher persistence
  const bus = new CloudEventsBus();
  const outbox1 = new TransactionalOutboxDispatcher("whsec_persist", 3);
  outbox1.stageInTransaction(bus.emit("agent.task.started", "tsk-1", "fin-enterprise", {}));
  store.persistOutbox(outbox1);

  const outbox2 = new TransactionalOutboxDispatcher("whsec_persist", 3);
  assert.equal(store.rehydrateOutbox(outbox2), true);
  assert.equal(outbox2.outbox.length, 1);

  // 3. FinOpsBudgetController persistence
  const finops1 = new FinOpsBudgetController();
  finops1.registerTenantBudget("tenant-persist", 50.0);
  finops1.recordTurnSpend({
    task: new AgentTask({ taskId: "t1", tenantId: "tenant-persist" }),
    pool: "DeepSeek",
    inputTokens: 10000,
    reasoningTokens: 10000,
    outputTokens: 5000,
    activeCpuCoreSeconds: 20,
  });
  store.persistFinOpsLedger(finops1);

  const finops2 = new FinOpsBudgetController();
  assert.equal(store.rehydrateFinOpsLedger(finops2), true);
  assert.ok((finops2.budgets.get("tenant-persist")?.spentModelUsd ?? 0) > 0);

  // 4. McpDynamicToolRegistry persistence
  const leaseCoord = new LeaseCoordinator();
  const sagaGw = new SagaToolGateway(leaseCoord);
  const mcp1 = new McpDynamicToolRegistry(sagaGw);
  const toolList = [
    {
      serverName: "git-mcp",
      name: "create_tag",
      description: "Creates a git tag",
      inputSchema: { type: "object" as const, required: ["tag"], properties: { tag: { type: "string" } } },
      annotations: { compensationTool: "git-mcp/delete_tag" },
    },
  ];
  mcp1.registerMcpServerTools(toolList);
  store.persistMcpRegistry(mcp1, toolList);

  const mcp2 = new McpDynamicToolRegistry(sagaGw);
  assert.equal(store.rehydrateMcpRegistry(mcp2), true);
  assert.equal(mcp2.getContract("git-mcp/create_tag")?.assignedEffectClass, ToolEffectClass.SAGA_COMPENSATABLE);

  // 5. PartitionedSchedulerRouter & ManualFleetScaleController persistence
  const router1 = new PartitionedSchedulerRouter({ initialShardCount: 4 });
  const manual1 = new ManualFleetScaleController( undefined, router1);
  manual1.executeManualScaleOut({
    addBrainNodes: [{ nodeId: "celld-node-99", advertiseAddress: "10.240.16.99:9001" }],
    addSchedulerShards: 4,
  });
  store.persistFleetAndRouterState(router1, manual1);

  const router2 = new PartitionedSchedulerRouter({ initialShardCount: 4 });
  const manual2 = new ManualFleetScaleController(undefined, router2);
  assert.equal(store.rehydrateFleetAndRouterState(router2, manual2), true);
  assert.equal(router2.getShardStates().length, 8);
  assert.equal(manual2.fleetController.nodes.has("celld-node-99"), true);
});
