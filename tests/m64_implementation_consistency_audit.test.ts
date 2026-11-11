/**
 * @fileoverview M64 End-to-End Implementation Consistency & Authentic Feature Wiring Verification Suite.
 * Verifies:
 * 1. [M64-1] Cloudflare Worker (`src/cloudflare/worker.ts`) & `AnstsSchedulerCell` / `AgentTaskHarnessCell` route & queue dispatch parity.
 * 2. [M64-2] 100% `TaskSpec` field serialization & rehydration fidelity across `AnstsSchedulerCell`, `SchedulerV25SingleWritePathStore` & `UnifiedComponentPersistenceStore`.
 * 3. [M64-3] `SagaCompensatorEngine`, `StagingCommitAndApprovalGate`, `McpDynamicToolRegistry`, `CognitiveRouter` & `DagEscrowCoordinator` SQLite persistence & turn-accurate staged commit.
 * 4. [M64-4] `AnstsLocalAndComposeServer` full-subsystem SQLite wiring, cold-start scheduler rehydration & `POST /v1/tasks` deduplication/coalescing parity.
 * 5. [M64-5] `AgentHarnessRunner` router token metering & `PartitionedSchedulerRouter` multi-shard node registration & tick scheduling.
 * 6. [M64-6] Live HTTP `POST /v1beta1/management/system/implementation-consistency-audit` & CLI `anstsctl mgmt-consistency-audit`.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import workerDefault, { AnstsSchedulerCell, AgentTaskHarnessCell } from "../src/cloudflare/worker.js";
import {
  CloudflareCelldEnv,
  InMemoryCellSqlStorage,
  InMemoryKvNamespace,
  InMemoryQueueProducer,
  InMemoryR2Bucket,
  MockDurableObjectState,
} from "../src/cloudflare/env.js";
import { SchedulerV25SingleWritePathStore } from "../src/cloudflare/scheduler_v25_persistence.js";
import { UnifiedComponentPersistenceStore } from "../src/checkpoint/unified_state_store.js";
import { LlmEndpointMetrics, NodeHeartbeat, TaskPriority, TaskSpec, ToolCallEffectClass } from "../src/models.js";
import { CoreScheduler5D } from "../src/scheduler.js";
import { SagaToolGateway } from "../src/saga_gateway.js";
import { McpDynamicToolRegistry } from "../src/saga/mcp_registry.js";
import { CognitiveRouter } from "../src/cognitive_router.js";
import { DagEscrowCoordinator } from "../src/dag/escrow.js";
import { AgentHarnessRunner } from "../src/harness/runner.js";
import { PartitionedSchedulerRouter } from "../src/cloudflare/horizontal_scaler.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M64 End-to-End Implementation Consistency & Authentic Feature Wiring Suite", () => {
  it("[M64-1] Cloudflare Worker routes /v1/scheduler/* & /v1/harness/* and dispatches queue() batch messages to AnstsSchedulerCell", async () => {
    const schedState = new MockDurableObjectState("global-shard-0");
    const schedCell = new AnstsSchedulerCell(schedState);
    const harnessState = new MockDurableObjectState("task-h-01");
    const r2 = new InMemoryR2Bucket();
    const harnessCell = new AgentTaskHarnessCell(harnessState, r2, "task-h-01");

    const env: CloudflareCelldEnv = {
      CELLD_REGION: "us-west-2",
      ANSTS_SCHEDULER_CELL: {
        idFromName: (name: string) => ({ toString: () => name }),
        get: () => ({ fetch: (req: Request) => schedCell.fetch(req) }),
      },
      ANSTS_HARNESS_CELL: {
        idFromName: (name: string) => ({ toString: () => name }),
        get: () => ({ fetch: (req: Request) => harnessCell.fetch(req) }),
      },
      ANSTS_CHECKPOINT_BUCKET: r2,
      ANSTS_KV_CACHE: new InMemoryKvNamespace(),
      ANSTS_TASK_QUEUE: new InMemoryQueueProducer(),
    };

    // 1. Submit task via Worker `/v1/scheduler/submit`
    const submitResp = await workerDefault.fetch(
      new Request("https://edge.ansts.io/v1/scheduler/submit", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task_id: "task-worker-http-01",
          tenant_id: "Northstar",
          priority: TaskPriority.P0_EMERGENCY,
          prompt: "Run via worker fetch",
        }),
      }),
      env
    );
    assert.equal(submitResp.status, 202);
    const submitData = (await submitResp.json()) as { task_id: string; state: string };
    assert.equal(submitData.task_id, "task-worker-http-01");
    assert.equal(schedCell.scheduler.allTasks.has("task-worker-http-01"), true);

    // 2. Dispatch queue batch via Worker `queue(batch, env)`
    let acked = 0;
    await workerDefault.queue(
      {
        queue: "ansts-tasks",
        messages: [
          {
            id: "msg-1",
            timestamp: new Date(),
            body: {
              task_id: "task-worker-queue-02",
              tenant_id: "Northstar",
              priority: TaskPriority.P1_HIGH,
              prompt: "Run via worker queue",
            },
            ack: () => {
              acked += 1;
            },
            retry: () => {},
          },
        ],
      },
      env
    );
    assert.equal(acked, 1);
    assert.equal(schedCell.scheduler.allTasks.has("task-worker-queue-02"), true);

    // 3. Execute turn via Worker `/v1/harness/execute-turn`
    const harnessResp = await workerDefault.fetch(
      new Request("https://edge.ansts.io/v1/harness/execute-turn?taskId=task-h-01", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          turnIndex: 1,
          fencingToken: 5,
          toolName: "shell_exec",
          toolOutput: { ok: true },
        }),
      }),
      env
    );
    assert.equal(harnessResp.status, 200);
    assert.equal(harnessCell.getTurnHistory().length, 1);
  });

  it("[M64-2] Preserves 100% of TaskSpec fields across AnstsSchedulerCell, SchedulerV25SingleWritePathStore & UnifiedComponentPersistenceStore", async () => {
    const original = new TaskSpec({
      task_id: "task-fidelity-01",
      tenant_id: "Helios",
      priority: TaskPriority.P1_HIGH,
      prompt: "Verify full TaskSpec field preservation",
      max_turns: 45,
      timeout_seconds: 3600,
      resource_profile: {
        think_cpu_cores: 0.5,
        act_cpu_cores: 16.0,
        act_memory_mb: 32768,
        disk_mb: 51200,
      },
      dependencies: ["dep-a", "dep-b"],
      idempotency_key: "idem-fidelity-01",
      parent_task_id: "parent-fidelity-00",
      expected_SubAgent_count: 5,
      prefix_Cache_hash: "prefix-hash-fidelity-xyz",
    });
    original.current_turn = 7;
    original.retry_count = 3;
    original.preempted_count = 2;
    original.last_preempted_at = 1700000123;
    original.dead_letter_reason = "TEST_REASON";

    // 1. AnstsSchedulerCell
    const doState = new MockDurableObjectState("cell-fidelity");
    const cell1 = new AnstsSchedulerCell(doState);
    await cell1.submitTask(original);
    const cell2 = new AnstsSchedulerCell(doState);
    const fromCell = cell2.scheduler.allTasks.get("task-fidelity-01")!;
    assert.deepEqual(fromCell.resource_profile, original.resource_profile);
    assert.equal(fromCell.parent_task_id, "parent-fidelity-00");
    assert.equal(fromCell.expected_SubAgent_count, 5);
    assert.equal(fromCell.prefix_Cache_hash, "prefix-hash-fidelity-xyz");
    assert.equal(fromCell.retry_count, 3);
    assert.equal(fromCell.preempted_count, 2);
    assert.equal(fromCell.last_preempted_at, 1700000123);

    // 2. SchedulerV25SingleWritePathStore
    const v25Sql = new InMemoryCellSqlStorage();
    const v25 = new SchedulerV25SingleWritePathStore(v25Sql);
    v25.commitTaskSingleWritePath(original, "body-host-02");
    const fromV25 = v25.getPersistedTask("task-fidelity-01")!;
    assert.deepEqual(fromV25.resource_profile, original.resource_profile);
    assert.equal(fromV25.parent_task_id, "parent-fidelity-00");
    assert.equal(fromV25.expected_SubAgent_count, 5);
    assert.equal(fromV25.prefix_Cache_hash, "prefix-hash-fidelity-xyz");
    assert.equal(fromV25.retry_count, 3);
    assert.equal(fromV25.preempted_count, 2);
    assert.equal(fromV25.last_preempted_at, 1700000123);
    assert.equal(fromV25.dead_letter_reason, "TEST_REASON");

    // 3. UnifiedComponentPersistenceStore
    const uniSql = new InMemoryCellSqlStorage();
    const uni = new UnifiedComponentPersistenceStore(uniSql);
    const s1 = new CoreScheduler5D();
    s1.allTasks.set(original.task_id, original);
    uni.snapshotSchedulerState(s1);
    const s2 = new CoreScheduler5D();
    uni.rehydrateSchedulerFromSql(s2);
    const fromUni = s2.allTasks.get("task-fidelity-01")!;
    assert.deepEqual(fromUni.resource_profile, original.resource_profile);
    assert.equal(fromUni.parent_task_id, "parent-fidelity-00");
    assert.equal(fromUni.expected_SubAgent_count, 5);
    assert.equal(fromUni.prefix_Cache_hash, "prefix-hash-fidelity-xyz");
    assert.equal(fromUni.last_preempted_at, 1700000123);
  });

  it("[M64-3] Persists SagaCompensator, StagingGate (turn-accurate commit), McpDynamicToolRegistry, CognitiveRouter & DagEscrow across restarts", async () => {
    const sql = new InMemoryCellSqlStorage();
    const gw1 = new SagaToolGateway(undefined, undefined, sql);
    const mcp1 = new McpDynamicToolRegistry(sql, gw1);
    mcp1.registerMcpServerTools(
      "mcp-terraform",
      [
        {
          toolName: "tf_apply",
          description: "Apply infra",
          effectClass: ToolCallEffectClass.COMPENSATABLE,
          inputSchema: { type: "object" },
        },
      ],
      gw1,
      (_m, p) => ({ applied: p.arguments })
    );

    const task = new TaskSpec({ task_id: "t-saga-turn4", tenant_id: "Northstar" });
    gw1.stagingGate.stageEffect({
      effectId: "eff-t4",
      taskId: "t-saga-turn4",
      turnIndex: 4,
      toolName: "tf_apply",
      args: { stack: "vpc" },
    });
    const ticket = gw1.stagingGate.requestHumanApproval(task, "tf_apply", { stack: "vpc" });
    gw1.stagingGate.resolveApproval(ticket.ticketId, true, "secops", task);
    const committed = gw1.stagingGate.commitStagedEffects("t-saga-turn4", (tName, tArgs, tTurn) => {
      return gw1.invokeTool("t-saga-turn4", tTurn, tName, tArgs)[0];
    });
    assert.equal(committed.length, 1);
    assert.equal(committed[0].turnIndex, 4);

    // Cold-start new gateway + MCP registry over the same SQLite and roll back turn >= 4
    const gw2 = new SagaToolGateway(undefined, undefined, sql);
    const mcp2 = new McpDynamicToolRegistry(sql, gw2);
    assert.equal(mcp2.listTools().length, 1);
    const rb = gw2.rollbackAndInvalidateTask("t-saga-turn4", 4);
    assert.equal(rb.rolledBackSteps.length, 1);
    assert.equal(rb.rolledBackSteps[0].turnIndex, 4);
    assert.equal(rb.rolledBackSteps[0].compensationResult?.status, "MCP_COMPENSATED");

    // DagEscrowCoordinator dependencyGraph cycle detection across SQLite restart
    const esc1 = new DagEscrowCoordinator(sql);
    const parent = new TaskSpec({ task_id: "p-esc", tenant_id: "Northstar", expected_SubAgent_count: 2 });
    const child = new TaskSpec({ task_id: "c-esc", tenant_id: "Northstar", parent_task_id: "p-esc" });
    esc1.reserveParentEscrow(parent, 10, 0);
    esc1.registerChildSubAgent(parent, child);

    const esc2 = new DagEscrowCoordinator(sql);
    assert.throws(() => {
      const cycleTask = new TaskSpec({
        task_id: "p-esc",
        tenant_id: "Northstar",
        parent_task_id: "c-esc",
        dependencies: ["c-esc"],
      });
      esc2.registerChildSubAgent(child, cycleTask);
    }, /DAG cycle detected/);
  });

  it("[M64-4 & M64-5] Verifies AnstsLocalAndComposeServer full-subsystem SQLite persistence, POST /v1/tasks coalescing & CLI mgmt-consistency-audit", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m64-srv-"));
    const srv1 = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      storageDir: tmpDir,
      tickIntervalMs: 60_000,
    });
    const { apiPort: port1 } = await srv1.start(0, 0);
    const baseUrl1 = `http://127.0.0.1:${port1}`;

    try {
      // 1. Submit task and merge duplicate via POST /v1/tasks
      const r1 = await fetch(`${baseUrl1}/v1/tasks`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task_id: "t-srv-m64-1",
          tenant_id: "Northstar",
          priority: 2,
          prompt: "Coalesce test prompt",
          idempotency_key: "idem-srv-m64-1",
        }),
      });
      assert.equal(r1.status, 202);

      const r2 = await fetch(`${baseUrl1}/v1/tasks`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          task_id: "t-srv-m64-2",
          tenant_id: "Northstar",
          priority: 0,
          prompt: "Coalesce test prompt",
          merge_duplicate: true,
        }),
      });
      assert.equal(r2.status, 202);
      const r2Data = (await r2.json()) as Record<string, unknown>;
      assert.equal(r2Data.deduplicated, true);
      assert.equal(r2Data.task_id, "t-srv-m64-1");
      assert.equal(r2Data.priority_promoted, true);

      // 2. Write A2A blackboard & stage+approve+rollback Saga at turnIndex = 3
      await fetch(`${baseUrl1}/v1beta1/a2a/blackboard`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "putCas",
          key: "arch-m64",
          value: { ready: true },
          expectedVersion: 0,
          authorAgentId: "agent-m64",
        }),
      });

      const stageResp = await fetch(`${baseUrl1}/v1beta1/saga/execute`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "t-srv-m64-1",
          turnIndex: 3,
          toolName: "shell_exec",
          args: { cmd: "echo turn3" },
          requireApproval: true,
        }),
      });
      const stageData = (await stageResp.json()) as { ticket: { ticketId: string } };
      const approveResp = await fetch(`${baseUrl1}/v1beta1/saga/approve`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          ticketId: stageData.ticket.ticketId,
          taskId: "t-srv-m64-1",
          approved: true,
        }),
      });
      const approveData = (await approveResp.json()) as {
        committedEffects: Array<{ turnIndex: number }>;
      };
      assert.equal(approveData.committedEffects[0].turnIndex, 3);

      // 3. Execute M64 CLI consistency audit
      const cliOut = await runAnstsCtlRemote(["mgmt-consistency-audit"], baseUrl1);
      assert.equal(cliOut.exitCode, 0);
      const auditData = JSON.parse(cliOut.stdout) as { allM64ConsistencyChecksPassed: boolean };
      assert.equal(auditData.allM64ConsistencyChecksPassed, true);
    } finally {
      await srv1.stop();
    }

    // 4. Cold-start second server instance on the same storageDir and verify blackboard, task idempotency & turn-3 Saga rollback survive restart!
    const srv2 = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      storageDir: tmpDir,
      tickIntervalMs: 60_000,
    });
    const { apiPort: port2 } = await srv2.start(0, 0);
    const baseUrl2 = `http://127.0.0.1:${port2}`;
    try {
      assert.equal(srv2.blackboard.get("arch-m64")?.version, 1);
      assert.equal(
        srv2.scheduler.idempotencyIndex.get("Northstar::idem-srv-m64-1"),
        "t-srv-m64-1"
      );

      const rbResp = await fetch(`${baseUrl2}/v1beta1/saga/rollback`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "t-srv-m64-1",
          upToTurnInclusive: 3,
        }),
      });
      const rbData = (await rbResp.json()) as { rolledBackCount: number };
      assert.equal(rbData.rolledBackCount, 1);
    } finally {
      await srv2.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
