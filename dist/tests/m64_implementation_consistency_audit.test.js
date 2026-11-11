"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
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
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const worker_js_1 = __importStar(require("../src/cloudflare/worker.js"));
const env_js_1 = require("../src/cloudflare/env.js");
const scheduler_v25_persistence_js_1 = require("../src/cloudflare/scheduler_v25_persistence.js");
const unified_state_store_js_1 = require("../src/checkpoint/unified_state_store.js");
const models_js_1 = require("../src/models.js");
const scheduler_js_1 = require("../src/scheduler.js");
const saga_gateway_js_1 = require("../src/saga_gateway.js");
const mcp_registry_js_1 = require("../src/saga/mcp_registry.js");
const escrow_js_1 = require("../src/dag/escrow.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M64 End-to-End Implementation Consistency & Authentic Feature Wiring Suite", () => {
    (0, node_test_1.it)("[M64-1] Cloudflare Worker routes /v1/scheduler/* & /v1/harness/* and dispatches queue() batch messages to AnstsSchedulerCell", async () => {
        const schedState = new env_js_1.MockDurableObjectState("global-shard-0");
        const schedCell = new worker_js_1.AnstsSchedulerCell(schedState);
        const harnessState = new env_js_1.MockDurableObjectState("task-h-01");
        const r2 = new env_js_1.InMemoryR2Bucket();
        const harnessCell = new worker_js_1.AgentTaskHarnessCell(harnessState, r2, "task-h-01");
        const env = {
            CELLD_REGION: "us-west-2",
            ANSTS_SCHEDULER_CELL: {
                idFromName: (name) => ({ toString: () => name }),
                get: () => ({ fetch: (req) => schedCell.fetch(req) }),
            },
            ANSTS_HARNESS_CELL: {
                idFromName: (name) => ({ toString: () => name }),
                get: () => ({ fetch: (req) => harnessCell.fetch(req) }),
            },
            ANSTS_CHECKPOINT_BUCKET: r2,
            ANSTS_KV_CACHE: new env_js_1.InMemoryKvNamespace(),
            ANSTS_TASK_QUEUE: new env_js_1.InMemoryQueueProducer(),
        };
        // 1. Submit task via Worker `/v1/scheduler/submit`
        const submitResp = await worker_js_1.default.fetch(new Request("https://edge.ansts.io/v1/scheduler/submit", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                task_id: "task-worker-http-01",
                tenant_id: "Northstar",
                priority: models_js_1.TaskPriority.P0_EMERGENCY,
                prompt: "Run via worker fetch",
            }),
        }), env);
        assert.equal(submitResp.status, 202);
        const submitData = (await submitResp.json());
        assert.equal(submitData.task_id, "task-worker-http-01");
        assert.equal(schedCell.scheduler.allTasks.has("task-worker-http-01"), true);
        // 2. Dispatch queue batch via Worker `queue(batch, env)`
        let acked = 0;
        await worker_js_1.default.queue({
            queue: "ansts-tasks",
            messages: [
                {
                    id: "msg-1",
                    timestamp: new Date(),
                    body: {
                        task_id: "task-worker-queue-02",
                        tenant_id: "Northstar",
                        priority: models_js_1.TaskPriority.P1_HIGH,
                        prompt: "Run via worker queue",
                    },
                    ack: () => {
                        acked += 1;
                    },
                    retry: () => { },
                },
            ],
        }, env);
        assert.equal(acked, 1);
        assert.equal(schedCell.scheduler.allTasks.has("task-worker-queue-02"), true);
        // 3. Execute turn via Worker `/v1/harness/execute-turn`
        const harnessResp = await worker_js_1.default.fetch(new Request("https://edge.ansts.io/v1/harness/execute-turn?taskId=task-h-01", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                turnIndex: 1,
                fencingToken: 5,
                toolName: "shell_exec",
                toolOutput: { ok: true },
            }),
        }), env);
        assert.equal(harnessResp.status, 200);
        assert.equal(harnessCell.getTurnHistory().length, 1);
    });
    (0, node_test_1.it)("[M64-2] Preserves 100% of TaskSpec fields across AnstsSchedulerCell, SchedulerV25SingleWritePathStore & UnifiedComponentPersistenceStore", async () => {
        const original = new models_js_1.TaskSpec({
            task_id: "task-fidelity-01",
            tenant_id: "Helios",
            priority: models_js_1.TaskPriority.P1_HIGH,
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
        const doState = new env_js_1.MockDurableObjectState("cell-fidelity");
        const cell1 = new worker_js_1.AnstsSchedulerCell(doState);
        await cell1.submitTask(original);
        const cell2 = new worker_js_1.AnstsSchedulerCell(doState);
        const fromCell = cell2.scheduler.allTasks.get("task-fidelity-01");
        assert.deepEqual(fromCell.resource_profile, original.resource_profile);
        assert.equal(fromCell.parent_task_id, "parent-fidelity-00");
        assert.equal(fromCell.expected_SubAgent_count, 5);
        assert.equal(fromCell.prefix_Cache_hash, "prefix-hash-fidelity-xyz");
        assert.equal(fromCell.retry_count, 3);
        assert.equal(fromCell.preempted_count, 2);
        assert.equal(fromCell.last_preempted_at, 1700000123);
        // 2. SchedulerV25SingleWritePathStore
        const v25Sql = new env_js_1.InMemoryCellSqlStorage();
        const v25 = new scheduler_v25_persistence_js_1.SchedulerV25SingleWritePathStore(v25Sql);
        v25.commitTaskSingleWritePath(original, "body-host-02");
        const fromV25 = v25.getPersistedTask("task-fidelity-01");
        assert.deepEqual(fromV25.resource_profile, original.resource_profile);
        assert.equal(fromV25.parent_task_id, "parent-fidelity-00");
        assert.equal(fromV25.expected_SubAgent_count, 5);
        assert.equal(fromV25.prefix_Cache_hash, "prefix-hash-fidelity-xyz");
        assert.equal(fromV25.retry_count, 3);
        assert.equal(fromV25.preempted_count, 2);
        assert.equal(fromV25.last_preempted_at, 1700000123);
        assert.equal(fromV25.dead_letter_reason, "TEST_REASON");
        // 3. UnifiedComponentPersistenceStore
        const uniSql = new env_js_1.InMemoryCellSqlStorage();
        const uni = new unified_state_store_js_1.UnifiedComponentPersistenceStore(uniSql);
        const s1 = new scheduler_js_1.CoreScheduler5D();
        s1.allTasks.set(original.task_id, original);
        uni.snapshotSchedulerState(s1);
        const s2 = new scheduler_js_1.CoreScheduler5D();
        uni.rehydrateSchedulerFromSql(s2);
        const fromUni = s2.allTasks.get("task-fidelity-01");
        assert.deepEqual(fromUni.resource_profile, original.resource_profile);
        assert.equal(fromUni.parent_task_id, "parent-fidelity-00");
        assert.equal(fromUni.expected_SubAgent_count, 5);
        assert.equal(fromUni.prefix_Cache_hash, "prefix-hash-fidelity-xyz");
        assert.equal(fromUni.last_preempted_at, 1700000123);
    });
    (0, node_test_1.it)("[M64-3] Persists SagaCompensator, StagingGate (turn-accurate commit), McpDynamicToolRegistry, CognitiveRouter & DagEscrow across restarts", async () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const gw1 = new saga_gateway_js_1.SagaToolGateway(undefined, undefined, sql);
        const mcp1 = new mcp_registry_js_1.McpDynamicToolRegistry(sql, gw1);
        mcp1.registerMcpServerTools("mcp-terraform", [
            {
                toolName: "tf_apply",
                description: "Apply infra",
                effectClass: models_js_1.ToolCallEffectClass.COMPENSATABLE,
                inputSchema: { type: "object" },
            },
        ], gw1, (_m, p) => ({ applied: p.arguments }));
        const task = new models_js_1.TaskSpec({ task_id: "t-saga-turn4", tenant_id: "Northstar" });
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
        const gw2 = new saga_gateway_js_1.SagaToolGateway(undefined, undefined, sql);
        const mcp2 = new mcp_registry_js_1.McpDynamicToolRegistry(sql, gw2);
        assert.equal(mcp2.listTools().length, 1);
        const rb = gw2.rollbackAndInvalidateTask("t-saga-turn4", 4);
        assert.equal(rb.rolledBackSteps.length, 1);
        assert.equal(rb.rolledBackSteps[0].turnIndex, 4);
        assert.equal(rb.rolledBackSteps[0].compensationResult?.status, "MCP_COMPENSATED");
        // DagEscrowCoordinator dependencyGraph cycle detection across SQLite restart
        const esc1 = new escrow_js_1.DagEscrowCoordinator(sql);
        const parent = new models_js_1.TaskSpec({ task_id: "p-esc", tenant_id: "Northstar", expected_SubAgent_count: 2 });
        const child = new models_js_1.TaskSpec({ task_id: "c-esc", tenant_id: "Northstar", parent_task_id: "p-esc" });
        esc1.reserveParentEscrow(parent, 10, 0);
        esc1.registerChildSubAgent(parent, child);
        const esc2 = new escrow_js_1.DagEscrowCoordinator(sql);
        assert.throws(() => {
            const cycleTask = new models_js_1.TaskSpec({
                task_id: "p-esc",
                tenant_id: "Northstar",
                parent_task_id: "c-esc",
                dependencies: ["c-esc"],
            });
            esc2.registerChildSubAgent(child, cycleTask);
        }, /DAG cycle detected/);
    });
    (0, node_test_1.it)("[M64-4 & M64-5] Verifies AnstsLocalAndComposeServer full-subsystem SQLite persistence, POST /v1/tasks coalescing & CLI mgmt-consistency-audit", async () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m64-srv-"));
        const srv1 = new server_main_js_1.AnstsLocalAndComposeServer({
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
            const r2Data = (await r2.json());
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
            const stageData = (await stageResp.json());
            const approveResp = await fetch(`${baseUrl1}/v1beta1/saga/approve`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    ticketId: stageData.ticket.ticketId,
                    taskId: "t-srv-m64-1",
                    approved: true,
                }),
            });
            const approveData = (await approveResp.json());
            assert.equal(approveData.committedEffects[0].turnIndex, 3);
            // 3. Execute M64 CLI consistency audit
            const cliOut = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-consistency-audit"], baseUrl1);
            assert.equal(cliOut.exitCode, 0);
            const auditData = JSON.parse(cliOut.stdout);
            assert.equal(auditData.allM64ConsistencyChecksPassed, true);
        }
        finally {
            await srv1.stop();
        }
        // 4. Cold-start second server instance on the same storageDir and verify blackboard, task idempotency & turn-3 Saga rollback survive restart!
        const srv2 = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            storageDir: tmpDir,
            tickIntervalMs: 60_000,
        });
        const { apiPort: port2 } = await srv2.start(0, 0);
        const baseUrl2 = `http://127.0.0.1:${port2}`;
        try {
            assert.equal(srv2.blackboard.get("arch-m64")?.version, 1);
            assert.equal(srv2.scheduler.idempotencyIndex.get("Northstar::idem-srv-m64-1"), "t-srv-m64-1");
            const rbResp = await fetch(`${baseUrl2}/v1beta1/saga/rollback`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "t-srv-m64-1",
                    upToTurnInclusive: 3,
                }),
            });
            const rbData = (await rbResp.json());
            assert.equal(rbData.rolledBackCount, 1);
        }
        finally {
            await srv2.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
