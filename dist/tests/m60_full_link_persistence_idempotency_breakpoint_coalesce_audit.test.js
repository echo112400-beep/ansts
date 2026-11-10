"use strict";















var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));

var _env = require("../src/cloudflare/env.js");




var _unified_state_store = require("../src/checkpoint/unified_state_store.js");
var _scheduler_cell = require("../src/cloudflare/scheduler_cell.js");
var _scheduler_v25_persistence = require("../src/cloudflare/scheduler_v25_persistence.js");
var _scheduler = require("../src/scheduler.js");
var _wal_engine = require("../src/checkpoint/wal_engine.js");
var _saga_gateway = require("../src/saga_gateway.js");
var _outbox_webhook = require("../src/telemetry/outbox_webhook.js");
var _sse_stream = require("../src/gateway/sse_stream.js");
var _cloudevents = require("../src/telemetry/cloudevents.js");
var _node_daemon = require("../src/node_daemon.js");
var _server = require("../src/gateway/server.js");
var _backpressure = require("../src/gateway/backpressure.js");
var _models = require("../src/models.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M60 End-to-End Full-Link Persistence, Tenant-Scoped & Deep-Canonical Idempotency,
 * Deterministic Breakpoint Crash-Recovery & Coalescing Duplicate Inspection Suite.
 *
 * Covers:
 * 1. [M60-1] End-to-end full-link SQLite persistence & cold-start rehydration across `UnifiedComponentPersistenceStore`,
 *    `AnstsSchedulerCell`, `TurnWalEngine`, `SagaToolGateway`, and `TransactionalOutboxDispatcher`.
 * 2. [M60-2] Tenant-scoped `idempotency_key` isolation in `CoreScheduler5D` & recursive canonical nested-JSON
 *    hashing in `SagaToolGateway.generateIdempotencyKey`.
 * 3. [M60-3] Idempotent `envelope.id` deduplication in `SseEventStreamGateway.publish` & `idempotencyKey`/`eventId`
 *    deduplication in `CloudEventsBus.emitEvent` preventing double FinOps billing.
 * 4. [M60-4] Deterministic WAL/checkpoint breakpoint recovery (`TurnWalEngine.recoverTaskBreakpoint` &
 *    `NodeDaemon.recoverTaskFromBreakpoint`) with mid-turn uncommitted side-effect rollback & idempotent WAL retry.
 * 5. [M60-5] Mergeable duplicate task inspection (`CoreScheduler5D.inspectAndMergeDuplicateTask`, `ApiGatewayServer`,
 *    `SchedulerV25SingleWritePathStore`) with content-fingerprint in-flight coalescing & priority promotion.
 */(0, _nodeTest.describe)("M60 Full-Link Persistence, Deep Idempotency, Breakpoint Recovery & Duplicate Coalescing Suite", () => {(0, _nodeTest.it)("1. Verifies M60-1 through M60-5 across SQLite persistence, deep idempotency, WAL breakpoint recovery & duplicate coalescing, plus HTTP & CLI endpoints", async () => {const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m60-sql-"));try {// =========================================================================
      // 1. [M60-1] End-to-End Full-Link Disk SQLite Persistence & Rehydration
      // =========================================================================
      const diskSql1 = new _env.DurableDiskCellSqlStorage(tmpDir, "m60-full-link");const store1 = new _unified_state_store.UnifiedComponentPersistenceStore(diskSql1);const sched1 = new _scheduler.CoreScheduler5D();const wal1 = new _wal_engine.TurnWalEngine(diskSql1);const saga1 = new _saga_gateway.SagaToolGateway(undefined, undefined, diskSql1);const outbox1 = new _outbox_webhook.TransactionalOutboxDispatcher("m60-secret", 3, diskSql1);let sideEffectExecCount = 0;saga1.registerTool("provision_db", _models.ToolCallEffectClass.IDEMPOTENT_WRITE, (args) => {sideEffectExecCount += 1;
        return { dbId: "pg-01", count: sideEffectExecCount, args };
      });

      const task1 = new _models.TaskSpec({
        task_id: "task-m60-1",
        tenant_id: "tenant-1",
        priority: _models.TaskPriority.P1_HIGH,
        prompt: "Provision database and run migrations",
        idempotency_key: "idem-provision-db"
      });
      task1.current_turn = 2;
      sched1.submitTask(task1);

      // Merge a duplicate into task1 before snapshot to verify merged metadata persistence
      sched1.submitTask(
      new _models.TaskSpec({
        task_id: "task-m60-1-dup",
        tenant_id: "tenant-1",
        priority: _models.TaskPriority.P0_EMERGENCY,
        prompt: "Provision database and run migrations",
        idempotency_key: "idem-provision-db-2"
      }),
      { mergeDuplicateInFlight: true });

      assert.equal(task1.priority, _models.TaskPriority.P0_EMERGENCY, "Urgent duplicate must promote priority");
      assert.equal(task1.merged_duplicate_count, 1);

      wal1.appendTurnDelta({
        taskId: "task-m60-1",
        turnIndex: 1,
        fencingToken: 1,
        changedFiles: ["/workspace/schema.sql"],
        sqliteWalPages: 3,
        highestSubmittedFencingToken: 1
      });
      wal1.appendTurnDelta({
        taskId: "task-m60-1",
        turnIndex: 2,
        fencingToken: 1,
        changedFiles: ["/workspace/migrations/001.sql"],
        sqliteWalPages: 5,
        highestSubmittedFencingToken: 1
      });

      const [res1, dedup1] = saga1.invokeTool("task-m60-1", 1, "provision_db", {
        spec: { replicas: 3, region: "us-west", tags: [{ k: "env", v: "prod" }] }
      });
      assert.equal(dedup1, false);
      assert.equal(res1.count, 1);

      outbox1.enqueueOutbox("webhook-m60-1", "task-m60-1", "https://hooks.example.com/events", {
        turn: 2,
        status: "checkpointed"
      });

      const snap = store1.snapshotFullLinkState({
        scheduler: sched1,
        walEngine: wal1,
        sagaGateway: saga1,
        outboxDispatcher: outbox1
      });
      assert.equal(snap.tasksSaved, 1);
      assert.equal(snap.walEntriesSaved, 2);
      assert.equal(snap.sagaJournalSaved, 1);
      assert.equal(snap.outboxMessagesSaved, 1);

      // Cold-start from disk SQLite
      const diskSql2 = new _env.DurableDiskCellSqlStorage(tmpDir, "m60-full-link");
      const store2 = new _unified_state_store.UnifiedComponentPersistenceStore(diskSql2);
      const sched2 = new _scheduler.CoreScheduler5D();
      const wal2 = new _wal_engine.TurnWalEngine();
      const saga2 = new _saga_gateway.SagaToolGateway();
      saga2.registerTool("provision_db", _models.ToolCallEffectClass.IDEMPOTENT_WRITE, (args) => {
        sideEffectExecCount += 1;
        return { dbId: "pg-01", count: sideEffectExecCount, args };
      });
      const outbox2 = new _outbox_webhook.TransactionalOutboxDispatcher("m60-secret", 3);

      const rehydrated = store2.rehydrateFullLinkState({
        scheduler: sched2,
        walEngine: wal2,
        sagaGateway: saga2,
        outboxDispatcher: outbox2
      });
      assert.equal(rehydrated.tasksRestored, 1);
      assert.equal(rehydrated.walEntriesRestored, 2);
      assert.equal(rehydrated.sagaJournalRestored, 1);
      assert.equal(rehydrated.outboxMessagesRestored, 1);

      // Verify AnstsSchedulerCell also persists and rehydrates idempotency_key, current_turn & merged metadata
      const doState = new _env.MockDurableObjectState("cell-m60");
      const cell1 = new _scheduler_cell.AnstsSchedulerCell(doState);
      await cell1.submitTask(
      new _models.TaskSpec({
        task_id: "cell-t1",
        tenant_id: "tenant-cell",
        priority: _models.TaskPriority.P2_STANDARD,
        prompt: "Cell durable task",
        idempotency_key: "cell-idem-1"
      }));

      const cell2 = new _scheduler_cell.AnstsSchedulerCell(doState);
      const cellDup = await cell2.submitTask(
      new _models.TaskSpec({
        task_id: "cell-t2",
        tenant_id: "tenant-cell",
        priority: _models.TaskPriority.P1_HIGH,
        prompt: "Cell durable task",
        idempotency_key: "cell-idem-1"
      }));

      assert.equal(cellDup.task_id, "cell-t1", "AnstsSchedulerCell must preserve idempotencyIndex across restart");
      assert.equal(cellDup.priority, _models.TaskPriority.P1_HIGH, "AnstsSchedulerCell must promote priority on duplicate merge");

      // =========================================================================
      // 2. [M60-2] Tenant-Scoped Task Idempotency & Recursive Canonical Tool Hash
      // =========================================================================
      // Retry on sched2 using either the original or merged idempotency_key must deduplicate for tenant-1
      const retryByOrigKey = sched2.submitTask(
      new _models.TaskSpec({
        task_id: "task-m60-retry-a",
        tenant_id: "tenant-1",
        prompt: "Provision database and run migrations",
        idempotency_key: "idem-provision-db"
      }));

      const retryByMergedKey = sched2.submitTask(
      new _models.TaskSpec({
        task_id: "task-m60-retry-b",
        tenant_id: "tenant-1",
        prompt: "Provision database and run migrations",
        idempotency_key: "idem-provision-db-2"
      }));

      assert.equal(retryByOrigKey.task_id, "task-m60-1");
      assert.equal(retryByMergedKey.task_id, "task-m60-1");

      // Different tenant using same idempotency_key must NOT collide
      const otherTenantTask = sched2.submitTask(
      new _models.TaskSpec({
        task_id: "task-m60-tenant2",
        tenant_id: "tenant-2",
        prompt: "Tenant 2 isolated task",
        idempotency_key: "idem-provision-db"
      }));

      assert.equal(otherTenantTask.task_id, "task-m60-tenant2");

      // Re-invoke tool on saga2 with differently ordered nested keys: must hit rehydrated SQLite journal!
      const [res2, dedup2] = saga2.invokeTool("task-m60-1", 1, "provision_db", {
        spec: { tags: [{ v: "prod", k: "env" }], region: "us-west", replicas: 3 }
      });
      assert.equal(dedup2, true, "Nested JSON with reordered keys must hit rehydrated idempotency journal");
      assert.equal(res2.count, 1);
      assert.equal(sideEffectExecCount, 1, "Side-effect handler must not execute a second time");

      // =========================================================================
      // 3. [M60-3] SSE Event-ID Deduplication & CloudEvents Idempotent Billing
      // =========================================================================
      const sse = new _sse_stream.SseEventStreamGateway(10);
      const sub = sse.subscribe("task-m60-1", "sub-m60");
      let receivedFrames = 0;
      sub.stream.on("data", () => {
        receivedFrames += 1;
      });
      const pub1 = sse.publish({
        id: "sse-evt-101",
        event: "turn.delta",
        taskId: "task-m60-1",
        turnIndex: 1,
        timestampMs: 1000,
        data: { step: 1 }
      });
      const pubDup = sse.publish({
        id: "sse-evt-101",
        event: "turn.delta",
        taskId: "task-m60-1",
        turnIndex: 1,
        timestampMs: 1002,
        data: { step: 1 }
      });
      assert.equal(pub1, 1);
      assert.equal(pubDup, 0, "Duplicate SSE event ID must be suppressed");
      assert.equal(sse.getTaskHistory("task-m60-1").length, 1);
      assert.equal(receivedFrames, 1);

      const ceBus = new _cloudevents.CloudEventsBus(50);
      const ce1 = ceBus.emitEvent({
        type: "ansts.turn.cost",
        subject: "task-m60-1",
        tenantId: "tenant-1",
        idempotencyKey: "cost-task-m60-1-t1",
        llmCostUsd: 0.75,
        sandboxCpuCoreSeconds: 20
      });
      const ce2 = ceBus.emitEvent({
        type: "ansts.turn.cost",
        subject: "task-m60-1",
        tenantId: "tenant-1",
        idempotencyKey: "cost-task-m60-1-t1",
        llmCostUsd: 0.75,
        sandboxCpuCoreSeconds: 20
      });
      assert.equal(ce1.id, ce2.id);
      assert.equal(ceBus.getTenantReport("tenant-1")?.totalLlmCostUsd, 0.75, "Must not double-bill on retry");

      // =========================================================================
      // 4. [M60-4] Deterministic WAL/Checkpoint Breakpoint Recovery & Mid-Turn Rollback
      // =========================================================================
      // Idempotent WAL append retry with identical payload must return existing entry
      const idempotentWalRetry = wal2.appendTurnDelta({
        taskId: "task-m60-1",
        turnIndex: 2,
        fencingToken: 1,
        changedFiles: ["/workspace/migrations/001.sql"],
        sqliteWalPages: 5,
        highestSubmittedFencingToken: 1,
        enforceTurnMonotonicity: true,
        allowIdempotentRetry: true
      });
      assert.equal(idempotentWalRetry.turnIndex, 2);
      assert.equal(wal2.getTaskWalEntries("task-m60-1").length, 2);

      // Simulate mid-turn 3 uncommitted tool side effect before crash
      let turn3Compensated = false;
      saga2.registerTool(
      "mutate_route_table",
      _models.ToolCallEffectClass.COMPENSATABLE,
      () => ({ routeUpdated: true }),
      () => {
        turn3Compensated = true;
        return { routeReverted: true };
      });

      saga2.invokeTool("task-m60-1", 3, "mutate_route_table", { cidr: "10.0.0.0/16" });

      // Acquire lease on new NodeDaemon and recover from WAL breakpoint (Turn 2)
      const daemonRecovered = new _node_daemon.NodeDaemon("node-recovery-02");
      sched2.registerOrUpdateNode(daemonRecovered.emitHeartbeat(0));
      const rehydratedTask = sched2.allTasks.get("task-m60-1");
      const lease = sched2.leaseCoordinator.acquireLease(rehydratedTask.task_id, daemonRecovered.nodeId);
      rehydratedTask.assigned_node_id = daemonRecovered.nodeId;
      rehydratedTask.lease_fencing_token = lease.fencing_token;

      const bpReport = daemonRecovered.recoverTaskFromBreakpoint(
      sched2,
      rehydratedTask,
      wal2,
      saga2);

      assert.equal(bpReport.restoredTurnIndex, 2);
      assert.equal(bpReport.rehydratedCheckpoints, 2);
      assert.equal(bpReport.nextTurnToExecute, 3);
      assert.equal(turn3Compensated, true, "Uncommitted Turn 3 side-effect must be rolled back on breakpoint recovery");

      // Next step on daemonRecovered must cleanly execute Turn 3
      const step3Out = daemonRecovered.executeAgentStep(
      sched2,
      saga2,
      rehydratedTask,
      "mutate_route_table",
      { cidr: "10.0.0.0/16" });

      assert.equal(step3Out.turn, 3);
      assert.equal(step3Out.deduplicated, false, "Rolled-back Turn 3 must re-execute cleanly after recovery");

      // =========================================================================
      // 5. [M60-5] ApiGatewayServer & SchedulerV25SingleWritePathStore Duplicate Coalescing
      // =========================================================================
      const gwSched = new _scheduler.CoreScheduler5D();
      const gw = new _server.ApiGatewayServer(gwSched, new _backpressure.IngressBackpressureController());
      const subA = gw.handleSubmitRequest({
        task_id: "gw-coal-1",
        tenant_id: "tenant-gw",
        priority: _models.TaskPriority.P3_BATCH,
        prompt: "Run nightly security audit on repo",
        dependencies: ["prep-2", "prep-1"],
        merge_duplicate: true
      });
      assert.equal(subA.statusCode, 202);
      assert.equal(subA.body.deduplicated, false);

      // Even under 99% cluster PSI pressure, an urgent duplicate merging into an existing task is coalesced and promotes priority!
      const subB = gw.handleSubmitRequest(
      {
        task_id: "gw-coal-2",
        tenant_id: "tenant-gw",
        priority: _models.TaskPriority.P0_EMERGENCY,
        prompt: "  Run nightly security audit on repo ",
        dependencies: ["prep-1", "prep-2"],
        idempotency_key: "gw-urgent-key",
        merge_duplicate: true
      },
      99);

      assert.equal(subB.statusCode, 202);
      assert.equal(subB.body.deduplicated, true);
      assert.equal(subB.body.task_id, "gw-coal-1");
      assert.equal(subB.body.merged_by, "CONTENT_FINGERPRINT");
      assert.equal(subB.body.priority_promoted, true);
      assert.equal(gwSched.allTasks.get("gw-coal-1")?.priority, _models.TaskPriority.P0_EMERGENCY);

      const v25Store = new _scheduler_v25_persistence.SchedulerV25SingleWritePathStore(new _env.InMemoryCellSqlStorage());
      const v25First = v25Store.commitTaskSingleWritePath(
      new _models.TaskSpec({
        task_id: "v25-1",
        tenant_id: "tenant-v25",
        priority: _models.TaskPriority.P2_STANDARD,
        prompt: "Build release artifact"
      }),
      "node-01",
      { mergeDuplicateInFlight: true });

      assert.equal(v25First.deduplicated, false);
      const v25Merged = v25Store.commitTaskSingleWritePath(
      new _models.TaskSpec({
        task_id: "v25-2",
        tenant_id: "tenant-v25",
        priority: _models.TaskPriority.P0_EMERGENCY,
        prompt: "Build release artifact"
      }),
      "node-01",
      { mergeDuplicateInFlight: true });

      assert.equal(v25Merged.deduplicated, true);
      assert.equal(v25Merged.mergedBy, "CONTENT_FINGERPRINT");
      assert.equal(v25Merged.priorityPromoted, true);
      assert.equal(v25Merged.task.task_id, "v25-1");
      assert.equal(v25Merged.task.priority, _models.TaskPriority.P0_EMERGENCY);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }

    // =========================================================================
    // 6. [M60-6] End-to-End HTTP & CLI Verification
    // =========================================================================
    const server = new _server_main.AnstsLocalAndComposeServer({ nodeId: "gw-m60-test" });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-persistence-idempotency-audit"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);
      assert.equal(body.allM60RisksEliminated, true);
      assert.equal(body.fullLinkPersistence1.fullLinkPersistenceVerified, true);
      assert.equal(body.tenantAndDeepIdempotency2.tenantAndDeepIdempotencyVerified, true);
      assert.equal(body.sseAndCloudEventsIdempotency3.sseAndCloudEventsIdempotencyVerified, true);
      assert.equal(body.breakpointRecovery4.breakpointRecoveryVerified, true);
      assert.equal(body.duplicateCoalescing5.duplicateCoalescingVerified, true);
    } finally {
      await server.stop();
    }
  });
});