import { strict as assert } from "assert";
import { test } from "node:test";

import { FenceToken, ToolEffectClass } from "../src/models";
import { MockDurableObjectState } from "../src/cloudflare/env";
import { initializeCellProtocol } from "../src/persistence/schema";
import { taskCoreMigration } from "../src/persistence/task-core";
import { stableTaskOperationId, TaskOperations, taskOperationsMigration } from "../src/persistence/task-operations";
import { LeaseCoordinator } from "../src/lease_coordinator";
import { SagaToolGateway } from "../src/saga_gateway";
import { SagaCompensatorEngine } from "../src/saga/compensator";

function createTaskOperations(taskId: string): { operations: TaskOperations; state: MockDurableObjectState } {
  const state = new MockDurableObjectState(taskId);
  initializeCellProtocol(state.storage.sql, { type: "task", key: taskId }, [taskCoreMigration, taskOperationsMigration]);
  state.storage.sql.exec(
    `INSERT INTO task_execution (
      task_id, tenant_id, region, resource_pool, lifecycle_state, business_epoch, execution_version,
      current_dispatch_id, pending_dispatch_id, current_node_id, node_incarnation, last_turn_seq,
      task_spec_json, execution_policy_json, progress_json, heartbeat_at, suspected_at, suspect_reason, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    taskId, "tenant-7", "default", "default", "RUNNING", 3, 0,
    "dispatch-1", null, "node-1", "incarnation-1", 0,
    "{}", "{}", "{}", null, null, null, Date.now(),
  );
  return { operations: new TaskOperations(state.storage), state };
}

test("external operation identity and digest survive replay and reject changed arguments", async () => {
  const { operations, state } = createTaskOperations("task-operation-identity");
  const operationId = stableTaskOperationId("task-operation-identity", "call-9");
  const request = {
    operationId,
    tenantId: "tenant-7",
    fence: new FenceToken(3, 1, "task-operation-identity"),
    toolName: "billing/refund",
    effectClass: ToolEffectClass.IDEMPOTENT_WRITE,
    args: { invoiceId: "inv-1", amount: 1250 },
  };

  const first = await operations.prepareOperation(request);
  state.storage.sql.exec("UPDATE task_execution SET business_epoch = ? WHERE task_id = ?", 4, "task-operation-identity");
  const replay = await operations.prepareOperation({
    ...request,
    fence: new FenceToken(4, 1, "task-operation-identity"),
    args: { amount: 1250, invoiceId: "inv-1" },
  });

  assert.equal(first.operationId, operationId);
  assert.equal(first.argumentDigest, replay.argumentDigest);
  assert.equal(replay.status, "READY");
  assert.equal(state.durableSyncCalls, 2);
  assert.equal(stableTaskOperationId("task-operation-identity", "call-9"), operationId);
  await assert.rejects(
    operations.prepareOperation({ ...request, args: { invoiceId: "inv-1", amount: 9999 } }),
    { code: "TASK_OPERATION_CONFLICT" },
  );
});

test("durable intent precedes provider execution and unresolved effects are not repeated", async () => {
  const { operations, state } = createTaskOperations("task-operation-unknown");
  const request = {
    operationId: stableTaskOperationId("task-operation-unknown", "call-unknown"),
    tenantId: "tenant-7",
    fence: new FenceToken(3, 1, "task-operation-unknown"),
    toolName: "payments/charge",
    effectClass: ToolEffectClass.SAGA_COMPENSATABLE,
    args: { invoiceId: "inv-2", cents: 5000 },
  };
  let effects = 0;
  const provider = {
    supportsIdempotency: false,
    async execute() {
      effects += 1;
      const saved = operations.getOperation(request.operationId);
      assert.equal(saved?.status, "RUNNING");
      assert.ok(state.durableSyncCalls >= 2);
      throw new Error("provider disconnected after accepting request");
    },
  };

  const first = await operations.executeOperation(request, provider);
  const recovery = await operations.executeOperation(request, provider);

  assert.equal(first.status, "OUTCOME_UNKNOWN");
  assert.equal(recovery.status, "OUTCOME_UNKNOWN");
  assert.equal(effects, 1);
  assert.equal(recovery.operation.attempts.length, 1);
});

test("provider effects stay closed when TaskCell cannot confirm durable storage", async () => {
  const { operations, state } = createTaskOperations("task-operation-no-proof");
  state.storage.sync = undefined;
  const request = {
    operationId: stableTaskOperationId("task-operation-no-proof", "call-no-proof"),
    tenantId: "tenant-7",
    fence: new FenceToken(3, 1, "task-operation-no-proof"),
    toolName: "payments/capture",
    effectClass: ToolEffectClass.IDEMPOTENT_WRITE,
    args: { paymentId: "pay-1" },
  };
  let effects = 0;
  await assert.rejects(operations.executeOperation(request, {
    supportsIdempotency: true,
    async execute() { effects += 1; return "captured"; },
  }), { code: "CELL_DURABILITY_UNAVAILABLE" });
  assert.equal(effects, 0);
});

test("stale execution epochs cannot authorize a new external operation", async () => {
  const { operations } = createTaskOperations("task-operation-stale-epoch");
  const request = {
    operationId: stableTaskOperationId("task-operation-stale-epoch", "call-stale"),
    tenantId: "tenant-7",
    fence: new FenceToken(2, 1, "task-operation-stale-epoch"),
    toolName: "payments/refund",
    effectClass: ToolEffectClass.IDEMPOTENT_WRITE,
    args: { paymentId: "pay-2" },
  };
  let effects = 0;
  await assert.rejects(operations.executeOperation(request, {
    supportsIdempotency: true,
    async execute() { effects += 1; return "refunded"; },
  }), { code: "STALE_EXECUTION_EPOCH" });
  assert.equal(effects, 0);
});

test("suspended tasks block new external effects but can persist an in-flight outcome", async () => {
  const { operations, state } = createTaskOperations("task-operation-suspended");
  const fence = new FenceToken(3, 1, "task-operation-suspended");
  const blockedRequest = {
    operationId: stableTaskOperationId("task-operation-suspended", "call-suspended"),
    tenantId: "tenant-7",
    fence,
    toolName: "payments/capture",
    effectClass: ToolEffectClass.IDEMPOTENT_WRITE,
    args: { paymentId: "pay-suspended" },
  };
  await operations.prepareOperation(blockedRequest);
  state.storage.sql.exec("UPDATE task_execution SET lifecycle_state = ? WHERE task_id = ?", "SUSPENDED", "task-operation-suspended");
  let effects = 0;
  await assert.rejects(operations.executeOperation(blockedRequest, {
    supportsIdempotency: true,
    async execute() { effects += 1; return "captured"; },
  }), { code: "TASK_NOT_EXECUTING" });
  assert.equal(effects, 0);

  state.storage.sql.exec("UPDATE task_execution SET lifecycle_state = ? WHERE task_id = ?", "RUNNING", "task-operation-suspended");
  const inFlightRequest = { ...blockedRequest, operationId: stableTaskOperationId("task-operation-suspended", "call-in-flight") };
  await operations.prepareOperation(inFlightRequest);
  const attempt = await operations.beginAttempt({ operationId: inFlightRequest.operationId, attemptId: "attempt-before-suspend", fence });
  assert.equal(attempt.status, "RUNNING");
  state.storage.sql.exec("UPDATE task_execution SET lifecycle_state = ? WHERE task_id = ?", "SUSPENDED", "task-operation-suspended");
  const completed = await operations.recordOperationOutcome({
    operationId: inFlightRequest.operationId, attemptId: attempt.attemptId, fence,
    outcome: "SUCCEEDED", result: { paymentId: "pay-suspended", captured: true },
  });
  assert.equal(completed.status, "SUCCEEDED");
});

test("a provider lookup resolves an unknown result without repeating the external effect", async () => {
  const { operations } = createTaskOperations("task-operation-reconcile");
  const request = {
    operationId: stableTaskOperationId("task-operation-reconcile", "call-reconcile"),
    tenantId: "tenant-7",
    fence: new FenceToken(3, 1, "task-operation-reconcile"),
    toolName: "payments/charge",
    effectClass: ToolEffectClass.IDEMPOTENT_WRITE,
    args: { invoiceId: "inv-3", cents: 8400 },
  };
  const first = await operations.executeOperation(request, {
    supportsIdempotency: false,
    async execute() { throw new Error("response lost"); },
  });
  assert.equal(first.status, "OUTCOME_UNKNOWN");

  let repeatedEffects = 0;
  const recovered = await operations.executeOperation(request, {
    supportsIdempotency: false,
    async lookup() { return { status: "FOUND", result: { chargeId: "ch-77" } }; },
    async execute() { repeatedEffects += 1; throw new Error("must not repeat"); },
  });
  assert.equal(recovered.status, "SUCCEEDED");
  assert.deepEqual(recovered.result, { chargeId: "ch-77" });
  assert.equal(repeatedEffects, 0);
  assert.equal(recovered.operation.attempts[0].outcomeSource, "PROVIDER_LOOKUP");
});

test("provider idempotency recovery reuses the logical key across attempts", async () => {
  const { operations } = createTaskOperations("task-operation-idempotent-retry");
  const request = {
    operationId: stableTaskOperationId("task-operation-idempotent-retry", "call-idempotent-retry"),
    tenantId: "tenant-7",
    fence: new FenceToken(3, 1, "task-operation-idempotent-retry"),
    toolName: "storage/write",
    effectClass: ToolEffectClass.IDEMPOTENT_WRITE,
    args: { key: "record-1", value: "v1" },
  };
  await operations.executeOperation(request, {
    supportsIdempotency: false,
    async execute() { throw new Error("response was lost"); },
  });
  const keys: string[] = [];
  const recovered = await operations.executeOperation(request, {
    supportsIdempotency: true,
    async execute({ idempotencyKey }) { keys.push(idempotencyKey); return "stored"; },
  });
  assert.equal(recovered.status, "SUCCEEDED");
  assert.equal(recovered.operation.attempts.length, 2);
  assert.deepEqual(keys, [request.operationId]);
});

test("parallel retries do not start a second provider effect while the first attempt is in flight", async () => {
  const { operations } = createTaskOperations("task-operation-concurrent");
  const request = {
    operationId: stableTaskOperationId("task-operation-concurrent", "call-concurrent"),
    tenantId: "tenant-7",
    fence: new FenceToken(3, 1, "task-operation-concurrent"),
    toolName: "payments/transfer",
    effectClass: ToolEffectClass.SAGA_COMPENSATABLE,
    args: { from: "a", to: "b", cents: 100 },
  };
  let started!: () => void;
  const effectStarted = new Promise<void>((resolve) => { started = resolve; });
  let finish!: (value: string) => void;
  const effect = new Promise<string>((resolve) => { finish = resolve; });
  let effects = 0;
  const provider = {
    supportsIdempotency: false,
    async execute() { effects += 1; started(); return effect; },
  };
  const firstPromise = operations.executeOperation(request, provider);
  await effectStarted;
  const duplicate = await operations.executeOperation(request, provider);
  assert.equal(duplicate.status, "OUTCOME_UNKNOWN");
  assert.equal(effects, 1);
  finish("transferred");
  const first = await firstPromise;
  assert.equal(first.status, "SUCCEEDED");
  assert.deepEqual(first.result, "transferred");
});

test("staged approval is bound to the argument digest and exact scope", async () => {
  const { operations } = createTaskOperations("task-operation-approval");
  const request = {
    operationId: stableTaskOperationId("task-operation-approval", "call-approval"),
    tenantId: "tenant-7",
    fence: new FenceToken(3, 1, "task-operation-approval"),
    toolName: "deploy/promote",
    effectClass: ToolEffectClass.STAGED_COMMIT,
    args: { environment: "production", release: "v9" },
    approval: { scope: { environment: "production", action: "promote" } },
  };
  const staged = await operations.prepareOperation(request);
  assert.equal(staged.status, "APPROVAL_REQUIRED");
  let effects = 0;
  const provider = { supportsIdempotency: true, async execute() { effects += 1; return { deployed: true }; } };
  assert.equal((await operations.executeOperation(request, provider)).status, "APPROVAL_REQUIRED");
  await assert.rejects(operations.approveOperation({
    approvalId: "approval-wrong-digest", operationId: request.operationId,
    argumentDigest: "changed", scope: request.approval.scope, approvedBy: "admin-1", expiresAt: Date.now() + 60000,
  }), { code: "TASK_OPERATION_APPROVAL_DIGEST_MISMATCH" });
  await assert.rejects(operations.approveOperation({
    approvalId: "approval-wrong-scope", operationId: request.operationId,
    argumentDigest: staged.argumentDigest, scope: { environment: "staging", action: "promote" },
    approvedBy: "admin-1", expiresAt: Date.now() + 60000,
  }), { code: "TASK_OPERATION_APPROVAL_SCOPE_MISMATCH" });

  const approved = await operations.approveOperation({
    approvalId: "approval-prod-v9", operationId: request.operationId,
    argumentDigest: staged.argumentDigest, scope: request.approval.scope,
    approvedBy: "admin-1", expiresAt: Date.now() + 60000,
  });
  const result = await operations.executeOperation(request, provider);
  assert.equal(approved.status, "READY");
  assert.equal(result.status, "SUCCEEDED");
  assert.equal(effects, 1);
});

test("concurrent approval commands install one bound approval and same-ID replay is idempotent", async () => {
  const { operations } = createTaskOperations("task-operation-approval-race");
  const request = {
    operationId: stableTaskOperationId("task-operation-approval-race", "call-approval-race"),
    tenantId: "tenant-7",
    fence: new FenceToken(3, 1, "task-operation-approval-race"),
    toolName: "deploy/promote",
    effectClass: ToolEffectClass.STAGED_COMMIT,
    args: { release: "v10" },
    approval: { scope: { environment: "production" } },
  };
  const staged = await operations.prepareOperation(request);
  const expiresAt = Date.now() + 60000;
  const approvals = await Promise.allSettled(["approval-a", "approval-b"].map((approvalId) => operations.approveOperation({
    approvalId, operationId: request.operationId, argumentDigest: staged.argumentDigest,
    scope: request.approval.scope, approvedBy: "admin-2", expiresAt,
  })));
  assert.equal(approvals.filter((approval) => approval.status === "fulfilled").length, 1);
  assert.equal(approvals.filter((approval) => approval.status === "rejected").length, 1);
  const saved = operations.getOperation(request.operationId)!;
  assert.ok(["approval-a", "approval-b"].includes(saved.approval!.approvalId));
  assert.equal((await operations.approveOperation({
    approvalId: saved.approval!.approvalId, operationId: request.operationId,
    argumentDigest: staged.argumentDigest, scope: request.approval.scope,
    approvedBy: "admin-2", expiresAt,
  })).approval?.approvalId, saved.approval!.approvalId);
});

test("irreversible Race branch effects stay blocked until one durable winner is selected", async () => {
  const { operations } = createTaskOperations("task-operation-race");
  const fence = new FenceToken(3, 1, "task-operation-race");
  const branchRequest = (branchId: string) => ({
    operationId: stableTaskOperationId("task-operation-race", `call-${branchId}`),
    tenantId: "tenant-7",
    fence,
    toolName: "payments/submit",
    effectClass: ToolEffectClass.IDEMPOTENT_WRITE,
    args: { branchId },
    race: { raceId: "race-4", branchId },
  });
  const winner = branchRequest("branch-a");
  const loser = branchRequest("branch-b");
  await operations.prepareOperation(winner);
  await operations.prepareOperation(loser);
  let effects = 0;
  const provider = { supportsIdempotency: true, async execute() { effects += 1; return "submitted"; } };

  assert.equal((await operations.executeOperation(winner, provider)).status, "RACE_BLOCKED");
  const winnerEvent = {
    eventId: "dag-event-4", decisionId: "dag-decision-4", taskId: "task-operation-race", tenantId: "tenant-7",
    raceId: "race-4", winnerBranchId: "branch-a", aggregateVersion: 4,
  };
  await operations.recordRaceWinner(winnerEvent);
  assert.equal(await operations.recordRaceWinner(winnerEvent), "branch-a");
  assert.equal((await operations.executeOperation(winner, provider)).status, "SUCCEEDED");
  assert.equal((await operations.executeOperation(loser, provider)).status, "RACE_BLOCKED");
  await assert.rejects(operations.recordRaceWinner({ ...winnerEvent, eventId: "dag-event-5", decisionId: "dag-decision-5", winnerBranchId: "branch-b" }), {
    code: "TASK_RACE_WINNER_CONFLICT",
  });
  assert.equal(effects, 1);
});

test("compensation attempts and failures remain queryable after the forward effect", async () => {
  const { operations } = createTaskOperations("task-operation-compensation");
  const fence = new FenceToken(3, 1, "task-operation-compensation");
  const request = {
    operationId: stableTaskOperationId("task-operation-compensation", "call-create"),
    tenantId: "tenant-7",
    fence,
    toolName: "orders/create",
    effectClass: ToolEffectClass.SAGA_COMPENSATABLE,
    args: { orderId: "order-8" },
    compensation: { toolName: "orders/cancel", args: { orderId: "order-8" } },
  };
  const provider = { supportsIdempotency: true, async execute() { return { created: true }; } };
  const forward = await operations.executeOperation(request, provider);
  assert.equal(forward.status, "SUCCEEDED");
  assert.equal(forward.operation.compensation?.status, "PENDING");

  await operations.beginCompensation({ operationId: request.operationId, attemptId: "cancel-attempt-1", fence });
  await operations.recordCompensationOutcome({
    operationId: request.operationId, attemptId: "cancel-attempt-1", fence,
    outcome: "COMPENSATION_FAILED", error: "provider rejected cancellation",
  });
  const failed = operations.listCompensations("task-operation-compensation", 0)[0];
  assert.equal(failed.status, "COMPENSATION_FAILED");
  assert.equal(failed.attempts, 1);
  assert.equal(failed.lastError, "provider rejected cancellation");

  await operations.beginCompensation({ operationId: request.operationId, attemptId: "cancel-attempt-2", fence });
  await operations.recordCompensationOutcome({
    operationId: request.operationId, attemptId: "cancel-attempt-2", fence, outcome: "COMPENSATED",
  });
  assert.equal(operations.getOperation(request.operationId)?.compensation?.status, "COMPENSATED");
  assert.equal(operations.getOperation(request.operationId)?.compensation?.attempts, 2);
});

test("Saga gateway uses the injected TaskCell ledger and compensates in persisted LIFO order", async () => {
  const { operations } = createTaskOperations("task-saga-adapter");
  const fence = new FenceToken(3, 1, "task-saga-adapter");
  const saga = new SagaToolGateway(new LeaseCoordinator(), operations);
  const gatewayResult = await saga.executeDurableTool({
    toolCallId: "call-gateway",
    tenantId: "tenant-7",
    fence,
    toolName: "inventory/reserve",
    effectClass: ToolEffectClass.SAGA_COMPENSATABLE,
    args: { sku: "sku-1", count: 2 },
    compensation: { toolName: "inventory/release", args: { sku: "sku-1", count: 2 } },
    provider: { supportsIdempotency: true, async execute() { return { reservationId: "res-1" }; } },
  });
  assert.equal(gatewayResult.status, "SUCCEEDED");
  assert.equal(gatewayResult.operation.operationId, stableTaskOperationId("task-saga-adapter", "call-gateway"));

  const makeAction = (toolCallId: string, sku: string) => operations.prepareOperation({
    operationId: stableTaskOperationId("task-saga-adapter", toolCallId),
    tenantId: "tenant-7",
    fence,
    toolName: "inventory/reserve",
    effectClass: ToolEffectClass.SAGA_COMPENSATABLE,
    args: { sku },
    compensation: { toolName: `inventory/release/${sku}`, args: { sku } },
  });
  await makeAction("call-next-1", "sku-2");
  await makeAction("call-next-2", "sku-3");
  const executorOrder: string[] = [];
  const results = await new SagaCompensatorEngine(3, operations).compensatePersisted(
    "task-saga-adapter", 0, fence,
    async ({ toolName }) => { executorOrder.push(toolName); return true; },
  );
  assert.deepEqual(executorOrder, ["inventory/release/sku-3", "inventory/release/sku-2", "inventory/release"]);
  assert.deepEqual(results.map((result) => result.status), ["COMPENSATED", "COMPENSATED", "COMPENSATED"]);
});
