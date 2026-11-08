"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M1-M6 Core End-to-End Integration Suite (7 tests).
 */









(0, _nodeTest.describe)("M1-M6 Core Integration Suite", () => {
  (0, _nodeTest.it)("1. 5D Scheduler places tasks onto optimal warm-pool node and increments fencing token", () => {
    const scheduler = new _index.CoreScheduler5D(15.0);
    const daemonA = new _index.NodeDaemon("node-a", 32, 65536, 8);
    const daemonB = new _index.NodeDaemon("node-b", 32, 65536, 0);
    scheduler.registerOrUpdateNode(daemonA.emitHeartbeat(0));
    scheduler.registerOrUpdateNode(daemonB.emitHeartbeat(4));

    const task = new _index.TaskSpec({
      task_id: "task-5d-001",
      tenant_id: "tenant-alpha",
      priority: _index.TaskPriority.P1_HIGH,
      prompt: "Analyze repository"
    });
    scheduler.submitTask(task);
    const placements = scheduler.scheduleTick();
    assert.equal(placements.length, 1);
    assert.equal(placements[0][0], "task-5d-001");
    assert.equal(placements[0][1], "node-a");
    assert.equal(task.state, _index.TaskState.LEASED);
    assert.ok(task.lease_fencing_token >= 1);
  });

  (0, _nodeTest.it)("2. Monotonic Fencing Token prevents split-brain zombie NodeDaemon checkpoint write", () => {
    const scheduler = new _index.CoreScheduler5D(15.0);
    const daemon1 = new _index.NodeDaemon("node-1");
    const daemon2 = new _index.NodeDaemon("node-2");
    scheduler.registerOrUpdateNode(daemon1.emitHeartbeat());
    scheduler.registerOrUpdateNode(daemon2.emitHeartbeat());

    const task = new _index.TaskSpec({ task_id: "task-fence-1", tenant_id: "t1", priority: _index.TaskPriority.P1_HIGH });
    scheduler.submitTask(task);
    scheduler.scheduleTick();
    const oldToken = task.lease_fencing_token;

    const newLease = scheduler.leaseCoordinator.acquireLease(task.task_id, "node-2");
    assert.ok(newLease.fencing_token > oldToken);

    task.lease_fencing_token = oldToken;
    assert.throws(() => {
      daemon1.writeTurnCheckpoint(scheduler, task, 1, "hash-1", "s3://zombie/1");
    }, /Fencing token .* rejected/);
  });

  (0, _nodeTest.it)("3. DRF Multi-Tenant Fairness schedules lower-share tenant ahead of dominant hog tenant", () => {
    const scheduler = new _index.CoreScheduler5D(15.0);
    const hogTask = new _index.TaskSpec({ task_id: "hog-1", tenant_id: "tenant-hog", priority: _index.TaskPriority.P2_STANDARD });
    const fairTask = new _index.TaskSpec({ task_id: "fair-1", tenant_id: "tenant-fair", priority: _index.TaskPriority.P2_STANDARD });
    scheduler.queueManager.tenantActiveCount.set("tenant-hog", 15);
    scheduler.queueManager.tenantActiveCount.set("tenant-fair", 0);

    scheduler.queueManager.enqueue(hogTask);
    scheduler.queueManager.enqueue(fairTask);

    const first = scheduler.queueManager.popNext();
    assert.equal(first?.task_id, "fair-1");
  });

  (0, _nodeTest.it)("4. Aging Starvation Prevention promotes long-waiting P3_BATCH task above newly arrived P2_STANDARD", () => {
    const scheduler = new _index.CoreScheduler5D(15.0);
    const agedP3 = new _index.TaskSpec({ task_id: "aged-p3", tenant_id: "t1", priority: _index.TaskPriority.P3_BATCH });
    agedP3.created_at = Date.now() / 1000 - 3600;
    const freshP2 = new _index.TaskSpec({ task_id: "fresh-p2", tenant_id: "t2", priority: _index.TaskPriority.P2_STANDARD });
    freshP2.created_at = Date.now() / 1000;

    scheduler.queueManager.enqueue(agedP3);
    scheduler.queueManager.enqueue(freshP2);

    const popped = scheduler.queueManager.popNext();
    assert.equal(popped?.task_id, "aged-p3");
  });

  (0, _nodeTest.it)("5. DAG Dependency Resolution unlocks child task only after parent completes", () => {
    const scheduler = new _index.CoreScheduler5D(15.0);
    const daemon = new _index.NodeDaemon("node-dag");
    scheduler.registerOrUpdateNode(daemon.emitHeartbeat());

    const parent = new _index.TaskSpec({ task_id: "parent-1", tenant_id: "t1", priority: _index.TaskPriority.P1_HIGH });
    const child = new _index.TaskSpec({
      task_id: "child-1",
      tenant_id: "t1",
      priority: _index.TaskPriority.P1_HIGH,
      dependencies: ["parent-1"]
    });
    scheduler.submitTask(parent);
    scheduler.submitTask(child);
    assert.equal(child.state, _index.TaskState.SUBMITTED);

    scheduler.scheduleTick();
    scheduler.completeTaskExecution("parent-1", "node-dag", parent.lease_fencing_token);
    assert.equal(child.state, _index.TaskState.QUEUED);
  });

  (0, _nodeTest.it)("6. Prefix-Cache Aware CognitiveRouter routes repeated prefix hash to sticky KV-warm endpoint", () => {
    const router = new _index.CognitiveRouter();
    router.registerEndpoint(
    new _index.LlmEndpointMetrics({
      endpoint_id: "ep-openai-1",
      provider: "openai",
      model_name: "gpt-4o",
      ttft_ewma_ms: 220,
      tpm_limit: 500_000
    }));

    router.registerEndpoint(
    new _index.LlmEndpointMetrics({
      endpoint_id: "ep-anthropic-1",
      provider: "anthropic",
      model_name: "claude-3-7-sonnet",
      ttft_ewma_ms: 210,
      tpm_limit: 500_000
    }));


    const prefix = _index.CognitiveRouter.computePrefixHash("sys", "tools", "ctx");
    const [firstEp, hit1] = router.selectEndpoint(prefix, 1000, _index.TaskPriority.P1_HIGH);
    assert.equal(hit1, false);
    router.releaseEndpoint(firstEp.endpoint_id);

    const [secondEp, hit2] = router.selectEndpoint(prefix, 1000, _index.TaskPriority.P1_HIGH);
    assert.equal(secondEp.endpoint_id, firstEp.endpoint_id);
    assert.equal(hit2, true);
  });

  (0, _nodeTest.it)("7. SagaToolGateway deduplicates mutating tool calls on retry with identical SHA-256 idempotency key", () => {
    const saga = new _index.SagaToolGateway();
    let invocations = 0;
    saga.registerTool("charge_card", _index.ToolCallEffectClass.COMPENSATABLE, (args) => {
      invocations += 1;
      return { charged: args.amount, seq: invocations };
    });

    const [res1, dedup1] = saga.invokeTool("task-1", 1, "charge_card", { amount: 99 });
    const [res2, dedup2] = saga.invokeTool("task-1", 1, "charge_card", { amount: 99 });
    assert.equal(dedup1, false);
    assert.equal(dedup2, true);
    assert.equal(invocations, 1);
    assert.deepEqual(res1, res2);
  });
});