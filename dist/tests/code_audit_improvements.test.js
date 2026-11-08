"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview Code Audit & Edge-Case Hardening Suite (4 tests).
 */






(0, _nodeTest.describe)("Code Audit & Edge-Case Hardening Suite", () => {
  (0, _nodeTest.it)("1. TaskSpecValidator rejects self-dependency and invalid CPU resource profiles", () => {
    assert.throws(() => {
      _index.TaskSpecValidator.validateAndNormalize({
        task_id: "self-dep",
        tenant_id: "t1",
        dependencies: ["self-dep"]
      });
    }, /cannot depend on itself/);
  });

  (0, _nodeTest.it)("2. IngressBackpressureController returns HTTP 429 with Retry-After header under PSI pressure", () => {
    const ctrl = new _index.IngressBackpressureController({ maxGlobalQueueDepth: 100 });
    const decision = ctrl.evaluateAdmission({
      priority: _index.TaskPriority.P2_STANDARD,
      globalQueueDepth: 20,
      tenantActiveTasks: 2,
      clusterPsiPressurePct: 90.0
    });
    assert.equal(decision.allowed, false);
    assert.equal(decision.statusCode, 429);
    assert.ok((decision.retryAfterSeconds ?? 0) > 0);
  });

  (0, _nodeTest.it)("3. DagEscrowCoordinator prevents parent-child quota deadlock and detects DAG cycles", () => {
    const coord = new _index.DagEscrowCoordinator();
    const parent = new _index.TaskSpec({
      task_id: "p-escrow",
      tenant_id: "t1",
      expected_SubAgent_count: 2
    });
    const escrow = coord.reserveParentEscrow(parent, 10, 1);
    assert.equal(escrow.escrowedSlots, 2);

    const child = new _index.TaskSpec({ task_id: "c-escrow-1", tenant_id: "t1", parent_task_id: "p-escrow" });
    const reg = coord.registerChildSubAgent(parent, child);
    assert.equal(reg.usedEscrowSlot, true);
    const resumed = coord.completeChildSubAgent(parent, "c-escrow-1");
    assert.equal(resumed, true);
  });

  (0, _nodeTest.it)("4. SyntacticAndVelocityBreaker trips on 3 consecutive identical tool calls", () => {
    const breaker = new _index.SyntacticAndVelocityBreaker(3, 100_000);
    const now = Date.now();
    breaker.recordAndCheckTurn("t-loop", { turnIndex: 1, toolName: "ls", argsHash: "h1", tokensConsumed: 100, timestampMs: now });
    breaker.recordAndCheckTurn("t-loop", { turnIndex: 2, toolName: "ls", argsHash: "h1", tokensConsumed: 100, timestampMs: now + 10 });
    const third = breaker.recordAndCheckTurn("t-loop", { turnIndex: 3, toolName: "ls", argsHash: "h1", tokensConsumed: 100, timestampMs: now + 20 });
    assert.equal(third.tripped, true);
    assert.equal(third.reason, "SEMANTIC_DEAD_LOOP");
  });
});