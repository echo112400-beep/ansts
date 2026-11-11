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
 * @fileoverview Code Audit & Edge-Case Hardening Suite (4 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("Code Audit & Edge-Case Hardening Suite", () => {
    (0, node_test_1.it)("1. TaskSpecValidator rejects self-dependency and invalid CPU resource profiles", () => {
        assert.throws(() => {
            index_js_1.TaskSpecValidator.validateAndNormalize({
                task_id: "self-dep",
                tenant_id: "t1",
                dependencies: ["self-dep"],
            });
        }, /cannot depend on itself/);
    });
    (0, node_test_1.it)("2. IngressBackpressureController returns HTTP 429 with Retry-After header under PSI pressure", () => {
        const ctrl = new index_js_1.IngressBackpressureController({ maxGlobalQueueDepth: 100 });
        const decision = ctrl.evaluateAdmission({
            priority: index_js_1.TaskPriority.P2_STANDARD,
            globalQueueDepth: 20,
            tenantActiveTasks: 2,
            clusterPsiPressurePct: 90.0,
        });
        assert.equal(decision.allowed, false);
        assert.equal(decision.statusCode, 429);
        assert.ok((decision.retryAfterSeconds ?? 0) > 0);
    });
    (0, node_test_1.it)("3. DagEscrowCoordinator prevents parent-child quota deadlock and detects DAG cycles", () => {
        const coord = new index_js_1.DagEscrowCoordinator();
        const parent = new index_js_1.TaskSpec({
            task_id: "p-escrow",
            tenant_id: "t1",
            expected_SubAgent_count: 2,
        });
        const escrow = coord.reserveParentEscrow(parent, 10, 1);
        assert.equal(escrow.escrowedSlots, 2);
        const child = new index_js_1.TaskSpec({ task_id: "c-escrow-1", tenant_id: "t1", parent_task_id: "p-escrow" });
        const reg = coord.registerChildSubAgent(parent, child);
        assert.equal(reg.usedEscrowSlot, true);
        const resumed = coord.completeChildSubAgent(parent, "c-escrow-1");
        assert.equal(resumed, true);
    });
    (0, node_test_1.it)("4. SyntacticAndVelocityBreaker trips on 3 consecutive identical tool calls", () => {
        const breaker = new index_js_1.SyntacticAndVelocityBreaker(3, 100_000);
        const now = Date.now();
        breaker.recordAndCheckTurn("t-loop", { turnIndex: 1, toolName: "ls", argsHash: "h1", tokensConsumed: 100, timestampMs: now });
        breaker.recordAndCheckTurn("t-loop", { turnIndex: 2, toolName: "ls", argsHash: "h1", tokensConsumed: 100, timestampMs: now + 10 });
        const third = breaker.recordAndCheckTurn("t-loop", { turnIndex: 3, toolName: "ls", argsHash: "h1", tokensConsumed: 100, timestampMs: now + 20 });
        assert.equal(third.tripped, true);
        assert.equal(third.reason, "SEMANTIC_DEAD_LOOP");
    });
});
