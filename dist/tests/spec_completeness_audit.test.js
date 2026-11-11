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
 * @fileoverview M1-M6 Spec Completeness Verification Suite (6 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("M1-M6 Spec Completeness Verification Suite", () => {
    (0, node_test_1.it)("1. SSE Event Stream Gateway supports Last-Event-ID history replay", () => {
        const sse = new index_js_1.SseEventStreamGateway();
        sse.publish({ id: "evt-1", event: "turn", taskId: "t1", turnIndex: 1, timestampMs: 1, data: { step: 1 } });
        sse.publish({ id: "evt-2", event: "turn", taskId: "t1", turnIndex: 2, timestampMs: 2, data: { step: 2 } });
        const sub = sse.subscribe("t1", "sub-1", "evt-1");
        const chunk = String(sub.stream.read() ?? "");
        assert.ok(chunk.includes("evt-2"));
        sub.close();
    });
    (0, node_test_1.it)("2. SagaCompensatorEngine and StagingCommitAndApprovalGate rollback and gate high-risk actions", () => {
        const comp = new index_js_1.SagaCompensatorEngine();
        comp.recordForwardStep({
            stepId: "s1",
            taskId: "t1",
            turnIndex: 1,
            toolName: "create_branch",
            forwardPayload: { branch: "feat-x" },
            compensateHandler: (p) => ({ deleted: p.branch }),
        });
        const rolled = comp.rollbackTask("t1");
        assert.equal(rolled.length, 1);
        const gate = new index_js_1.StagingCommitAndApprovalGate();
        const task = new index_js_1.TaskSpec({ task_id: "t-app", tenant_id: "t1" });
        const ticket = gate.requestHumanApproval(task, "drop_prod_table", { table: "users" });
        const resolved = gate.resolveApproval(ticket.ticketId, true, "sec-ops", task);
        assert.equal(resolved.status, "APPROVED");
    });
    (0, node_test_1.it)("3. CgroupPhaseController decouples THINK (0.25 core) and ACT (4.0 cores) limits", () => {
        const ctrl = new index_js_1.CgroupPhaseController();
        const think = ctrl.applyThinkPhase("t1", {
            think_cpu_cores: 0.25,
            act_cpu_cores: 4.0,
            act_memory_mb: 8192,
            disk_mb: 10240,
        });
        const act = ctrl.applyActPhase("t1", {
            think_cpu_cores: 0.25,
            act_cpu_cores: 4.0,
            act_memory_mb: 8192,
            disk_mb: 10240,
        });
        assert.ok(act.cpuQuotaUsPer100ms > think.cpuQuotaUsPer100ms * 10);
    });
    (0, node_test_1.it)("4. ZeroTrustVsockBridge blocks unauthorized external domains", () => {
        const bridge = new index_js_1.ZeroTrustVsockBridge(["api.openai.com"]);
        assert.throws(() => {
            bridge.sendRequest({
                requestId: "r1",
                vmId: "vm-1",
                method: "HTTP_EGRESS",
                payload: { url: "https://evil-exfil.example.com/steal" },
            });
        }, /blocked connection/);
    });
    (0, node_test_1.it)("5. TurnWalEngine, WalCompactor, and DeterministicTimeTravelReplayer fork task at historical turn", () => {
        const wal = new index_js_1.TurnWalEngine();
        const e1 = wal.appendTurnDelta({
            taskId: "t-wal",
            turnIndex: 1,
            fencingToken: 1,
            changedFiles: ["a.ts"],
            sqliteWalPages: 10,
            highestSubmittedFencingToken: 1,
        });
        const e2 = wal.appendTurnDelta({
            taskId: "t-wal",
            turnIndex: 2,
            fencingToken: 1,
            changedFiles: ["a.ts", "b.ts"],
            sqliteWalPages: 12,
            highestSubmittedFencingToken: 1,
        });
        const compactor = new index_js_1.WalCompactorAndPagedRestorer();
        const snap = compactor.compactLayers("t-wal", [e1, e2]);
        assert.equal(snap.mergedFileSet.length, 2);
        const replayer = new index_js_1.DeterministicTimeTravelReplayer();
        const orig = new index_js_1.TaskSpec({ task_id: "t-wal", tenant_id: "t1", prompt: "old" });
        const branch = replayer.forkTaskAtTurn(orig, [e1, e2], 1, "t-wal-fork", "new prompt");
        assert.equal(branch.restoredTurnIndex, 1);
        assert.deepEqual(branch.inheritedFiles, ["a.ts"]);
    });
    (0, node_test_1.it)("6. CloudEventsBus, OutboxWebhook & BidirectionalBackpressureController close telemetry loop", () => {
        const bus = new index_js_1.CloudEventsBus();
        bus.emitEvent({
            type: "io.scheduler.agent.turn.completed",
            subject: "task-1",
            tenantId: "tenant-1",
            llmCostUsd: 0.05,
            sandboxCpuCoreSeconds: 12,
        });
        assert.ok((bus.getTenantReport("tenant-1")?.grandTotalUsd ?? 0) > 0.05);
        const outbox = new index_js_1.TransactionalOutboxDispatcher();
        outbox.enqueueOutbox("m1", "task-1", "https://example.com/hook", { ok: true });
        const res = outbox.processPending(() => true);
        assert.equal(res.deliveredCount, 1);
        const loop = new index_js_1.BidirectionalBackpressureController();
        const state = loop.computeClusterHealthState({
            maxNodePsiSomePct: 80,
            globalRateLimitedEndpoints: 3,
            totalEndpoints: 4,
            globalQueueDepth: 2000,
        });
        assert.equal(state.mode, "RED_SHEDDING");
    });
});
