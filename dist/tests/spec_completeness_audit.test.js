"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M1-M6 Spec Completeness Verification Suite (6 tests).
 */












(0, _nodeTest.describe)("M1-M6 Spec Completeness Verification Suite", () => {
  (0, _nodeTest.it)("1. SSE Event Stream Gateway supports Last-Event-ID history replay", () => {
    const sse = new _index.SseEventStreamGateway();
    sse.publish({ id: "evt-1", event: "turn", taskId: "t1", turnIndex: 1, timestampMs: 1, data: { step: 1 } });
    sse.publish({ id: "evt-2", event: "turn", taskId: "t1", turnIndex: 2, timestampMs: 2, data: { step: 2 } });
    const sub = sse.subscribe("t1", "sub-1", "evt-1");
    const chunk = String(sub.stream.read() ?? "");
    assert.ok(chunk.includes("evt-2"));
    sub.close();
  });

  (0, _nodeTest.it)("2. SagaCompensatorEngine and StagingCommitAndApprovalGate rollback and gate high-risk actions", () => {
    const comp = new _index.SagaCompensatorEngine();
    comp.recordForwardStep({
      stepId: "s1",
      taskId: "t1",
      turnIndex: 1,
      toolName: "create_branch",
      forwardPayload: { branch: "feat-x" },
      compensateHandler: (p) => ({ deleted: p.branch })
    });
    const rolled = comp.rollbackTask("t1");
    assert.equal(rolled.length, 1);

    const gate = new _index.StagingCommitAndApprovalGate();
    const task = new _index.TaskSpec({ task_id: "t-app", tenant_id: "t1" });
    const ticket = gate.requestHumanApproval(task, "drop_prod_table", { table: "users" });
    const resolved = gate.resolveApproval(ticket.ticketId, true, "sec-ops", task);
    assert.equal(resolved.status, "APPROVED");
  });

  (0, _nodeTest.it)("3. CgroupPhaseController decouples THINK (0.25 core) and ACT (4.0 cores) limits", () => {
    const ctrl = new _index.CgroupPhaseController();
    const think = ctrl.applyThinkPhase("t1", {
      think_cpu_cores: 0.25,
      act_cpu_cores: 4.0,
      act_memory_mb: 8192,
      disk_mb: 10240
    });
    const act = ctrl.applyActPhase("t1", {
      think_cpu_cores: 0.25,
      act_cpu_cores: 4.0,
      act_memory_mb: 8192,
      disk_mb: 10240
    });
    assert.ok(act.cpuQuotaUsPer100ms > think.cpuQuotaUsPer100ms * 10);
  });

  (0, _nodeTest.it)("4. ZeroTrustVsockBridge blocks unauthorized external domains", () => {
    const bridge = new _index.ZeroTrustVsockBridge(["api.openai.com"]);
    assert.throws(() => {
      bridge.sendRequest({
        requestId: "r1",
        vmId: "vm-1",
        method: "HTTP_EGRESS",
        payload: { url: "https://evil-exfil.example.com/steal" }
      });
    }, /blocked connection/);
  });

  (0, _nodeTest.it)("5. TurnWalEngine, WalCompactor, and DeterministicTimeTravelReplayer fork task at historical turn", () => {
    const wal = new _index.TurnWalEngine();
    const e1 = wal.appendTurnDelta({
      taskId: "t-wal",
      turnIndex: 1,
      fencingToken: 1,
      changedFiles: ["a.ts"],
      sqliteWalPages: 10,
      highestSubmittedFencingToken: 1
    });
    const e2 = wal.appendTurnDelta({
      taskId: "t-wal",
      turnIndex: 2,
      fencingToken: 1,
      changedFiles: ["a.ts", "b.ts"],
      sqliteWalPages: 12,
      highestSubmittedFencingToken: 1
    });
    const compactor = new _index.WalCompactorAndPagedRestorer();
    const snap = compactor.compactLayers("t-wal", [e1, e2]);
    assert.equal(snap.mergedFileSet.length, 2);

    const replayer = new _index.DeterministicTimeTravelReplayer();
    const orig = new _index.TaskSpec({ task_id: "t-wal", tenant_id: "t1", prompt: "old" });
    const branch = replayer.forkTaskAtTurn(orig, [e1, e2], 1, "t-wal-fork", "new prompt");
    assert.equal(branch.restoredTurnIndex, 1);
    assert.deepEqual(branch.inheritedFiles, ["a.ts"]);
  });

  (0, _nodeTest.it)("6. CloudEventsBus, OutboxWebhook & BidirectionalBackpressureController close telemetry loop", () => {
    const bus = new _index.CloudEventsBus();
    bus.emitEvent({
      type: "io.scheduler.agent.turn.completed",
      subject: "task-1",
      tenantId: "tenant-1",
      llmCostUsd: 0.05,
      sandboxCpuCoreSeconds: 12
    });
    assert.ok((bus.getTenantReport("tenant-1")?.grandTotalUsd ?? 0) > 0.05);

    const outbox = new _index.TransactionalOutboxDispatcher();
    outbox.enqueueOutbox("m1", "task-1", "https://example.com/hook", { ok: true });
    const res = outbox.processPending(() => true);
    assert.equal(res.deliveredCount, 1);

    const loop = new _index.BidirectionalBackpressureController();
    const state = loop.computeClusterHealthState({
      maxNodePsiSomePct: 80,
      globalRateLimitedEndpoints: 3,
      totalEndpoints: 4,
      globalQueueDepth: 2000
    });
    assert.equal(state.mode, "RED_SHEDDING");
  });
});