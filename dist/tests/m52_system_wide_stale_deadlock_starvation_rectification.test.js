"use strict";







var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M52 End-to-End & Subsystem Verification: System-Wide Stale, Deadlock, Starvation & State-Consistency Rectification Suite.
 * Verifies:
 * 1. [Cell-Stale-1] `AnstsSchedulerCell` & `LeaseCoordinator` cold-start fencing watermark rehydration, interrupted `LEASED` -> `QUEUED` recovery, and active-lease DO alarm renewal.
 * 2. [Router-Stale-2] `CognitiveRouter` 60-second sliding-window quota decay, half-open circuit-breaker auto-recovery from rate-limit degradation, and fallback `active_requests` concurrency accounting.
 * 3. [Escrow-Deadlock-3] `DagEscrowCoordinator` child sub-agent failure parent unblocking & escrow slot reclamation + `A2aWaitForGraphDeadlockDetector` priority-aware victim selection.
 * 4. [Tunnel-Deadlock-4] `TunnelGatewayCell` stale-heartbeat tunnel auto-demotion and unacknowledged in-flight command re-queueing onto secondary failover gateway.
 */(0, _nodeTest.describe)("M52 Comprehensive System-Wide Stale, Deadlock, Starvation & State-Consistency Rectification Suite", () => {(0, _nodeTest.it)("verifies Cell cold-start lease/fencing rehydration & alarm renewal, CognitiveRouter window decay & half-open recovery, DagEscrow child-failure unblocking & WFG priority victim, and Tunnel in-flight command re-queueing", async () => {const tmpDir = `/tmp/ansts-m52-${process.pid}-${Date.now()}`;fs.mkdirSync(tmpDir, { recursive: true });const server = new _server_main.AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: tmpDir
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-rectify-audit"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);


























      assert.equal(body.cellStale1.activeLeaseAlarmRenewed, true);
      assert.equal(body.cellStale1.interruptedLeaseConvertedToQueued, true);
      assert.equal(body.cellStale1.monotonicFencePreservedAcrossColdStart, true);
      assert.ok(body.cellStale1.fenceAfterRestart > body.cellStale1.fenceBeforeRestart);

      assert.equal(body.routerStale2.fallbackIncrementedActiveRequests, true);
      assert.equal(body.routerStale2.fallbackReleasedCleanly, true);
      assert.equal(body.routerStale2.markedUnhealthyOnRateLimit, true);
      assert.equal(body.routerStale2.routerWindowAndHalfOpenRecovered, true);

      assert.equal(body.escrowAndWfgDeadlock3.escrowUnblockedAndReclaimed, true);
      assert.equal(body.escrowAndWfgDeadlock3.wfgLowestPriorityVictimChosen, true);
      assert.equal(body.escrowAndWfgDeadlock3.wfgVictimAgentId, "agent-p3-helper");

      assert.equal(body.tunnelDeadlock4.demotedGateways.length, 1);
      assert.equal(body.tunnelDeadlock4.demotedGateways[0].gatewayId, "celld-gw-01");
      assert.equal(body.tunnelDeadlock4.requeuedCommandIds.length, 1);
      assert.equal(body.tunnelDeadlock4.tunnelInFlightRequeuedAndAcked, true);

      assert.equal(body.allM52RectificationsVerified, true);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});