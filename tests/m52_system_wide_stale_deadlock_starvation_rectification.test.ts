/**
 * @fileoverview M52 End-to-End & Subsystem Verification: System-Wide Stale, Deadlock, Starvation & State-Consistency Rectification Suite.
 * Verifies:
 * 1. [Cell-Stale-1] `AnstsSchedulerCell` & `LeaseCoordinator` cold-start fencing watermark rehydration, interrupted `LEASED` -> `QUEUED` recovery, and active-lease DO alarm renewal.
 * 2. [Router-Stale-2] `CognitiveRouter` 60-second sliding-window quota decay, half-open circuit-breaker auto-recovery from rate-limit degradation, and fallback `active_requests` concurrency accounting.
 * 3. [Escrow-Deadlock-3] `DagEscrowCoordinator` child sub-agent failure parent unblocking & escrow slot reclamation + `A2aWaitForGraphDeadlockDetector` priority-aware victim selection.
 * 4. [Tunnel-Deadlock-4] `TunnelGatewayCell` stale-heartbeat tunnel auto-demotion and unacknowledged in-flight command re-queueing onto secondary failover gateway.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M52 Comprehensive System-Wide Stale, Deadlock, Starvation & State-Consistency Rectification Suite", () => {
  it("verifies Cell cold-start lease/fencing rehydration & alarm renewal, CognitiveRouter window decay & half-open recovery, DagEscrow child-failure unblocking & WFG priority victim, and Tunnel in-flight command re-queueing", async () => {
    const tmpDir = `/tmp/ansts-m52-${process.pid}-${Date.now()}`;
    fs.mkdirSync(tmpDir, { recursive: true });

    const server = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: tmpDir,
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const cliRes = await runAnstsCtlRemote(["mgmt-rectify-audit"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout) as {
        cellStale1: {
          activeLeaseAlarmRenewed: boolean;
          interruptedLeaseConvertedToQueued: boolean;
          monotonicFencePreservedAcrossColdStart: boolean;
          fenceBeforeRestart: number;
          fenceAfterRestart: number;
        };
        routerStale2: {
          fallbackIncrementedActiveRequests: boolean;
          fallbackReleasedCleanly: boolean;
          markedUnhealthyOnRateLimit: boolean;
          routerWindowAndHalfOpenRecovered: boolean;
        };
        escrowAndWfgDeadlock3: {
          escrowUnblockedAndReclaimed: boolean;
          wfgLowestPriorityVictimChosen: boolean;
          wfgVictimAgentId: string;
        };
        tunnelDeadlock4: {
          demotedGateways: Array<{ hostId: string; gatewayId: string }>;
          requeuedCommandIds: string[];
          tunnelInFlightRequeuedAndAcked: boolean;
        };
        allM52RectificationsVerified: boolean;
      };

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
