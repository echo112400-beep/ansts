/**
 * @fileoverview feat/celld-v25 M22 & M23 CoW Branch Forker, Zero-Trust Security, TEE/mTLS & A2A Blackboard/Deadlock/Bisect Suite.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  SharedBlackboardAndGitArbiter,
  A2aWaitForGraphDeadlockDetector,
  ParallelCowGitBisectCoordinator,
  CoWSandboxBranchForker,
  ZeroTrustSandboxSecurityGuard,
  ConfidentialTeeAttestationBridge,
  MtlsReverseTunnelBridge,
} from "../src/index.js";

describe("feat/celld-v25 M22 & M23 CoW, Zero-Trust & A2A Suite", () => {
  it("1. SharedBlackboardAndGitArbiter enforces CAS versions and arbitrates overlapping file patches", () => {
    const bb = new SharedBlackboardAndGitArbiter();
    const p1 = bb.putCas("plan:root", { step: 1 }, 0, "agent-1");
    assert.equal(p1.entry.version, 1);
    assert.throws(() => bb.putCas("plan:root", { step: 2 }, 0, "agent-2"), /Blackboard CAS conflict/);

    const arb = bb.arbitrateGitPatchTouches([
      { agentId: "a-high", touchedFiles: ["src/core.ts"], priorityScore: 95 },
      { agentId: "a-low", touchedFiles: ["src/core.ts", "src/ui.ts"], priorityScore: 40 },
      { agentId: "a-indep", touchedFiles: ["src/db.ts"], priorityScore: 60 },
    ]);
    assert.deepEqual(arb.acceptedAgentIds, ["a-high", "a-indep"]);
    assert.deepEqual(arb.conflictedAgentIds, ["a-low"]);
  });

  it("2. A2aWaitForGraphDeadlockDetector detects circular multi-agent wait dependencies", () => {
    const wfg = new A2aWaitForGraphDeadlockDetector();
    wfg.addWaitEdge("agent-A", "agent-B");
    wfg.addWaitEdge("agent-B", "agent-C");
    assert.equal(wfg.detectCycle().hasDeadlock, false);

    wfg.addWaitEdge("agent-C", "agent-A");
    const cycle = wfg.detectCycle();
    assert.equal(cycle.hasDeadlock, true);
    assert.ok(cycle.cyclePath.length >= 3);
  });

  it("3. ParallelCowGitBisectCoordinator & CoWSandboxBranchForker isolate bad commits in O(log N)", () => {
    const bisect = new ParallelCowGitBisectCoordinator();
    const commits = ["C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8"];
    const res = bisect.identifyFirstBadCommit(commits, (c) => ["C1", "C2", "C3", "C4"].includes(c));
    assert.equal(res.firstBadCommit, "C5");
    assert.ok(res.probesExecuted <= 4);

    const forker = new CoWSandboxBranchForker();
    const branches = forker.forkParallelBranches("main", ["exp-1", "exp-2", "exp-3"]);
    assert.equal(branches.length, 3);
    assert.equal(branches[0].copiedFilesCount, 0);
  });

  it("4. ZeroTrustSandboxSecurityGuard, ConfidentialTeeAttestationBridge & MtlsReverseTunnelBridge enforce security boundaries", () => {
    const guard = new ZeroTrustSandboxSecurityGuard();
    assert.equal(guard.evaluateAction({ filePath: "/etc/shadow" }).allowed, false);
    assert.equal(guard.evaluateAction({ filePath: "/workspace/app.ts", egressUrl: "https://api.openai.com/v1" }).allowed, true);

    const tee = new ConfidentialTeeAttestationBridge();
    const quote = tee.generateAndVerifyAttestationQuote({
      vmId: "vm-sev-1",
      measurementDigestHex: "0123456789abcdef0123456789abcdef",
      nonce: "n1",
    });
    assert.equal(quote.verified, true);

    const mtls = new MtlsReverseTunnelBridge();
    const cert = mtls.verifySpiffeCertificate("spiffe://ansts.io/ns/fleet/sa/body-daemon", Date.now() + 60_000);
    assert.equal(cert.authenticated, true);
    assert.equal(cert.nodeRole, "body-daemon");
  });
});
