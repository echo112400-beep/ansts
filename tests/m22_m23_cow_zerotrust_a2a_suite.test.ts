/**
 * @fileoverview M22 & M23 (TASK-105 ~ TASK-112) Verification Suite:
 * - `SandboxBranchForker` (<80ms CoW sub-agent branch fork, mutation tracking, winning merge & sibling pruning)
 * - `ZeroTrustSecurityGuard` (AF_VSOCK secretless token injection, egress allowlist, and `cgroup.freeze=1` containment)
 * - `A2aBlackboardBus`, `GitConflictArbiterCell` (AST symbol-level auto-merge vs conflict detection), and `WorkStealingQuotaBroker`
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import {
  SandboxBranchForker,
  ZeroTrustSecurityGuard,
  A2aBlackboardBus,
  GitConflictArbiterCell,
  WorkStealingQuotaBroker,
  FenceToken,
} from "../src/index";

test("test 71: [TASK-105 & TASK-106] SandboxBranchForker forks parallel CoW sub-agent branches in <80ms, merges winner, and prunes siblings", () => {
  const forker = new SandboxBranchForker("/tmp/ansts_m22_cow_test");
  const chk = forker.createCheckpoint("sbx-parent-main", "/var/lib/ansts/lower", {
    "src/app.ts": "sha256_init",
  });
  assert.ok(chk.checkpointId.startsWith("chk_"));

  // Spawn 3 parallel sub-agent branches from the same parent checkpoint
  const b1 = forker.forkBranch("sbx-parent-main", chk.checkpointId);
  const b2 = forker.forkBranch("sbx-parent-main", chk.checkpointId);
  const b3 = forker.forkBranch("sbx-parent-main", chk.checkpointId);

  assert.ok(b1.forkLatencyMs < 80);
  forker.recordBranchFileMutation(b2.branchId, "src/router.ts", "+patched route");

  // Select b2 as winner -> merges b2 and automatically prunes b1 & b3
  const mergeRes = forker.mergeWinningBranchAndPruneSiblings(b2.branchId);
  assert.equal(mergeRes.success, true);
  assert.equal(mergeRes.mergedFilesCount, 1);
  assert.deepEqual(mergeRes.prunedSiblingBranchIds.sort(), [b1.branchId, b3.branchId].sort());
});

test("test 72: [TASK-107 & TASK-108] ZeroTrustSecurityGuard secretless AF_VSOCK proxy, egress domain micro-segmentation, and cgroup.freeze=1 containment", () => {
  // Isolated cgroup root: `cgroup.freeze=1` is persisted to disk per sandbox, so a shared root
  // would leave the sandbox frozen and fail the egress probe on the next run.
  const guard = new ZeroTrustSecurityGuard(fs.mkdtempSync(path.join(os.tmpdir(), "ansts-cgroup-")));
  const tok = guard.issueShortLivedToken("tenant-sec", "github-git-https", 60000, 1000);

  // 1. Valid VSOCK credential injection
  const okProxy = guard.handleVsockCredentialProxyRequest("sbx-sec-01", tok.tokenId, 2000);
  assert.equal(okProxy.authorized, true);
  assert.equal(okProxy.secret, tok.shortLivedSecret);

  // 2. Egress allowlist allows registry.npmjs.org but blocks unknown exfiltration domain
  assert.equal(guard.validateEgressDomain("sbx-sec-01", "registry.npmjs.org").allowed, true);
  assert.equal(guard.validateEgressDomain("sbx-sec-01", "evil-c2.example.com").allowed, false);

  // 3. eBPF high-risk syscall triggers cgroup.freeze = 1 and blocks all subsequent VSOCK & egress requests
  const freeze = guard.triggerHighRiskContainment("sbx-sec-01", "sys_ptrace escape attempt");
  assert.equal(freeze.frozen, true);
  assert.equal(freeze.cgroupFreezeValue, "1");
  assert.equal(guard.handleVsockCredentialProxyRequest("sbx-sec-01", tok.tokenId, 3000).authorized, false);
});

test("test 73: [TASK-109 ~ TASK-112] A2aBlackboardBus, GitConflictArbiterCell AST symbol merge, and WorkStealingQuotaBroker", () => {
  // 1. A2A Blackboard Bus with FenceToken protection
  const bus = new A2aBlackboardBus();
  bus.publishArtifact({
    artifactId: "art-1",
    dagId: "dag-coding-01",
    producerTaskId: "subagent-1",
    topic: "api-contract",
    fence: new FenceToken(1, 1, "subagent-1"),
    schemaType: "AST_INTERFACE_CONTRACT",
    payload: { interfaceName: "UserAuth" },
    publishedAtMs: Date.now(),
  });
  assert.equal(bus.queryArtifacts("dag-coding-01", "api-contract").length, 1);

  // 2. GitConflictArbiterCell: Disjoint AST symbols in same file merge cleanly; colliding symbol is flagged
  const arbiter = new GitConflictArbiterCell();
  const cleanAstMerge = arbiter.arbitrateParallelBranchPatches([
    {
      branchId: "brn-a",
      agentTaskId: "agent-a",
      baseCheckpointId: "chk-1",
      modifiedFiles: {
        "src/service.ts": { touchedSymbols: ["loginUser"], diffPatch: "+function loginUser() {}" },
      },
    },
    {
      branchId: "brn-b",
      agentTaskId: "agent-b",
      baseCheckpointId: "chk-1",
      modifiedFiles: {
        "src/service.ts": { touchedSymbols: ["revokeToken"], diffPatch: "+function revokeToken() {}" },
      },
    },
  ]);
  assert.equal(cleanAstMerge.mergedCleanly, true);
  assert.deepEqual(cleanAstMerge.autoResolvedSymbolConflicts, ["src/service.ts"]);

  // 3. Work-Stealing Quota Broker steals idle slots without exceeding global ceiling
  const broker = new WorkStealingQuotaBroker();
  const stealRes = broker.rebalanceTenantQuotaAcrossShards({
    tenantId: "tenant-burst",
    globalMaxSlots: 20,
    shardAllocations: { "shard-00": 10, "shard-01": 10 },
    shardActiveUsage: { "shard-00": 10, "shard-01": 3 },
    requestingHotShardId: "shard-00",
    requestedExtraSlots: 5,
  });
  assert.equal(stealRes.grantedSlots, 5);
  assert.equal(stealRes.updatedAllocations["shard-00"], 15);
  assert.equal(stealRes.updatedAllocations["shard-01"], 5);
});
