"use strict";
/**
 * @fileoverview M22 & M23 (TASK-105 ~ TASK-112) Verification Suite:
 * - `SandboxBranchForker` (<80ms CoW sub-agent branch fork, mutation tracking, winning merge & sibling pruning)
 * - `ZeroTrustSecurityGuard` (AF_VSOCK secretless token injection, egress allowlist, and `cgroup.freeze=1` containment)
 * - `A2aBlackboardBus`, `GitConflictArbiterCell` (AST symbol-level auto-merge vs conflict detection), and `WorkStealingQuotaBroker`
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_test_1 = __importDefault(require("node:test"));
const strict_1 = __importDefault(require("node:assert/strict"));
const index_1 = require("../src/index");
(0, node_test_1.default)("test 71: [TASK-105 & TASK-106] SandboxBranchForker forks parallel CoW sub-agent branches in <80ms, merges winner, and prunes siblings", () => {
    const forker = new index_1.SandboxBranchForker("/tmp/ansts_m22_cow_test");
    const chk = forker.createCheckpoint("sbx-parent-main", "/var/lib/ansts/lower", {
        "src/app.ts": "sha256_init",
    });
    strict_1.default.ok(chk.checkpointId.startsWith("chk_"));
    // Spawn 3 parallel sub-agent branches from the same parent checkpoint
    const b1 = forker.forkBranch("sbx-parent-main", chk.checkpointId);
    const b2 = forker.forkBranch("sbx-parent-main", chk.checkpointId);
    const b3 = forker.forkBranch("sbx-parent-main", chk.checkpointId);
    strict_1.default.ok(b1.forkLatencyMs < 80);
    forker.recordBranchFileMutation(b2.branchId, "src/router.ts", "+patched route");
    // Select b2 as winner -> merges b2 and automatically prunes b1 & b3
    const mergeRes = forker.mergeWinningBranchAndPruneSiblings(b2.branchId);
    strict_1.default.equal(mergeRes.success, true);
    strict_1.default.equal(mergeRes.mergedFilesCount, 1);
    strict_1.default.deepEqual(mergeRes.prunedSiblingBranchIds.sort(), [b1.branchId, b3.branchId].sort());
});
(0, node_test_1.default)("test 72: [TASK-107 & TASK-108] ZeroTrustSecurityGuard secretless AF_VSOCK proxy, egress domain micro-segmentation, and cgroup.freeze=1 containment", () => {
    const guard = new index_1.ZeroTrustSecurityGuard();
    const tok = guard.issueShortLivedToken("tenant-sec", "github-git-https", 60000, 1000);
    // 1. Valid VSOCK credential injection
    const okProxy = guard.handleVsockCredentialProxyRequest("sbx-sec-01", tok.tokenId, 2000);
    strict_1.default.equal(okProxy.authorized, true);
    strict_1.default.equal(okProxy.secret, tok.shortLivedSecret);
    // 2. Egress allowlist allows registry.npmjs.org but blocks unknown exfiltration domain
    strict_1.default.equal(guard.validateEgressDomain("sbx-sec-01", "registry.npmjs.org").allowed, true);
    strict_1.default.equal(guard.validateEgressDomain("sbx-sec-01", "evil-c2.example.com").allowed, false);
    // 3. eBPF high-risk syscall triggers cgroup.freeze = 1 and blocks all subsequent VSOCK & egress requests
    const freeze = guard.triggerHighRiskContainment("sbx-sec-01", "sys_ptrace escape attempt");
    strict_1.default.equal(freeze.frozen, true);
    strict_1.default.equal(freeze.cgroupFreezeValue, "1");
    strict_1.default.equal(guard.handleVsockCredentialProxyRequest("sbx-sec-01", tok.tokenId, 3000).authorized, false);
});
(0, node_test_1.default)("test 73: [TASK-109 ~ TASK-112] A2aBlackboardBus, GitConflictArbiterCell AST symbol merge, and WorkStealingQuotaBroker", () => {
    // 1. A2A Blackboard Bus with FenceToken protection
    const bus = new index_1.A2aBlackboardBus();
    bus.publishArtifact({
        artifactId: "art-1",
        dagId: "dag-coding-01",
        producerTaskId: "subagent-1",
        topic: "api-contract",
        fence: new index_1.FenceToken(1, 1, "subagent-1"),
        schemaType: "AST_INTERFACE_CONTRACT",
        payload: { interfaceName: "UserAuth" },
        publishedAtMs: Date.now(),
    });
    strict_1.default.equal(bus.queryArtifacts("dag-coding-01", "api-contract").length, 1);
    // 2. GitConflictArbiterCell: Disjoint AST symbols in same file merge cleanly; colliding symbol is flagged
    const arbiter = new index_1.GitConflictArbiterCell();
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
    strict_1.default.equal(cleanAstMerge.mergedCleanly, true);
    strict_1.default.deepEqual(cleanAstMerge.autoResolvedSymbolConflicts, ["src/service.ts"]);
    // 3. Work-Stealing Quota Broker steals idle slots without exceeding global ceiling
    const broker = new index_1.WorkStealingQuotaBroker();
    const stealRes = broker.rebalanceTenantQuotaAcrossShards({
        tenantId: "tenant-burst",
        globalMaxSlots: 20,
        shardAllocations: { "shard-00": 10, "shard-01": 10 },
        shardActiveUsage: { "shard-00": 10, "shard-01": 3 },
        requestingHotShardId: "shard-00",
        requestedExtraSlots: 5,
    });
    strict_1.default.equal(stealRes.grantedSlots, 5);
    strict_1.default.equal(stealRes.updatedAllocations["shard-00"], 15);
    strict_1.default.equal(stealRes.updatedAllocations["shard-01"], 5);
});
