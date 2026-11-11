"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CoWSandboxBranchForker = void 0;
/**
 * @fileoverview M22 & M37 Copy-on-Write Sandbox Branch Forker for Parallel Speculative & Deterministic Execution.
 */
const layerfs_workspace_driver_js_1 = require("./layerfs_workspace_driver.js");
const deterministic_envelope_js_1 = require("./deterministic_envelope.js");
class CoWSandboxBranchForker {
    driver;
    constructor(driver = new layerfs_workspace_driver_js_1.LayerFsWorkspaceDriver()) {
        this.driver = driver;
    }
    forkParallelBranches(sourceBranch, branchNames) {
        return branchNames.map((name) => this.driver.forkBranch(sourceBranch, name));
    }
    /**
     * Forks N speculative CoW branches from `sourceBranch`, runs `command` inside each branch
     * under a `DeterministicExecutionEnvelope(seed)`, and returns each branch's commit & stdout.
     */
    executeDeterministicSpeculativeBranches(params) {
        return params.branches.map((b, idx) => {
            try {
                this.driver.getBranchHeadCommit(b.branchName);
            }
            catch {
                this.driver.forkBranch(params.sourceBranch, b.branchName);
            }
            const seed = b.seed ?? 42;
            const envEnvelope = new deterministic_envelope_js_1.DeterministicExecutionEnvelope(seed);
            const wrappedCmd = `${envEnvelope.buildDeterministicShellExportPrefix(0)}${b.command}`;
            const execRes = this.driver.executeEphemeralBashWorkspace({
                branchName: b.branchName,
                agentId: `speculative-agent-${idx}`,
                stepId: `spec-step-${b.branchName}`,
                command: wrappedCmd,
            });
            return {
                branchName: b.branchName,
                commitId: execRes.commitId,
                seed,
                stdout: execRes.stdout,
            };
        });
    }
    /**
     * [M71-2 / F2] Best-of-N Speculative Parallel Sandbox & Verifier:
     * - Forks N zero-copy LayerFS CoW branches from `sourceBranch`.
     * - Runs each candidate's `patchCommand` and `verifyCommand` under a deterministic execution envelope.
     * - Scores each candidate on verification status (`passed`), `testsPassed`, `lintWarnings`, and execution latency.
     * - Promotes (`fast-forward` forks) `targetPromoteBranch` to the winning candidate's commit and prunes all losing
     *   branches + unreachable commits/B-Tree pages via `gcUnreachableCommits()`.
     */
    executeBestOfNSpeculativeVerifier(params) {
        if (!Array.isArray(params.candidates) || params.candidates.length === 0) {
            throw new Error("executeBestOfNSpeculativeVerifier requires at least 1 candidate branch");
        }
        const evaluated = params.candidates.map((cand, idx) => {
            try {
                this.driver.deleteBranch(cand.branchName);
            }
            catch {
                // ignore if branch is main
            }
            this.driver.forkBranch(params.sourceBranch, cand.branchName);
            const seed = cand.seed ?? 100 + idx;
            const envEnvelope = new deterministic_envelope_js_1.DeterministicExecutionEnvelope(seed);
            const combinedCmd = `${envEnvelope.buildDeterministicShellExportPrefix(idx)}( ${cand.patchCommand} ) && ( ${cand.verifyCommand} ) && echo "__ANSTS_VERIFY_OK__=1" || echo "__ANSTS_VERIFY_OK__=0"`;
            const execRes = this.driver.executeEphemeralBashWorkspace({
                branchName: cand.branchName,
                agentId: `spec-bestofn-${idx}`,
                stepId: `bestofn-step-${cand.branchName}`,
                command: combinedCmd,
            });
            const out = execRes.stdout ?? "";
            const passed = out.includes("__ANSTS_VERIFY_OK__=1");
            const testsMatch = out.match(/TESTS_PASSED=(\d+)/);
            const lintMatch = out.match(/LINT_WARNINGS=(\d+)/);
            const testsPassed = testsMatch ? Number(testsMatch[1]) : passed ? 10 : 0;
            const lintWarnings = lintMatch ? Number(lintMatch[1]) : 0;
            const latencyWeight = cand.latencyPenaltyWeight ?? 0.01;
            const rawScore = passed
                ? 70 +
                    Math.min(25, testsPassed * 2.0) +
                    (execRes.committedNewSnapshot ? 5 : 0) -
                    lintWarnings * 3.0 -
                    execRes.mountLatencyMs * latencyWeight
                : Math.max(0, Math.min(45, testsPassed * 2.0 - lintWarnings * 5.0));
            const score = Number(Math.max(0, Math.min(100, rawScore)).toFixed(2));
            return {
                branchName: cand.branchName,
                commitId: execRes.commitId,
                modelTier: cand.modelTier ?? `tier-cand-${idx + 1}`,
                temperature: cand.temperature ?? Number((0.1 + idx * 0.2).toFixed(2)),
                seed,
                passed,
                testsPassed,
                lintWarnings,
                mountLatencyMs: Number(execRes.mountLatencyMs.toFixed(2)),
                score,
                stdout: out.trim(),
            };
        });
        const passing = evaluated.filter((c) => c.passed);
        const pool = passing.length > 0 ? passing : evaluated;
        const sorted = [...pool].sort((a, b) => {
            if (b.score !== a.score)
                return b.score - a.score;
            return a.mountLatencyMs - b.mountLatencyMs;
        });
        const winner = sorted[0];
        const promotedBranchName = params.targetPromoteBranch ?? `${params.sourceBranch}-verified`;
        if (promotedBranchName !== winner.branchName) {
            this.driver.forkBranch(winner.branchName, promotedBranchName);
        }
        const prunedLoserBranches = [];
        for (const cand of evaluated) {
            if (cand.branchName !== winner.branchName && cand.branchName !== "main") {
                if (this.driver.deleteBranch(cand.branchName)) {
                    prunedLoserBranches.push(cand.branchName);
                }
            }
        }
        const gcRes = this.driver.gcUnreachableCommits();
        return {
            sourceBranch: params.sourceBranch,
            promotedBranchName,
            winnerBranchName: winner.branchName,
            winnerCommitId: winner.commitId,
            winnerScore: winner.score,
            winnerModelTier: winner.modelTier,
            prunedLoserBranches,
            prunedCommitCount: gcRes.prunedCommitIds.length,
            candidates: evaluated,
        };
    }
}
exports.CoWSandboxBranchForker = CoWSandboxBranchForker;
