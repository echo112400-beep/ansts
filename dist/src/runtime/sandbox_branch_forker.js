"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CoWSandboxBranchForker = void 0;


var _layerfs_workspace_driver = require("./layerfs_workspace_driver.js");
var _deterministic_envelope = require("./deterministic_envelope.js"); /**
 * @fileoverview M22 & M37 Copy-on-Write Sandbox Branch Forker for Parallel Speculative & Deterministic Execution.
 */class CoWSandboxBranchForker {
  driver;

  constructor(driver = new _layerfs_workspace_driver.LayerFsWorkspaceDriver()) {
    this.driver = driver;
  }

  forkParallelBranches(sourceBranch, branchNames)



  {
    return branchNames.map((name) => this.driver.forkBranch(sourceBranch, name));
  }

  /**
   * Forks N speculative CoW branches from `sourceBranch`, runs `command` inside each branch
   * under a `DeterministicExecutionEnvelope(seed)`, and returns each branch's commit & stdout.
   */
  executeDeterministicSpeculativeBranches(params)







  {
    return params.branches.map((b, idx) => {
      try {
        this.driver.getBranchHeadCommit(b.branchName);
      } catch {
        this.driver.forkBranch(params.sourceBranch, b.branchName);
      }
      const seed = b.seed ?? 42;
      const envEnvelope = new _deterministic_envelope.DeterministicExecutionEnvelope(seed);
      const wrappedCmd = `${envEnvelope.buildDeterministicShellExportPrefix(0)}${b.command}`;
      const execRes = this.driver.executeEphemeralBashWorkspace({
        branchName: b.branchName,
        agentId: `speculative-agent-${idx}`,
        stepId: `spec-step-${b.branchName}`,
        command: wrappedCmd
      });
      return {
        branchName: b.branchName,
        commitId: execRes.commitId,
        seed,
        stdout: execRes.stdout
      };
    });
  }
}exports.CoWSandboxBranchForker = CoWSandboxBranchForker;