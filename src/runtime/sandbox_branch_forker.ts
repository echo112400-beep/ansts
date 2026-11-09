/**
 * @fileoverview M22 & M37 Copy-on-Write Sandbox Branch Forker for Parallel Speculative & Deterministic Execution.
 */
import { LayerFsWorkspaceDriver } from "./layerfs_workspace_driver.js";
import { DeterministicExecutionEnvelope } from "./deterministic_envelope.js";

export class CoWSandboxBranchForker {
  public readonly driver: LayerFsWorkspaceDriver;

  constructor(driver = new LayerFsWorkspaceDriver()) {
    this.driver = driver;
  }

  public forkParallelBranches(sourceBranch: string, branchNames: string[]): Array<{
    branchName: string;
    headCommitId: string;
    copiedFilesCount: 0;
  }> {
    return branchNames.map((name) => this.driver.forkBranch(sourceBranch, name));
  }

  /**
   * Forks N speculative CoW branches from `sourceBranch`, runs `command` inside each branch
   * under a `DeterministicExecutionEnvelope(seed)`, and returns each branch's commit & stdout.
   */
  public executeDeterministicSpeculativeBranches(params: {
    sourceBranch: string;
    branches: Array<{ branchName: string; command: string; seed?: number }>;
  }): Array<{
    branchName: string;
    commitId: string;
    seed: number;
    stdout: string;
  }> {
    return params.branches.map((b, idx) => {
      try {
        this.driver.getBranchHeadCommit(b.branchName);
      } catch {
        this.driver.forkBranch(params.sourceBranch, b.branchName);
      }
      const seed = b.seed ?? 42;
      const envEnvelope = new DeterministicExecutionEnvelope(seed);
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
}
