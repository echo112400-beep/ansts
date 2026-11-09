/**
 * @fileoverview M22 Copy-on-Write Sandbox Branch Forker for Parallel Speculative Execution.
 */
import { LayerFsWorkspaceDriver } from "./layerfs_workspace_driver.js";

export class CoWSandboxBranchForker {
  private readonly driver: LayerFsWorkspaceDriver;

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
}
