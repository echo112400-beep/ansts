/**
 * @fileoverview M22 Parallel Copy-on-Write Git Bisect & Speculative Multi-Agent Test Runner.
 * Connected to `LayerFsWorkspaceDriver` for real CoW branch forking and shell test execution.
 */
import { LayerFsWorkspaceDriver } from "../runtime/layerfs_workspace_driver.js";

export class ParallelCowGitBisectCoordinator {
  public identifyFirstBadCommit(
    commits: string[],
    predicateIsGood: (commitId: string) => boolean
  ): { firstBadCommit: string | null; probesExecuted: number } {
    let low = 0;
    let high = commits.length - 1;
    let firstBadIdx = -1;
    let probesExecuted = 0;

    while (low <= high) {
      const mid = (low + high) >> 1;
      probesExecuted += 1;
      if (predicateIsGood(commits[mid])) {
        low = mid + 1;
      } else {
        firstBadIdx = mid;
        high = mid - 1;
      }
    }
    return {
      firstBadCommit: firstBadIdx >= 0 ? commits[firstBadIdx] : null,
      probesExecuted,
    };
  }

  /**
   * Executes a live binary-search CoW bisect across real LayerFS commits by forking ephemeral
   * zero-copy branches (`forkCommitBranch`) and running `testShellCommand` in each materialized workspace.
   */
  public executeLiveLayerFsBisect(params: {
    driver: LayerFsWorkspaceDriver;
    commits: string[];
    testShellCommand: string;
  }): {
    firstBadCommit: string | null;
    probesExecuted: number;
    probedCommits: Array<{ commitId: string; passed: boolean; branchName: string }>;
  } {
    const probedCommits: Array<{ commitId: string; passed: boolean; branchName: string }> = [];
    const res = this.identifyFirstBadCommit(params.commits, (commitId) => {
      const branchName = `bisect-probe-${commitId}-${probedCommits.length}`;
      params.driver.forkBranch(commitId, branchName);
      try {
        params.driver.executeEphemeralBashWorkspace({
          branchName,
          agentId: "bisect-agent",
          stepId: `bisect-${commitId}`,
          command: params.testShellCommand,
        });
        probedCommits.push({ commitId, passed: true, branchName });
        return true;
      } catch {
        probedCommits.push({ commitId, passed: false, branchName });
        return false;
      }
    });

    return {
      firstBadCommit: res.firstBadCommit,
      probesExecuted: res.probesExecuted,
      probedCommits,
    };
  }
}
