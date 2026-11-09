"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ParallelCowGitBisectCoordinator = void 0; /**
 * @fileoverview M22 Parallel Copy-on-Write Git Bisect & Speculative Multi-Agent Test Runner.
 * Connected to `LayerFsWorkspaceDriver` for real CoW branch forking and shell test execution.
 */


class ParallelCowGitBisectCoordinator {
  identifyFirstBadCommit(
  commits,
  predicateIsGood)
  {
    let low = 0;
    let high = commits.length - 1;
    let firstBadIdx = -1;
    let probesExecuted = 0;

    while (low <= high) {
      const mid = low + high >> 1;
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
      probesExecuted
    };
  }

  /**
   * Executes a live binary-search CoW bisect across real LayerFS commits by forking ephemeral
   * zero-copy branches (`forkCommitBranch`) and running `testShellCommand` in each materialized workspace.
   */
  executeLiveLayerFsBisect(params)







  {
    const probedCommits = [];
    const res = this.identifyFirstBadCommit(params.commits, (commitId) => {
      const branchName = `bisect-probe-${commitId}-${probedCommits.length}`;
      params.driver.forkBranch(commitId, branchName);
      try {
        params.driver.executeEphemeralBashWorkspace({
          branchName,
          agentId: "bisect-agent",
          stepId: `bisect-${commitId}`,
          command: params.testShellCommand
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
      probedCommits
    };
  }
}exports.ParallelCowGitBisectCoordinator = ParallelCowGitBisectCoordinator;