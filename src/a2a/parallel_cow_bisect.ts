/**
 * @fileoverview M22 & M57-2: Parallel Copy-on-Write Git Bisect & Speculative Multi-Agent Test Runner.
 * Connected to `LayerFsWorkspaceDriver` for real CoW branch forking, shell test execution, and automatic probe branch cleanup.
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
   * zero-copy branches (`forkBranch`), running `testShellCommand` in each materialized workspace,
   * and automatically cleaning up temporary probe branches (`deleteBranch`) so ephemeral probe
   * objects do not leak or pin orphan CAS blobs.
   */
  public executeLiveLayerFsBisect(params: {
    driver: LayerFsWorkspaceDriver;
    commits: string[];
    testShellCommand: string;
    retainProbeBranches?: boolean;
  }): {
    firstBadCommit: string | null;
    probesExecuted: number;
    probedCommits: Array<{ commitId: string; passed: boolean; branchName: string; cleanedUp: boolean }>;
  } {
    const probedCommits: Array<{ commitId: string; passed: boolean; branchName: string; cleanedUp: boolean }> = [];
    const res = this.identifyFirstBadCommit(params.commits, (commitId) => {
      const branchName = `bisect-probe-${commitId}-${probedCommits.length}`;
      params.driver.forkBranch(commitId, branchName);
      let passed = false;
      try {
        params.driver.executeEphemeralBashWorkspace({
          branchName,
          agentId: "bisect-agent",
          stepId: `bisect-${commitId}`,
          command: params.testShellCommand,
        });
        passed = true;
        return true;
      } catch {
        passed = false;
        return false;
      } finally {
        let cleanedUp = false;
        if (!params.retainProbeBranches) {
          cleanedUp = params.driver.deleteBranch(branchName);
        }
        probedCommits.push({ commitId, passed, branchName, cleanedUp });
      }
    });

    return {
      firstBadCommit: res.firstBadCommit,
      probesExecuted: res.probesExecuted,
      probedCommits,
    };
  }
}
