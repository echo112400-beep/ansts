"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ParallelCowGitBisectCoordinator = void 0; /**
 * @fileoverview M22 Parallel Copy-on-Write Git Bisect & Speculative Multi-Agent Test Runner.
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
}exports.ParallelCowGitBisectCoordinator = ParallelCowGitBisectCoordinator;