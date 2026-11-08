"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LayerFsMultiAgentBranchCoordinator = exports.LayerFsMergeConflictError = void 0; /**
 * @fileoverview M17 TASK-85: Multi-Agent Branch Coordination (`Layer -> Branch -> Merge -> Publish`),
 * Byte-Identity Merge, Mode Preservation, `.lww` (Last-Write-Wins) Rules & Explicit Conflict Refusal.
 */


class LayerFsMergeConflictError extends Error {
  conflictingFiles;
  constructor(conflictingFiles) {
    super(`LayerFS Conflict: concurrent modifications refused on [${conflictingFiles.join(", ")}]`);
    this.name = "LayerFsMergeConflictError";
    this.conflictingFiles = conflictingFiles;
  }
}exports.LayerFsMergeConflictError = LayerFsMergeConflictError;

class LayerFsMultiAgentBranchCoordinator {
  driver;
  lwwPatterns;
  publishedLayerSeq = 0;

  constructor(
  driver,
  lwwPatterns = ["pnpm-lock.yaml", "package-lock.json", ".cache/", "dist/"])
  {
    this.driver = driver;
    this.lwwPatterns = lwwPatterns;
  }

  isLwwPath(filePath) {
    return this.lwwPatterns.some((pat) =>
    pat.endsWith("/") ? filePath.startsWith(pat) : filePath === pat || filePath.endsWith(`/${pat}`));

  }

  mergeBranchIntoTarget(sourceBranch, targetBranch, baseCommitId = "C0")




  {
    const base = this.driver.getCommit(baseCommitId);
    if (!base) {
      throw new Error(`Base commit '${baseCommitId}' not found`);
    }
    const sourceHead = this.driver.getBranchHeadCommit(sourceBranch);
    const targetHead = this.driver.getBranchHeadCommit(targetBranch);

    const allPaths = new Set([
    ...sourceHead.filesTree.keys(),
    ...targetHead.filesTree.keys()]);


    const conflicts = [];
    const autoResolvedByByteIdentity = [];
    const autoResolvedByLww = [];
    const modeMergedFiles = [];

    const mergedTree = new Map(targetHead.filesTree);
    const mergedBlame = new Map(targetHead.lineBlameMap);

    for (const filePath of allPaths) {
      const baseFile = base.filesTree.get(filePath);
      const srcFile = sourceHead.filesTree.get(filePath);
      const tgtFile = targetHead.filesTree.get(filePath);

      const srcChangedContent = srcFile?.content !== baseFile?.content;
      const tgtChangedContent = tgtFile?.content !== baseFile?.content;
      const srcChangedMode = srcFile && baseFile && srcFile.mode !== baseFile.mode;
      const tgtChangedMode = tgtFile && baseFile && tgtFile.mode !== baseFile.mode;

      if (!srcChangedContent && !srcChangedMode) {
        continue;
      }
      if (!tgtChangedContent && !tgtChangedMode && srcFile) {
        mergedTree.set(filePath, { ...srcFile });
        const b = sourceHead.lineBlameMap.get(filePath);
        if (b) mergedBlame.set(filePath, [...b]);
        continue;
      }

      if (srcFile && tgtFile) {
        if (srcChangedContent && tgtChangedContent) {
          if (srcFile.content === tgtFile.content) {
            autoResolvedByByteIdentity.push(filePath);
            mergedTree.set(filePath, { ...srcFile });
          } else if (this.isLwwPath(filePath)) {
            autoResolvedByLww.push(filePath);
            mergedTree.set(filePath, { ...srcFile });
          } else {
            conflicts.push(filePath);
          }
        } else if (srcChangedContent && !tgtChangedContent && tgtChangedMode) {
          modeMergedFiles.push(filePath);
          mergedTree.set(filePath, {
            ...srcFile,
            mode: tgtFile.mode
          });
        } else if (!srcChangedContent && srcChangedMode && tgtChangedContent) {
          modeMergedFiles.push(filePath);
          mergedTree.set(filePath, {
            ...tgtFile,
            mode: srcFile.mode
          });
        }
      }
    }

    if (conflicts.length > 0) {
      throw new LayerFsMergeConflictError(conflicts);
    }

    this.publishedLayerSeq += 1;
    const mergedCommitId = `L${this.publishedLayerSeq}-${sourceHead.commitId}-${targetHead.commitId}`;
    const mergedCommit = {
      commitId: mergedCommitId,
      parentCommitId: targetHead.commitId,
      branchName: targetBranch,
      agentId: sourceHead.agentId,
      stepId: `merge:${sourceBranch}->${targetBranch}`,
      toolCategory: "FILE_FAST_PATH",
      filesTree: mergedTree,
      lineBlameMap: mergedBlame,
      includesIgnoredDependencies: true,
      mountLatencyMs: 0
    };
    this.driver.setBranchHeadCommit(targetBranch, mergedCommit);
    return {
      mergedCommitId,
      autoResolvedByByteIdentity,
      autoResolvedByLww,
      modeMergedFiles
    };
  }
}exports.LayerFsMultiAgentBranchCoordinator = LayerFsMultiAgentBranchCoordinator;