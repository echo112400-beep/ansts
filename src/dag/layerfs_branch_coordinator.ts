/**
 * @fileoverview M17 TASK-85, M56-2 & M67-5: Multi-Agent Branch Coordination (`Layer -> Branch -> Merge -> Publish`),
 * Byte-Identity Merge, Mode Preservation, `.lww` (Last-Write-Wins) Rules, Clean File Deletion Propagation,
 * Delete/Modify Conflict Refusal, Idempotent Same-Commit Fast-Forward & Merge B-Tree Directory Page Sync.
 */
import { LayerFsCommitRecord, LayerFsWorkspaceDriver } from "../runtime/layerfs_workspace_driver.js";

export class LayerFsMergeConflictError extends Error {
  public readonly conflictingFiles: string[];
  constructor(conflictingFiles: string[]) {
    super(`LayerFS Conflict: concurrent modifications refused on [${conflictingFiles.join(", ")}]`);
    this.name = "LayerFsMergeConflictError";
    this.conflictingFiles = conflictingFiles;
  }
}

export class LayerFsMultiAgentBranchCoordinator {
  private readonly driver: LayerFsWorkspaceDriver;
  private readonly lwwPatterns: string[];
  private publishedLayerSeq = 0;

  constructor(
    driver: LayerFsWorkspaceDriver,
    lwwPatterns: string[] = ["pnpm-lock.yaml", "package-lock.json", ".cache/", "dist/"]
  ) {
    this.driver = driver;
    this.lwwPatterns = lwwPatterns;
  }

  public isLwwPath(filePath: string): boolean {
    return this.lwwPatterns.some((pat) =>
      pat.endsWith("/") ? filePath.startsWith(pat) : filePath === pat || filePath.endsWith(`/${pat}`)
    );
  }

  public mergeBranchIntoTarget(sourceBranch: string, targetBranch: string, baseCommitId = "C0"): {
    mergedCommitId: string;
    autoResolvedByByteIdentity: string[];
    autoResolvedByLww: string[];
    modeMergedFiles: string[];
    deletedFiles: string[];
  } {
    const base = this.driver.getCommit(baseCommitId);
    if (!base) {
      throw new Error(`Base commit '${baseCommitId}' not found`);
    }
    const sourceHead = this.driver.getBranchHeadCommit(sourceBranch);
    const targetHead = this.driver.getBranchHeadCommit(targetBranch);

    // [M67-5] Idempotent no-op if sourceBranch and targetBranch already point to the exact same commit
    if (sourceHead.commitId === targetHead.commitId) {
      return {
        mergedCommitId: targetHead.commitId,
        autoResolvedByByteIdentity: [],
        autoResolvedByLww: [],
        modeMergedFiles: [],
        deletedFiles: [],
      };
    }

    const allPaths = new Set<string>([
      ...base.filesTree.keys(),
      ...sourceHead.filesTree.keys(),
      ...targetHead.filesTree.keys(),
    ]);

    const conflicts: string[] = [];
    const autoResolvedByByteIdentity: string[] = [];
    const autoResolvedByLww: string[] = [];
    const modeMergedFiles: string[] = [];
    const deletedFiles: string[] = [];

    const mergedTree = new Map(targetHead.filesTree);
    const mergedBlame = new Map(targetHead.lineBlameMap);

    for (const filePath of allPaths) {
      const baseFile = base.filesTree.get(filePath);
      const srcFile = sourceHead.filesTree.get(filePath);
      const tgtFile = targetHead.filesTree.get(filePath);

      const srcDeleted = Boolean(baseFile && !srcFile);
      const tgtDeleted = Boolean(baseFile && !tgtFile);
      const srcChangedContent = srcFile?.content !== baseFile?.content;
      const tgtChangedContent = tgtFile?.content !== baseFile?.content;
      const srcChangedMode = Boolean(srcFile && baseFile && srcFile.mode !== baseFile.mode);
      const tgtChangedMode = Boolean(tgtFile && baseFile && tgtFile.mode !== baseFile.mode);

      // [M56-2] Handle file deletions and delete/modify conflicts in 3-way merge
      if (srcDeleted && tgtDeleted) {
        mergedTree.delete(filePath);
        mergedBlame.delete(filePath);
        deletedFiles.push(filePath);
        continue;
      }
      if (srcDeleted) {
        if (!tgtChangedContent && !tgtChangedMode) {
          mergedTree.delete(filePath);
          mergedBlame.delete(filePath);
          deletedFiles.push(filePath);
        } else if (this.isLwwPath(filePath)) {
          autoResolvedByLww.push(filePath);
          mergedTree.delete(filePath);
          mergedBlame.delete(filePath);
          deletedFiles.push(filePath);
        } else {
          conflicts.push(filePath);
        }
        continue;
      }
      if (tgtDeleted) {
        if (!srcChangedContent && !srcChangedMode) {
          mergedTree.delete(filePath);
          mergedBlame.delete(filePath);
          deletedFiles.push(filePath);
        } else if (this.isLwwPath(filePath) && srcFile) {
          autoResolvedByLww.push(filePath);
          mergedTree.set(filePath, { ...srcFile });
        } else {
          conflicts.push(filePath);
        }
        continue;
      }

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
            mode: tgtFile.mode,
          });
        } else if (!srcChangedContent && srcChangedMode && tgtChangedContent) {
          modeMergedFiles.push(filePath);
          mergedTree.set(filePath, {
            ...tgtFile,
            mode: srcFile.mode,
          });
        }
      }
    }

    if (conflicts.length > 0) {
      throw new LayerFsMergeConflictError(conflicts);
    }

    this.publishedLayerSeq += 1;
    const mergedCommitId = `L${this.publishedLayerSeq}-${sourceHead.commitId}-${targetHead.commitId}`;
    const stepId = `merge:${sourceBranch}->${targetBranch}`;
    const changedForBtree = Array.from(mergedTree.entries()).map(([pathKey, entry]) => ({
      path: pathKey,
      objectId: entry.objectId,
      mode: entry.mode,
      turnId: entry.lastStepId || stepId,
    }));
    this.driver.storeEngine.updateDirectoryBTreePages(
      mergedCommitId,
      targetHead.commitId,
      changedForBtree
    );

    const mergedCommit: LayerFsCommitRecord = {
      commitId: mergedCommitId,
      parentCommitId: targetHead.commitId,
      branchName: targetBranch,
      agentId: sourceHead.agentId,
      stepId,
      toolCategory: "FILE_FAST_PATH",
      filesTree: mergedTree,
      lineBlameMap: mergedBlame,
      includesIgnoredDependencies: true,
      mountLatencyMs: 0,
    };
    this.driver.setBranchHeadCommit(targetBranch, mergedCommit);
    return {
      mergedCommitId,
      autoResolvedByByteIdentity,
      autoResolvedByLww,
      modeMergedFiles,
      deletedFiles,
    };
  }
}
