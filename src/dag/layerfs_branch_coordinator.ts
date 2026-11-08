/**
 * @fileoverview M17 TASK-85: Multi-Agent Branch Coordination (`Layer -> Branch -> Merge -> Publish`),
 * Byte-Identity Merge, Mode Preservation, `.lww` (Last-Write-Wins) Rules & Explicit Conflict Refusal.
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
  } {
    const base = this.driver.getCommit(baseCommitId);
    if (!base) {
      throw new Error(`Base commit '${baseCommitId}' not found`);
    }
    const sourceHead = this.driver.getBranchHeadCommit(sourceBranch);
    const targetHead = this.driver.getBranchHeadCommit(targetBranch);

    const allPaths = new Set<string>([
      ...sourceHead.filesTree.keys(),
      ...targetHead.filesTree.keys(),
    ]);

    const conflicts: string[] = [];
    const autoResolvedByByteIdentity: string[] = [];
    const autoResolvedByLww: string[] = [];
    const modeMergedFiles: string[] = [];

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
    const mergedCommit: LayerFsCommitRecord = {
      commitId: mergedCommitId,
      parentCommitId: targetHead.commitId,
      branchName: targetBranch,
      agentId: sourceHead.agentId,
      stepId: `merge:${sourceBranch}->${targetBranch}`,
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
    };
  }
}
