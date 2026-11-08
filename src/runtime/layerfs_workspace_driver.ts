/**
 * @fileoverview M17 TASK-84: LayerFS Dual-Path Execution Driver
 * - Fast Path (`@layerfs/agent` + `@layerfs/backend`): In-process file tool ops (`read`, `write`, `edit`, `grep`) -> atomic Commit (0 container boot)
 * - Sandbox Path (`@layerfs/runtime`): Real Ephemeral Workspace (`mount -> real child_process exec -> scan diff -> commit -> unmount`)
 */
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as childProcess from "node:child_process";
import { LayerFsContentStoreEngine } from "../checkpoint/layerfs_store_engine.js";

export type LayerFsToolCategory = "FILE_FAST_PATH" | "BASH_SANDBOX_MOUNT";

export interface LayerFsCommitRecord {
  commitId: string;
  parentCommitId: string | null;
  branchName: string;
  agentId: string;
  stepId: string;
  toolCategory: LayerFsToolCategory;
  filesTree: Map<string, { content: string; mode: number; objectId: string; lastStepId: string }>;
  lineBlameMap: Map<string, string[]>;
  includesIgnoredDependencies: boolean;
  mountLatencyMs: number;
}

export class LayerFsWorkspaceDriver {
  public readonly storeEngine: LayerFsContentStoreEngine;
  private readonly commits: Map<string, LayerFsCommitRecord> = new Map();
  private readonly branchHeads: Map<string, string> = new Map();
  private commitSeq = 0;

  constructor(storeEngine = new LayerFsContentStoreEngine()) {
    this.storeEngine = storeEngine;
    this.initMainRoot();
  }

  private initMainRoot(): void {
    const rootCommitId = "C0";
    const filesTree = new Map<string, { content: string; mode: number; objectId: string; lastStepId: string }>();
    const pkgJson = '{"name":"workspace-app","dependencies":{"react":"^19.0.0"}}';
    const nodeModReact = "module.exports = { version: '19.0.0' };";
    const obj1 = this.storeEngine.putFileObject("package.json", pkgJson).object;
    const obj2 = this.storeEngine.putFileObject("node_modules/react/index.js", nodeModReact).object;

    filesTree.set("package.json", {
      content: pkgJson,
      mode: 0o644,
      objectId: obj1.objectId,
      lastStepId: "bootstrap-init",
    });
    filesTree.set("node_modules/react/index.js", {
      content: nodeModReact,
      mode: 0o644,
      objectId: obj2.objectId,
      lastStepId: "bootstrap-init",
    });

    const rootCommit: LayerFsCommitRecord = {
      commitId: rootCommitId,
      parentCommitId: null,
      branchName: "main",
      agentId: "system",
      stepId: "bootstrap-init",
      toolCategory: "FILE_FAST_PATH",
      filesTree,
      lineBlameMap: new Map([
        ["package.json", ["bootstrap-init"]],
        ["node_modules/react/index.js", ["bootstrap-init"]],
      ]),
      includesIgnoredDependencies: true,
      mountLatencyMs: 0,
    };
    this.commits.set(rootCommitId, rootCommit);
    this.branchHeads.set("main", rootCommitId);
  }

  public forkBranch(sourceBranchOrCommit: string, newBranchName: string): {
    branchName: string;
    headCommitId: string;
    copiedFilesCount: 0;
  } {
    const headId = this.branchHeads.get(sourceBranchOrCommit) ?? sourceBranchOrCommit;
    if (!this.commits.has(headId)) {
      throw new Error(`Cannot fork branch '${newBranchName}': source '${sourceBranchOrCommit}' not found`);
    }
    this.branchHeads.set(newBranchName, headId);
    return {
      branchName: newBranchName,
      headCommitId: headId,
      copiedFilesCount: 0,
    };
  }

  public executeInProcessFileTool(params: {
    branchName: string;
    agentId: string;
    stepId: string;
    operation: "read" | "write" | "edit" | "grep";
    filePath: string;
    newContent?: string;
    mode?: number;
  }): {
    commitId: string;
    committedNewSnapshot: boolean;
    containerSpawned: false;
    readResult?: string;
  } {
    const headId = this.branchHeads.get(params.branchName);
    if (!headId) {
      throw new Error(`Branch '${params.branchName}' not found`);
    }
    const parentCommit = this.commits.get(headId)!;
    if (params.operation === "read" || params.operation === "grep") {
      const entry = parentCommit.filesTree.get(params.filePath);
      return {
        commitId: headId,
        committedNewSnapshot: false,
        containerSpawned: false,
        readResult: entry?.content ?? "",
      };
    }

    const content = params.newContent ?? "";
    const prevEntry = parentCommit.filesTree.get(params.filePath);
    if (prevEntry && prevEntry.content === content && (params.mode === undefined || prevEntry.mode === params.mode)) {
      return {
        commitId: headId,
        committedNewSnapshot: false,
        containerSpawned: false,
      };
    }

    const nextCommit = this.createChildCommit({
      parentCommit,
      branchName: params.branchName,
      agentId: params.agentId,
      stepId: params.stepId,
      toolCategory: "FILE_FAST_PATH",
      mountLatencyMs: 0,
      mutations: [
        {
          filePath: params.filePath,
          content,
          mode: params.mode ?? prevEntry?.mode ?? 0o644,
        },
      ],
    });
    return {
      commitId: nextCommit.commitId,
      committedNewSnapshot: true,
      containerSpawned: false,
    };
  }

  public executeEphemeralBashWorkspace(params: {
    branchName: string;
    agentId: string;
    stepId: string;
    command: string;
    overlayMutations?: Array<{ filePath: string; content: string; mode?: number }>;
    uncommittedDirtyExit?: boolean;
  }): {
    commitId: string;
    committedNewSnapshot: boolean;
    mountLatencyMs: number;
    nodeModulesPreMounted: boolean;
    unmountedCleanly: boolean;
    stdout: string;
  } {
    if (params.uncommittedDirtyExit) {
      throw new Error("LayerFS Workspace clean End refused uncommitted dirty state without explicit Commit or Abort");
    }
    const headId = this.branchHeads.get(params.branchName);
    if (!headId) {
      throw new Error(`Branch '${params.branchName}' not found`);
    }
    const parentCommit = this.commits.get(headId)!;

    const tMountStart = performance.now();
    const mountDir = fs.mkdtempSync(path.join(os.tmpdir(), `layerfs-ws-${params.branchName}-`));
    let stdout = "";
    try {
      for (const [relPath, fileEntry] of parentCommit.filesTree.entries()) {
        const fullPath = path.join(mountDir, relPath);
        fs.mkdirSync(path.dirname(fullPath), { recursive: true });
        fs.writeFileSync(fullPath, fileEntry.content, { encoding: "utf-8", mode: fileEntry.mode });
      }
      const mountLatencyMs = Math.max(0.01, Number((performance.now() - tMountStart).toFixed(3)));
      const nodeModulesPreMounted = fs.existsSync(path.join(mountDir, "node_modules/react/index.js"));

      if (params.command && params.command.trim().length > 0) {
        stdout = childProcess
          .execSync(params.command, { cwd: mountDir, stdio: "pipe", timeout: 10_000 })
          .toString("utf-8");
      }

      for (const mut of params.overlayMutations ?? []) {
        const fullPath = path.join(mountDir, mut.filePath);
        fs.mkdirSync(path.dirname(fullPath), { recursive: true });
        fs.writeFileSync(fullPath, mut.content, { encoding: "utf-8", mode: mut.mode ?? 0o644 });
      }

      const discoveredMutations: Array<{ filePath: string; content: string; mode: number }> = [];
      const scanDir = (dir: string): void => {
        for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
          const full = path.join(dir, entry.name);
          if (entry.isDirectory()) {
            scanDir(full);
          } else if (entry.isFile()) {
            const rel = path.relative(mountDir, full).split(path.sep).join("/");
            const content = fs.readFileSync(full, "utf-8");
            const mode = fs.statSync(full).mode & 0o777;
            const prev = parentCommit.filesTree.get(rel);
            if (!prev || prev.content !== content || prev.mode !== mode) {
              discoveredMutations.push({ filePath: rel, content, mode });
            }
          }
        }
      };
      scanDir(mountDir);

      if (discoveredMutations.length === 0) {
        return {
          commitId: headId,
          committedNewSnapshot: false,
          mountLatencyMs,
          nodeModulesPreMounted,
          unmountedCleanly: true,
          stdout,
        };
      }

      const nextCommit = this.createChildCommit({
        parentCommit,
        branchName: params.branchName,
        agentId: params.agentId,
        stepId: params.stepId,
        toolCategory: "BASH_SANDBOX_MOUNT",
        mountLatencyMs,
        mutations: discoveredMutations,
      });

      return {
        commitId: nextCommit.commitId,
        committedNewSnapshot: true,
        mountLatencyMs,
        nodeModulesPreMounted,
        unmountedCleanly: true,
        stdout,
      };
    } finally {
      fs.rmSync(mountDir, { recursive: true, force: true });
    }
  }

  private createChildCommit(params: {
    parentCommit: LayerFsCommitRecord;
    branchName: string;
    agentId: string;
    stepId: string;
    toolCategory: LayerFsToolCategory;
    mountLatencyMs: number;
    mutations: Array<{ filePath: string; content: string; mode: number }>;
  }): LayerFsCommitRecord {
    this.commitSeq += 1;
    const commitId = `C${this.commitSeq}`;
    const filesTree = new Map(params.parentCommit.filesTree);
    const lineBlameMap = new Map<string, string[]>();
    for (const [k, v] of params.parentCommit.lineBlameMap.entries()) {
      lineBlameMap.set(k, [...v]);
    }

    const changedForBtree: Array<{ path: string; objectId: string; mode: number; turnId: string }> = [];
    for (const mut of params.mutations) {
      const prev = filesTree.get(mut.filePath);
      const { object } = this.storeEngine.putFileObject(
        mut.filePath,
        mut.content,
        prev?.content,
        mut.mode
      );
      filesTree.set(mut.filePath, {
        content: mut.content,
        mode: mut.mode,
        objectId: object.objectId,
        lastStepId: params.stepId,
      });

      const oldLines = prev ? prev.content.split("\n") : [];
      const oldBlame = lineBlameMap.get(mut.filePath) ?? [];
      const newLines = mut.content.split("\n");
      const nextBlame = newLines.map((line, idx) =>
        idx < oldLines.length && oldLines[idx] === line
          ? (oldBlame[idx] ?? params.stepId)
          : params.stepId
      );
      lineBlameMap.set(mut.filePath, nextBlame);
      changedForBtree.push({
        path: mut.filePath,
        objectId: object.objectId,
        mode: mut.mode,
        turnId: params.stepId,
      });
    }

    this.storeEngine.updateDirectoryBTreePages(
      commitId,
      params.parentCommit.commitId,
      changedForBtree
    );

    const record: LayerFsCommitRecord = {
      commitId,
      parentCommitId: params.parentCommit.commitId,
      branchName: params.branchName,
      agentId: params.agentId,
      stepId: params.stepId,
      toolCategory: params.toolCategory,
      filesTree,
      lineBlameMap,
      includesIgnoredDependencies: true,
      mountLatencyMs: params.mountLatencyMs,
    };
    this.commits.set(commitId, record);
    this.branchHeads.set(params.branchName, commitId);
    return record;
  }

  public getCommit(commitId: string): LayerFsCommitRecord | undefined {
    return this.commits.get(commitId);
  }

  public getBranchHeadCommit(branchName: string): LayerFsCommitRecord {
    const headId = this.branchHeads.get(branchName);
    if (!headId || !this.commits.has(headId)) {
      throw new Error(`Branch '${branchName}' has no head commit`);
    }
    return this.commits.get(headId)!;
  }

  public setBranchHeadCommit(branchName: string, commitRecord: LayerFsCommitRecord): void {
    this.commits.set(commitRecord.commitId, commitRecord);
    this.branchHeads.set(branchName, commitRecord.commitId);
  }

  public blameFile(branchOrCommitId: string, filePath: string): Array<{ lineNumber: number; text: string; stepId: string }> {
    const commitId = this.branchHeads.get(branchOrCommitId) ?? branchOrCommitId;
    const commit = this.commits.get(commitId);
    if (!commit) {
      throw new Error(`Commit or branch '${branchOrCommitId}' not found`);
    }
    const file = commit.filesTree.get(filePath);
    if (!file) {
      throw new Error(`File '${filePath}' not found in commit '${commitId}'`);
    }
    const lines = file.content.split("\n");
    const blames = commit.lineBlameMap.get(filePath) ?? [];
    return lines.map((text, idx) => ({
      lineNumber: idx + 1,
      text,
      stepId: blames[idx] ?? file.lastStepId,
    }));
  }
}
