"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LayerFsWorkspaceDriver = void 0;




var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var childProcess = _interopRequireWildcard(require("node:child_process"));
var _layerfs_store_engine = require("../checkpoint/layerfs_store_engine.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M17 TASK-84: LayerFS Dual-Path Execution Driver
 * - Fast Path (`@layerfs/agent` + `@layerfs/backend`): In-process file tool ops (`read`, `write`, `edit`, `grep`) -> atomic Commit (0 container boot)
 * - Sandbox Path (`@layerfs/runtime`): Real Ephemeral Workspace (`mount -> real child_process exec -> scan diff -> commit -> unmount`)
 */












class LayerFsWorkspaceDriver {
  storeEngine;
  commits = new Map();
  branchHeads = new Map();
  commitSeq = 0;

  constructor(storeEngine = new _layerfs_store_engine.LayerFsContentStoreEngine()) {
    this.storeEngine = storeEngine;
    this.initMainRoot();
  }

  initMainRoot() {
    const rootCommitId = "C0";
    const filesTree = new Map();
    const pkgJson = '{"name":"workspace-app","dependencies":{"react":"^19.0.0"}}';
    const nodeModReact = "module.exports = { version: '19.0.0' };";
    const obj1 = this.storeEngine.putFileObject("package.json", pkgJson).object;
    const obj2 = this.storeEngine.putFileObject("node_modules/react/index.js", nodeModReact).object;

    filesTree.set("package.json", {
      content: pkgJson,
      mode: 0o644,
      objectId: obj1.objectId,
      lastStepId: "bootstrap-init"
    });
    filesTree.set("node_modules/react/index.js", {
      content: nodeModReact,
      mode: 0o644,
      objectId: obj2.objectId,
      lastStepId: "bootstrap-init"
    });

    const rootCommit = {
      commitId: rootCommitId,
      parentCommitId: null,
      branchName: "main",
      agentId: "system",
      stepId: "bootstrap-init",
      toolCategory: "FILE_FAST_PATH",
      filesTree,
      lineBlameMap: new Map([
      ["package.json", ["bootstrap-init"]],
      ["node_modules/react/index.js", ["bootstrap-init"]]]
      ),
      includesIgnoredDependencies: true,
      mountLatencyMs: 0
    };
    this.commits.set(rootCommitId, rootCommit);
    this.branchHeads.set("main", rootCommitId);
  }

  forkBranch(sourceBranchOrCommit, newBranchName)



  {
    const headId = this.branchHeads.get(sourceBranchOrCommit) ?? sourceBranchOrCommit;
    if (!this.commits.has(headId)) {
      throw new Error(`Cannot fork branch '${newBranchName}': source '${sourceBranchOrCommit}' not found`);
    }
    this.branchHeads.set(newBranchName, headId);
    return {
      branchName: newBranchName,
      headCommitId: headId,
      copiedFilesCount: 0
    };
  }

  executeInProcessFileTool(params)












  {
    const headId = this.branchHeads.get(params.branchName);
    if (!headId) {
      throw new Error(`Branch '${params.branchName}' not found`);
    }
    const parentCommit = this.commits.get(headId);
    if (params.operation === "read" || params.operation === "grep") {
      const entry = parentCommit.filesTree.get(params.filePath);
      return {
        commitId: headId,
        committedNewSnapshot: false,
        containerSpawned: false,
        readResult: entry?.content ?? ""
      };
    }

    const content = params.newContent ?? "";
    const prevEntry = parentCommit.filesTree.get(params.filePath);
    if (prevEntry && prevEntry.content === content && (params.mode === undefined || prevEntry.mode === params.mode)) {
      return {
        commitId: headId,
        committedNewSnapshot: false,
        containerSpawned: false
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
        mode: params.mode ?? prevEntry?.mode ?? 0o644
      }]

    });
    return {
      commitId: nextCommit.commitId,
      committedNewSnapshot: true,
      containerSpawned: false
    };
  }

  executeEphemeralBashWorkspace(params)













  {
    if (params.uncommittedDirtyExit) {
      throw new Error("LayerFS Workspace clean End refused uncommitted dirty state without explicit Commit or Abort");
    }
    const headId = this.branchHeads.get(params.branchName);
    if (!headId) {
      throw new Error(`Branch '${params.branchName}' not found`);
    }
    const parentCommit = this.commits.get(headId);

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
        stdout = childProcess.
        execSync(params.command, { cwd: mountDir, stdio: "pipe", timeout: 10_000 }).
        toString("utf-8");
      }

      for (const mut of params.overlayMutations ?? []) {
        const fullPath = path.join(mountDir, mut.filePath);
        fs.mkdirSync(path.dirname(fullPath), { recursive: true });
        fs.writeFileSync(fullPath, mut.content, { encoding: "utf-8", mode: mut.mode ?? 0o644 });
      }

      const discoveredMutations = [];
      const scanDir = (dir) => {
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
          stdout
        };
      }

      const nextCommit = this.createChildCommit({
        parentCommit,
        branchName: params.branchName,
        agentId: params.agentId,
        stepId: params.stepId,
        toolCategory: "BASH_SANDBOX_MOUNT",
        mountLatencyMs,
        mutations: discoveredMutations
      });

      return {
        commitId: nextCommit.commitId,
        committedNewSnapshot: true,
        mountLatencyMs,
        nodeModulesPreMounted,
        unmountedCleanly: true,
        stdout
      };
    } finally {
      fs.rmSync(mountDir, { recursive: true, force: true });
    }
  }

  createChildCommit(params)







  {
    this.commitSeq += 1;
    const commitId = `C${this.commitSeq}`;
    const filesTree = new Map(params.parentCommit.filesTree);
    const lineBlameMap = new Map();
    for (const [k, v] of params.parentCommit.lineBlameMap.entries()) {
      lineBlameMap.set(k, [...v]);
    }

    const changedForBtree = [];
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
        lastStepId: params.stepId
      });

      const oldLines = prev ? prev.content.split("\n") : [];
      const oldBlame = lineBlameMap.get(mut.filePath) ?? [];
      const newLines = mut.content.split("\n");
      const nextBlame = newLines.map((line, idx) =>
      idx < oldLines.length && oldLines[idx] === line ?
      oldBlame[idx] ?? params.stepId :
      params.stepId
      );
      lineBlameMap.set(mut.filePath, nextBlame);
      changedForBtree.push({
        path: mut.filePath,
        objectId: object.objectId,
        mode: mut.mode,
        turnId: params.stepId
      });
    }

    this.storeEngine.updateDirectoryBTreePages(
      commitId,
      params.parentCommit.commitId,
      changedForBtree
    );

    const record = {
      commitId,
      parentCommitId: params.parentCommit.commitId,
      branchName: params.branchName,
      agentId: params.agentId,
      stepId: params.stepId,
      toolCategory: params.toolCategory,
      filesTree,
      lineBlameMap,
      includesIgnoredDependencies: true,
      mountLatencyMs: params.mountLatencyMs
    };
    this.commits.set(commitId, record);
    this.branchHeads.set(params.branchName, commitId);
    return record;
  }

  getCommit(commitId) {
    return this.commits.get(commitId);
  }

  getBranchHeadCommit(branchName) {
    const headId = this.branchHeads.get(branchName);
    if (!headId || !this.commits.has(headId)) {
      throw new Error(`Branch '${branchName}' has no head commit`);
    }
    return this.commits.get(headId);
  }

  setBranchHeadCommit(branchName, commitRecord) {
    this.commits.set(commitRecord.commitId, commitRecord);
    this.branchHeads.set(branchName, commitRecord.commitId);
  }

  blameFile(branchOrCommitId, filePath) {
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
      stepId: blames[idx] ?? file.lastStepId
    }));
  }
}exports.LayerFsWorkspaceDriver = LayerFsWorkspaceDriver;