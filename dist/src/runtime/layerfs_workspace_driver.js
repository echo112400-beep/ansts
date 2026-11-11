"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LayerFsWorkspaceDriver = void 0;






var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var childProcess = _interopRequireWildcard(require("node:child_process"));
var _layerfs_store_engine = require("../checkpoint/layerfs_store_engine.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M17 TASK-84, M61-5 & M66-3: LayerFS Dual-Path Execution Driver
 * - Fast Path (`@layerfs/agent` + `@layerfs/backend`): In-process file tool ops (`read`, `write`, `edit`, `grep`) -> atomic Commit (0 container boot)
 * - Sandbox Path (`@layerfs/runtime`): Real Ephemeral Workspace (`mount -> real child_process exec -> scan diff -> commit -> unmount`)
 * - [M61-5] Step-Level Idempotent Commit Deduplication & Branch Breakpoint Rollback (`rollbackBranchToCommitOrStep`).
 * - [M66-3] Workspace Path-Traversal Sanitization (`normalizeSafeWorkspacePath`) & Unreachable Commit/B-Tree GC (`gcUnreachableCommits`).
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

  /**
   * [M66-3] Normalizes and validates workspace-relative file paths, rejecting `..` path traversal,
   * leading `/` or drive roots, and null bytes so file operations cannot escape `mountDir`.
   */
  static normalizeSafeWorkspacePath(rawPath) {
    if (typeof rawPath !== "string" || rawPath.trim().length === 0 || rawPath.includes("\0")) {
      throw new Error(`Unsafe workspace filePath rejected: '${rawPath}'`);
    }
    const slashed = rawPath.replace(/\\/g, "/").trim();
    if (slashed.startsWith("/") || /^[a-zA-Z]:/.test(slashed)) {
      throw new Error(`Unsafe workspace filePath rejected (absolute path): '${rawPath}'`);
    }
    const segments = slashed.split("/");
    if (segments.some((seg) => seg === "..")) {
      throw new Error(`Unsafe workspace filePath rejected (directory traversal): '${rawPath}'`);
    }
    const normalized = path.posix.normalize(slashed).replace(/^(\.\/)+/, "");
    if (
    !normalized ||
    normalized === "." ||
    normalized === ".." ||
    normalized.startsWith("../") ||
    normalized.startsWith("/"))
    {
      throw new Error(`Unsafe workspace filePath rejected: '${rawPath}'`);
    }
    return normalized;
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
      ["node_modules/react/index.js", ["bootstrap-init"]]]),

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

  /**
   * [M61-5] Rolls back a branch head to a historical `commitId` or `stepId` breakpoint so uncommitted
   * or rolled-back turns leave zero orphan workspace mutations on the branch.
   */
  rollbackBranchToCommitOrStep(
  branchName,
  targetCommitOrStepId)
  {
    const headId = this.branchHeads.get(branchName);
    if (!headId) {
      throw new Error(`Branch '${branchName}' not found`);
    }
    let cur = this.commits.get(headId);
    while (cur) {
      if (cur.commitId === targetCommitOrStepId || cur.stepId === targetCommitOrStepId) {
        this.branchHeads.set(branchName, cur.commitId);
        return {
          branchName,
          restoredCommitId: cur.commitId,
          restoredStepId: cur.stepId
        };
      }
      cur = cur.parentCommitId ? this.commits.get(cur.parentCommitId) : undefined;
    }
    throw new Error(
    `Cannot rollback branch '${branchName}': target commit or step '${targetCommitOrStepId}' not found in ancestry`);

  }

  executeInProcessFileTool(params)












  {
    const safeFilePath = LayerFsWorkspaceDriver.normalizeSafeWorkspacePath(params.filePath);
    const headId = this.branchHeads.get(params.branchName);
    if (!headId) {
      throw new Error(`Branch '${params.branchName}' not found`);
    }
    const parentCommit = this.commits.get(headId);
    if (params.operation === "read" || params.operation === "grep") {
      const entry = parentCommit.filesTree.get(safeFilePath);
      return {
        commitId: headId,
        committedNewSnapshot: false,
        containerSpawned: false,
        readResult: entry?.content ?? ""
      };
    }

    if (params.operation === "delete") {
      if (!parentCommit.filesTree.has(safeFilePath)) {
        return { commitId: headId, committedNewSnapshot: false, containerSpawned: false };
      }
      const nextCommit = this.createChildCommit({
        parentCommit,
        branchName: params.branchName,
        agentId: params.agentId,
        stepId: params.stepId,
        toolCategory: "FILE_FAST_PATH",
        mountLatencyMs: 0,
        mutations: [],
        deletions: [safeFilePath]
      });
      return { commitId: nextCommit.commitId, committedNewSnapshot: true, containerSpawned: false };
    }
    const content = params.newContent ?? "";
    const prevEntry = parentCommit.filesTree.get(safeFilePath);
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
        filePath: safeFilePath,
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
    const sanitizedOverlays = (params.overlayMutations ?? []).map((mut) => ({
      filePath: LayerFsWorkspaceDriver.normalizeSafeWorkspacePath(mut.filePath),
      content: mut.content,
      mode: mut.mode
    }));

    const tMountStart = performance.now();
    const mountDir = fs.mkdtempSync(path.join(os.tmpdir(), `layerfs-ws-${params.branchName}-`));
    let stdout = "";
    try {
      const baselineStats = new Map();
      for (const [relPath, fileEntry] of parentCommit.filesTree.entries()) {
        const safeRel = LayerFsWorkspaceDriver.normalizeSafeWorkspacePath(relPath);
        const fullPath = path.join(mountDir, safeRel);
        fs.mkdirSync(path.dirname(fullPath), { recursive: true });
        fs.writeFileSync(fullPath, fileEntry.content, { encoding: "utf-8", mode: fileEntry.mode });
        try {
          fs.utimesSync(fullPath, 1700000000, 1700000000);
        } catch {

          // ignore if utimes unsupported
        }const st = fs.statSync(fullPath);
        baselineStats.set(safeRel, { size: st.size, mode: st.mode & 0o777 });
      }
      const mountLatencyMs = Math.max(0.01, Number((performance.now() - tMountStart).toFixed(3)));
      const nodeModulesPreMounted = fs.existsSync(path.join(mountDir, "node_modules/react/index.js"));

      if (params.command && params.command.trim().length > 0) {
        stdout = childProcess.
        execSync(params.command, { cwd: mountDir, stdio: "pipe", timeout: 10_000 }).
        toString("utf-8");
      }

      for (const mut of sanitizedOverlays) {
        const fullPath = path.join(mountDir, mut.filePath);
        fs.mkdirSync(path.dirname(fullPath), { recursive: true });
        fs.writeFileSync(fullPath, mut.content, { encoding: "utf-8", mode: mut.mode ?? 0o644 });
      }

      const discoveredMutations = [];
      const seenFilesOnDisk = new Set();
      const scanDir = (dir) => {
        for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
          const full = path.join(dir, entry.name);
          if (entry.isDirectory()) {
            scanDir(full);
          } else if (entry.isFile()) {
            const rel = path.relative(mountDir, full).split(path.sep).join("/");
            seenFilesOnDisk.add(rel);
            const st = fs.statSync(full);
            const mode = st.mode & 0o777;
            const baseSt = baselineStats.get(rel);
            const prev = parentCommit.filesTree.get(rel);
            if (prev && baseSt && Math.floor(st.mtimeMs / 1000) === 1700000000 && st.size === baseSt.size && mode === baseSt.mode) {
              continue;
            }
            const content = fs.readFileSync(full, "utf-8");
            if (!prev || prev.content !== content || prev.mode !== mode) {
              discoveredMutations.push({ filePath: rel, content, mode });
            }
          }
        }
      };
      scanDir(mountDir);
      const discoveredDeletions = [];
      for (const existingPath of parentCommit.filesTree.keys()) {
        if (!seenFilesOnDisk.has(existingPath)) {
          discoveredDeletions.push(existingPath);
        }
      }

      if (discoveredMutations.length === 0 && discoveredDeletions.length === 0) {
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
        mutations: discoveredMutations,
        deletions: discoveredDeletions
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

    for (const delPath of params.deletions ?? []) {
      filesTree.delete(delPath);
      lineBlameMap.delete(delPath);
    }
    const changedForBtree = [];
    for (const mut of params.mutations) {
      const prev = filesTree.get(mut.filePath);
      const { object } = this.storeEngine.putFileObject(
      mut.filePath,
      mut.content,
      prev?.content,
      mut.mode);

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
      params.stepId);

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
    changedForBtree);


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

  getCommitCount() {
    return this.commits.size;
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

  exportBranchBundle(branchName)













  {
    const headRecord = this.getBranchHeadCommit(branchName);
    const chain = [];
    let cur = headRecord;
    while (cur) {
      chain.unshift(cur);
      cur = cur.parentCommitId ? this.commits.get(cur.parentCommitId) : undefined;
    }
    return {
      branchName,
      headCommitId: headRecord.commitId,
      commits: chain.map((c) => ({
        commitId: c.commitId,
        parentCommitId: c.parentCommitId,
        branchName: c.branchName,
        agentId: c.agentId,
        stepId: c.stepId,
        toolCategory: c.toolCategory,
        files: Array.from(c.filesTree.entries()),
        blame: Array.from(c.lineBlameMap.entries()),
        mountLatencyMs: c.mountLatencyMs
      }))
    };
  }

  importBranchBundle(bundle) {
    const idRemap = new Map();
    for (const c of bundle.commits) {
      let targetId = c.commitId;
      const existing = this.commits.get(c.commitId);
      if (existing && c.commitId !== "C0" && (existing.branchName !== c.branchName || existing.stepId !== c.stepId)) {
        targetId = `${c.commitId}-${bundle.branchName}`;
      }
      idRemap.set(c.commitId, targetId);
    }
    for (const c of bundle.commits) {
      const mappedCommitId = idRemap.get(c.commitId) ?? c.commitId;
      const mappedParentId = c.parentCommitId ? idRemap.get(c.parentCommitId) ?? c.parentCommitId : null;
      const filesTree = new Map(c.files);
      const lineBlameMap = new Map(c.blame);
      for (const [relPath, entry] of filesTree.entries()) {
        this.storeEngine.putFileObject(relPath, entry.content, undefined, entry.mode);
      }
      const record = {
        commitId: mappedCommitId,
        parentCommitId: mappedParentId,
        branchName: c.branchName,
        agentId: c.agentId,
        stepId: c.stepId,
        toolCategory: c.toolCategory,
        filesTree,
        lineBlameMap,
        includesIgnoredDependencies: true,
        mountLatencyMs: c.mountLatencyMs
      };
      this.commits.set(mappedCommitId, record);
      const num = Number(c.commitId.replace(/^C/, ""));
      if (!Number.isNaN(num) && num > this.commitSeq) {
        this.commitSeq = num;
      }
    }
    this.branchHeads.set(bundle.branchName, idRemap.get(bundle.headCommitId) ?? bundle.headCommitId);
  }

  async pushBranchToRemoteS3(branchName, s3Endpoint) {
    try {
      const bundle = this.exportBranchBundle(branchName);
      const resp = await fetch(
      `${s3Endpoint.replace(/\/$/, "")}/ansts-fleet/layerfs/branches/${encodeURIComponent(branchName)}.json`,
      {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(bundle)
      });

      return resp.ok;
    } catch {
      return false;
    }
  }

  async pullBranchFromRemoteS3(branchName, s3Endpoint) {
    try {
      const resp = await fetch(
      `${s3Endpoint.replace(/\/$/, "")}/ansts-fleet/layerfs/branches/${encodeURIComponent(branchName)}.json`);

      if (!resp.ok) return false;
      const bundle = await resp.json();
      if (bundle && Array.isArray(bundle.commits)) {
        this.importBranchBundle(bundle);
        return true;
      }
      return false;
    } catch {
      return false;
    }
  }

  deleteBranch(branchName) {
    if (branchName === "main") {
      throw new Error("Cannot delete default branch 'main'");
    }
    return this.branchHeads.delete(branchName);
  }

  /**
   * [M66-3] Reclaims unreachable `LayerFsCommitRecord` nodes and their corresponding B-Tree directory pages
   * after branches are deleted or rolled back.
   */
  gcUnreachableCommits()



  {
    const reachableCommitIds = new Set(["C0"]);
    for (const headCommitId of this.branchHeads.values()) {
      let cur = this.commits.get(headCommitId);
      while (cur && !reachableCommitIds.has(cur.commitId)) {
        reachableCommitIds.add(cur.commitId);
        cur = cur.parentCommitId ? this.commits.get(cur.parentCommitId) : undefined;
      }
    }
    const prunedCommitIds = [];
    for (const commitId of Array.from(this.commits.keys())) {
      if (!reachableCommitIds.has(commitId)) {
        this.commits.delete(commitId);
        prunedCommitIds.push(commitId);
      }
    }
    const prunedBTreeCommitCount = this.storeEngine.gcUnreachableCommitPages(reachableCommitIds);
    return {
      prunedCommitIds,
      prunedBTreeCommitCount,
      retainedCommitCount: this.commits.size
    };
  }

  listBranchesSummary()







  {
    const out =







    [];
    for (const [branchName, headId] of this.branchHeads.entries()) {
      const c = this.commits.get(headId);
      if (!c) continue;
      const files = Array.from(c.filesTree.entries()).map(([filePath, entry]) => ({
        filePath,
        objectId: entry.objectId,
        lastStepId: entry.lastStepId,
        sizeBytes: Buffer.byteLength(entry.content, "utf-8")
      }));
      out.push({
        branchName,
        headCommitId: headId,
        agentId: c.agentId,
        stepId: c.stepId,
        toolCategory: c.toolCategory,
        mountLatencyMs: c.mountLatencyMs,
        files
      });
    }
    return out;
  }

  collectReachableBranchObjects() {
    const result = new Map();
    for (const [branchName, headCommitId] of this.branchHeads.entries()) {
      const objIds = new Set();
      let cur = this.commits.get(headCommitId);
      while (cur) {
        for (const entry of cur.filesTree.values()) {
          objIds.add(entry.objectId);
          let baseId = this.storeEngine.getObjectBaseId(entry.objectId);
          while (baseId) {
            objIds.add(baseId);
            baseId = this.storeEngine.getObjectBaseId(baseId);
          }
        }
        cur = cur.parentCommitId ? this.commits.get(cur.parentCommitId) : undefined;
      }
      result.set(branchName, Array.from(objIds));
    }
    return result;
  }

  /**
   * [M72-1 / F4] Promotes a verified branch/commit into an immutable Golden Base Image (`golden/<episodeId>`),
   * records its trajectory in `storeEngine.recordEpisodicTrajectory`, and enables 0ms CoW mounting for future tasks.
   */
  hatchGoldenBaseBranch(params)














  {
    const goldenBaseBranch = `golden/${params.episodeId}`;
    const forked = this.forkBranch(params.sourceBranchOrCommit, goldenBaseBranch);
    const commitRec = this.commits.get(forked.headCommitId);
    const reusableFilePaths = Array.from(commitRec.filesTree.keys());
    this.storeEngine.recordEpisodicTrajectory({
      episodeId: params.episodeId,
      taskId: params.taskId,
      repoSignature: params.repoSignature,
      problemPattern: params.problemPattern,
      resolutionSummary: params.resolutionSummary,
      verifiedCommitId: forked.headCommitId,
      goldenBaseBranch,
      reusableFilePaths,
      tags: params.tags,
      savedColdStartMs: params.savedColdStartMs ?? 4500
    });
    return {
      episodeId: params.episodeId,
      goldenBaseBranch,
      goldenCommitId: forked.headCommitId,
      pinnedFilesCount: reusableFilePaths.length,
      savedColdStartMs: params.savedColdStartMs ?? 4500
    };
  }

  /**
   * [M72-1 / F4] Queries Episodic Memory for a matching Golden Base Image and forks `targetBranchName`
   * in O(1) (0ms cold-start, 0 copied bytes) from the recommended golden commit.
   */
  mountWorkspaceFromEpisodicMemory(params)










  {
    const retrieval = this.storeEngine.retrieveSimilarEpisodesAndGoldenBase({
      queryText: params.queryText,
      repoSignature: params.repoSignature,
      topK: 3,
      minSimilarity: 0.1
    });
    const best = retrieval.matches[0];
    const sourceRef = best ? best.goldenBaseBranch : "main";
    const forked = this.forkBranch(sourceRef, params.targetBranchName);
    return {
      mountedBranchName: forked.branchName,
      baseCommitId: forked.headCommitId,
      matchedEpisodeId: best ? best.episodeId : null,
      similarityScore: best ? best.similarityScore : 0,
      savedColdStartMs: best ? best.savedColdStartMs : 0,
      copiedFilesCount: 0
    };
  }
}exports.LayerFsWorkspaceDriver = LayerFsWorkspaceDriver;