"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CompanionContainerAndOverlayDriver = void 0;



var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));
var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M11 TASK-39 & M58-2: Companion Sandbox Container & OverlayFS Upperdir Tarball Diff Driver
 * with Recursive Subdirectory Walk & TaskId Path-Traversal Sanitization.
 */class CompanionContainerAndOverlayDriver {rootWorkspaceDir;

  constructor(rootWorkspaceDir) {
    this.rootWorkspaceDir = rootWorkspaceDir;
    fs.mkdirSync(this.rootWorkspaceDir, { recursive: true });
  }

  static sanitizeTaskId(taskId) {
    const safe = taskId.trim().replace(/[^a-zA-Z0-9._-]/g, "_");
    if (!safe || safe === "." || safe === "..") {
      throw new Error(`Invalid taskId '${taskId}' for OverlayFS mount layout`);
    }
    return safe;
  }

  createOverlayMountLayout(taskId)




  {
    const safeTaskId = CompanionContainerAndOverlayDriver.sanitizeTaskId(taskId);
    const base = path.join(this.rootWorkspaceDir, safeTaskId);
    const lowerDir = path.join(base, "lower");
    const upperDir = path.join(base, "upper");
    const workDir = path.join(base, "work");
    const mergedDir = path.join(base, "merged");
    for (const d of [lowerDir, upperDir, workDir, mergedDir]) {
      fs.mkdirSync(d, { recursive: true });
    }
    return { lowerDir, upperDir, workDir, mergedDir };
  }

  static collectFilesRecursively(rootDir, currentDir, out) {
    if (!fs.existsSync(currentDir)) return;
    const entries = fs.readdirSync(currentDir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name));
    for (const entry of entries) {
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        CompanionContainerAndOverlayDriver.collectFilesRecursively(rootDir, fullPath, out);
      } else if (entry.isFile()) {
        const relPath = path.relative(rootDir, fullPath).split(path.sep).join("/");
        out[relPath] = fs.readFileSync(fullPath, "utf-8");
      }
    }
  }

  captureUpperdirDeltaSnapshot(taskId, turnIndex)



  {
    const { upperDir } = this.createOverlayMountLayout(taskId);
    const bundle = {};
    CompanionContainerAndOverlayDriver.collectFilesRecursively(upperDir, upperDir, bundle);
    const rawBytes = new TextEncoder().encode(
    JSON.stringify({ taskId, turnIndex, files: bundle }));

    const sha256 = crypto.createHash("sha256").update(rawBytes).digest("hex");
    return {
      changedFiles: Object.keys(bundle),
      serializedArchiveBytes: rawBytes,
      sha256
    };
  }
}exports.CompanionContainerAndOverlayDriver = CompanionContainerAndOverlayDriver;