"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CompanionContainerAndOverlayDriver = void 0;


var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));
var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M11 TASK-39: Companion Sandbox Container & OverlayFS Upperdir Tarball Diff Driver.
 */class CompanionContainerAndOverlayDriver {
  rootWorkspaceDir;

  constructor(rootWorkspaceDir) {
    this.rootWorkspaceDir = rootWorkspaceDir;
    fs.mkdirSync(this.rootWorkspaceDir, { recursive: true });
  }

  createOverlayMountLayout(taskId)




  {
    const base = path.join(this.rootWorkspaceDir, taskId);
    const lowerDir = path.join(base, "lower");
    const upperDir = path.join(base, "upper");
    const workDir = path.join(base, "work");
    const mergedDir = path.join(base, "merged");
    for (const d of [lowerDir, upperDir, workDir, mergedDir]) {
      fs.mkdirSync(d, { recursive: true });
    }
    return { lowerDir, upperDir, workDir, mergedDir };
  }

  captureUpperdirDeltaSnapshot(taskId, turnIndex)



  {
    const { upperDir } = this.createOverlayMountLayout(taskId);
    const entries = fs.existsSync(upperDir) ? fs.readdirSync(upperDir) : [];
    const bundle = {};
    for (const fname of entries) {
      const full = path.join(upperDir, fname);
      if (fs.statSync(full).isFile()) {
        bundle[fname] = fs.readFileSync(full, "utf-8");
      }
    }
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