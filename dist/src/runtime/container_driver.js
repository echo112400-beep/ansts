"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CompanionContainerAndOverlayDriver = void 0;


var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));
var crypto = _interopRequireWildcard(require("node:crypto"));var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
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
      JSON.stringify({ taskId, turnIndex, files: bundle })
    );
    const sha256 = crypto.createHash("sha256").update(rawBytes).digest("hex");
    return {
      changedFiles: Object.keys(bundle),
      serializedArchiveBytes: rawBytes,
      sha256
    };
  }
}exports.CompanionContainerAndOverlayDriver = CompanionContainerAndOverlayDriver;