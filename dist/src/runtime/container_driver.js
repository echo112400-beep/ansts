"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.CompanionContainerAndOverlayDriver = void 0;
/**
 * @fileoverview M11 TASK-39 & M58-2: Companion Sandbox Container & OverlayFS Upperdir Tarball Diff Driver
 * with Recursive Subdirectory Walk & TaskId Path-Traversal Sanitization.
 */
const fs = __importStar(require("node:fs"));
const path = __importStar(require("node:path"));
const crypto = __importStar(require("node:crypto"));
class CompanionContainerAndOverlayDriver {
    rootWorkspaceDir;
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
    createOverlayMountLayout(taskId) {
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
        if (!fs.existsSync(currentDir))
            return;
        const entries = fs.readdirSync(currentDir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name));
        for (const entry of entries) {
            const fullPath = path.join(currentDir, entry.name);
            if (entry.isDirectory()) {
                CompanionContainerAndOverlayDriver.collectFilesRecursively(rootDir, fullPath, out);
            }
            else if (entry.isFile()) {
                const relPath = path.relative(rootDir, fullPath).split(path.sep).join("/");
                out[relPath] = fs.readFileSync(fullPath, "utf-8");
            }
        }
    }
    captureUpperdirDeltaSnapshot(taskId, turnIndex) {
        const { upperDir } = this.createOverlayMountLayout(taskId);
        const bundle = {};
        CompanionContainerAndOverlayDriver.collectFilesRecursively(upperDir, upperDir, bundle);
        const rawBytes = new TextEncoder().encode(JSON.stringify({ taskId, turnIndex, files: bundle }));
        const sha256 = crypto.createHash("sha256").update(rawBytes).digest("hex");
        return {
            changedFiles: Object.keys(bundle),
            serializedArchiveBytes: rawBytes,
            sha256,
        };
    }
}
exports.CompanionContainerAndOverlayDriver = CompanionContainerAndOverlayDriver;
