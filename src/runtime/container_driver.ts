/**
 * @fileoverview M11 TASK-39 & M58-2: Companion Sandbox Container & OverlayFS Upperdir Tarball Diff Driver
 * with Recursive Subdirectory Walk & TaskId Path-Traversal Sanitization.
 */
import * as fs from "node:fs";
import * as path from "node:path";
import * as crypto from "node:crypto";

export class CompanionContainerAndOverlayDriver {
  private readonly rootWorkspaceDir: string;

  constructor(rootWorkspaceDir: string) {
    this.rootWorkspaceDir = rootWorkspaceDir;
    fs.mkdirSync(this.rootWorkspaceDir, { recursive: true });
  }

  private static sanitizeTaskId(taskId: string): string {
    const safe = taskId.trim().replace(/[^a-zA-Z0-9._-]/g, "_");
    if (!safe || safe === "." || safe === "..") {
      throw new Error(`Invalid taskId '${taskId}' for OverlayFS mount layout`);
    }
    return safe;
  }

  public createOverlayMountLayout(taskId: string): {
    lowerDir: string;
    upperDir: string;
    workDir: string;
    mergedDir: string;
  } {
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

  private static collectFilesRecursively(rootDir: string, currentDir: string, out: Record<string, string>): void {
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

  public captureUpperdirDeltaSnapshot(taskId: string, turnIndex: number): {
    changedFiles: string[];
    serializedArchiveBytes: Uint8Array;
    sha256: string;
  } {
    const { upperDir } = this.createOverlayMountLayout(taskId);
    const bundle: Record<string, string> = {};
    CompanionContainerAndOverlayDriver.collectFilesRecursively(upperDir, upperDir, bundle);
    const rawBytes = new TextEncoder().encode(
      JSON.stringify({ taskId, turnIndex, files: bundle })
    );
    const sha256 = crypto.createHash("sha256").update(rawBytes).digest("hex");
    return {
      changedFiles: Object.keys(bundle),
      serializedArchiveBytes: rawBytes,
      sha256,
    };
  }
}
