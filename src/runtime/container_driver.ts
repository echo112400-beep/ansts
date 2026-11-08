/**
 * @fileoverview M11 TASK-39: Companion Sandbox Container & OverlayFS Upperdir Tarball Diff Driver.
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

  public createOverlayMountLayout(taskId: string): {
    lowerDir: string;
    upperDir: string;
    workDir: string;
    mergedDir: string;
  } {
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

  public captureUpperdirDeltaSnapshot(taskId: string, turnIndex: number): {
    changedFiles: string[];
    serializedArchiveBytes: Uint8Array;
    sha256: string;
  } {
    const { upperDir } = this.createOverlayMountLayout(taskId);
    const entries = fs.existsSync(upperDir) ? fs.readdirSync(upperDir) : [];
    const bundle: Record<string, string> = {};
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
      sha256,
    };
  }
}
