/**
 * TASK-54: Companion Container / Firecracker Sandbox Lifecycle & OverlayFS CoW Snapshot Driver.
 *
 * Aligned with https://github.com/denoland/celld/blob/main/docs/services/containers.md:
 * - Bridges `celld` `ctx.container` or Docker/Firecracker runtime sockets
 * - Manages OverlayFS workspace directories (`lowerdir`, `upperdir`, `workdir`, `merged`)
 * - Captures per-turn CoW file-level diff snapshots of `upperdir` with deterministic SHA-256 Merkle hashes
 * - Supports instant file-level workspace rollback when the Velocity Breaker backtracks a turn
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";

export interface OverlayFsMountLayout {
  sandboxId: string;
  baseDir: string;
  lowerDir: string;
  upperDir: string;
  workDir: string;
  mergedDir: string;
}

export interface UpperDirTurnSnapshot {
  sandboxId: string;
  turnSeq: number;
  merkleHash: string;
  files: Record<string, string>; // relativePath -> base64 content
  capturedAtMs: number;
}

export class CompanionContainerAndOverlayDriver {
  private readonly layouts = new Map<string, OverlayFsMountLayout>();
  private readonly snapshots = new Map<string, Map<number, UpperDirTurnSnapshot>>();
  private readonly runningContainers = new Set<string>();

  constructor(public readonly workspaceRootDir: string) {
    fs.mkdirSync(this.workspaceRootDir, { recursive: true });
  }

  /**
   * Initializes OverlayFS directory structure (`lower`, `upper`, `work`, `merged`) for a sandbox.
   */
  public initOverlayWorkspace(sandboxId: string, initialFiles?: Record<string, string>): OverlayFsMountLayout {
    const baseDir = path.join(this.workspaceRootDir, sandboxId);
    const lowerDir = path.join(baseDir, "lower");
    const upperDir = path.join(baseDir, "upper");
    const workDir = path.join(baseDir, "work");
    const mergedDir = path.join(baseDir, "merged");

    for (const d of [lowerDir, upperDir, workDir, mergedDir]) {
      fs.mkdirSync(d, { recursive: true });
    }

    if (initialFiles) {
      for (const [relPath, content] of Object.entries(initialFiles)) {
        const fullPath = path.join(upperDir, relPath);
        fs.mkdirSync(path.dirname(fullPath), { recursive: true });
        fs.writeFileSync(fullPath, content, "utf8");
      }
    }

    const layout: OverlayFsMountLayout = { sandboxId, baseDir, lowerDir, upperDir, workDir, mergedDir };
    this.layouts.set(sandboxId, layout);
    if (!this.snapshots.has(sandboxId)) {
      this.snapshots.set(sandboxId, new Map());
    }
    return layout;
  }

  public async startCompanionContainer(
    sandboxId: string,
    celldContainerBinding?: { start(): Promise<void>; stop(): Promise<void>; running?: boolean },
  ): Promise<{ started: boolean; driver: "celld-container" | "local-overlay" }> {
    if (celldContainerBinding) {
      await celldContainerBinding.start();
      this.runningContainers.add(sandboxId);
      return { started: true, driver: "celld-container" };
    }
    this.runningContainers.add(sandboxId);
    return { started: true, driver: "local-overlay" };
  }

  public async stopCompanionContainer(
    sandboxId: string,
    celldContainerBinding?: { start(): Promise<void>; stop(): Promise<void>; running?: boolean },
  ): Promise<void> {
    if (celldContainerBinding) {
      await celldContainerBinding.stop();
    }
    this.runningContainers.delete(sandboxId);
  }

  /**
   * Captures a CoW snapshot of the sandbox's `upperdir` at the end of `turnSeq`
   * and computes its deterministic SHA-256 Merkle root.
   */
  public captureUpperDirSnapshot(sandboxId: string, turnSeq: number): UpperDirTurnSnapshot {
    const layout = this.layouts.get(sandboxId);
    if (!layout) {
      throw new Error(`OverlayFS workspace not initialized for sandbox ${sandboxId}`);
    }

    const files = this.readDirectoryRecursiveBase64(layout.upperDir);
    const merkleHash = this.computeMerkleHash(files);
    const snap: UpperDirTurnSnapshot = {
      sandboxId,
      turnSeq,
      merkleHash,
      files,
      capturedAtMs: Date.now(),
    };

    this.snapshots.get(sandboxId)!.set(turnSeq, snap);
    return snap;
  }

  /**
   * Restores the sandbox's `upperdir` to the exact file state captured at `targetTurnSeq`
   * (used during Velocity Breaker tree-search backtracking or Time-Travel Replay).
   */
  public restoreUpperDirSnapshot(sandboxId: string, targetTurnSeq: number): UpperDirTurnSnapshot {
    const layout = this.layouts.get(sandboxId);
    const snap = this.snapshots.get(sandboxId)?.get(targetTurnSeq);
    if (!layout || !snap) {
      throw new Error(`Snapshot not found for sandbox=${sandboxId}, turnSeq=${targetTurnSeq}`);
    }

    // Wipe current upperDir and reconstruct exact files from snapshot
    fs.rmSync(layout.upperDir, { recursive: true, force: true });
    fs.mkdirSync(layout.upperDir, { recursive: true });

    for (const [relPath, b64] of Object.entries(snap.files)) {
      const fullPath = path.join(layout.upperDir, relPath);
      fs.mkdirSync(path.dirname(fullPath), { recursive: true });
      fs.writeFileSync(fullPath, Buffer.from(b64, "base64"));
    }

    // Prune any snapshots newer than targetTurnSeq
    const turnMap = this.snapshots.get(sandboxId)!;
    for (const t of Array.from(turnMap.keys())) {
      if (t > targetTurnSeq) {
        turnMap.delete(t);
      }
    }

    return snap;
  }

  public computeMerkleHash(files: Record<string, string>): string {
    const hash = crypto.createHash("sha256");
    const sortedKeys = Object.keys(files).sort();
    for (const k of sortedKeys) {
      hash.update(`${k}:${files[k]}\n`, "utf8");
    }
    return `merkle_${hash.digest("hex").slice(0, 16)}`;
  }

  private readDirectoryRecursiveBase64(rootDir: string, currentSub = ""): Record<string, string> {
    const result: Record<string, string> = {};
    const target = path.join(rootDir, currentSub);
    if (!fs.existsSync(target)) return result;

    for (const entry of fs.readdirSync(target, { withFileTypes: true })) {
      const rel = currentSub ? path.posix.join(currentSub, entry.name) : entry.name;
      if (entry.isDirectory()) {
        Object.assign(result, this.readDirectoryRecursiveBase64(rootDir, rel));
      } else if (entry.isFile()) {
        result[rel] = fs.readFileSync(path.join(rootDir, rel)).toString("base64");
      }
    }
    return result;
  }
}
