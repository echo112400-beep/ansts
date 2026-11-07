/**
 * @fileoverview TASK-24 & M26 (TASK-122): Real Disk-Backed Binary/JSON TurnWAL Engine
 * with OS-Level Atomic `O_CREAT | O_EXCL` (`flag: "wx"`) Conditional Write & `fsyncSync` Durability.
 *
 * Replaces the purely in-memory Map store with real filesystem persistence under `storageDir`
 * while maintaining the exact `If-None-Match` (`412 PreconditionFailed`) and monotonic Epoch
 * (`409 StaleFenceRejected`) semantics.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { StaleFenceError } from "../lease_coordinator";
import { FenceToken, TurnWalFrame } from "../models";

export class TurnWalEngine {
  private highestEpochByTask: Map<string, number> = new Map();

  constructor(public readonly storageRootDir: string = "/tmp/ansts_turnwal_store") {
    fs.mkdirSync(this.storageRootDir, { recursive: true });
  }

  public objectKey(fence: FenceToken): string {
    const ep = String(fence.epoch).padStart(2, "0");
    const tr = String(fence.turnSeq).padStart(4, "0");
    return `checkpoints/${fence.taskId}/ep_${ep}/turn_${tr}.wal`;
  }

  public resolveFilePath(objectKey: string): string {
    return path.join(this.storageRootDir, objectKey);
  }

  /**
   * Atomically writes a TurnWAL frame to disk using OS-level `O_CREAT | O_EXCL` (`flag: "wx"`)
   * and flushes to disk with `fs.fsyncSync(fd)`.
   */
  public writeWalFrameIfNoneMatch(frame: TurnWalFrame): string {
    const highestEp = this.highestEpochByTask.get(frame.taskId) ?? this.scanHighestEpochOnDisk(frame.taskId);
    if (frame.fence.epoch < highestEp) {
      throw new StaleFenceError(
        `409 StaleFenceRejected on storage write: epoch ${frame.fence.epoch} < ${highestEp}`,
      );
    }
    this.highestEpochByTask.set(frame.taskId, frame.fence.epoch);

    const key = this.objectKey(frame.fence);
    const fullPath = this.resolveFilePath(key);
    fs.mkdirSync(path.dirname(fullPath), { recursive: true });

    const serialized = JSON.stringify(frame);
    let fd: number;
    try {
      // OS kernel atomic If-None-Match via O_CREAT | O_EXCL ("wx")
      fd = fs.openSync(fullPath, "wx", 0o644);
    } catch (err: any) {
      if (err && err.code === "EEXIST") {
        throw new StaleFenceError(`412 PreconditionFailed (If-None-Match): ${key} already exists on disk`);
      }
      throw err;
    }

    try {
      fs.writeFileSync(fd, serialized, "utf8");
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }

    return key;
  }

  public listTaskFrames(taskId: string): TurnWalFrame[] {
    const taskDir = path.join(this.storageRootDir, "checkpoints", taskId);
    if (!fs.existsSync(taskDir)) {
      return [];
    }

    const walFiles: string[] = [];
    for (const epEntry of fs.readdirSync(taskDir, { withFileTypes: true })) {
      if (epEntry.isDirectory() && epEntry.name.startsWith("ep_")) {
        const epDir = path.join(taskDir, epEntry.name);
        for (const fEntry of fs.readdirSync(epDir, { withFileTypes: true })) {
          if (fEntry.isFile() && fEntry.name.endsWith(".wal")) {
            walFiles.push(path.join(epDir, fEntry.name));
          }
        }
      }
    }

    walFiles.sort();
    return walFiles.map((filePath) => {
      const raw = fs.readFileSync(filePath, "utf8");
      const parsed = JSON.parse(raw);
      return {
        ...parsed,
        fence: new FenceToken(parsed.fence.epoch, parsed.fence.turnSeq, parsed.fence.taskId),
      } as TurnWalFrame;
    });
  }

  private scanHighestEpochOnDisk(taskId: string): number {
    const taskDir = path.join(this.storageRootDir, "checkpoints", taskId);
    if (!fs.existsSync(taskDir)) return 0;
    let maxEp = 0;
    for (const entry of fs.readdirSync(taskDir, { withFileTypes: true })) {
      if (entry.isDirectory() && entry.name.startsWith("ep_")) {
        const epNum = parseInt(entry.name.slice(3), 10);
        if (!Number.isNaN(epNum) && epNum > maxEp) {
          maxEp = epNum;
        }
      }
    }
    return maxEp;
  }
}
