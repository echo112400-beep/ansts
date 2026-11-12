/**
 * @fileoverview M24/M26 (TASK-115, TASK-116 & TASK-122): Three-Tier Hierarchical Context Compactor
 * (`HierarchicalContextCompactor`) with Real Filesystem `fs.openSync` + `fs.readSync(fd, buf, 0, len, offset)`
 * Byte-Range Pagination.
 *
 * Offloads L3 Cold Archive logs to real files on disk (`archiveRootDir`) and executes true OS
 * pread-style positional `fs.readSync` byte-range reads without loading the entire file into memory.
 */

import * as fs from "node:fs";
import * as path from "node:path";

export interface RawAgentTurnRecord {
  turnSeq: number;
  epoch: number;
  reasoningText: string;
  toolName?: string;
  rawToolOutput: string;
  modifiedFiles: string[];
  errorSignature?: string;
}

export interface EpisodicSummaryFrame {
  fromTurnSeq: number;
  toTurnSeq: number;
  touchedFiles: string[];
  errorSignatures: string[];
  condensedDecisionSummary: string;
  coldArchiveS3Uris: string[];
  savedBytes: number;
}

export class HierarchicalContextCompactor {
  public readonly l1HotTurns: RawAgentTurnRecord[] = [];
  public readonly l2EpisodicSummaries: EpisodicSummaryFrame[] = [];
  private readonly uriToFilePath = new Map<string, string>();

  constructor(
    public readonly taskId: string,
    public readonly hotWindowTurns: number = 4,
    public readonly archiveRootDir: string = "/tmp/ansts_cold_archive",
  ) {
    fs.mkdirSync(path.join(this.archiveRootDir, this.taskId), { recursive: true });
  }

  public appendTurnAndCompactIfNeeded(turn: RawAgentTurnRecord): {
    compacted: boolean;
    latestEpisodicFrame?: EpisodicSummaryFrame;
  } {
    this.l1HotTurns.push(turn);
    if (this.l1HotTurns.length <= this.hotWindowTurns) {
      return { compacted: false };
    }

    const evictCount = this.l1HotTurns.length - this.hotWindowTurns;
    const evicted = this.l1HotTurns.splice(0, evictCount);

    const touchedFilesSet = new Set<string>();
    const errorSignaturesSet = new Set<string>();
    const coldArchiveS3Uris: string[] = [];
    const summaryLines: string[] = [];
    let savedBytes = 0;

    for (const oldTurn of evicted) {
      for (const f of oldTurn.modifiedFiles) touchedFilesSet.add(f);
      if (oldTurn.errorSignature) errorSignaturesSet.add(oldTurn.errorSignature);

      const s3Uri = `s3://ansts-fleet/archive/${this.taskId}/turn-${oldTurn.turnSeq}.log`;
      const diskPath = path.join(this.archiveRootDir, this.taskId, `turn-${oldTurn.turnSeq}.log`);
      fs.mkdirSync(path.dirname(diskPath), { recursive: true });
      fs.writeFileSync(diskPath, oldTurn.rawToolOutput, "utf8");
      this.uriToFilePath.set(s3Uri, diskPath);
      coldArchiveS3Uris.push(s3Uri);

      const preview = oldTurn.rawToolOutput.slice(0, 80);
      savedBytes += Math.max(0, Buffer.byteLength(oldTurn.rawToolOutput, "utf8") - 80);
      summaryLines.push(
        `[Turn #${oldTurn.turnSeq}] tool=${oldTurn.toolName ?? "none"} preview="${preview}" archive=${s3Uri}`,
      );
    }

    const episodicFrame: EpisodicSummaryFrame = {
      fromTurnSeq: evicted[0].turnSeq,
      toTurnSeq: evicted[evicted.length - 1].turnSeq,
      touchedFiles: Array.from(touchedFilesSet),
      errorSignatures: Array.from(errorSignaturesSet),
      condensedDecisionSummary: summaryLines.join(" | "),
      coldArchiveS3Uris,
      savedBytes,
    };

    this.l2EpisodicSummaries.push(episodicFrame);
    return { compacted: true, latestEpisodicFrame: episodicFrame };
  }

  /**
   * Executes real OS positional `fs.readSync(fd, buffer, 0, lengthBytes, offsetBytes)`
   * to paginate cold archive logs from disk without reading the entire file into the V8 heap.
   */
  public readColdArchiveRange(
    s3Uri: string,
    offsetBytes: number,
    lengthBytes: number,
  ): { found: boolean; slice: string; totalLength: number; hasMore: boolean } {
    const diskPath = this.uriToFilePath.get(s3Uri);
    if (!diskPath || !fs.existsSync(diskPath)) {
      return { found: false, slice: "", totalLength: 0, hasMore: false };
    }

    const stat = fs.statSync(diskPath);
    const totalLength = stat.size;
    const start = Math.max(0, Math.min(totalLength, offsetBytes));
    const bytesToRead = Math.max(0, Math.min(lengthBytes, totalLength - start));

    if (bytesToRead === 0) {
      return { found: true, slice: "", totalLength, hasMore: false };
    }

    const buf = Buffer.allocUnsafe(bytesToRead);
    const fd = fs.openSync(diskPath, "r");
    try {
      const bytesRead = fs.readSync(fd, buf, 0, bytesToRead, start);
      const slice = buf.subarray(0, bytesRead).toString("utf8");
      return {
        found: true,
        slice,
        totalLength,
        hasMore: start + bytesRead < totalLength,
      };
    } finally {
      fs.closeSync(fd);
    }
  }
}
