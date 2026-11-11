"use strict";
/**
 * @fileoverview M24/M26 (TASK-115, TASK-116 & TASK-122): Three-Tier Hierarchical Context Compactor
 * (`HierarchicalContextCompactor`) with Real Filesystem `fs.openSync` + `fs.readSync(fd, buf, 0, len, offset)`
 * Byte-Range Pagination.
 *
 * Offloads L3 Cold Archive logs to real files on disk (`archiveRootDir`) and executes true OS
 * pread-style positional `fs.readSync` byte-range reads without loading the entire file into memory.
 */
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
exports.HierarchicalContextCompactor = void 0;
const fs = __importStar(require("node:fs"));
const path = __importStar(require("node:path"));
class HierarchicalContextCompactor {
    taskId;
    hotWindowTurns;
    archiveRootDir;
    l1HotTurns = [];
    l2EpisodicSummaries = [];
    uriToFilePath = new Map();
    constructor(taskId, hotWindowTurns = 4, archiveRootDir = "/tmp/ansts_cold_archive") {
        this.taskId = taskId;
        this.hotWindowTurns = hotWindowTurns;
        this.archiveRootDir = archiveRootDir;
        fs.mkdirSync(path.join(this.archiveRootDir, this.taskId), { recursive: true });
    }
    appendTurnAndCompactIfNeeded(turn) {
        this.l1HotTurns.push(turn);
        if (this.l1HotTurns.length <= this.hotWindowTurns) {
            return { compacted: false };
        }
        const evictCount = this.l1HotTurns.length - this.hotWindowTurns;
        const evicted = this.l1HotTurns.splice(0, evictCount);
        const touchedFilesSet = new Set();
        const errorSignaturesSet = new Set();
        const coldArchiveS3Uris = [];
        const summaryLines = [];
        let savedBytes = 0;
        for (const oldTurn of evicted) {
            for (const f of oldTurn.modifiedFiles)
                touchedFilesSet.add(f);
            if (oldTurn.errorSignature)
                errorSignaturesSet.add(oldTurn.errorSignature);
            const s3Uri = `s3://ansts-fleet/archive/${this.taskId}/turn-${oldTurn.turnSeq}.log`;
            const diskPath = path.join(this.archiveRootDir, this.taskId, `turn-${oldTurn.turnSeq}.log`);
            fs.mkdirSync(path.dirname(diskPath), { recursive: true });
            fs.writeFileSync(diskPath, oldTurn.rawToolOutput, "utf8");
            this.uriToFilePath.set(s3Uri, diskPath);
            coldArchiveS3Uris.push(s3Uri);
            const preview = oldTurn.rawToolOutput.slice(0, 80);
            savedBytes += Math.max(0, Buffer.byteLength(oldTurn.rawToolOutput, "utf8") - 80);
            summaryLines.push(`[Turn #${oldTurn.turnSeq}] tool=${oldTurn.toolName ?? "none"} preview="${preview}" archive=${s3Uri}`);
        }
        const episodicFrame = {
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
    readColdArchiveRange(s3Uri, offsetBytes, lengthBytes) {
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
        }
        finally {
            fs.closeSync(fd);
        }
    }
}
exports.HierarchicalContextCompactor = HierarchicalContextCompactor;
