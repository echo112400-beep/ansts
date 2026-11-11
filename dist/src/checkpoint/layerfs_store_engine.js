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
exports.LayerFsContentStoreEngine = void 0;
/**
 * @fileoverview M17 TASK-83, M56-3 & M66-3: LayerFS Content-Addressed Store Engine (`layerfs/object/v2` Domain-Separated Digest,
 * Real `node:zlib` Deflate Compression, Cycle-Safe `FULL`/`PREFIX`/`DELTA` Reconstruction, Reference-Pinned Deletion,
 * Accurate Logical-Byte Accounting & Unreachable Commit B-Tree Page GC).
 */
const crypto = __importStar(require("node:crypto"));
const zlib = __importStar(require("node:zlib"));
class LayerFsContentStoreEngine {
    static DOMAIN_SEPARATOR = "layerfs/object/v2";
    static CHUNK_CUTOFF_BYTES = 128 * 1024;
    static MAX_DIR_PAGE_BYTES = 8192;
    static MAX_INODE_LEAF_ROWS = 100;
    objects = new Map();
    dirPagesByCommit = new Map();
    episodicMemories = new Map();
    pinnedGoldenCommitIds = new Set();
    logicalTotalBytes = 0;
    storedCompressedBytes = 0;
    static computeCanonicalObjectId(payload) {
        const raw = typeof payload === "string" ? Buffer.from(payload, "utf-8") : Buffer.from(payload);
        return crypto
            .createHash("sha256")
            .update(LayerFsContentStoreEngine.DOMAIN_SEPARATOR, "utf-8")
            .update(raw)
            .digest("hex");
    }
    putFileObject(filePath, content, previousContent, mode = 0o644) {
        const contentBuf = Buffer.from(content, "utf-8");
        const logicalBytes = contentBuf.byteLength;
        this.logicalTotalBytes += logicalBytes;
        const objectId = LayerFsContentStoreEngine.computeCanonicalObjectId(contentBuf);
        const existing = this.objects.get(objectId);
        if (existing) {
            return { object: existing, deduplicated: true };
        }
        let lane = "whole-file";
        if (logicalBytes > LayerFsContentStoreEngine.CHUNK_CUTOFF_BYTES) {
            lane = "chunk";
        }
        else if (filePath.endsWith(".lock") || filePath.includes("node_modules/")) {
            lane = "ordinary";
        }
        let encoding = "FULL";
        let baseObjectId;
        let sharedPrefixByteLength;
        let payloadToCompress = contentBuf;
        if (previousContent && previousContent.length > 0) {
            const prevBuf = Buffer.from(previousContent, "utf-8");
            const baseId = LayerFsContentStoreEngine.computeCanonicalObjectId(prevBuf);
            if (baseId !== objectId) {
                if (!this.objects.has(baseId)) {
                    this.putFileObject(filePath, previousContent, undefined, mode);
                }
                baseObjectId = baseId;
                let commonPrefix = 0;
                const maxCheck = Math.min(contentBuf.length, prevBuf.length);
                while (commonPrefix < maxCheck && contentBuf[commonPrefix] === prevBuf[commonPrefix]) {
                    commonPrefix += 1;
                }
                if (commonPrefix >= 32) {
                    encoding = "PREFIX";
                    sharedPrefixByteLength = commonPrefix;
                    payloadToCompress = contentBuf.subarray(commonPrefix);
                }
                else {
                    encoding = "DELTA";
                    const deltaEnvelope = JSON.stringify({
                        baseLen: prevBuf.length,
                        newText: content,
                    });
                    payloadToCompress = Buffer.from(deltaEnvelope, "utf-8");
                }
            }
        }
        const compressedBuf = zlib.deflateRawSync(payloadToCompress, { level: 6 });
        const compressedBytes = compressedBuf.byteLength;
        const stored = {
            objectId,
            lane,
            encoding,
            baseObjectId,
            sharedPrefixByteLength,
            logicalBytes,
            compressedBytes,
            compressedPayload: new Uint8Array(compressedBuf),
            mode,
        };
        this.objects.set(objectId, stored);
        this.storedCompressedBytes += compressedBytes;
        return { object: stored, deduplicated: false };
    }
    /**
     * [M56-3] Cycle-safe object content reconstruction with visited-set loop detection.
     */
    reconstructObjectContent(objectId, visited = new Set()) {
        if (visited.has(objectId)) {
            throw new Error(`Cyclic LayerFS baseObjectId chain detected at '${objectId}'`);
        }
        visited.add(objectId);
        const obj = this.objects.get(objectId);
        if (!obj) {
            throw new Error(`LayerFS object '${objectId}' not found in Store`);
        }
        const decompressed = zlib.inflateRawSync(Buffer.from(obj.compressedPayload));
        if (obj.encoding === "FULL") {
            return decompressed.toString("utf-8");
        }
        if (!obj.baseObjectId) {
            throw new Error(`Corrupt ${obj.encoding} object '${objectId}': missing baseObjectId`);
        }
        const baseText = this.reconstructObjectContent(obj.baseObjectId, visited);
        if (obj.encoding === "PREFIX") {
            const baseBuf = Buffer.from(baseText, "utf-8");
            const prefixSlice = baseBuf.subarray(0, obj.sharedPrefixByteLength ?? 0);
            return Buffer.concat([prefixSlice, decompressed]).toString("utf-8");
        }
        const deltaParsed = JSON.parse(decompressed.toString("utf-8"));
        return deltaParsed.newText;
    }
    updateDirectoryBTreePages(commitId, parentCommitId, changedEntries) {
        const parentPages = parentCommitId ? this.dirPagesByCommit.get(parentCommitId) : undefined;
        const nextPages = new Map();
        if (parentPages) {
            for (const [idx, page] of parentPages.entries()) {
                nextPages.set(idx, {
                    pageId: page.pageId,
                    entries: new Map(page.entries),
                    byteSize: page.byteSize,
                });
            }
        }
        const touchedBuckets = new Set();
        for (const item of changedEntries) {
            const hashNum = crypto.createHash("md5").update(item.path, "utf-8").digest().readUInt16BE(0);
            const bucketIdx = hashNum % 64;
            touchedBuckets.add(bucketIdx);
            const existing = nextPages.get(bucketIdx);
            const page = existing
                ? {
                    pageId: `page-${commitId}-${bucketIdx}`,
                    entries: new Map(existing.entries),
                    byteSize: existing.byteSize,
                }
                : {
                    pageId: `page-${commitId}-${bucketIdx}`,
                    entries: new Map(),
                    byteSize: 128,
                };
            page.entries.set(item.path, {
                objectId: item.objectId,
                mode: item.mode,
                updatedTurnId: item.turnId,
            });
            page.byteSize = Math.min(LayerFsContentStoreEngine.MAX_DIR_PAGE_BYTES, 128 + page.entries.size * 72);
            nextPages.set(bucketIdx, page);
        }
        this.dirPagesByCommit.set(commitId, nextPages);
        return {
            rewrittenLeafPages: touchedBuckets.size,
            totalPages: nextPages.size,
        };
    }
    /**
     * [M66-3] Reclaims B-Tree directory page maps belonging to commits that are no longer reachable
     * from any active branch head.
     */
    gcUnreachableCommitPages(reachableCommitIds) {
        let pruned = 0;
        for (const commitId of Array.from(this.dirPagesByCommit.keys())) {
            if (!reachableCommitIds.has(commitId) && !this.pinnedGoldenCommitIds.has(commitId)) {
                this.dirPagesByCommit.delete(commitId);
                pruned += 1;
            }
        }
        return pruned;
    }
    getTrackedCommitPageCount() {
        return this.dirPagesByCommit.size;
    }
    getStorageStats() {
        const ratio = this.storedCompressedBytes > 0
            ? Number((this.logicalTotalBytes / this.storedCompressedBytes).toFixed(2))
            : 1.0;
        return {
            distinctObjects: this.objects.size,
            logicalTotalBytes: this.logicalTotalBytes,
            storedCompressedBytes: this.storedCompressedBytes,
            dedupAndCompressionRatio: ratio,
        };
    }
    listAllObjectIds() {
        return Array.from(this.objects.keys());
    }
    getObjectBaseId(objectId) {
        return this.objects.get(objectId)?.baseObjectId;
    }
    /**
     * [M56-3 & M66-3] Reference-pinned object deletion with accurate `logicalTotalBytes` accounting:
     * Refuses to delete `objectId` (`return 0`) if any other stored `PREFIX`/`DELTA` object still
     * depends on `objectId` as its `baseObjectId`, and decrements both `storedCompressedBytes` and
     * `logicalTotalBytes` when deleting unreferenced objects.
     */
    deleteObjectById(objectId) {
        const obj = this.objects.get(objectId);
        if (!obj)
            return 0;
        for (const [otherId, otherObj] of this.objects.entries()) {
            if (otherId !== objectId && otherObj.baseObjectId === objectId) {
                return 0;
            }
        }
        const freed = obj.compressedBytes;
        this.storedCompressedBytes = Math.max(0, this.storedCompressedBytes - freed);
        this.logicalTotalBytes = Math.max(0, this.logicalTotalBytes - obj.logicalBytes);
        this.objects.delete(objectId);
        return freed;
    }
    /**
     * [M72-1 / F4] Computes a normalized sparse token-weight vector (unigrams + character 3-grams for CJK/identifiers)
     * for semantic episodic trajectory retrieval.
     */
    static computeEpisodicVector(text) {
        const cleaned = String(text ?? "")
            .toLowerCase()
            .replace(/[^\p{L}\p{N}_\-\s]/gu, " ")
            .trim();
        const tokens = cleaned.split(/\s+/).filter((t) => t.length >= 2);
        const weights = new Map();
        for (const tok of tokens) {
            weights.set(tok, (weights.get(tok) ?? 0) + 1.0);
            if (tok.length >= 4) {
                for (let i = 0; i <= tok.length - 3; i++) {
                    const trigram = `g3:${tok.slice(i, i + 3)}`;
                    weights.set(trigram, (weights.get(trigram) ?? 0) + 0.35);
                }
            }
        }
        let normSq = 0;
        for (const v of weights.values()) {
            normSq += v * v;
        }
        const norm = Math.sqrt(normSq) || 1;
        for (const [k, v] of weights.entries()) {
            weights.set(k, v / norm);
        }
        return weights;
    }
    static cosineSimilarity(a, b) {
        let dot = 0;
        const [smaller, larger] = a.size <= b.size ? [a, b] : [b, a];
        for (const [k, v] of smaller.entries()) {
            const other = larger.get(k);
            if (other !== undefined) {
                dot += v * other;
            }
        }
        return Number(Math.max(0, Math.min(1, dot)).toFixed(4));
    }
    /**
     * [M72-1 / F4] Records a verified successful task trajectory into the Episodic Memory Store
     * and pins `verifiedCommitId` as an immutable Golden Base Image (`pinnedGoldenCommitIds`).
     */
    recordEpisodicTrajectory(params) {
        const tags = params.tags ?? [];
        const combinedText = `${params.repoSignature} ${params.problemPattern} ${params.resolutionSummary} ${tags.join(" ")}`;
        const tokenWeights = LayerFsContentStoreEngine.computeEpisodicVector(combinedText);
        const goldenBaseBranch = params.goldenBaseBranch ?? `golden/${params.episodeId}`;
        this.pinnedGoldenCommitIds.add(params.verifiedCommitId);
        this.episodicMemories.set(params.episodeId, {
            episodeId: params.episodeId,
            taskId: params.taskId,
            repoSignature: params.repoSignature,
            problemPattern: params.problemPattern,
            resolutionSummary: params.resolutionSummary,
            verifiedCommitId: params.verifiedCommitId,
            goldenBaseBranch,
            reusableFilePaths: params.reusableFilePaths ?? [],
            tags,
            savedColdStartMs: params.savedColdStartMs ?? 4200,
            createdAtMs: Date.now(),
            tokenWeights,
        });
        return {
            episodeId: params.episodeId,
            verifiedCommitId: params.verifiedCommitId,
            goldenBaseBranch,
            pinnedGoldenCommitsCount: this.pinnedGoldenCommitIds.size,
        };
    }
    /**
     * [M72-1 / F4] Retrieves top-K semantically similar historical episodes and the best matching
     * Golden Base Commit for 0ms instant workspace mounting.
     */
    retrieveSimilarEpisodesAndGoldenBase(params) {
        const queryVec = LayerFsContentStoreEngine.computeEpisodicVector(`${params.repoSignature ?? ""} ${params.queryText}`);
        const minSim = params.minSimilarity ?? 0.1;
        const topK = Math.max(1, params.topK ?? 3);
        const scored = [];
        for (const ep of this.episodicMemories.values()) {
            let sim = LayerFsContentStoreEngine.cosineSimilarity(queryVec, ep.tokenWeights);
            if (params.repoSignature && ep.repoSignature === params.repoSignature) {
                sim = Number(Math.min(1, sim + 0.15).toFixed(4));
            }
            if (sim >= minSim) {
                scored.push({
                    episodeId: ep.episodeId,
                    taskId: ep.taskId,
                    repoSignature: ep.repoSignature,
                    problemPattern: ep.problemPattern,
                    resolutionSummary: ep.resolutionSummary,
                    verifiedCommitId: ep.verifiedCommitId,
                    goldenBaseBranch: ep.goldenBaseBranch,
                    similarityScore: sim,
                    savedColdStartMs: ep.savedColdStartMs,
                });
            }
        }
        scored.sort((a, b) => b.similarityScore - a.similarityScore);
        const matches = scored.slice(0, topK);
        const best = matches[0] ?? null;
        return {
            recommendedGoldenBaseBranch: best ? best.goldenBaseBranch : null,
            recommendedGoldenCommitId: best ? best.verifiedCommitId : null,
            estimatedSavedColdStartMs: best ? best.savedColdStartMs : 0,
            matches,
        };
    }
}
exports.LayerFsContentStoreEngine = LayerFsContentStoreEngine;
