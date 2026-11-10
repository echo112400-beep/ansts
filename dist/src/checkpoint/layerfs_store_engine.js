"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LayerFsContentStoreEngine = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));
var zlib = _interopRequireWildcard(require("node:zlib"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M17 TASK-83 & M56-3: LayerFS Content-Addressed Store Engine (`layerfs/object/v2` Domain-Separated Digest,
 * Real `node:zlib` Deflate Compression, Cycle-Safe `FULL`/`PREFIX`/`DELTA` Reconstruction, Reference-Pinned Deletion & 8 KiB B-Tree Pages).
 */



















class LayerFsContentStoreEngine {
  static DOMAIN_SEPARATOR = "layerfs/object/v2";
  static CHUNK_CUTOFF_BYTES = 128 * 1024;
  static MAX_DIR_PAGE_BYTES = 8192;
  static MAX_INODE_LEAF_ROWS = 100;

  objects = new Map();
  dirPagesByCommit = new Map();
  logicalTotalBytes = 0;
  storedCompressedBytes = 0;

  static computeCanonicalObjectId(payload) {
    const raw = typeof payload === "string" ? Buffer.from(payload, "utf-8") : Buffer.from(payload);
    return crypto.
    createHash("sha256").
    update(LayerFsContentStoreEngine.DOMAIN_SEPARATOR, "utf-8").
    update(raw).
    digest("hex");
  }

  putFileObject(
  filePath,
  content,
  previousContent,
  mode = 0o644)
  {
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
    } else if (filePath.endsWith(".lock") || filePath.includes("node_modules/")) {
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
        } else {
          encoding = "DELTA";
          const deltaEnvelope = JSON.stringify({
            baseLen: prevBuf.length,
            newText: content
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
      mode
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

  updateDirectoryBTreePages(
  commitId,
  parentCommitId,
  changedEntries)
  {
    const parentPages = parentCommitId ? this.dirPagesByCommit.get(parentCommitId) : undefined;
    const nextPages = new Map();
    if (parentPages) {
      for (const [idx, page] of parentPages.entries()) {
        nextPages.set(idx, {
          pageId: page.pageId,
          entries: new Map(page.entries),
          byteSize: page.byteSize
        });
      }
    }

    const touchedBuckets = new Set();
    for (const item of changedEntries) {
      const hashNum = crypto.createHash("md5").update(item.path, "utf-8").digest().readUInt16BE(0);
      const bucketIdx = hashNum % 64;
      touchedBuckets.add(bucketIdx);
      const existing = nextPages.get(bucketIdx);
      const page = existing ?
      {
        pageId: `page-${commitId}-${bucketIdx}`,
        entries: new Map(existing.entries),
        byteSize: existing.byteSize
      } :
      {
        pageId: `page-${commitId}-${bucketIdx}`,
        entries: new Map(),
        byteSize: 128
      };
      page.entries.set(item.path, {
        objectId: item.objectId,
        mode: item.mode,
        updatedTurnId: item.turnId
      });
      page.byteSize = Math.min(
      LayerFsContentStoreEngine.MAX_DIR_PAGE_BYTES,
      128 + page.entries.size * 72);

      nextPages.set(bucketIdx, page);
    }

    this.dirPagesByCommit.set(commitId, nextPages);
    return {
      rewrittenLeafPages: touchedBuckets.size,
      totalPages: nextPages.size
    };
  }

  getStorageStats()




  {
    const ratio =
    this.storedCompressedBytes > 0 ?
    Number((this.logicalTotalBytes / this.storedCompressedBytes).toFixed(2)) :
    1.0;
    return {
      distinctObjects: this.objects.size,
      logicalTotalBytes: this.logicalTotalBytes,
      storedCompressedBytes: this.storedCompressedBytes,
      dedupAndCompressionRatio: ratio
    };
  }

  listAllObjectIds() {
    return Array.from(this.objects.keys());
  }

  getObjectBaseId(objectId) {
    return this.objects.get(objectId)?.baseObjectId;
  }

  /**
   * [M56-3] Reference-pinned object deletion:
   * Refuses to delete `objectId` (`return 0`) if any other stored `PREFIX`/`DELTA` object still
   * depends on `objectId` as its `baseObjectId`, preventing broken delta chains during GC sweeps.
   */
  deleteObjectById(objectId) {
    const obj = this.objects.get(objectId);
    if (!obj) return 0;
    for (const [otherId, otherObj] of this.objects.entries()) {
      if (otherId !== objectId && otherObj.baseObjectId === objectId) {
        return 0;
      }
    }
    const freed = obj.compressedBytes;
    this.storedCompressedBytes = Math.max(0, this.storedCompressedBytes - freed);
    this.objects.delete(objectId);
    return freed;
  }
}exports.LayerFsContentStoreEngine = LayerFsContentStoreEngine;