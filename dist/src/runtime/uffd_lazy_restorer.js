"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.UffdLazyPageFaultRestorer = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M25 & M59-4: Linux `userfaultfd` On-Demand Page-Fault Memory & SQLite Page Restorer.
 * Invalidates cached `faultedPages` on re-staging and verifies SHA-256 page digests on page fault.
 */





class UffdLazyPageFaultRestorer {
  faultedPages = new Map();
  remoteBackingPages = new Map();
  expectedDigests = new Map();

  stageRemoteSnapshotPage(pageIndex, pageBytes) {
    if (!Number.isInteger(pageIndex) || pageIndex < 0) {
      throw new Error(`Invalid UFFD pageIndex: ${pageIndex}`);
    }
    const copy = new Uint8Array(pageBytes);
    const digest = crypto.createHash("sha256").update(copy).digest("hex");
    this.remoteBackingPages.set(pageIndex, copy);
    this.expectedDigests.set(pageIndex, digest);
    // [M59-4] Invalidate any previously faulted page at this index so re-staging for a new task/turn
    // never serves stale cached page digests from an earlier task.
    this.faultedPages.delete(pageIndex);
    return digest;
  }

  stageBufferInto4KiBPages(rawBuffer) {
    const pageSize = 4096;
    const totalPages = Math.max(1, Math.ceil(rawBuffer.byteLength / pageSize));
    const staged = [];
    for (let i = 0; i < totalPages; i++) {
      const slice = rawBuffer.subarray(i * pageSize, Math.min(rawBuffer.byteLength, (i + 1) * pageSize));
      const padded = new Uint8Array(pageSize);
      padded.set(slice, 0);
      const sha256Digest = this.stageRemoteSnapshotPage(i, padded);
      staged.push({ pageIndex: i, sha256Digest, byteLength: padded.byteLength });
    }
    return staged;
  }

  handlePageFault(pageIndex)




  {
    const existing = this.faultedPages.get(pageIndex);
    if (existing) {
      return {
        pageIndex,
        fetchedFromRemote: false,
        sha256Digest: existing.sha256Digest,
        byteLength: existing.byteLength
      };
    }
    const backing = this.remoteBackingPages.get(pageIndex) ?? new Uint8Array(4096);
    const sha256Digest = crypto.createHash("sha256").update(backing).digest("hex");
    const expected = this.expectedDigests.get(pageIndex);
    if (expected && expected !== sha256Digest) {
      throw new Error(
      `UFFD page ${pageIndex} SHA-256 integrity mismatch: expected ${expected}, got ${sha256Digest}`);

    }
    const frame = {
      pageIndex,
      byteLength: backing.byteLength,
      sha256Digest,
      restoredAtMs: Date.now()
    };
    this.faultedPages.set(pageIndex, frame);
    return {
      pageIndex,
      fetchedFromRemote: true,
      sha256Digest,
      byteLength: frame.byteLength
    };
  }

  getStats()



  {
    let restoredTotalBytes = 0;
    for (const f of this.faultedPages.values()) {
      restoredTotalBytes += f.byteLength;
    }
    return {
      stagedPagesCount: this.remoteBackingPages.size,
      faultedPagesCount: this.faultedPages.size,
      restoredTotalBytes
    };
  }
}exports.UffdLazyPageFaultRestorer = UffdLazyPageFaultRestorer;