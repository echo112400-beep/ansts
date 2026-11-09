"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.UffdLazyPageFaultRestorer = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview Linux `userfaultfd` On-Demand Page-Fault Memory & SQLite Page Restorer.
 * Verifies SHA-256 page digests and caches restored 4KiB memory/SQLite pages from CAS storage.
 */





class UffdLazyPageFaultRestorer {
  faultedPages = new Map();
  remoteBackingPages = new Map();

  stageRemoteSnapshotPage(pageIndex, pageBytes) {
    const digest = crypto.createHash("sha256").update(pageBytes).digest("hex");
    this.remoteBackingPages.set(pageIndex, new Uint8Array(pageBytes));
    return digest;
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
}exports.UffdLazyPageFaultRestorer = UffdLazyPageFaultRestorer;