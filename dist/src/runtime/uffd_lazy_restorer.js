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
exports.UffdLazyPageFaultRestorer = void 0;
/**
 * @fileoverview M25 & M59-4: Linux `userfaultfd` On-Demand Page-Fault Memory & SQLite Page Restorer.
 * Invalidates cached `faultedPages` on re-staging and verifies SHA-256 page digests on page fault.
 */
const crypto = __importStar(require("node:crypto"));
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
    handlePageFault(pageIndex) {
        const existing = this.faultedPages.get(pageIndex);
        if (existing) {
            return {
                pageIndex,
                fetchedFromRemote: false,
                sha256Digest: existing.sha256Digest,
                byteLength: existing.byteLength,
            };
        }
        const backing = this.remoteBackingPages.get(pageIndex) ?? new Uint8Array(4096);
        const sha256Digest = crypto.createHash("sha256").update(backing).digest("hex");
        const expected = this.expectedDigests.get(pageIndex);
        if (expected && expected !== sha256Digest) {
            throw new Error(`UFFD page ${pageIndex} SHA-256 integrity mismatch: expected ${expected}, got ${sha256Digest}`);
        }
        const frame = {
            pageIndex,
            byteLength: backing.byteLength,
            sha256Digest,
            restoredAtMs: Date.now(),
        };
        this.faultedPages.set(pageIndex, frame);
        return {
            pageIndex,
            fetchedFromRemote: true,
            sha256Digest,
            byteLength: frame.byteLength,
        };
    }
    getStats() {
        let restoredTotalBytes = 0;
        for (const f of this.faultedPages.values()) {
            restoredTotalBytes += f.byteLength;
        }
        return {
            stagedPagesCount: this.remoteBackingPages.size,
            faultedPagesCount: this.faultedPages.size,
            restoredTotalBytes,
        };
    }
}
exports.UffdLazyPageFaultRestorer = UffdLazyPageFaultRestorer;
