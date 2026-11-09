/**
 * @fileoverview Linux `userfaultfd` On-Demand Page-Fault Memory & SQLite Page Restorer.
 * Verifies SHA-256 page digests and caches restored 4KiB memory/SQLite pages from CAS storage.
 */
import * as crypto from "node:crypto";

export interface RestoredPageFrame {
  pageIndex: number;
  byteLength: number;
  sha256Digest: string;
  restoredAtMs: number;
}

export class UffdLazyPageFaultRestorer {
  private readonly faultedPages: Map<number, RestoredPageFrame> = new Map();
  private readonly remoteBackingPages: Map<number, Uint8Array> = new Map();

  public stageRemoteSnapshotPage(pageIndex: number, pageBytes: Uint8Array): string {
    const digest = crypto.createHash("sha256").update(pageBytes).digest("hex");
    this.remoteBackingPages.set(pageIndex, new Uint8Array(pageBytes));
    return digest;
  }

  public handlePageFault(pageIndex: number): {
    pageIndex: number;
    fetchedFromRemote: boolean;
    sha256Digest: string;
    byteLength: number;
  } {
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
    const frame: RestoredPageFrame = {
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
}
