/**
 * @fileoverview M33 P2 (TASK-140 & TASK-141): Cross-Region Incremental LTX Streaming Replication Mesh
 * (`CrossRegionLtxReplicationMesh`) & GDPR / Data-Sovereignty Virtual Slot Pinning Manager
 * (`GeoSovereigntySlotPinningManager`).
 *
 * Implements:
 * 1. Monotonic `(epoch, txSeq)` LTX & `.wal` Asynchronous Cross-Region Replication: Streams committed
 *    SQLite LTX frames from a primary regional `RustFS` cluster (e.g. `us-west-2`) to standby regional
 *    clusters (`eu-central-1`, `ap-northeast-1`) with SHA-256 checksum verification, idempotent cursor
 *    deduplication, and real disk persistence (`fs.writeFileSync` + `fs.fsyncSync`).
 * 2. Geo-Pinned Data Sovereignty (`GeoSovereigntySlotPinningManager`): Pins dedicated virtual hash
 *    slots (`192..255` for `EU_GDPR_STRICT`, `128..191` for `APAC_PDPA`) strictly to in-region
 *    `celld-brain` shards and local `RustFS` buckets, blocking unauthorized cross-border state egress.
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { StaleFenceError } from "../lease_coordinator";

export type SovereigntyComplianceRegime = "GLOBAL_DEFAULT" | "EU_GDPR_STRICT" | "APAC_PDPA_STRICT";

export interface LtxReplicationFrame {
  cellId: string;
  sourceRegion: string;
  targetRegion: string;
  epoch: number;
  txSeq: number;
  ltxPayload: string;
  payloadSha256: string;
  complianceRegime: SovereigntyComplianceRegime;
  emittedAtMs: number;
}

export interface RegionalReplicaWatermarks {
  regionId: string;
  highestEpochByCell: Map<string, number>;
  highestTxSeqByCell: Map<string, number>;
  replicatedFramesCount: number;
  storageDir: string;
}

export class SovereigntyEgressViolationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SovereigntyEgressViolationError";
  }
}

export class CrossRegionLtxReplicationMesh {
  public readonly regions = new Map<string, RegionalReplicaWatermarks>();

  constructor(
    regionIds: string[] = ["us-west-2", "eu-central-1", "ap-northeast-1"],
    public readonly baseReplicationDir: string = "/tmp/ansts_cross_region_ltx",
  ) {
    for (const rId of regionIds) {
      const dir = path.join(this.baseReplicationDir, rId);
      fs.mkdirSync(dir, { recursive: true });
      this.regions.set(rId, {
        regionId: rId,
        highestEpochByCell: new Map(),
        highestTxSeqByCell: new Map(),
        replicatedFramesCount: 0,
        storageDir: dir,
      });
    }
  }

  /**
   * Replicates an LTX transaction frame from `sourceRegion` to `targetRegion`:
   * - Enforces GDPR / Data Sovereignty residency rules (blocks `EU_GDPR_STRICT` frames from leaving `eu-*`).
   * - Validates SHA-256 payload integrity and monotonic `(epoch, txSeq)` watermarks.
   * - Persists the frame to disk with `fs.fsyncSync`.
   */
  public replicateLtxFrame(frame: LtxReplicationFrame): {
    replicated: boolean;
    deduplicated: boolean;
    persistedFilePath: string;
    newWatermark: { epoch: number; txSeq: number };
  } {
    // 1. Enforce Sovereignty Data Residency Gate
    if (
      frame.complianceRegime === "EU_GDPR_STRICT" &&
      !frame.targetRegion.startsWith("eu-")
    ) {
      throw new SovereigntyEgressViolationError(
        `451 UnavailableForLegalReasons: EU_GDPR_STRICT cell ${frame.cellId} cannot replicate from ${frame.sourceRegion} to non-EU region ${frame.targetRegion}`,
      );
    }

    const expectedSha = crypto.createHash("sha256").update(frame.ltxPayload, "utf8").digest("hex");
    if (expectedSha !== frame.payloadSha256) {
      throw new Error(`LtxChecksumMismatch: expected ${frame.payloadSha256}, actual ${expectedSha}`);
    }

    const target = this.regions.get(frame.targetRegion);
    if (!target) {
      throw new Error(`UnknownTargetRegion: ${frame.targetRegion}`);
    }

    const prevEpoch = target.highestEpochByCell.get(frame.cellId) ?? 0;
    const prevTxSeq = target.highestTxSeqByCell.get(frame.cellId) ?? 0;

    if (frame.epoch < prevEpoch) {
      throw new StaleFenceError(
        `409 StaleReplicationEpoch: cell ${frame.cellId} frame epoch ${frame.epoch} < target watermark ${prevEpoch}`,
      );
    }

    const fileName = `${frame.cellId}_ep${String(frame.epoch).padStart(4, "0")}_tx${String(frame.txSeq).padStart(6, "0")}.ltx`;
    const filePath = path.join(target.storageDir, fileName);

    // Idempotent duplicate frame check
    if (frame.epoch === prevEpoch && frame.txSeq <= prevTxSeq) {
      return {
        replicated: true,
        deduplicated: true,
        persistedFilePath: filePath,
        newWatermark: { epoch: prevEpoch, txSeq: prevTxSeq },
      };
    }

    const fd = fs.openSync(filePath, "w", 0o644);
    try {
      fs.writeFileSync(fd, frame.ltxPayload, "utf8");
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }

    target.highestEpochByCell.set(frame.cellId, frame.epoch);
    target.highestTxSeqByCell.set(frame.cellId, frame.txSeq);
    target.replicatedFramesCount += 1;

    return {
      replicated: true,
      deduplicated: false,
      persistedFilePath: filePath,
      newWatermark: { epoch: frame.epoch, txSeq: frame.txSeq },
    };
  }
}

/**
 * TASK-141: Geo-Sovereignty Virtual Slot Pinning Manager (`0..255`).
 * Partitions the 256 virtual hash slots into compliance-pinned ranges:
 * - Slots `0..127`: `GLOBAL_DEFAULT` (`us-west-2` primary, global failover allowed)
 * - Slots `128..191`: `APAC_PDPA_STRICT` (`ap-northeast-1` pinned)
 * - Slots `192..255`: `EU_GDPR_STRICT` (`eu-central-1` pinned, non-EU failover prohibited)
 */
export class GeoSovereigntySlotPinningManager {
  public resolveSovereignSlotForTenant(
    tenantId: string,
    regime: SovereigntyComplianceRegime,
  ): {
    slotIndex: number;
    pinnedRegion: string;
    ownerShardId: string;
    crossRegionFailoverAllowed: boolean;
  } {
    const hashByte = crypto.createHash("sha256").update(tenantId, "utf8").digest()[0];

    if (regime === "EU_GDPR_STRICT") {
      const slotIndex = 192 + (hashByte % 64); // 192..255
      return {
        slotIndex,
        pinnedRegion: "eu-central-1",
        ownerShardId: "scheduler-shard-eu-gdpr-02",
        crossRegionFailoverAllowed: false,
      };
    }

    if (regime === "APAC_PDPA_STRICT") {
      const slotIndex = 128 + (hashByte % 64); // 128..191
      return {
        slotIndex,
        pinnedRegion: "ap-northeast-1",
        ownerShardId: "scheduler-shard-apac-01",
        crossRegionFailoverAllowed: false,
      };
    }

    const slotIndex = hashByte % 128; // 0..127
    return {
      slotIndex,
      pinnedRegion: "us-west-2",
      ownerShardId: "scheduler-shard-global-00",
      crossRegionFailoverAllowed: true,
    };
  }
}
