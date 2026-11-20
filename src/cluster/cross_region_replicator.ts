/**
 * @fileoverview M11 & M62-5: Cross-Region Asynchronous LTX Snapshot Replicator
 * with RPO Lag Tracking, S3 Sync, URL Sanitization, Safe HTTP Status Validation & Non-Blocking Error Isolation.
 */
import { createHash, randomUUID } from "node:crypto";
import { closeSync, fsyncSync, linkSync, mkdirSync, openSync, readFileSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { FenceToken } from "../models.js";
import { StaleFenceError } from "../lease_coordinator.js";

export type SovereigntyComplianceRegime = "GLOBAL_DEFAULT" | "EU_GDPR_STRICT" | "APAC_PDPA_STRICT";

export class SovereigntyEgressViolationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SovereigntyEgressViolationError";
  }
}

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

const slotZones = {
  GLOBAL_DEFAULT: { offset: 0, count: 128, pinnedRegion: "us-west-2", ownerShardId: "scheduler-shard-global-00", regionPrefix: "" },
  APAC_PDPA_STRICT: { offset: 128, count: 64, pinnedRegion: "ap-northeast-1", ownerShardId: "scheduler-shard-apac-01", regionPrefix: "ap-" },
  EU_GDPR_STRICT: { offset: 192, count: 64, pinnedRegion: "eu-central-1", ownerShardId: "scheduler-shard-eu-gdpr-02", regionPrefix: "eu-" },
} as const;

function slotZone(regime: SovereigntyComplianceRegime) {
  if (!Object.hasOwn(slotZones, regime)) throw new SovereigntyEgressViolationError("Unknown slot routing policy");
  return slotZones[regime];
}

export class GeoSovereigntySlotPinningManager {
  public resolveSovereignSlotForTenant(tenantId: string, regime: SovereigntyComplianceRegime) {
    const zone = slotZone(regime);
    const hashByte = createHash("sha256").update(tenantId, "utf8").digest()[0];
    return {
      slotIndex: zone.offset + (hashByte % zone.count),
      pinnedRegion: zone.pinnedRegion,
      ownerShardId: zone.ownerShardId,
      crossRegionFailoverAllowed: zone.regionPrefix === "",
    };
  }
}

export class CrossRegionLtxReplicationMesh {
  private readonly directories = new Map<string, string>();

  constructor(regionIds: string[], baseReplicationDir: string) {
    for (const regionId of regionIds) {
      if (!/^[a-z][a-z0-9-]{0,62}$/.test(regionId)) throw new Error("INVALID_REPLICA_REGION");
      const directory = resolve(baseReplicationDir, regionId);
      mkdirSync(directory, { recursive: true, mode: 0o700 });
      this.directories.set(regionId, directory);
    }
  }

  public replicateLtxFrame(frame: LtxReplicationFrame) {
    const zone = slotZone(frame.complianceRegime);
    if (![frame.sourceRegion, frame.targetRegion].every(region => region.startsWith(zone.regionPrefix))) {
      throw new SovereigntyEgressViolationError("Replication source or destination is outside the configured region policy");
    }
    const directory = this.directories.get(frame.targetRegion);
    if (!directory || !this.directories.has(frame.sourceRegion)) throw new Error("UNKNOWN_REPLICA_REGION");
    if (!frame.cellId || ![frame.epoch, frame.txSeq].every(value => Number.isSafeInteger(value) && value > 0)) throw new Error("INVALID_LTX_FRAME");
    const sha256 = (value: string | Buffer) => createHash("sha256").update(value).digest("hex");
    if (sha256(frame.ltxPayload) !== frame.payloadSha256) throw new Error("LTX_CHECKSUM_MISMATCH");
    const cellKey = sha256(frame.cellId);
    const frameName = `${cellKey}_ep${frame.epoch}_tx${frame.txSeq}.ltx`;
    const persistedFilePath = join(directory, frameName);
    let latest = { epoch: 0, txSeq: 0 };
    let duplicate = false;
    for (const name of readdirSync(directory)) {
      if (!name.startsWith(cellKey + "_")) continue;
      const cursor = /^_ep([1-9]\d*)_tx([1-9]\d*)\.ltx$/.exec(name.slice(cellKey.length));
      if (!cursor) continue;
      const epoch = Number(cursor[1]), txSeq = Number(cursor[2]);
      if (!Number.isSafeInteger(epoch) || !Number.isSafeInteger(txSeq)) throw new Error("INVALID_REPLICA_WATERMARK");
      if (epoch > latest.epoch || (epoch === latest.epoch && txSeq > latest.txSeq)) latest = { epoch, txSeq };
      if (name === frameName) duplicate = true;
    }
    if (frame.epoch < latest.epoch || (!duplicate && frame.epoch === latest.epoch && frame.txSeq <= latest.txSeq)) {
      throw new StaleFenceError(frame.cellId, new FenceToken(frame.epoch, frame.txSeq, frame.cellId), new FenceToken(latest.epoch, latest.txSeq, frame.cellId));
    }
    if (duplicate) {
      if (sha256(readFileSync(persistedFilePath)) !== frame.payloadSha256) throw new Error("LTX_FRAME_CONFLICT");
      return { replicated: true, deduplicated: true, persistedFilePath, newWatermark: latest };
    }

    const tempPath = join(directory, `.${randomUUID()}.ltx.pending`);
    const file = openSync(tempPath, "wx", 0o600);
    try {
      try {
        writeFileSync(file, frame.ltxPayload, "utf8");
        fsyncSync(file);
      } finally {
        closeSync(file);
      }
      linkSync(tempPath, persistedFilePath);
      const dirHandle = openSync(directory, "r");
      try { fsyncSync(dirHandle); } finally { closeSync(dirHandle); }
    } finally {
      unlinkSync(tempPath);
    }
    return { replicated: true, deduplicated: false, persistedFilePath, newWatermark: { epoch: frame.epoch, txSeq: frame.txSeq } };
  }
}

export class CrossRegionAsyncReplicator {
  private readonly replicatedByRegion: Map<string, Set<string>> = new Map();
  private readonly lastReplicationLagMs: Map<string, number> = new Map();

  public replicateLtxObject(targetRegion: string, objectKey: string): {
    targetRegion: string;
    replicatedCount: number;
  } {
    let set = this.replicatedByRegion.get(targetRegion);
    if (!set) {
      set = new Set();
      this.replicatedByRegion.set(targetRegion, set);
    }
    set.add(objectKey);
    this.lastReplicationLagMs.set(targetRegion, 0);
    return { targetRegion, replicatedCount: set.size };
  }

  /**
   * [M62-5] DFX-Maintainability & Deployability: Sanitizes targetRegion/objectKey,
   * isolates network and HTTP fetch rejections without crashing the caller, and returns
   * safe structured RPO lag and storage telemetry.
   */
  public async replicateToRemoteS3Region(params: {
    s3Endpoint: string;
    sourceRegion: string;
    targetRegion: string;
    objectKey: string;
    payload: Record<string, unknown>;
  }): Promise<{
    targetRegion: string;
    objectKey: string;
    replicatedCount: number;
    rpoLagMs: number;
    s3Stored: boolean;
  }> {
    const t0 = performance.now();
    let s3Stored = false;
    const safeRegion = params.targetRegion.trim();
    const safeKey = params.objectKey.trim();

    try {
      const endpoint = params.s3Endpoint.trim().replace(/\/$/, "");
      if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
        const resp = await fetch(
          `${endpoint}/ansts-fleet/regions/${encodeURIComponent(safeRegion)}/${encodeURIComponent(safeKey)}.json`,
          {
            method: "PUT",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              sourceRegion: params.sourceRegion,
              targetRegion: safeRegion,
              objectKey: safeKey,
              replicatedAtMs: Date.now(),
              payload: params.payload,
            }),
          }
        );
        s3Stored = resp.ok;
      }
    } catch {
      s3Stored = false;
    }
    const rpoLagMs = Number(Math.max(0, performance.now() - t0).toFixed(3));
    const local = this.replicateLtxObject(safeRegion, safeKey);
    this.lastReplicationLagMs.set(safeRegion, rpoLagMs);
    return {
      targetRegion: safeRegion,
      objectKey: safeKey,
      replicatedCount: local.replicatedCount,
      rpoLagMs,
      s3Stored,
    };
  }

  public getRegionLagMs(region: string): number {
    return this.lastReplicationLagMs.get(region) ?? 0;
  }
}
