/**
 * @fileoverview M10 TASK-36 & M47-2: denoland/celld Admin API Client (Cell Freeze, Migrate, SQLite Placement & R2 LTX Rehydrate).
 */
import * as crypto from "node:crypto";
import { InMemoryCellSqlStorage, InMemoryR2Bucket, R2BucketLike, SqlStorageLike } from "./env.js";

export interface CellPlacementRecord {
  cellId: string;
  hostNodeId: string;
  epochFencingToken: number;
  state: "ACTIVE" | "FROZEN" | "MIGRATING";
  lastLtxTxId: number;
  lastMigrationSha256?: string;
}

export class CelldAdminClient {
  private readonly placements: Map<string, CellPlacementRecord> = new Map();
  private readonly sql: SqlStorageLike;
  private readonly bucket: R2BucketLike;

  constructor(sql: SqlStorageLike = new InMemoryCellSqlStorage(), bucket: R2BucketLike = new InMemoryR2Bucket()) {
    this.sql = sql;
    this.bucket = bucket;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS celld_admin_placements (cell_id TEXT PRIMARY KEY, host_node_id TEXT, epoch_token INTEGER, state TEXT, last_ltx_txid INTEGER, migration_sha256 TEXT)"
    );
  }

  public registerCell(cellId: string, hostNodeId: string, lastLtxTxId = 1): CellPlacementRecord {
    const rec: CellPlacementRecord = {
      cellId,
      hostNodeId,
      epochFencingToken: 1,
      state: "ACTIVE",
      lastLtxTxId,
    };
    this.placements.set(cellId, rec);
    this.sql.exec(
      "INSERT OR REPLACE INTO celld_admin_placements (cell_id, host_node_id, epoch_token, state, last_ltx_txid, migration_sha256) VALUES (?, ?, ?, ?, ?, ?)",
      cellId,
      hostNodeId,
      rec.epochFencingToken,
      rec.state,
      rec.lastLtxTxId,
      ""
    );
    return rec;
  }

  public async migrateCellToNode(
    cellId: string,
    targetNodeId: string
  ): Promise<{
    previousNodeId: string;
    newNodeId: string;
    newEpochFencingToken: number;
    rehydratedTxId: number;
    ltxMigrationKey: string;
    migrationSha256: string;
  }> {
    let rec = this.placements.get(cellId);
    if (!rec) {
      const row = this.sql
        .exec<Record<string, unknown>>("SELECT * FROM celld_admin_placements WHERE cell_id = ?", cellId)
        .one();
      if (!row) {
        throw new Error(`Cell '${cellId}' is not registered in CelldAdminClient`);
      }
      rec = {
        cellId: String(row.cell_id),
        hostNodeId: String(row.host_node_id),
        epochFencingToken: Number(row.epoch_token ?? 1),
        state: "ACTIVE",
        lastLtxTxId: Number(row.last_ltx_txid ?? 1),
      };
    }
    const previousNodeId = rec.hostNodeId;
    rec.state = "FROZEN";
    rec.epochFencingToken += 1;
    rec.hostNodeId = targetNodeId;
    rec.state = "ACTIVE";

    const manifestPayload = JSON.stringify({
      cellId,
      previousNodeId,
      newNodeId: targetNodeId,
      epochFencingToken: rec.epochFencingToken,
      rehydratedTxId: rec.lastLtxTxId,
      migratedAtMs: Date.now(),
    });
    const migrationSha256 = crypto.createHash("sha256").update(manifestPayload, "utf-8").digest("hex");
    const ltxMigrationKey = `cells/${cellId}/migrations/epoch-${rec.epochFencingToken}.json`;
    await this.bucket.put(ltxMigrationKey, manifestPayload, {
      customMetadata: {
        cellId,
        previousNodeId,
        newNodeId: targetNodeId,
        epochFencingToken: String(rec.epochFencingToken),
        migrationSha256,
      },
    });
    rec.lastMigrationSha256 = migrationSha256;
    this.placements.set(cellId, rec);
    this.sql.exec(
      "INSERT OR REPLACE INTO celld_admin_placements (cell_id, host_node_id, epoch_token, state, last_ltx_txid, migration_sha256) VALUES (?, ?, ?, ?, ?, ?)",
      cellId,
      rec.hostNodeId,
      rec.epochFencingToken,
      rec.state,
      rec.lastLtxTxId,
      migrationSha256
    );

    return {
      previousNodeId,
      newNodeId: targetNodeId,
      newEpochFencingToken: rec.epochFencingToken,
      rehydratedTxId: rec.lastLtxTxId,
      ltxMigrationKey,
      migrationSha256,
    };
  }

  public getCellPlacement(cellId: string): CellPlacementRecord | undefined {
    return this.placements.get(cellId);
  }
}
