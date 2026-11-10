"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CelldAdminClient = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));
var _env = require("./env.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M10 TASK-36 & M47-2: denoland/celld Admin API Client (Cell Freeze, Migrate, SQLite Placement & R2 LTX Rehydrate).
 */








class CelldAdminClient {
  placements = new Map();
  sql;
  bucket;

  constructor(sql = new _env.InMemoryCellSqlStorage(), bucket = new _env.InMemoryR2Bucket()) {
    this.sql = sql;
    this.bucket = bucket;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS celld_admin_placements (cell_id TEXT PRIMARY KEY, host_node_id TEXT, epoch_token INTEGER, state TEXT, last_ltx_txid INTEGER, migration_sha256 TEXT)");

  }

  registerCell(cellId, hostNodeId, lastLtxTxId = 1) {
    const rec = {
      cellId,
      hostNodeId,
      epochFencingToken: 1,
      state: "ACTIVE",
      lastLtxTxId
    };
    this.placements.set(cellId, rec);
    this.sql.exec(
    "INSERT OR REPLACE INTO celld_admin_placements (cell_id, host_node_id, epoch_token, state, last_ltx_txid, migration_sha256) VALUES (?, ?, ?, ?, ?, ?)",
    cellId,
    hostNodeId,
    rec.epochFencingToken,
    rec.state,
    rec.lastLtxTxId,
    "");

    return rec;
  }

  async migrateCellToNode(
  cellId,
  targetNodeId)







  {
    let rec = this.placements.get(cellId);
    if (!rec) {
      const row = this.sql.
      exec("SELECT * FROM celld_admin_placements WHERE cell_id = ?", cellId).
      one();
      if (!row) {
        throw new Error(`Cell '${cellId}' is not registered in CelldAdminClient`);
      }
      rec = {
        cellId: String(row.cell_id),
        hostNodeId: String(row.host_node_id),
        epochFencingToken: Number(row.epoch_token ?? 1),
        state: "ACTIVE",
        lastLtxTxId: Number(row.last_ltx_txid ?? 1)
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
      migratedAtMs: Date.now()
    });
    const migrationSha256 = crypto.createHash("sha256").update(manifestPayload, "utf-8").digest("hex");
    const ltxMigrationKey = `cells/${cellId}/migrations/epoch-${rec.epochFencingToken}.json`;
    await this.bucket.put(ltxMigrationKey, manifestPayload, {
      customMetadata: {
        cellId,
        previousNodeId,
        newNodeId: targetNodeId,
        epochFencingToken: String(rec.epochFencingToken),
        migrationSha256
      }
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
    migrationSha256);


    return {
      previousNodeId,
      newNodeId: targetNodeId,
      newEpochFencingToken: rec.epochFencingToken,
      rehydratedTxId: rec.lastLtxTxId,
      ltxMigrationKey,
      migrationSha256
    };
  }

  getCellPlacement(cellId) {
    return this.placements.get(cellId);
  }
}exports.CelldAdminClient = CelldAdminClient;