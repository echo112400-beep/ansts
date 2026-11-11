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
exports.CelldAdminClient = void 0;
/**
 * @fileoverview M10 TASK-36, M47-2 & M56-4: denoland/celld Admin API Client
 * with Cold-Start SQLite Placement Rehydration & Non-Regressive Epoch Fencing Preservation.
 */
const crypto = __importStar(require("node:crypto"));
const env_js_1 = require("./env.js");
class CelldAdminClient {
    placements = new Map();
    sql;
    bucket;
    constructor(sql = new env_js_1.InMemoryCellSqlStorage(), bucket = new env_js_1.InMemoryR2Bucket()) {
        this.sql = sql;
        this.bucket = bucket;
        this.sql.exec("CREATE TABLE IF NOT EXISTS celld_admin_placements (cell_id TEXT PRIMARY KEY, host_node_id TEXT, epoch_token INTEGER, state TEXT, last_ltx_txid INTEGER, migration_sha256 TEXT)");
        this.rehydratePlacementsFromSql();
    }
    rehydratePlacementsFromSql() {
        const rows = this.sql.exec("SELECT * FROM celld_admin_placements").toArray();
        for (const row of rows) {
            const cellId = String(row.cell_id ?? "");
            if (!cellId)
                continue;
            this.placements.set(cellId, {
                cellId,
                hostNodeId: String(row.host_node_id ?? ""),
                epochFencingToken: Number(row.epoch_token ?? 1),
                state: row.state ?? "ACTIVE",
                lastLtxTxId: Number(row.last_ltx_txid ?? 1),
                lastMigrationSha256: row.migration_sha256 ? String(row.migration_sha256) : undefined,
            });
        }
    }
    /**
     * [M56-4] Non-regressive cell registration: preserves existing `epochFencingToken` if `cellId`
     * was already registered or migrated earlier, preventing epoch rollback on re-registration.
     */
    registerCell(cellId, hostNodeId, lastLtxTxId = 1) {
        const existing = this.getCellPlacement(cellId);
        const preservedEpoch = existing ? Math.max(1, existing.epochFencingToken) : 1;
        const preservedTxId = existing ? Math.max(lastLtxTxId, existing.lastLtxTxId) : lastLtxTxId;
        const rec = {
            cellId,
            hostNodeId,
            epochFencingToken: preservedEpoch,
            state: "ACTIVE",
            lastLtxTxId: preservedTxId,
            lastMigrationSha256: existing?.lastMigrationSha256,
        };
        this.placements.set(cellId, rec);
        this.sql.exec("INSERT OR REPLACE INTO celld_admin_placements (cell_id, host_node_id, epoch_token, state, last_ltx_txid, migration_sha256) VALUES (?, ?, ?, ?, ?, ?)", cellId, hostNodeId, rec.epochFencingToken, rec.state, rec.lastLtxTxId, rec.lastMigrationSha256 ?? "");
        return rec;
    }
    async migrateCellToNode(cellId, targetNodeId) {
        let rec = this.getCellPlacement(cellId);
        if (!rec) {
            throw new Error(`Cell '${cellId}' is not registered in CelldAdminClient`);
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
        this.sql.exec("INSERT OR REPLACE INTO celld_admin_placements (cell_id, host_node_id, epoch_token, state, last_ltx_txid, migration_sha256) VALUES (?, ?, ?, ?, ?, ?)", cellId, rec.hostNodeId, rec.epochFencingToken, rec.state, rec.lastLtxTxId, migrationSha256);
        return {
            previousNodeId,
            newNodeId: targetNodeId,
            newEpochFencingToken: rec.epochFencingToken,
            rehydratedTxId: rec.lastLtxTxId,
            ltxMigrationKey,
            migrationSha256,
        };
    }
    getCellPlacement(cellId) {
        const mem = this.placements.get(cellId);
        if (mem)
            return mem;
        const row = this.sql
            .exec("SELECT * FROM celld_admin_placements WHERE cell_id = ?", cellId)
            .one();
        if (!row)
            return undefined;
        const hydrated = {
            cellId: String(row.cell_id),
            hostNodeId: String(row.host_node_id),
            epochFencingToken: Number(row.epoch_token ?? 1),
            state: row.state ?? "ACTIVE",
            lastLtxTxId: Number(row.last_ltx_txid ?? 1),
            lastMigrationSha256: row.migration_sha256 ? String(row.migration_sha256) : undefined,
        };
        this.placements.set(cellId, hydrated);
        return hydrated;
    }
}
exports.CelldAdminClient = CelldAdminClient;
