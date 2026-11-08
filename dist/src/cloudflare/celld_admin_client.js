"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CelldAdminClient = void 0; /**
 * @fileoverview M10 TASK-36: denoland/celld Admin API Client (Cell Freeze, Migrate & R2 LTX Rehydrate).
 */








class CelldAdminClient {
  placements = new Map();

  registerCell(cellId, hostNodeId, lastLtxTxId = 1) {
    const rec = {
      cellId,
      hostNodeId,
      epochFencingToken: 1,
      state: "ACTIVE",
      lastLtxTxId
    };
    this.placements.set(cellId, rec);
    return rec;
  }

  async migrateCellToNode(
  cellId,
  targetNodeId)
  {
    const rec = this.placements.get(cellId);
    if (!rec) {
      throw new Error(`Cell '${cellId}' is not registered in CelldAdminClient`);
    }
    const previousNodeId = rec.hostNodeId;
    rec.state = "FROZEN";
    rec.epochFencingToken += 1;
    rec.hostNodeId = targetNodeId;
    rec.state = "ACTIVE";
    return {
      previousNodeId,
      newNodeId: targetNodeId,
      newEpochFencingToken: rec.epochFencingToken,
      rehydratedTxId: rec.lastLtxTxId
    };
  }

  getCellPlacement(cellId) {
    return this.placements.get(cellId);
  }
}exports.CelldAdminClient = CelldAdminClient;