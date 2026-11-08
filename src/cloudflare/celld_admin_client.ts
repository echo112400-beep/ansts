/**
 * @fileoverview M10 TASK-36: denoland/celld Admin API Client (Cell Freeze, Migrate & R2 LTX Rehydrate).
 */
export interface CellPlacementRecord {
  cellId: string;
  hostNodeId: string;
  epochFencingToken: number;
  state: "ACTIVE" | "FROZEN" | "MIGRATING";
  lastLtxTxId: number;
}

export class CelldAdminClient {
  private readonly placements: Map<string, CellPlacementRecord> = new Map();

  public registerCell(cellId: string, hostNodeId: string, lastLtxTxId = 1): CellPlacementRecord {
    const rec: CellPlacementRecord = {
      cellId,
      hostNodeId,
      epochFencingToken: 1,
      state: "ACTIVE",
      lastLtxTxId,
    };
    this.placements.set(cellId, rec);
    return rec;
  }

  public async migrateCellToNode(
    cellId: string,
    targetNodeId: string
  ): Promise<{ previousNodeId: string; newNodeId: string; newEpochFencingToken: number; rehydratedTxId: number }> {
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
      rehydratedTxId: rec.lastLtxTxId,
    };
  }

  public getCellPlacement(cellId: string): CellPlacementRecord | undefined {
    return this.placements.get(cellId);
  }
}
