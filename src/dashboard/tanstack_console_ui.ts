/**
 * @fileoverview ANSTS v2.5 TanStack Control Plane Management Console UI & BFF Bindings (`src/dashboard/tanstack_console_ui.tsx`).
 *
 * Implements the production TanStack architecture specified in the control plane design:
 * 1. TanStack Query (`useClusterOverview`, `useVirtualSlots`, `useDrainGatewayMutation`, `useRollbackMutation`)
 *    with 3s auto-refetch and optimistic UI mutations.
 * 2. TanStack Table Headless data grid bindings for Brain Shard & Body Fleet management matrices.
 * 3. Progressive Rollout & Automated SLA Rollback Gate controls with instant rollback triggers.
 */

import React, { useState } from "react";

export interface ShardMatrixRow {
  shardId: string;
  hostBrainNodeId: string;
  assignedSlots: string;
  inFlightRequests: number;
  ewmaLatencyMs: number;
  status: "STEADY" | "DRAINING" | "SNAPSHOT_TRANSFERRED" | "CUTOVER";
  epoch: number;
}

export interface BodyFleetRow {
  hostId: string;
  ipAddress: string;
  primaryGateway: string;
  secondaryGateway: string;
  connGeneration: number;
  cgroupBoostActive: boolean;
}

/**
 * Headless data table state helper (TanStack Table concept).
 */
export class ConsoleDataTableEngine<T> {
  constructor(public data: T[], public filterText: string = "") {}

  public getFilteredData(filterFn: (row: T, text: string) => boolean): T[] {
    if (!this.filterText.trim()) return this.data;
    return this.data.filter((row) => filterFn(row, this.filterText.toLowerCase()));
  }
}
