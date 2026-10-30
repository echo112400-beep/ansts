/**
 * TASK-51: `celld` Internal Admin Control-Plane Client (`:9001`) & Zero-Downtime Rolling Restart Orchestrator.
 *
 * Implements the operational endpoints from https://github.com/denoland/celld/blob/main/docs/README.md:
 * - `GET /state` on internal port (:9001) -> inspects `owned_cells`, `resident_cells`, `restoring`,
 *   `pressured`, `memory_headroom`, and `shed_cells`
 * - `POST /shutdown?handoff=preserve` -> cancels cold activations, uploads stopped SQLite DBs first,
 *   and preserves ownership records in `RustFS` for `<1.5s` same-node restart
 * - Rolling Restart Gate (`executeRollingRestart`) -> drains/restarts one node at a time and polls
 *   `GET /state` across all nodes until `restoring === 0` before advancing to the next node
 */

import * as http from "node:http";
import * as https from "node:https";
import { CelldNodeLoadBlock, FleetHaAndAutoscalerController } from "./celld_ha_engine";

export interface CelldInternalStateResponse {
  node_id: string;
  healthy: boolean;
  draining: boolean;
  node_load: {
    owned_cells: number;
    resident_cells: number;
    restoring: number;
    pressured: boolean;
    memory_headroom: number;
    shed_cells: number;
  };
}

export interface PreserveShutdownResponse {
  node_id: string;
  handoff_mode: "preserve" | "drain";
  cold_activations_cancelled: number;
  stopped_dbs_uploaded: number;
  ownership_records_preserved: boolean;
}

export class CelldAdminClient {
  public readonly fleetController = new FleetHaAndAutoscalerController();

  /**
   * Queries `GET /state` on a `celld` node's internal listener (e.g. `http://10.0.1.11:9001/state`).
   */
  public async fetchNodeState(internalBaseUrl: string): Promise<CelldNodeLoadBlock> {
    const raw = await this.httpJson<CelldInternalStateResponse>("GET", `${internalBaseUrl.replace(/\/$/, "")}/state`);
    const loadBlock: CelldNodeLoadBlock = {
      node_id: raw.node_id,
      sampled_ms: Date.now(),
      owned_cells: raw.node_load.owned_cells,
      placement_weight: 100,
      resident_cells: raw.node_load.resident_cells,
      host_websockets: 0,
      rss_bytes: 256 * 1024 * 1024,
      in_use_bytes: 128 * 1024 * 1024,
      cpu_percent_x100: 2500,
      open_fds: 64,
      pressured: raw.node_load.pressured,
      memory_headroom: raw.node_load.memory_headroom > 0.2,
      shed_cells: raw.node_load.shed_cells,
      restoring: raw.node_load.restoring,
      draining: raw.draining,
    };
    this.fleetController.registerOrUpdateNodeLoad(loadBlock);
    return loadBlock;
  }

  /**
   * Triggers `POST /shutdown?handoff=preserve` on a `celld` node for fast in-place restart.
   */
  public async triggerPreserveShutdown(internalBaseUrl: string): Promise<PreserveShutdownResponse> {
    const res = await this.httpJson<PreserveShutdownResponse>(
      "POST",
      `${internalBaseUrl.replace(/\/$/, "")}/shutdown?handoff=preserve`,
    );
    this.fleetController.prepareShutdownHandoff(res.node_id, true);
    return res;
  }

  /**
   * Executes a zero-downtime rolling restart across a list of `celld` node internal URLs.
   */
  public async executeRollingRestart(
    nodeInternalUrls: string[],
    onNodeRestart?: (nodeUrl: string, index: number) => Promise<void>,
  ): Promise<{
    completedNodes: string[];
    rebalancePlanBeforeRestart: ReturnType<FleetHaAndAutoscalerController["computeRebalanceBatch"]>;
    finalRolloutStatus: ReturnType<FleetHaAndAutoscalerController["evaluateFleetAutoscalingAndRollout"]>;
  }> {
    const completedNodes: string[] = [];

    for (const url of nodeInternalUrls) {
      await this.fetchNodeState(url);
    }
    const rebalancePlanBeforeRestart = this.fleetController.computeRebalanceBatch();

    for (let i = 0; i < nodeInternalUrls.length; i++) {
      const targetUrl = nodeInternalUrls[i];
      await this.waitUntilClusterRestoringZero(nodeInternalUrls);

      const shutdownInfo = await this.triggerPreserveShutdown(targetUrl);
      if (onNodeRestart) {
        await onNodeRestart(targetUrl, i);
      }

      await this.waitUntilClusterRestoringZero(nodeInternalUrls);
      completedNodes.push(shutdownInfo.node_id);
    }

    return {
      completedNodes,
      rebalancePlanBeforeRestart,
      finalRolloutStatus: this.fleetController.evaluateFleetAutoscalingAndRollout(),
    };
  }

  private async waitUntilClusterRestoringZero(
    nodeUrls: string[],
    maxAttempts = 10,
    pollIntervalMs = 15,
  ): Promise<void> {
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      for (const url of nodeUrls) {
        await this.fetchNodeState(url);
      }
      const gate = this.fleetController.evaluateFleetAutoscalingAndRollout();
      if (gate.safeToAdvanceRollingUpdate) {
        return;
      }
      await new Promise((r) => setTimeout(r, pollIntervalMs));
    }
    throw new Error("Timed out waiting for celld cluster restoring=0 gate");
  }

  private httpJson<T>(method: "GET" | "POST", fullUrl: string): Promise<T> {
    const parsed = new URL(fullUrl);
    const transport = parsed.protocol === "https:" ? https : http;
    return new Promise((resolve, reject) => {
      const req = transport.request(
        {
          protocol: parsed.protocol,
          hostname: parsed.hostname,
          port: parsed.port,
          method,
          path: `${parsed.pathname}${parsed.search}`,
          headers: { accept: "application/json" },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (c: Buffer) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
          res.on("end", () => {
            try {
              const text = Buffer.concat(chunks).toString("utf8");
              resolve(JSON.parse(text) as T);
            } catch (e) {
              reject(e);
            }
          });
        },
      );
      req.on("error", reject);
      req.end();
    });
  }
}
