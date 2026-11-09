/**
 * @fileoverview M13 TASK-47: Real-Time Control Plane Web UI & Operational Dashboard Backend Server.
 */
import * as http from "node:http";
import { CoreScheduler5D } from "../scheduler.js";
import { ManualFleetScaleController } from "../cluster/manual_scale_controller.js";

export class DashboardBridgeService {
  private readonly scheduler: CoreScheduler5D;
  private readonly fleet: ManualFleetScaleController;

  constructor(scheduler: CoreScheduler5D, fleet = new ManualFleetScaleController()) {
    this.scheduler = scheduler;
    this.fleet = fleet;
  }

  public buildClusterOverview(): Record<string, unknown> {
    return {
      totalTasks: this.scheduler.allTasks.size,
      queuedTasks: this.scheduler.queueManager.totalQueued(),
      completedTasks: this.scheduler.completedIds.size,
      nodes: this.fleet.listNodes(),
    };
  }
}

export class DashboardBackendServer {
  private readonly bridge: DashboardBridgeService;
  private readonly host: string;
  private server: http.Server | null = null;

  constructor(bridge: DashboardBridgeService, host = "127.0.0.1") {
    this.bridge = bridge;
    this.host = host;
  }

  public async listen(port = 0): Promise<number> {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => {
        if (req.url === "/api/overview" || req.url === "/healthz") {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify(this.bridge.buildClusterOverview()));
          return;
        }
        res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        res.end("<!doctype html><html><head><title>ANSTS v2 Control Plane</title></head><body><h1>ANSTS v2 Dashboard</h1></body></html>");
      });
      this.server.on("error", reject);
      this.server.listen(port, this.host, () => {
        const addr = this.server!.address();
        resolve(typeof addr === "object" && addr ? addr.port : port);
      });
    });
  }

  public async close(): Promise<void> {
    if (!this.server) return;
    return new Promise((resolve) => {
      (this.server as unknown as { closeAllConnections?: () => void }).closeAllConnections?.();
      this.server!.close(() => {
        this.server = null;
        resolve();
      });
    });
  }
}
