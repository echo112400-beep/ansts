"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.DashboardBridgeService = exports.DashboardBackendServer = void 0;


var http = _interopRequireWildcard(require("node:http"));

var _manual_scale_controller = require("../cluster/manual_scale_controller.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M13 TASK-47: Real-Time Control Plane Web UI & Operational Dashboard Backend Server.
 */class DashboardBridgeService {
  scheduler;
  fleet;

  constructor(scheduler, fleet = new _manual_scale_controller.ManualFleetScaleController()) {
    this.scheduler = scheduler;
    this.fleet = fleet;
  }

  buildClusterOverview() {
    return {
      totalTasks: this.scheduler.allTasks.size,
      queuedTasks: this.scheduler.queueManager.totalQueued(),
      completedTasks: this.scheduler.completedIds.size,
      nodes: this.fleet.listNodes()
    };
  }
}exports.DashboardBridgeService = DashboardBridgeService;

class DashboardBackendServer {
  bridge;
  server = null;

  constructor(bridge) {
    this.bridge = bridge;
  }

  async listen(port = 0) {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => {
        if (req.url === "/api/overview") {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify(this.bridge.buildClusterOverview()));
          return;
        }
        res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        res.end("<!doctype html><html><head><title>ANSTS v2 Control Plane</title></head><body><h1>ANSTS v2 Dashboard</h1></body></html>");
      });
      this.server.on("error", reject);
      this.server.listen(port, "127.0.0.1", () => {
        const addr = this.server.address();
        resolve(typeof addr === "object" && addr ? addr.port : port);
      });
    });
  }

  async close() {
    if (!this.server) return;
    return new Promise((resolve) => {
      this.server.close(() => {
        this.server = null;
        resolve();
      });
    });
  }
}exports.DashboardBackendServer = DashboardBackendServer;