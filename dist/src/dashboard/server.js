"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.DashboardBridgeService = exports.DashboardBackendServer = void 0;


var http = _interopRequireWildcard(require("node:http"));

var _manual_scale_controller = require("../cluster/manual_scale_controller.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
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
  host;
  server = null;

  constructor(bridge, host = "127.0.0.1") {
    this.bridge = bridge;
    this.host = host;
  }

  async listen(port = 0) {
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
        const addr = this.server.address();
        resolve(typeof addr === "object" && addr ? addr.port : port);
      });
    });
  }

  async close() {
    if (!this.server) return;
    return new Promise((resolve) => {
      this.server.closeAllConnections?.();
      this.server.close(() => {
        this.server = null;
        resolve();
      });
    });
  }
}exports.DashboardBackendServer = DashboardBackendServer;