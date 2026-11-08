"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ApiGatewayServer = void 0;


var http = _interopRequireWildcard(require("node:http"));
var _validator = require("./validator.js");
var _backpressure = require("./backpressure.js");
var _sse_stream = require("./sse_stream.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M1 TASK-04: Stateless HTTP/REST & SSE Gateway Server.
 */
class ApiGatewayServer {
  scheduler;
  backpressure;
  sseGateway;
  server = null;

  constructor(
  scheduler,
  backpressure = new _backpressure.IngressBackpressureController(),
  sseGateway = new _sse_stream.SseEventStreamGateway())
  {
    this.scheduler = scheduler;
    this.backpressure = backpressure;
    this.sseGateway = sseGateway;
  }

  handleSubmitRequest(
  payload,
  clusterPsiPressurePct = 0)
  {
    try {
      const spec = _validator.TaskSpecValidator.validateAndNormalize(payload);
      const decision = this.backpressure.evaluateAdmission({
        priority: spec.priority,
        globalQueueDepth: this.scheduler.queueManager.totalQueued(),
        tenantActiveTasks: this.scheduler.queueManager.getTenantActiveCount(spec.tenant_id),
        clusterPsiPressurePct
      });
      if (!decision.allowed) {
        return {
          statusCode: decision.statusCode,
          headers: { "Retry-After": String(decision.retryAfterSeconds ?? 15) },
          body: { error: "TOO_MANY_REQUESTS", reason: decision.reason }
        };
      }
      const submitted = this.scheduler.submitTask(spec);
      return {
        statusCode: 202,
        headers: {},
        body: {
          task_id: submitted.task_id,
          state: submitted.state,
          lease_fencing_token: submitted.lease_fencing_token
        }
      };
    } catch (err) {
      if (err instanceof _validator.TaskSpecValidationError) {
        return {
          statusCode: 400,
          headers: {},
          body: { error: "VALIDATION_ERROR", field: err.field, message: err.message }
        };
      }
      throw err;
    }
  }

  async listen(port = 0) {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => {
        if (req.method === "GET" && req.url === "/healthz") {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ status: "ok", queued: this.scheduler.queueManager.totalQueued() }));
          return;
        }
        if (req.method === "POST" && req.url === "/v1/tasks") {
          const chunks = [];
          req.on("data", (c) => chunks.push(c));
          req.on("end", () => {
            try {
              const body = JSON.parse(Buffer.concat(chunks).toString("utf-8"));
              const resp = this.handleSubmitRequest(body);
              res.writeHead(resp.statusCode, { "content-type": "application/json", ...resp.headers });
              res.end(JSON.stringify(resp.body));
            } catch (e) {
              res.writeHead(400, { "content-type": "application/json" });
              res.end(JSON.stringify({ error: e.message }));
            }
          });
          return;
        }
        res.writeHead(404);
        res.end();
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
}exports.ApiGatewayServer = ApiGatewayServer;