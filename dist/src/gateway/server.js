"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ApiGatewayServer = void 0;


var http = _interopRequireWildcard(require("node:http"));
var _validator = require("./validator.js");
var _backpressure = require("./backpressure.js");
var _sse_stream = require("./sse_stream.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
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