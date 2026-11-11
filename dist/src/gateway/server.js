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
exports.ApiGatewayServer = void 0;
/**
 * @fileoverview M1 TASK-04, M57-5 & M60-5: Stateless HTTP/REST & SSE Gateway Server
 * with Bounded Payload Size Guard (HTTP 413) & Mergeable Duplicate Submission Inspection.
 */
const http = __importStar(require("node:http"));
const validator_js_1 = require("./validator.js");
const backpressure_js_1 = require("./backpressure.js");
const sse_stream_js_1 = require("./sse_stream.js");
class ApiGatewayServer {
    scheduler;
    backpressure;
    sseGateway;
    maxBodyBytes;
    server = null;
    constructor(scheduler, backpressure = new backpressure_js_1.IngressBackpressureController(), sseGateway = new sse_stream_js_1.SseEventStreamGateway(), maxBodyBytes = 1024 * 1024) {
        this.scheduler = scheduler;
        this.backpressure = backpressure;
        this.sseGateway = sseGateway;
        this.maxBodyBytes = maxBodyBytes;
    }
    handleSubmitRequest(payload, clusterPsiPressurePct = 0) {
        try {
            const spec = validator_js_1.TaskSpecValidator.validateAndNormalize(payload);
            const mergeDuplicateInFlight = Boolean(payload.merge_duplicate ?? payload.deduplicate_by_fingerprint ?? false);
            // [M60-5] Pre-check duplicate/mergeable task BEFORE backpressure rejection so idempotent retries
            // or urgent priority-promoting duplicate requests can merge into an already-admitted task!
            const dupCheck = this.scheduler.inspectAndMergeDuplicateTask(spec, {
                mergeDuplicateInFlight,
            });
            if (dupCheck.deduplicated) {
                const canonical = dupCheck.canonicalTask;
                return {
                    statusCode: 202,
                    headers: { "X-Ansts-Deduplicated": dupCheck.mergedBy },
                    body: {
                        task_id: canonical.task_id,
                        state: canonical.state,
                        lease_fencing_token: canonical.lease_fencing_token,
                        deduplicated: true,
                        merged_by: dupCheck.mergedBy,
                        priority_promoted: dupCheck.priorityPromoted,
                        merged_duplicate_count: canonical.merged_duplicate_count,
                    },
                };
            }
            const decision = this.backpressure.evaluateAdmission({
                priority: spec.priority,
                globalQueueDepth: this.scheduler.queueManager.totalQueued(),
                tenantActiveTasks: this.scheduler.queueManager.getTenantActiveCount(spec.tenant_id),
                clusterPsiPressurePct,
            });
            if (!decision.allowed) {
                return {
                    statusCode: decision.statusCode,
                    headers: { "Retry-After": String(decision.retryAfterSeconds ?? 15) },
                    body: { error: "TOO_MANY_REQUESTS", reason: decision.reason },
                };
            }
            const submitted = this.scheduler.submitTask(spec, { mergeDuplicateInFlight });
            return {
                statusCode: 202,
                headers: {},
                body: {
                    task_id: submitted.task_id,
                    state: submitted.state,
                    lease_fencing_token: submitted.lease_fencing_token,
                    deduplicated: false,
                    merged_duplicate_count: submitted.merged_duplicate_count,
                },
            };
        }
        catch (err) {
            if (err instanceof validator_js_1.TaskSpecValidationError) {
                return {
                    statusCode: 400,
                    headers: {},
                    body: { error: "VALIDATION_ERROR", field: err.field, message: err.message },
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
                    let totalBytes = 0;
                    let aborted = false;
                    req.on("data", (c) => {
                        if (aborted)
                            return;
                        totalBytes += c.byteLength;
                        if (totalBytes > this.maxBodyBytes) {
                            aborted = true;
                            res.writeHead(413, { "content-type": "application/json" });
                            res.end(JSON.stringify({ error: "PAYLOAD_TOO_LARGE", maxBodyBytes: this.maxBodyBytes }));
                            return;
                        }
                        chunks.push(c);
                    });
                    req.on("end", () => {
                        if (aborted)
                            return;
                        try {
                            const body = JSON.parse(Buffer.concat(chunks).toString("utf-8"));
                            const resp = this.handleSubmitRequest(body);
                            res.writeHead(resp.statusCode, { "content-type": "application/json", ...resp.headers });
                            res.end(JSON.stringify(resp.body));
                        }
                        catch (e) {
                            res.writeHead(400, { "content-type": "application/json" });
                            res.end(JSON.stringify({ error: e.message }));
                        }
                    });
                    return;
                }
                res.writeHead(404, { "content-type": "application/json" });
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
        if (!this.server)
            return;
        return new Promise((resolve) => {
            this.server.closeAllConnections?.();
            this.server.close(() => {
                this.server = null;
                resolve();
            });
        });
    }
}
exports.ApiGatewayServer = ApiGatewayServer;
