/**
 * @fileoverview M1 TASK-04: Stateless HTTP/REST & SSE Gateway Server.
 */
import * as http from "node:http";
import { TaskSpecValidator, TaskSpecValidationError } from "./validator.js";
import { IngressBackpressureController } from "./backpressure.js";
import { SseEventStreamGateway } from "./sse_stream.js";
import { CoreScheduler5D } from "../scheduler.js";

export class ApiGatewayServer {
  private readonly scheduler: CoreScheduler5D;
  private readonly backpressure: IngressBackpressureController;
  public readonly sseGateway: SseEventStreamGateway;
  private server: http.Server | null = null;

  constructor(
    scheduler: CoreScheduler5D,
    backpressure = new IngressBackpressureController(),
    sseGateway = new SseEventStreamGateway()
  ) {
    this.scheduler = scheduler;
    this.backpressure = backpressure;
    this.sseGateway = sseGateway;
  }

  public handleSubmitRequest(
    payload: Record<string, unknown>,
    clusterPsiPressurePct = 0
  ): { statusCode: number; headers: Record<string, string>; body: Record<string, unknown> } {
    try {
      const spec = TaskSpecValidator.validateAndNormalize(payload);
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
      const submitted = this.scheduler.submitTask(spec);
      return {
        statusCode: 202,
        headers: {},
        body: {
          task_id: submitted.task_id,
          state: submitted.state,
          lease_fencing_token: submitted.lease_fencing_token,
        },
      };
    } catch (err) {
      if (err instanceof TaskSpecValidationError) {
        return {
          statusCode: 400,
          headers: {},
          body: { error: "VALIDATION_ERROR", field: err.field, message: err.message },
        };
      }
      throw err;
    }
  }

  public async listen(port = 0): Promise<number> {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => {
        if (req.method === "GET" && req.url === "/healthz") {
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ status: "ok", queued: this.scheduler.queueManager.totalQueued() }));
          return;
        }
        if (req.method === "POST" && req.url === "/v1/tasks") {
          const chunks: Buffer[] = [];
          req.on("data", (c: Buffer) => chunks.push(c));
          req.on("end", () => {
            try {
              const body = JSON.parse(Buffer.concat(chunks).toString("utf-8")) as Record<string, unknown>;
              const resp = this.handleSubmitRequest(body);
              res.writeHead(resp.statusCode, { "content-type": "application/json", ...resp.headers });
              res.end(JSON.stringify(resp.body));
            } catch (e) {
              res.writeHead(400, { "content-type": "application/json" });
              res.end(JSON.stringify({ error: (e as Error).message }));
            }
          });
          return;
        }
        res.writeHead(404);
        res.end();
      });
      this.server.on("error", reject);
      this.server.listen(port, "127.0.0.1", () => {
        const addr = this.server!.address();
        resolve(typeof addr === "object" && addr ? addr.port : port);
      });
    });
  }

  public async close(): Promise<void> {
    if (!this.server) return;
    return new Promise((resolve) => {
      this.server!.close(() => {
        this.server = null;
        resolve();
      });
    });
  }
}
