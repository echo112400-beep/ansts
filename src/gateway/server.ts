/**
 * @fileoverview M1 TASK-04, M57-5 & M60-5: Stateless HTTP/REST & SSE Gateway Server
 * with Bounded Payload Size Guard (HTTP 413) & Mergeable Duplicate Submission Inspection.
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
  public readonly maxBodyBytes: number;
  private server: http.Server | null = null;

  constructor(
    scheduler: CoreScheduler5D,
    backpressure = new IngressBackpressureController(),
    sseGateway = new SseEventStreamGateway(),
    maxBodyBytes = 1024 * 1024
  ) {
    this.scheduler = scheduler;
    this.backpressure = backpressure;
    this.sseGateway = sseGateway;
    this.maxBodyBytes = maxBodyBytes;
  }

  public handleSubmitRequest(
    payload: Record<string, unknown>,
    clusterPsiPressurePct = 0
  ): { statusCode: number; headers: Record<string, string>; body: Record<string, unknown> } {
    try {
      const spec = TaskSpecValidator.validateAndNormalize(payload);
      const mergeDuplicateInFlight = Boolean(
        payload.merge_duplicate ?? payload.deduplicate_by_fingerprint ?? false
      );
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
          let totalBytes = 0;
          let aborted = false;
          req.on("data", (c: Buffer) => {
            if (aborted) return;
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
            if (aborted) return;
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
        res.writeHead(404, { "content-type": "application/json" });
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
      (this.server as unknown as { closeAllConnections?: () => void }).closeAllConnections?.();
      this.server!.close(() => {
        this.server = null;
        resolve();
      });
    });
  }
}
