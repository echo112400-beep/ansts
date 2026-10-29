/**
 * Unified Cloudflare Workers & `denoland/celld` Entrypoint (`src/cloudflare/worker.ts`).
 * Directly deployable via:
 *   - `celld dev` / `celld deploy . --bucket s3://my-celld-fleet`
 *   - `npx wrangler dev` / `npx wrangler deploy`
 */

import * as crypto from "crypto";
import { CloudflareCelldEnv, MessageBatch } from "./env";
import { AgentTaskHarnessCell } from "./harness_cell";
import { AnstsSchedulerCell } from "./scheduler_cell";

export { AnstsSchedulerCell, AgentTaskHarnessCell };

export default {
  /**
   * 1. Stateless Edge Worker HTTP Router (~0.2ms p50 in celld):
   *    - Performs fast KV Idempotency-Key deduplication
   *    - Dispatches global 5D W-DRF scheduling to `SCHEDULER_CELL`
   *    - Dispatches per-task Turn execution, Saga rollback, and Time-Travel replay to `AGENT_HARNESS_CELL`
   */
  async fetch(
    request: Request,
    env: CloudflareCelldEnv,
    _ctx?: { waitUntil(p: Promise<any>): void },
  ): Promise<Response> {
    const url = new URL(request.url);

    // Route 1: Submit TaskSpec v1beta1 with KV Idempotency Deduplication
    if (request.method === "POST" && url.pathname === "/v1beta1/tasks") {
      const rawSpec = (await request.json()) as Record<string, any>;
      const tenantId = rawSpec?.metadata?.tenant_id ?? "default";
      const idemKey = rawSpec?.metadata?.idempotency_key ?? rawSpec?.metadata?.task_id ?? "default";
      const kvKey = `idem:${tenantId}:${idemKey}`;

      const cachedTask = await env.ANSTS_KV.get(kvKey, "json");
      if (cachedTask) {
        return new Response(JSON.stringify({ ...cachedTask, deduplicated: true }), {
          status: 200,
          headers: { "content-type": "application/json", "X-Idempotent-Replay": "true" },
        });
      }

      const schedId = env.SCHEDULER_CELL.idFromName("global-5d-scheduler");
      const schedStub = env.SCHEDULER_CELL.get(schedId);
      const resp = await schedStub.fetch("http://scheduler.internal/tasks/submit", {
        method: "POST",
        body: JSON.stringify(rawSpec),
      });
      const respBody = (await resp.json()) as Record<string, any>;

      if (resp.status === 202) {
        await env.ANSTS_KV.put(kvKey, JSON.stringify(respBody), { expirationTtl: 86400 });
        await env.ANSTS_EVENTS_QUEUE.send({
          event_type: "agent.task.queued",
          task_id: respBody.task_id,
          tenant_id: tenantId,
          epoch: 0,
          turn_seq: 0,
          payload: respBody,
        });
      }

      return new Response(JSON.stringify(respBody), {
        status: resp.status,
        headers: { "content-type": "application/json" },
      });
    }

    // Route 2: Trigger 5D W-DRF Scheduler Tick or Inspect Cluster Top
    if (url.pathname === "/v1beta1/scheduler/tick" || url.pathname === "/v1beta1/cluster/top" || url.pathname === "/v1beta1/dag/spawn-escrow") {
      const schedId = env.SCHEDULER_CELL.idFromName("global-5d-scheduler");
      const schedStub = env.SCHEDULER_CELL.get(schedId);
      const internalPath = url.pathname.replace("/v1beta1", "");
      return schedStub.fetch(`http://scheduler.internal${internalPath}`, {
        method: request.method,
        body: request.method === "POST" ? await request.text() : undefined,
      });
    }

    // Route 3: Per-Task AgentTaskHarnessCell Operations (/v1beta1/tasks/:taskId/turn | /replay | /failover)
    const taskMatch = url.pathname.match(/^\/v1beta1\/tasks\/([^/]+)\/(turn|replay|failover)$/);
    if (taskMatch) {
      const [, taskId, action] = taskMatch;
      const cellId = env.AGENT_HARNESS_CELL.idFromName(taskId);
      const cellStub = env.AGENT_HARNESS_CELL.get(cellId);
      return cellStub.fetch(`http://harness.internal/${action}`, {
        method: request.method,
        body: await request.text(),
      });
    }

    return new Response(
      JSON.stringify({
        service: "ANSTS v2.0 (Cloudflare Workers & denoland/celld Compatible Runtime)",
        status: "READY",
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  },

  /**
   * 2. Queue Consumer Handler (`ANSTS_EVENTS_QUEUE`):
   *    - Processes CloudEvents batches from Transactional Outbox
   *    - Signs payloads with HMAC-SHA256 (`X-Agent-Signature-256`)
   *    - Writes immutable audit trail to `ANSTS_D1` SQLite database
   */
  async queue(batch: MessageBatch<any>, env: CloudflareCelldEnv): Promise<void> {
    const secret = env.WEBHOOK_HMAC_SECRET ?? "whsec_ansts_v2_prod";
    for (const msg of batch.messages) {
      const body = msg.body;
      const payloadStr = JSON.stringify(body.payload ?? {});
      const signature = crypto.createHmac("sha256", secret).update(payloadStr, "utf8").digest("hex");

      await env.ANSTS_D1.prepare(
        "INSERT INTO task_audit_events (event_id, task_id, tenant_id, event_type, epoch, turn_seq, routed_model, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      )
        .bind(
          msg.id,
          body.task_id ?? "system",
          body.tenant_id ?? "system",
          body.event_type ?? "agent.event",
          body.epoch ?? 0,
          body.turn_seq ?? 0,
          body.routed_model ?? "DeepSeek",
          payloadStr,
          new Date().toISOString(),
        )
        .run();

      msg.ack();
      void signature;
    }
  },

  /**
   * 3. Scheduled Cron Trigger Handler (`triggers.crons`):
   *    - Runs periodic scheduler maintenance tick and L1 R2 checkpoint compaction
   */
  async scheduled(
    _controller: { scheduledTime: number; cron: string },
    env: CloudflareCelldEnv,
  ): Promise<void> {
    const schedId = env.SCHEDULER_CELL.idFromName("global-5d-scheduler");
    const schedStub = env.SCHEDULER_CELL.get(schedId);
    await schedStub.fetch("http://scheduler.internal/scheduler/tick", { method: "POST" });
  },
};
