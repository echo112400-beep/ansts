/**
 * AnstsSchedulerCell: Durable Object / celld Cell implementing 5D Phase & Effort-Aware W-DRF,
 * Multi-Tenant Queueing, DAG Quota Escrow Vouchers, and SQLite/LTX-backed CAS Fencing.
 */

import { AnstsClusterRuntime } from "../cli/anstsctl";
import { ClusterPressureSignals } from "../cluster/backpressure_loop";
import { AgentTask } from "../models";
import { CloudflareCelldEnv, DurableObjectState } from "./env";

export class AnstsSchedulerCell {
  public readonly cluster: AnstsClusterRuntime;

  constructor(
    public readonly ctx: DurableObjectState,
    public readonly env: CloudflareCelldEnv,
  ) {
    this.cluster = new AnstsClusterRuntime();
    // Initialize celld/DO private SQLite tables for LTX RPO=0 lease & queue persistence
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cell_leases (task_id TEXT, node_id TEXT, model_pool TEXT, epoch INTEGER, turn_seq INTEGER)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cell_dag_vouchers (child_task_id TEXT, parent_task_id TEXT, delegated_tps INTEGER, delegated_tokens INTEGER)",
    );
  }

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const now = Date.now() / 1000.0;

    if (request.method === "POST" && url.pathname === "/tasks/submit") {
      const rawSpec = (await request.json()) as Record<string, any>;
      const res = this.cluster.apiGateway.submitTaskSpec(rawSpec, now);
      return new Response(JSON.stringify(res.body), {
        status: res.statusCode,
        headers: { "content-type": "application/json", ...res.headers },
      });
    }

    if (request.method === "POST" && url.pathname === "/scheduler/tick") {
      const dispatched = this.cluster.scheduler.scheduleTick(now);
      for (const t of dispatched) {
        this.ctx.storage.sql.exec(
          "INSERT INTO cell_leases (task_id, node_id, model_pool, epoch, turn_seq) VALUES (?, ?, ?, ?, ?)",
          t.taskId,
          t.assignedNodeId ?? "worker-us-west-01",
          t.assignedModelPool ?? t.primaryModel,
          t.fence.epoch,
          t.fence.turnSeq,
        );
      }
      return new Response(
        JSON.stringify({
          dispatched_count: dispatched.length,
          dispatched_tasks: dispatched.map((t) => ({
            task_id: t.taskId,
            node_id: t.assignedNodeId,
            model_pool: t.assignedModelPool,
            fence: { epoch: t.fence.epoch, turn_seq: t.fence.turnSeq },
          })),
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    if (request.method === "POST" && url.pathname === "/dag/spawn-escrow") {
      const body = (await request.json()) as {
        dag_id: string;
        parent_task_id: string;
        children_ids: string[];
        race_mode?: boolean;
      };
      let parent = this.cluster.scheduler.tasks.get(body.parent_task_id);
      if (!parent) {
        parent = new AgentTask({ taskId: body.parent_task_id, tenantId: "fin-enterprise" });
        this.cluster.scheduler.submitTask(parent);
        this.cluster.scheduler.scheduleTick(now);
      }
      const children = body.children_ids.map(
        (cid) => new AgentTask({ taskId: cid, tenantId: parent!.tenantId }),
      );
      const vouchers = this.cluster.queueMgr.spawnDagWithEscrow(
        body.dag_id,
        parent,
        children,
        Boolean(body.race_mode),
      );
      for (const v of vouchers) {
        this.ctx.storage.sql.exec(
          "INSERT INTO cell_dag_vouchers (child_task_id, parent_task_id, delegated_tps, delegated_tokens) VALUES (?, ?, ?, ?)",
          v.childTaskId,
          v.parentTaskId,
          v.delegatedTps,
          v.delegatedTokens,
        );
      }
      const dispatchedChildren = this.cluster.scheduler.scheduleTick(now + 0.1);
      return new Response(
        JSON.stringify({
          parent_state: parent.state,
          vouchers_issued: vouchers.length,
          fast_path_dispatched: dispatchedChildren.map((c) => c.taskId),
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    if (request.method === "POST" && url.pathname === "/chaos/inject") {
      const body = (await request.json()) as { scenario: string; error_rate?: number };
      if (body.scenario === "deepseek-429") {
        this.cluster.cogRouter.tripPoolBreaker("DeepSeek", body.error_rate ?? 0.50);
      }
      const stage = this.cluster.bpController.evaluate(
        { model429Rate: body.error_rate ?? 0.50 } as ClusterPressureSignals,
        Array.from(this.cluster.scheduler.tasks.values()),
      );
      return new Response(
        JSON.stringify({
          scenario: body.scenario,
          deepseek_breaker: this.cluster.cogRouter.pools.DeepSeek.breakerState,
          backpressure_stage: stage,
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    if (request.method === "GET" && url.pathname === "/cluster/top") {
      const snap = this.cluster.clusterTopSnapshot();
      const persistedLeases = this.ctx.storage.sql.exec("SELECT * FROM cell_leases").toArray();
      return new Response(
        JSON.stringify({
          ...snap,
          celld_sqlite_leases_count: persistedLeases.length,
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    return new Response(JSON.stringify({ error: "NotFound", path: url.pathname }), { status: 404 });
  }
}
