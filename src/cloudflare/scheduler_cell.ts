/** Durable regional/resource-pool scheduler adapter. */

import { AnstsClusterRuntime } from "../cli/anstsctl";
import { ClusterPressureSignals } from "../cluster/backpressure_loop";
import { CloudflareCelldEnv, DurableObjectState, DurableObjectStub } from "./env";
import { initializeCellProtocol } from "../persistence/schema";
import { SchedulerCandidate, SchedulerPool, schedulerPoolMigration } from "../persistence/scheduler-pool";
import { PersistenceCellNamespace } from "../persistence/contracts";
import { persistenceCellStub } from "../persistence/registry";
import { NodeResourceVector } from "../persistence/node-capacity";

interface OwnerResult<T = any> {
  response: Response;
  body: T;
}

export class AnstsSchedulerCell {
  public readonly cluster: AnstsClusterRuntime;
  public readonly schedulerPool: SchedulerPool;

  constructor(
    public readonly ctx: DurableObjectState,
    public readonly env: CloudflareCelldEnv,
  ) {
    this.cluster = new AnstsClusterRuntime();
    const poolKey = ctx.id.name ?? ctx.id.toString();
    initializeCellProtocol(this.ctx.storage, { type: "scheduler-pool", key: poolKey }, [schedulerPoolMigration]);
    this.schedulerPool = new SchedulerPool(this.ctx.storage, poolKey);
  }

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (request.method === "POST" && url.pathname === "/scheduler/tick") {
        const body = await readOptionalJson(request);
        const nodes = Array.isArray(body.nodes) ? body.nodes : [];
        await this.registerNodes(nodes);
        const suspectedTasks = await this.reconcileHeartbeatLoss();
        const candidates = Array.isArray(body.candidates)
          ? body.candidates as SchedulerCandidate[]
          : this.pendingCandidates();
        const results = await this.scheduleCandidates(candidates);
        return json({
          dispatched_count: results.filter((result) => result.stage === "STARTED").length,
          blocked_count: results.filter((result) => result.status === "BLOCKED_CAPACITY").length,
          suspected_tasks: suspectedTasks,
          dispatches: results,
        });
      }

      if (request.method === "POST" && url.pathname === "/scheduler/dispatch") {
        const body = await request.json() as SchedulerCandidate & { nodes?: any[] };
        await this.registerNodes(body.nodes ?? []);
        const [result] = await this.scheduleCandidates([body]);
        return json(result, result.status === "BLOCKED_CAPACITY" ? 202 : 200);
      }

      if (request.method === "POST" && url.pathname === "/scheduler/started") {
        const body = await request.json() as {
          dispatch_id: string;
          node_id: string;
          node_incarnation: string;
          authorization_token: string;
          business_epoch: number;
          start_receipt: string;
        };
        return await this.recordNodeStart(body);
      }

      if (request.method === "POST" && url.pathname === "/scheduler/stopped") {
        const body = await request.json() as {
          dispatch_id: string; task_id: string; tenant_id: string; node_id: string; node_incarnation: string;
          business_epoch: number; evidence_type: "STOPPED" | "ISOLATED"; evidence_id: string;
        };
        const dispatch = this.schedulerPool.getDispatch(body.dispatch_id);
        if (!dispatch || dispatch.task_id !== body.task_id || dispatch.tenant_id !== body.tenant_id ||
            dispatch.node_id !== body.node_id || dispatch.node_incarnation !== body.node_incarnation ||
            Number(dispatch.business_epoch) !== body.business_epoch) {
          return json({ error: "NODE_STOP_DISPATCH_MISMATCH" }, 409);
        }
        const task = await requireOwner(persistenceCellStub(this.env, "task", body.task_id), "/execution/state", "GET");
        if (task.tenant_id !== body.tenant_id || task.current_dispatch_id !== body.dispatch_id || Number(task.business_epoch) !== body.business_epoch) {
          return json({ error: "NODE_STOP_TASK_MISMATCH" }, 409);
        }
        if (body.evidence_type === "STOPPED" && ["RUNNING", "STARTING", "SUSPECT"].includes(task.lifecycle_state)) {
          return json({ error: "TASK_STOP_NOT_CONFIRMED", message: "TaskCell must be terminal or suspended before STOPPED evidence is accepted." }, 409);
        }
        return requireOwner(this.nodeStub(body.node_id), `/reservations/${encodeURIComponent(body.dispatch_id)}/release`, "POST", {
          node_incarnation: body.node_incarnation,
          evidence_type: body.evidence_type,
          evidence_id: body.evidence_id,
        }).then((reservation) => json({ reservation }));
      }

      if (request.method === "POST" && url.pathname === "/tasks/submit") {
        const rawSpec = (await request.json()) as Record<string, any>;
        const res = this.cluster.apiGateway.submitTaskSpec(rawSpec, Date.now() / 1000.0);
        return json(res.body, res.statusCode, res.headers);
      }

      if (request.method === "POST" && url.pathname === "/dag/spawn-escrow") {
        const body = await request.json() as { candidates?: SchedulerCandidate[]; nodes?: any[] };
        if (!Array.isArray(body.candidates) || body.candidates.some((candidate) => candidate.kind !== "dag")) {
          return json({ error: "DAG_CAPACITY_REQUEST_REQUIRED", message: "DAG starts must provide candidates for the common NodeCapacity dispatch path." }, 400);
        }
        await this.registerNodes(body.nodes ?? []);
        const results = await this.scheduleCandidates(body.candidates);
        return json({ dispatched_children: results.filter((result) => result.stage === "STARTED").map((result) => result.task_id), dispatches: results });
      }

      if (request.method === "POST" && url.pathname === "/chaos/inject") {
        const body = (await request.json()) as { scenario: string; error_rate?: number };
        if (body.scenario === "deepseek-429") this.cluster.cogRouter.tripPoolBreaker("DeepSeek", body.error_rate ?? 0.50);
        const stage = this.cluster.bpController.evaluate(
          { model429Rate: body.error_rate ?? 0.50 } as ClusterPressureSignals,
          Array.from(this.cluster.scheduler.tasks.values()),
        );
        return json({ scenario: body.scenario, deepseek_breaker: this.cluster.cogRouter.pools.DeepSeek.breakerState, backpressure_stage: stage });
      }

      if (request.method === "GET" && url.pathname === "/cluster/top") {
        const snap = this.cluster.clusterTopSnapshot();
        const activeDispatches = this.schedulerPool.listDispatches().filter((dispatch) => dispatch.stage !== "STARTED");
        return json({ ...snap, celld_sqlite_leases_count: activeDispatches.length });
      }

      return json({ error: "NotFound", path: url.pathname }, 404);
    } catch (error) {
      if (error instanceof OwnerRequestError) return json({ error: error.code, message: error.message }, error.status);
      throw error;
    }
  }

  private pendingCandidates(): SchedulerCandidate[] {
    return this.schedulerPool.listDispatches()
      .filter((dispatch) => dispatch.stage !== "STARTED")
      .map((dispatch) => JSON.parse(dispatch.request_json) as SchedulerCandidate);
  }

  private async reconcileHeartbeatLoss(): Promise<Array<{ task_id: string; dispatch_id: string; reason: string }>> {
    const suspected: Array<{ task_id: string; dispatch_id: string; reason: string }> = [];
    const now = Date.now();
    for (const dispatch of this.schedulerPool.listDispatches().filter((row) => row.stage === "STARTED")) {
      const taskStub = persistenceCellStub(this.env, "task", dispatch.task_id);
      const taskResult = await callOwner(taskStub, "/execution/state", "GET");
      if (!taskResult.response.ok) continue;
      const execution = taskResult.body;
      if (execution.current_dispatch_id !== dispatch.dispatch_id || Number(execution.business_epoch) !== Number(dispatch.business_epoch)) continue;
      if (execution.lifecycle_state !== "RUNNING" && execution.lifecycle_state !== "STARTING") continue;

      const nodeResult = await callOwner(this.nodeStub(dispatch.node_id), "/state", "GET");
      const currentNode = nodeResult.response.ok ? nodeResult.body : null;
      let reason: string | null = null;
      if (!currentNode || currentNode.incarnation !== dispatch.node_incarnation) reason = "node-incarnation-changed";
      else if (now - Number(currentNode.updated_at) > 12_000) reason = "node-heartbeat-timeout";
      if (!reason) continue;

      await requireOwner(taskStub, "/execution/suspect", "POST", {
        epoch: Number(dispatch.business_epoch),
        reason,
      });
      await callOwner(this.nodeStub(dispatch.node_id), `/reservations/${encodeURIComponent(dispatch.dispatch_id)}/suspect`, "POST", { reason });
      suspected.push({ task_id: dispatch.task_id, dispatch_id: dispatch.dispatch_id, reason });
    }
    return suspected;
  }

  private async registerNodes(nodes: any[]): Promise<void> {
    for (const node of nodes) {
      if (!node || typeof node.node_id !== "string" || typeof node.incarnation !== "string") {
        throw new OwnerRequestError("INVALID_NODE_REGISTRATION", "Each scheduler node requires node_id and incarnation.", 400);
      }
      await requireOwner(this.nodeStub(node.node_id), "/nodes/register", "POST", {
        node_id: node.node_id,
        incarnation: node.incarnation,
        capacity: node.capacity,
        reboot_evidence_id: node.reboot_evidence_id,
      });
    }
  }

  private async scheduleCandidates(candidates: SchedulerCandidate[]): Promise<Array<Record<string, unknown>>> {
    if (candidates.length === 0) return [];
    this.schedulerPool.validateCandidates(candidates);
    const resolved: SchedulerCandidate[] = [];
    const capacitiesByNode = new Map<string, NodeResourceVector>();
    for (const candidate of candidates) {
      const nodeResult = await callOwner(this.nodeStub(candidate.node_id), "/state", "GET");
      if (!nodeResult.response.ok) {
        resolved.push({ ...candidate, node_incarnation: "" });
        continue;
      }
      const nodeState = nodeResult.body as { incarnation: string; capacity: NodeResourceVector };
      if (candidate.node_incarnation && candidate.node_incarnation !== nodeState.incarnation) {
        resolved.push({ ...candidate, node_incarnation: "" });
        continue;
      }
      capacitiesByNode.set(candidate.node_id, nodeState.capacity);
      resolved.push({ ...candidate, node_incarnation: nodeState.incarnation });
    }
    const poolCapacity = sumNodeCapacities(capacitiesByNode.values());
    const results: Array<Record<string, unknown>> = [];
    for (const candidate of resolved.filter((item) => !item.node_incarnation)) {
      results.push({ task_id: candidate.task_id, dispatch_id: candidate.dispatch_id, status: "NODE_NOT_READY" });
    }
    const readyCandidates = resolved.filter((item) => Boolean(item.node_incarnation));
    if (readyCandidates.length === 0) return results;
    for (const candidate of this.schedulerPool.fairOrder(readyCandidates, poolCapacity)) {
      results.push(await this.dispatchCandidate(candidate));
    }
    return results;
  }

  private async dispatchCandidate(candidate: SchedulerCandidate): Promise<Record<string, unknown>> {
    const dispatchId = candidate.dispatch_id ?? `${candidate.kind ?? "ordinary"}:${candidate.task_id}:${candidate.expected_task_version ?? 0}`;
    const normalized: SchedulerCandidate = { ...candidate, dispatch_id: dispatchId };
    if (!candidate.node_incarnation) return { task_id: candidate.task_id, dispatch_id: dispatchId, status: "NODE_NOT_READY" };
    let delegatedQuota: { delegated_tokens: number; delegated_tps: number } | undefined;

    if (candidate.kind === "dag") {
      if (!candidate.dag_id || !candidate.voucher_id || !this.env.DAG_CELL) {
        return { task_id: candidate.task_id, dispatch_id: dispatchId, status: "BLOCKED_DAG_VOUCHER" };
      }
      const dagStub = this.env.DAG_CELL.get(this.env.DAG_CELL.idFromName(candidate.dag_id));
      const authority = await callOwner(dagStub, "/dags/dispatch-authority", "POST", {
        tenant_id: candidate.tenant_id,
        task_id: candidate.task_id,
        voucher_id: candidate.voucher_id,
      });
      if (!authority.response.ok || authority.body?.authorized !== true) {
        return { task_id: candidate.task_id, dispatch_id: dispatchId, status: "BLOCKED_DAG_VOUCHER", error: authority.body?.error };
      }
      const tokens = Number(authority.body.delegated_tokens);
      const tps = Number(authority.body.delegated_tps);
      if (!Number.isSafeInteger(tokens) || tokens < 0 || !Number.isSafeInteger(tps) || tps < 0 || candidate.resources.tps < tps) {
        return { task_id: candidate.task_id, dispatch_id: dispatchId, status: "BLOCKED_DAG_QUOTA" };
      }
      delegatedQuota = { delegated_tokens: tokens, delegated_tps: tps };
    }

    try {
      let dispatch = await this.schedulerPool.ensureIntent(normalized);
      const taskStub = persistenceCellStub(this.env, "task", candidate.task_id);
      const taskBase = {
        dispatch_id: dispatchId,
        task_id: candidate.task_id,
        tenant_id: candidate.tenant_id,
        kind: candidate.kind ?? "ordinary",
        node_id: candidate.node_id,
        node_incarnation: candidate.node_incarnation,
        region: candidate.region ?? "default",
        resource_pool: candidate.resource_pool ?? "default",
        task_spec: candidate.task_spec,
        execution_policy: candidate.execution_policy,
        ...(delegatedQuota ? { dag_id: candidate.dag_id, voucher_id: candidate.voucher_id, ...delegatedQuota } : {}),
      };

      await requireOwner(taskStub, "/execution/prepare", "POST", taskBase);

      if (dispatch.stage === "PREPARING") {
        const reservationResult = await callOwner(this.nodeStub(candidate.node_id), "/reservations", "POST", {
          dispatch_id: dispatchId,
          task_id: candidate.task_id,
          tenant_id: candidate.tenant_id,
          kind: candidate.kind ?? "ordinary",
          node_incarnation: candidate.node_incarnation,
          resources: candidate.resources,
        });
        if (!reservationResult.response.ok && reservationResult.body?.error === "NODE_CAPACITY_EXCEEDED") {
          return { task_id: candidate.task_id, dispatch_id: dispatchId, stage: dispatch.stage, status: "BLOCKED_CAPACITY" };
        }
        if (!reservationResult.response.ok) throw ownerError(reservationResult);
        const reservation = reservationResult.body as any;
        const taskReservation = { ...taskBase, reservation_id: dispatchId, reservation_token: reservation.reservation_token };
        await requireOwner(taskStub, "/execution/reserved", "POST", taskReservation);
        dispatch = await this.schedulerPool.advance({
          dispatch_id: dispatchId,
          stage: "RESERVED",
          reservation_id: dispatchId,
          reservation_token: reservation.reservation_token,
        });
      }

      if (dispatch.stage === "RESERVED") {
        const nodeReservation = await callOwner(this.nodeStub(candidate.node_id), `/reservations/${encodeURIComponent(dispatchId)}`, "GET");
        if (!nodeReservation.response.ok) throw ownerError(nodeReservation);
        const reservation = nodeReservation.body as any;
        const authorized = await callOwner(taskStub, "/execution/authorize", "POST", {
          ...taskBase,
          reservation_id: dispatch.reservation_id,
          reservation_token: dispatch.reservation_token,
        });
        if (!authorized.response.ok) throw ownerError(authorized);
        const authorization = authorized.body as any;
        await requireOwner(this.nodeStub(candidate.node_id), `/reservations/${encodeURIComponent(dispatchId)}/authorize`, "POST", {
          reservation_token: reservation.reservation_token,
          authorization_token: authorization.authorization_token,
          business_epoch: authorization.business_epoch,
          node_incarnation: candidate.node_incarnation,
        });
        dispatch = await this.schedulerPool.advance({
          dispatch_id: dispatchId,
          stage: "AUTHORIZED",
          authorization_token: authorization.authorization_token,
          business_epoch: authorization.business_epoch,
        });
      }

      return {
        task_id: dispatch.task_id,
        tenant_id: dispatch.tenant_id,
        dispatch_id: dispatch.dispatch_id,
        node_id: dispatch.node_id,
        node_incarnation: dispatch.node_incarnation,
        region: dispatch.region,
        resource_pool: dispatch.resource_pool,
        stage: dispatch.stage,
        business_epoch: dispatch.business_epoch,
        authorization_token: dispatch.authorization_token,
        ...(dispatch.stage === "AUTHORIZED" ? { status: "AWAITING_NODE_START" } : {}),
      };
    } catch (error) {
      if (error instanceof OwnerRequestError && error.code === "NODE_CAPACITY_EXCEEDED") {
        return { task_id: candidate.task_id, dispatch_id: dispatchId, stage: "PREPARING", status: "BLOCKED_CAPACITY" };
      }
      throw error;
    }
  }

  private async recordNodeStart(body: {
    dispatch_id: string;
    node_id: string;
    node_incarnation: string;
    authorization_token: string;
    business_epoch: number;
    start_receipt: string;
  }): Promise<Response> {
    const dispatch = this.schedulerPool.getDispatch(body.dispatch_id);
    if (!dispatch) return json({ error: "DISPATCH_NOT_FOUND" }, 404);
    if (
      dispatch.node_id !== body.node_id || dispatch.node_incarnation !== body.node_incarnation ||
      dispatch.authorization_token !== body.authorization_token || Number(dispatch.business_epoch) !== body.business_epoch
    ) return json({ error: "NODE_START_AUTHORIZATION_MISMATCH" }, 409);
    if (dispatch.stage !== "AUTHORIZED" && dispatch.stage !== "STARTED") {
      return json({ error: "DISPATCH_NOT_AUTHORIZED", stage: dispatch.stage }, 409);
    }

    const taskStub = persistenceCellStub(this.env, "task", dispatch.task_id);
    const taskDispatch = await requireOwner(taskStub, `/execution/dispatches/${encodeURIComponent(body.dispatch_id)}`, "GET");
    const taskState = await requireOwner(taskStub, "/execution/state", "GET");
    if (
      taskDispatch.authorization_token !== body.authorization_token ||
      Number(taskDispatch.business_epoch) !== body.business_epoch ||
      taskDispatch.node_id !== body.node_id || taskDispatch.node_incarnation !== body.node_incarnation ||
      taskDispatch.reservation_id !== dispatch.reservation_id ||
      Number(taskState.business_epoch) !== body.business_epoch || taskState.current_dispatch_id !== body.dispatch_id
    ) return json({ error: "TASK_START_AUTHORIZATION_MISMATCH" }, 409);

    const authorization = {
      dispatch_id: body.dispatch_id,
      task_id: dispatch.task_id,
      node_id: body.node_id,
      node_incarnation: body.node_incarnation,
      reservation_id: taskDispatch.reservation_id,
      reservation_token: taskDispatch.reservation_token,
      authorization_token: body.authorization_token,
      business_epoch: body.business_epoch,
      turn_seq: Number(taskState.last_turn_seq) + 1,
    };
    await requireOwner(this.nodeStub(body.node_id), `/reservations/${encodeURIComponent(body.dispatch_id)}/started`, "POST", {
      authorization_token: body.authorization_token,
      business_epoch: body.business_epoch,
      start_receipt: body.start_receipt,
    });
    await requireOwner(taskStub, "/execution/started", "POST", { authorization, start_receipt: body.start_receipt });
    const started = await this.schedulerPool.advance({
      dispatch_id: body.dispatch_id,
      stage: "STARTED",
      authorization_token: body.authorization_token,
      business_epoch: body.business_epoch,
    });
    return json({ dispatch_id: started.dispatch_id, task_id: started.task_id, stage: started.stage, business_epoch: started.business_epoch });
  }

  private nodeStub(nodeId: string): DurableObjectStub {
    const namespace = this.env.NODE_CAPACITY_CELL as PersistenceCellNamespace | undefined;
    if (!namespace) throw new OwnerRequestError("NODE_CAPACITY_UNAVAILABLE", "NODE_CAPACITY_CELL binding is required for task dispatch.", 503);
    return namespace.get(namespace.idFromName(nodeId));
  }
}

function sumNodeCapacities(capacities: Iterable<NodeResourceVector>): NodeResourceVector {
  const total: NodeResourceVector = { cpu: 0, memory_mb: 0, burst_cpu: 0, sessions: 0, tps: 0 };
  let found = false;
  for (const capacity of capacities) {
    found = true;
    total.cpu += capacity.cpu;
    total.memory_mb += capacity.memory_mb;
    total.burst_cpu += capacity.burst_cpu;
    total.sessions += capacity.sessions;
    total.tps += capacity.tps;
  }
  return found ? total : { cpu: 1, memory_mb: 1, burst_cpu: 1, sessions: 1, tps: 1 };
}

class OwnerRequestError extends Error {
  constructor(public readonly code: string, message: string, public readonly status: number) {
    super(message);
    this.name = "OwnerRequestError";
  }
}

async function callOwner(stub: DurableObjectStub, path: string, method: string, body?: unknown): Promise<OwnerResult<any>> {
  const response = await stub.fetch(`http://owner.internal${path}`, {
    method,
    headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  let parsed: any = null;
  try { parsed = await response.json(); } catch { parsed = null; }
  return { response, body: parsed };
}

async function requireOwner(stub: DurableObjectStub, path: string, method: string, body?: unknown): Promise<any> {
  const result = await callOwner(stub, path, method, body);
  if (!result.response.ok) throw ownerError(result);
  return result.body;
}

function ownerError(result: OwnerResult): OwnerRequestError {
  return new OwnerRequestError(result.body?.error ?? "OWNER_UNAVAILABLE", result.body?.message ?? "An owner Cell rejected the request.", result.response.status);
}

async function readOptionalJson(request: Request): Promise<any> {
  const text = await request.text();
  if (!text.trim()) return {};
  try { return JSON.parse(text); } catch { throw new OwnerRequestError("INVALID_JSON", "Scheduler request must contain valid JSON.", 400); }
}

function json(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });
}
