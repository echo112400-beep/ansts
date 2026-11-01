import { DagEscrowCoordinator } from "../dag/escrow";
import {
  CreateDagRequest,
  DagChildRecord,
  DagPersistenceError,
  DagRecord,
  DagBoostVoucher,
  DagStore,
  DagVoucher,
  dagBoostMigration,
  dagMigration,
} from "../persistence/dag/dag";
import { initializeCellProtocol } from "../persistence/schema";
import { CloudflareCelldEnv, DurableObjectState } from "./env";

/** Durable coordinator for one tenant DAG. Monetary balances remain owned by TenantLedgerCell. */
export class DagCell {
  public readonly dagId: string;
  public readonly dag: DagStore;

  constructor(public readonly ctx: DurableObjectState, public readonly env: CloudflareCelldEnv) {
    this.dagId = ctx.id.name ?? ctx.id.toString();
    initializeCellProtocol(ctx.storage.sql, { type: "dag", key: this.dagId }, [dagMigration, dagBoostMigration]);
    this.dag = new DagStore(ctx.storage, this.dagId);
  }

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (request.method === "POST" && url.pathname === "/dags") {
        const body = await request.json() as CreateDagRequest;
        const signer = body.children.length > 0 ? this.voucherSigner() : null;
        const created = await this.dag.create(body, (voucher) => signer!.signDurableVoucher(voucher));
        await this.ctx.storage.setAlarm(Date.now() + 1_000);
        await this.reconcile(created.tenant_id);
        return json(this.publicDag(this.dag.get()!), 200);
      }

      if (request.method === "GET" && url.pathname === "/dags/state") {
        const dag = this.requireTenant(url.searchParams.get("tenant_id"));
        return json(this.publicDag(dag));
      }

      if (request.method === "POST" && url.pathname === "/dags/reconcile") {
        const tenantId = requireBodyTenant(await request.json());
        const dag = await this.reconcile(tenantId);
        return json(this.publicDag(dag));
      }

      if (request.method === "POST" && url.pathname === "/dags/boosts") {
        const body = await request.json() as {
          tenant_id?: string; boost_id?: string; source_task_id?: string; target_task_id?: string;
          amount_minor?: number; delegated_tps?: number; delegated_tokens?: number;
        };
        const dag = this.requireTenant(body.tenant_id);
        const boostId = String(body.boost_id ?? "");
        const sourceTaskId = String(body.source_task_id ?? "");
        const targetTaskId = String(body.target_task_id ?? "");
        const amountMinor = Number(body.amount_minor);
        const delegatedTps = Number(body.delegated_tps ?? 0);
        const delegatedTokens = Number(body.delegated_tokens ?? 0);
        const voucher: DagBoostVoucher = {
          voucher_id: `dag-boost-voucher:${dag.dag_id}:${encodeURIComponent(boostId)}`,
          boost_id: boostId,
          tenant_id: dag.tenant_id,
          dag_id: dag.dag_id,
          source_task_id: sourceTaskId,
          target_task_id: targetTaskId,
          amount_minor: amountMinor,
          delegated_tps: delegatedTps,
          delegated_tokens: delegatedTokens,
          period_key: dag.period_key,
          currency: dag.currency,
        };
        const signature = this.voucherSigner().signBoostVoucher(voucher);
        const existing = this.dag.boosts().find((boost) => boost.boost_id === boostId);
        if (!existing) {
          const source = this.requireChild(sourceTaskId);
          const target = this.requireChild(targetTaskId);
          const targetExecution = await this.ownerJson("task", targetTaskId, "/execution/state", "GET");
          if (targetExecution.tenant_id !== dag.tenant_id || targetExecution.lifecycle_state !== "RUNNING" || target.observed_state !== "RUNNING") {
            throw new DagPersistenceError("DAG_BOOST_TARGET_NOT_RUNNING", "Boost target must be a RUNNING child in TaskCell and DagCell.", 409);
          }
          const sourceExecution = await this.ownerJson("task", sourceTaskId, "/execution/state", "GET");
          if (sourceExecution.tenant_id !== dag.tenant_id || !["SUCCEEDED", "FAILED", "SUSPENDED", "QUEUED"].includes(sourceExecution.lifecycle_state)) {
            throw new DagPersistenceError("DAG_BOOST_SOURCE_NOT_TERMINAL", "Boost source must be terminal or safely stopped in TaskCell.", 409);
          }
          await this.verifyStoppedChild(source);
          await this.assertChildSettled(dag, sourceTaskId);
        }
        const boost = await this.dag.beginBoost({
          boost_id: boostId,
          source_task_id: sourceTaskId,
          target_task_id: targetTaskId,
          amount_minor: amountMinor,
          delegated_tps: delegatedTps,
          delegated_tokens: delegatedTokens,
          voucher,
          signature,
        });
        if (boost.status === "PENDING") {
          await this.ownerJson("tenant-ledger", dag.tenant_id, "/allowance-transfers", "POST", {
            tenant_id: dag.tenant_id,
            period_key: dag.period_key,
            transfer_id: boost.transfer_id,
            source_task_id: boost.source_task_id,
            target_task_id: boost.target_task_id,
            amount_minor: boost.amount_minor,
            currency: dag.currency,
          });
          await this.dag.finishBoost(boostId);
        }
        return json({
          boost: this.publicBoost(this.dag.boosts().find((entry) => entry.boost_id === boostId)!),
          dag: this.publicDag(this.dag.get()!),
        }, existing ? 200 : 201);
      }

      const redeemMatch = url.pathname.match(/^\/dags\/children\/([^/]+)\/redeem$/);
      if (request.method === "POST" && redeemMatch) {
        const body = await request.json() as { tenant_id?: string; voucher_id?: string; signature?: string };
        this.requireTenant(body.tenant_id);
        const childTaskId = decodeURIComponent(redeemMatch[1]);
        const child = this.requireChild(childTaskId);
        const voucher = JSON.parse(child.voucher_json) as DagVoucher;
        if (body.voucher_id !== child.voucher_id || !this.voucherSigner().verifyDurableVoucher(voucher, String(body.signature ?? "")) || child.voucher_signature !== body.signature) {
          throw new DagPersistenceError("DAG_VOUCHER_SIGNATURE_INVALID", "Voucher identity or signature is invalid.", 403);
        }
        const redeemed = await this.dag.redeem(childTaskId, child.voucher_id);
        return json({ dag_id: this.dagId, tenant_id: redeemed.tenant_id, child_task_id: redeemed.task_id, voucher_id: redeemed.voucher_id, status: redeemed.voucher_status, dispatch_kind: "dag" });
      }

      const observeMatch = url.pathname.match(/^\/dags\/children\/([^/]+)\/observations$/);
      if (request.method === "POST" && observeMatch) {
        const body = await request.json() as { tenant_id?: string; event_id?: string; aggregate_version?: number; state?: string; evidence?: unknown };
        const dag = this.requireTenant(body.tenant_id);
        const childTaskId = decodeURIComponent(observeMatch[1]);
        await this.verifyObservationState(dag, childTaskId, String(body.state ?? ""));
        if (body.state === "STOPPED") await this.verifyStoppedChild(this.requireChild(childTaskId));
        const result = await this.dag.observe({
          event_id: String(body.event_id ?? ""), task_id: childTaskId,
          aggregate_version: Number(body.aggregate_version), state: body.state as any,
          ...(body.evidence === undefined ? {} : { evidence: body.evidence }),
        });
        if (dag.race_mode && body.state === "SUCCEEDED") await this.dag.selectWinner(childTaskId);
        const updated = await this.reconcile(dag.tenant_id);
        return json({ duplicate: result.duplicate, child: this.publicChild(this.requireChild(childTaskId)), dag: this.publicDag(updated) });
      }

      const returnMatch = url.pathname.match(/^\/dags\/children\/([^/]+)\/return-allowance$/);
      if (request.method === "POST" && returnMatch) {
        const body = await request.json() as { tenant_id?: string };
        const dag = this.requireTenant(body.tenant_id);
        const child = this.requireChild(decodeURIComponent(returnMatch[1]));
        const response = await this.returnAllowance(dag, child);
        return json({ ...response, dag: this.publicDag(this.dag.get()!) });
      }

      if (request.method === "POST" && url.pathname === "/dags/dispatch-authority") {
        const body = await request.json() as { tenant_id?: string; task_id?: string; voucher_id?: string };
        const dag = this.requireTenant(body.tenant_id);
        const authorized = await this.dag.isDispatchAuthorized(String(body.task_id ?? ""), String(body.voucher_id ?? ""));
        const raceAllowsDispatch = !dag.race_mode || !dag.winner_task_id || dag.winner_task_id === body.task_id;
        return authorized && raceAllowsDispatch
          ? json({ authorized: true, dag_id: this.dagId, task_id: body.task_id, voucher_id: body.voucher_id })
          : json({ error: "DAG_DISPATCH_NOT_AUTHORIZED", authorized: false }, 403);
      }

      if (request.method === "POST" && url.pathname === "/dags/branch-authority") {
        const body = await request.json() as { tenant_id?: string; task_id?: string; race_id?: string; branch_id?: string };
        const dag = this.requireTenant(body.tenant_id);
        if (!dag.race_mode || body.race_id !== dag.dag_id) throw new DagPersistenceError("DAG_RACE_IDENTITY_INVALID", "Raced operation does not identify this speculative DagCell.", 403);
        const child = this.requireChild(String(body.task_id ?? ""));
        if (child.branch_id !== body.branch_id) throw new DagPersistenceError("DAG_RACE_BRANCH_MISMATCH", "Operation branch does not match the durable child topology.", 403);
        if (dag.winner_task_id && dag.winner_task_id !== child.task_id) {
          throw new DagPersistenceError("DAG_RACE_LOST", `Branch ${child.branch_id} lost race ${dag.dag_id}.`, 409);
        }
        return json({ authorized: true, dag_id: dag.dag_id, task_id: child.task_id, branch_id: child.branch_id,
          winner_task_id: dag.winner_task_id, winner_selected: Boolean(dag.winner_task_id) });
      }

      return json({ error: "NotFound", path: url.pathname }, 404);
    } catch (error) {
      const current = this.dag.get();
      if (current && this.needsReconcile(current)) {
        await this.ctx.storage.setAlarm(Date.now() + 5_000).catch(() => undefined);
      }
      if (error instanceof DagPersistenceError) return json({ error: error.code, message: error.message }, error.status);
      if (error instanceof Error && error.name === "DagOwnerRequestError") {
        const requestError = error as Error & { status?: number; body?: unknown };
        return json(requestError.body ?? { error: "DAG_OWNER_UNAVAILABLE", message: error.message }, requestError.status ?? 503);
      }
      throw error;
    }
  }

  /** Durable wake-up for interrupted parent yield, allowance transfer, winner delivery or resume. */
  public async alarm(): Promise<void> {
    const dag = this.dag.get();
    if (!dag || !this.needsReconcile(dag)) return;
    try {
      await this.reconcile(dag.tenant_id);
    } catch {
      await this.ctx.storage.setAlarm(Date.now() + 5_000);
    }
    const latest = this.dag.get();
    if (latest && this.needsReconcile(latest)) await this.ctx.storage.setAlarm(Date.now() + 5_000);
  }

  private needsReconcile(dag: DagRecord): boolean {
    if (["PREPARING", "TRANSFERRING", "RESUMING"].includes(dag.state)) return true;
    if (dag.state !== "ACTIVE") return false;
    if (dag.race_mode) return !dag.winner_task_id && dag.children.some((child) => child.observed_state === "SUCCEEDED");
    return dag.children.length > 0 && dag.children.every((child) => ["SUCCEEDED", "FAILED", "STOPPED"].includes(child.observed_state));
  }

  private async reconcile(tenantId: string): Promise<DagRecord> {
    let dag = this.requireTenant(tenantId);
    if (!dag.parent_yielded) {
      for (const child of dag.children) {
        const admission = await this.ownerJson("tenant-ledger", dag.tenant_id, `/tasks/${encodeURIComponent(child.task_id)}?tenant_id=${encodeURIComponent(dag.tenant_id)}`, "GET");
        if (admission.task_id !== child.task_id || admission.tenant_id !== dag.tenant_id) {
          throw new DagPersistenceError("DAG_CHILD_TENANT_MISMATCH", `Child ${child.task_id} is not admitted under the DAG tenant.`, 403);
        }
      }
      const parent = await this.ownerJson("task", dag.parent_task_id, "/execution/state", "GET");
      if (parent.tenant_id !== dag.tenant_id || Number(parent.business_epoch) !== dag.parent_epoch) {
        throw new DagPersistenceError("DAG_PARENT_FENCE_MISMATCH", "Parent task tenant or execution epoch does not match the durable DAG fence.", 409);
      }
      if (parent.lifecycle_state === "RUNNING") {
        await this.ownerJson("task", dag.parent_task_id, "/execution/suspend", "POST", { epoch: dag.parent_epoch, progress: { yielded_to_dag: dag.dag_id, parent_turn_seq: dag.parent_turn_seq } });
      } else if (parent.lifecycle_state !== "SUSPENDED" && parent.lifecycle_state !== "QUEUED") {
        throw new DagPersistenceError("DAG_PARENT_NOT_YIELDABLE", `Parent is ${String(parent.lifecycle_state)} and cannot safely yield.`, 409);
      }
      await this.dag.markParentYielded();
      dag = this.dag.get()!;
    }

    for (const child of dag.children) {
      if (child.transfer_status !== "PENDING") continue;
      await this.ownerJson("tenant-ledger", dag.tenant_id, "/allowance-transfers", "POST", {
        tenant_id: dag.tenant_id,
        period_key: dag.period_key,
        transfer_id: child.transfer_id,
        source_task_id: dag.parent_task_id,
        target_task_id: child.task_id,
        amount_minor: child.allowance_minor,
        currency: dag.currency,
      });
      await this.dag.markTransferred(child.task_id);
    }

    dag = this.dag.get()!;
    if (dag.children.length === 0) {
      if (dag.state !== "RESUMING" && dag.state !== "COMPLETED") await this.dag.setState("RESUMING", "empty-join-ready");
      await this.resumeParent(dag);
      await this.dag.setState("COMPLETED", "empty-parent-resumed");
      return this.dag.get()!;
    }

    if (dag.state === "TRANSFERRING" || dag.state === "PREPARING") await this.dag.activate();
    dag = this.dag.get()!;
    if (dag.race_mode && dag.winner_task_id) {
      await this.reconcileRace(dag);
      return this.dag.get()!;
    }
    if (!dag.race_mode && dag.children.every((child) => ["SUCCEEDED", "FAILED", "STOPPED"].includes(child.observed_state))) {
      if (dag.state !== "RESUMING" && dag.state !== "COMPLETED") await this.dag.setState("RESUMING", "join-ready");
      await this.resumeParent(dag);
      await this.dag.setState("COMPLETED", "parent-resumed");
    }
    return this.dag.get()!;
  }

  private async reconcileRace(dag: DagRecord): Promise<void> {
    const winner = dag.winner_task_id!;
    for (const child of dag.children) {
      if (Number(child.winner_notified) === 1) continue;
      const event = {
        event_id: `dag-winner:${dag.dag_id}:${encodeURIComponent(child.task_id)}`,
        decision_id: dag.decision_id!,
        task_id: child.task_id,
        tenant_id: dag.tenant_id,
        race_id: dag.dag_id,
        winner_branch_id: dag.children.find((entry) => entry.task_id === winner)!.branch_id,
        aggregate_version: 1,
      };
      await this.ownerJson("task", child.task_id, `/race/winner?tenant_id=${encodeURIComponent(dag.tenant_id)}`, "POST", event);
      await this.dag.markWinnerNotified(child.task_id);
    }

    for (const originalChild of dag.children.filter((entry) => entry.task_id !== winner)) {
      let child = this.requireChild(originalChild.task_id);
      const execution = await this.ownerJson("task", child.task_id, "/execution/state", "GET");
      if (execution.tenant_id !== dag.tenant_id) throw new DagPersistenceError("DAG_CHILD_TENANT_MISMATCH", `Child ${child.task_id} belongs to another tenant.`, 403);
      if (execution.lifecycle_state === "RUNNING") {
        await this.ownerJson("task", child.task_id, "/execution/suspend", "POST", { epoch: Number(execution.business_epoch), progress: { pruned_by_dag: dag.dag_id, winner_task_id: winner } });
        await this.markChildStopping(child);
      } else if (["SUSPENDED", "FAILED", "SUCCEEDED"].includes(execution.lifecycle_state)) {
        await this.markChildStopping(child);
      } else if (execution.lifecycle_state !== "QUEUED") {
        throw new DagPersistenceError("DAG_LOSER_STOP_UNKNOWN", `Race loser ${child.task_id} is ${execution.lifecycle_state}; stop state is unresolved.`, 503);
      }
      child = this.requireChild(child.task_id);
      const operations = await this.ownerJson("task", child.task_id, `/operations?tenant_id=${encodeURIComponent(dag.tenant_id)}`, "GET");
      const unresolved = (operations.operations ?? []).filter((operation: any) => ["RUNNING", "OUTCOME_UNKNOWN"].includes(operation.status));
      if (unresolved.length > 0) throw new DagPersistenceError("DAG_LOSER_OPERATION_UNKNOWN", `Race loser ${child.task_id} has ${unresolved.length} unresolved external operation(s); reconciliation is required before parent resume.`, 409);
      const stopEvidence = await this.verifyStoppedChild(child);
      if (!(["STOPPED", "SUCCEEDED", "FAILED"].includes(child.observed_state))) {
        child = this.requireChild(child.task_id);
        await this.dag.observe({
          event_id: `dag-stopped:${dag.dag_id}:${encodeURIComponent(child.task_id)}`,
          task_id: child.task_id,
          aggregate_version: child.observation_version + 1,
          state: "STOPPED",
          evidence: stopEvidence,
        });
      }
      await this.assertChildSettled(dag, child.task_id);
    }

    if (dag.children.every((child) => Number(child.winner_notified) === 1)) {
      await this.resumeParent(this.dag.get()!);
      await this.dag.setState("COMPLETED", "race-parent-resumed");
    }
  }

  private async markChildStopping(child: DagChildRecord): Promise<void> {
    if (child.observed_state === "STOPPING" || ["STOPPED", "SUCCEEDED", "FAILED"].includes(child.observed_state)) return;
    await this.dag.observe({
      event_id: `dag-stop-requested:${this.dagId}:${encodeURIComponent(child.task_id)}`,
      task_id: child.task_id,
      aggregate_version: child.observation_version + 1,
      state: "STOPPING",
      evidence: { requested: true, reason: "race_loser", dag_id: this.dagId },
    });
  }

  private async verifyObservationState(dag: DagRecord, taskId: string, state: string): Promise<void> {
    if (!["RUNNING", "SUCCEEDED", "FAILED", "STOPPING", "STOPPED"].includes(state)) return;
    const execution = await this.ownerJson("task", taskId, "/execution/state", "GET");
    if (execution.tenant_id !== dag.tenant_id) throw new DagPersistenceError("DAG_CHILD_TENANT_MISMATCH", `Child ${taskId} belongs to another tenant.`, 403);
    if (state === "RUNNING" && execution.lifecycle_state !== "RUNNING") throw new DagPersistenceError("DAG_CHILD_STATE_UNVERIFIED", "RUNNING observation does not match TaskCell state.", 409);
    if (state === "SUCCEEDED" && execution.lifecycle_state !== "SUCCEEDED") throw new DagPersistenceError("DAG_CHILD_STATE_UNVERIFIED", "SUCCEEDED observation does not match TaskCell state.", 409);
    if (state === "FAILED" && execution.lifecycle_state !== "FAILED") throw new DagPersistenceError("DAG_CHILD_STATE_UNVERIFIED", "FAILED observation does not match TaskCell state.", 409);
    if (state === "STOPPING" && !["SUSPENDED", "QUEUED", "FAILED"].includes(execution.lifecycle_state)) throw new DagPersistenceError("DAG_CHILD_STATE_UNVERIFIED", "STOPPING requires the TaskCell to be suspended, queued, or failed.", 409);
  }

  private async resumeParent(dag: DagRecord): Promise<void> {
    const current = await this.ownerJson("task", dag.parent_task_id, "/execution/state", "GET");
    if (current.tenant_id !== dag.tenant_id) throw new DagPersistenceError("DAG_PARENT_TENANT_MISMATCH", "Parent task belongs to another tenant.", 403);
    if (Number(current.business_epoch) !== dag.parent_epoch) throw new DagPersistenceError("DAG_PARENT_FENCE_MISMATCH", "Parent business epoch changed before DAG resume.");
    if (current.lifecycle_state === "SUSPENDED") {
      await this.ownerJson("task", dag.parent_task_id, "/execution/resume", "POST", {});
    } else if (current.lifecycle_state !== "QUEUED" && current.lifecycle_state !== "RUNNING") {
      throw new DagPersistenceError("DAG_PARENT_RESUME_STATE", `Parent cannot resume from ${String(current.lifecycle_state)}.`, 409);
    }
  }

  private async returnAllowance(dag: DagRecord, child: DagChildRecord): Promise<Record<string, unknown>> {
    if (child.transfer_status === "RETURNED") return { task_id: child.task_id, status: "RETURNED", amount_minor: Number(child.returned_minor), transfer_id: child.return_transfer_id };
    if (!["STOPPED", "SUCCEEDED", "FAILED", "STOPPING"].includes(child.observed_state)) {
      throw new DagPersistenceError("DAG_CHILD_NOT_RECONCILED", "Allowance cannot return until the child is stopped and reconciled.", 409);
    }
    const stopEvidence = await this.verifyStoppedChild(child);
    if (child.observed_state === "STOPPING") {
      await this.dag.observe({ event_id: `dag-stopped:${dag.dag_id}:${encodeURIComponent(child.task_id)}`,
        task_id: child.task_id, aggregate_version: child.observation_version + 1, state: "STOPPED", evidence: stopEvidence });
    }

    const account = await this.assertChildSettled(dag, child.task_id);
    const amount = Number(account.available_minor);
    if (!Number.isSafeInteger(amount) || amount < 0) throw new DagPersistenceError("DAG_CHILD_ACCOUNT_INVALID", "TenantLedger returned an invalid available balance.", 503);
    const transferId = `dag-return:${dag.dag_id}:${encodeURIComponent(child.task_id)}`;
    if (amount > 0) {
      await this.ownerJson("tenant-ledger", dag.tenant_id, "/allowance-transfers", "POST", {
        tenant_id: dag.tenant_id, period_key: dag.period_key, transfer_id: transferId,
        source_task_id: child.task_id, target_task_id: dag.parent_task_id,
        amount_minor: amount, currency: dag.currency,
      });
    }
    await this.dag.markAllowanceReturned(child.task_id, transferId, amount);
    return { task_id: child.task_id, status: "RETURNED", amount_minor: amount, transfer_id: transferId };
  }

  private async assertChildSettled(dag: DagRecord, taskId: string): Promise<any> {
    const holds = await this.ownerJson("tenant-ledger", dag.tenant_id, `/budget-holds?period_key=${encodeURIComponent(dag.period_key)}&tenant_id=${encodeURIComponent(dag.tenant_id)}`, "GET");
    const unresolved = (holds.holds ?? []).filter((hold: any) => hold.task_id === taskId && ["HELD", "UNKNOWN"].includes(hold.status));
    if (unresolved.length > 0) throw new DagPersistenceError("DAG_CHILD_BUDGET_UNSETTLED", `Child ${taskId} has ${unresolved.length} unresolved budget hold(s).`, 409);
    const account = await this.ownerJson("tenant-ledger", dag.tenant_id, `/budget-accounts/${encodeURIComponent(taskId)}?period_key=${encodeURIComponent(dag.period_key)}&tenant_id=${encodeURIComponent(dag.tenant_id)}`, "GET");
    if (Number(account.reserved_minor) !== 0) throw new DagPersistenceError("DAG_CHILD_BUDGET_UNSETTLED", "Reserved child allowance cannot be returned or treated as settled.", 409);
    return account;
  }

  private async verifyStoppedChild(child: DagChildRecord): Promise<Record<string, unknown>> {
    const execution = await this.ownerJson("task", child.task_id, "/execution/state", "GET");
    if (execution.tenant_id !== child.tenant_id) throw new DagPersistenceError("DAG_CHILD_TENANT_MISMATCH", "Stop evidence task belongs to another tenant.", 403);
    if (["RUNNING", "STARTING", "RESERVED", "SUSPECT"].includes(execution.lifecycle_state)) {
      throw new DagPersistenceError("DAG_CHILD_STOP_UNCONFIRMED", `Child task is still ${String(execution.lifecycle_state)}.`, 409);
    }
    if (!execution.current_dispatch_id || !execution.current_node_id) return { task_id: child.task_id, lifecycle_state: execution.lifecycle_state, no_active_dispatch: true };
    const reservation = await this.ownerJson("node-capacity", execution.current_node_id, `/reservations/${encodeURIComponent(execution.current_dispatch_id)}`, "GET");
    if (!["RELEASED", "ISOLATED"].includes(reservation.status) || !reservation.release_evidence_id || !["STOPPED", "ISOLATED"].includes(reservation.release_evidence_type)) {
      throw new DagPersistenceError("DAG_CHILD_STOP_UNCONFIRMED", "NodeCapacity has not durably recorded stop or isolation evidence for the child dispatch.", 409);
    }
    return { task_id: child.task_id, dispatch_id: execution.current_dispatch_id, node_id: execution.current_node_id,
      reservation_status: reservation.status, release_evidence_id: reservation.release_evidence_id,
      release_evidence_type: reservation.release_evidence_type };
  }

  private async ownerJson(type: "task" | "tenant-ledger" | "node-capacity", key: string, path: string, method: string, body?: unknown): Promise<any> {
    let namespace;
    if (type === "task") namespace = this.env.TASK_CELL ?? this.env.AGENT_HARNESS_CELL;
    else if (type === "tenant-ledger") namespace = this.env.TENANT_LEDGER_CELL;
    else namespace = this.env.NODE_CAPACITY_CELL;
    if (!namespace) throw new DagPersistenceError("DAG_OWNER_UNAVAILABLE", `${type} Cell binding is required.`, 503);
    const stub = namespace.get(namespace.idFromName(key));
    const response = await stub.fetch(`http://${type}.internal${path}`, {
      method,
      headers: body === undefined ? undefined : { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    let parsed: any;
    try { parsed = await response.json(); } catch { parsed = { error: "INVALID_OWNER_RESPONSE" }; }
    if (!response.ok) {
      const error = new Error(String(parsed.message ?? parsed.error ?? `${type} Cell returned ${response.status}`)) as Error & { status: number; body: unknown };
      error.name = "DagOwnerRequestError";
      error.status = response.status;
      error.body = parsed;
      throw error;
    }
    return parsed;
  }

  private voucherSigner(): DagEscrowCoordinator {
    const secret = this.env.DAG_VOUCHER_SECRET;
    if (!secret || secret.length < 32) throw new DagPersistenceError("DAG_VOUCHER_SECRET_REQUIRED", "Configure a dedicated DAG_VOUCHER_SECRET of at least 32 characters before creating DAG vouchers.", 503);
    return new DagEscrowCoordinator(secret);
  }

  private requireTenant(tenantId: string | null | undefined): DagRecord {
    const dag = this.dag.get();
    if (!dag) throw new DagPersistenceError("DAG_NOT_FOUND", `DAG ${this.dagId} is unknown.`, 404);
    if (tenantId !== dag.tenant_id) throw new DagPersistenceError("DAG_NOT_FOUND", `DAG ${this.dagId} is not available in this tenant scope.`, 404);
    return dag;
  }

  private requireChild(taskId: string): DagChildRecord {
    const child = this.dag.child(taskId);
    if (!child) throw new DagPersistenceError("DAG_CHILD_NOT_FOUND", `Task ${taskId} is not a child of DAG ${this.dagId}.`, 404);
    return child;
  }

  private publicDag(dag: DagRecord): Record<string, unknown> {
    return { ...dag, children: dag.children.map((child) => this.publicChild(child)), boosts: this.dag.boosts().map((boost) => this.publicBoost(boost)) };
  }
  private publicBoost(boost: ReturnType<DagStore["boosts"]>[number]): Record<string, unknown> {
    return { ...boost, voucher: JSON.parse(boost.voucher_json) };
  }
  private publicChild(child: DagChildRecord): Record<string, unknown> {
    return { dag_id: child.dag_id, tenant_id: child.tenant_id, task_id: child.task_id, branch_id: child.branch_id,
      voucher_id: child.voucher_id, voucher: JSON.parse(child.voucher_json), voucher_signature: child.voucher_signature,
      delegated_tps: Number(child.delegated_tps), delegated_tokens: Number(child.delegated_tokens),
      allowance_minor: Number(child.allowance_minor), transfer_id: child.transfer_id, transfer_status: child.transfer_status,
      returned_minor: Number(child.returned_minor ?? 0), voucher_status: child.voucher_status,
      observed_state: child.observed_state, observation_version: Number(child.observation_version),
      winner_notified: Number(child.winner_notified) === 1, stop_evidence: child.stop_evidence_json ? JSON.parse(child.stop_evidence_json) : null,
      return_transfer_id: child.return_transfer_id };
  }
}

function requireBodyTenant(body: any): string {
  if (!body || typeof body.tenant_id !== "string" || !body.tenant_id.trim()) throw new DagPersistenceError("TENANT_REQUIRED", "tenant_id is required.", 400);
  return body.tenant_id;
}
function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
