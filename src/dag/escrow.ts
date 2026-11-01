/**
 * TASK-08: Multi-Agent DAG State Machine, HMAC-Signed Quota Escrow Vouchers & Straggler Acceleration.
 */

import * as crypto from "crypto";
import { AgentTask, EscrowVoucher, SandboxPhase, TaskState } from "../models";
import { CreateDagRequest, DagBoostVoucher, DagRecord, DagVoucher } from "../persistence/dag/dag";
import { PersistenceCellNamespace } from "../persistence/contracts";

export class DurableDagRequestError extends Error {
  constructor(public readonly code: string, message: string, public readonly status: number) {
    super(message);
    this.name = "DurableDagRequestError";
  }
}

/** Thin adapter used by schedulers and task coordinators to call the durable DagCell owner. */
export class DurableDagCellClient {
  constructor(public readonly namespace: PersistenceCellNamespace) {}

  public create(request: CreateDagRequest): Promise<DagRecord> {
    return this.call(request.dag_id, "/dags", "POST", request);
  }

  public reconcile(dagId: string, tenantId: string): Promise<DagRecord> {
    return this.call(dagId, "/dags/reconcile", "POST", { tenant_id: tenantId });
  }

  public redeem(dagId: string, tenantId: string, childTaskId: string, voucherId: string, signature: string): Promise<unknown> {
    return this.call(dagId, `/dags/children/${encodeURIComponent(childTaskId)}/redeem`, "POST", {
      tenant_id: tenantId, voucher_id: voucherId, signature,
    });
  }

  public observe(dagId: string, tenantId: string, childTaskId: string, observation: {
    event_id: string; aggregate_version: number; state: string; evidence?: unknown;
  }): Promise<unknown> {
    return this.call(dagId, `/dags/children/${encodeURIComponent(childTaskId)}/observations`, "POST", {
      tenant_id: tenantId, ...observation,
    });
  }

  public returnAllowance(dagId: string, tenantId: string, childTaskId: string): Promise<unknown> {
    return this.call(dagId, `/dags/children/${encodeURIComponent(childTaskId)}/return-allowance`, "POST", { tenant_id: tenantId });
  }

  public boostAllowance(dagId: string, input: {
    tenant_id: string; boost_id: string; source_task_id: string; target_task_id: string;
    amount_minor: number; delegated_tps: number; delegated_tokens: number;
  }): Promise<unknown> {
    return this.call(dagId, "/dags/boosts", "POST", input);
  }

  private async call<T>(dagId: string, path: string, method: string, body: unknown): Promise<T> {
    const stub = this.namespace.get(this.namespace.idFromName(dagId));
    const response = await stub.fetch(`http://dag.internal${path}`, {
      method,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    let result: any;
    try { result = await response.json(); } catch { result = { error: "INVALID_DAG_RESPONSE" }; }
    if (!response.ok) throw new DurableDagRequestError(result.error ?? "DAG_CELL_UNAVAILABLE", result.message ?? "DagCell rejected the request.", response.status);
    return result as T;
  }
}

export class DagEscrowCoordinator {
  private secret: Buffer;
  private signatures: Map<string, string> = new Map();

  constructor(clusterSecret: string = "ansts-escrow-hmac-secret-v2") {
    this.secret = Buffer.from(clusterSecret, "utf8");
  }

  public signVoucher(voucher: EscrowVoucher): string {
    const msg = `${voucher.parentTaskId}:${voucher.parentFence.epoch}:${voucher.parentFence.turnSeq}:${voucher.childTaskId}:${voucher.delegatedTps}:${voucher.delegatedTokens}`;
    const sig = crypto.createHmac("sha256", this.secret).update(msg, "utf8").digest("hex");
    this.signatures.set(voucher.childTaskId, sig);
    return sig;
  }

  public verifyVoucher(voucher: EscrowVoucher, signature: string): boolean {
    const expected = this.signVoucher(voucher);
    return safeEqualHex(expected, signature);
  }

  /** Stable signature for durable DagCell vouchers; it contains the tenant, fence and money grant. */
  public signDurableVoucher(voucher: DagVoucher): string {
    const message = durableVoucherMessage(voucher);
    return crypto.createHmac("sha256", this.secret).update(message, "utf8").digest("hex");
  }

  public verifyDurableVoucher(voucher: DagVoucher, signature: string): boolean {
    return safeEqualHex(this.signDurableVoucher(voucher), signature);
  }

  public signBoostVoucher(voucher: DagBoostVoucher): string {
    return crypto.createHmac("sha256", this.secret).update(JSON.stringify([
      voucher.voucher_id, voucher.boost_id, voucher.tenant_id, voucher.dag_id,
      voucher.source_task_id, voucher.target_task_id, voucher.amount_minor,
      voucher.delegated_tps, voucher.delegated_tokens, voucher.period_key, voucher.currency,
    ]), "utf8").digest("hex");
  }

  public yieldParentAndIssueVouchers(parent: AgentTask, children: AgentTask[]): EscrowVoucher[] {
    parent.state = TaskState.HIBERNATED_YIELD;
    parent.phase = SandboxPhase.IDLE;
    parent.activeSessions = 0;

    const m = Math.max(1, children.length);
    const perTps = Math.floor(parent.reservedTps / m);
    const remTok = Math.max(0, parent.maxTotalTokens - parent.usedTokens);
    const perTok = Math.floor(remTok / m);

    const vouchers: EscrowVoucher[] = [];
    for (const child of children) {
      child.reservedTps = perTps;
      child.maxTotalTokens = perTok;
      const v: EscrowVoucher = {
        parentTaskId: parent.taskId,
        parentFence: parent.fence,
        childTaskId: child.taskId,
        delegatedTps: perTps,
        delegatedTokens: perTok,
      };
      this.signVoucher(v);
      vouchers.push(v);
    }
    return vouchers;
  }
}

function durableVoucherMessage(voucher: DagVoucher): string {
  return JSON.stringify([
    voucher.voucher_id,
    voucher.tenant_id,
    voucher.dag_id,
    voucher.parent_task_id,
    voucher.parent_epoch,
    voucher.parent_turn_seq,
    voucher.child_task_id,
    voucher.branch_id,
    voucher.delegated_tps,
    voucher.delegated_tokens,
    voucher.allowance_minor,
    voucher.period_key,
    voucher.currency,
  ]);
}

function safeEqualHex(expected: string, actual: string): boolean {
  if (!/^[a-f0-9]{64}$/i.test(actual)) return false;
  return crypto.timingSafeEqual(Buffer.from(expected, "hex"), Buffer.from(actual, "hex"));
}
