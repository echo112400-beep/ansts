import { CellOperationCommand, CellMigration, PersistenceObjectStorage, PersistenceSqlStorage } from "../contracts";
import { digestJson } from "../digest";
import { executeCellOperation } from "../operations";
import { confirmCellWrite } from "../transactions";

export type DagState = "PREPARING" | "TRANSFERRING" | "ACTIVE" | "JOIN_READY" | "RESUMING" | "COMPLETED";
export type DagChildState = "WAITING" | "READY" | "RUNNING" | "SUCCEEDED" | "FAILED" | "STOPPING" | "STOPPED";

export interface DagChildSpec {
  task_id: string;
  branch_id?: string;
  delegated_tps: number;
  delegated_tokens: number;
  allowance_minor: number;
}

export interface CreateDagRequest {
  tenant_id: string;
  dag_id: string;
  parent_task_id: string;
  parent_epoch: number;
  parent_turn_seq: number;
  period_key: string;
  currency: string;
  race_mode?: boolean;
  children: DagChildSpec[];
}

export interface DagVoucher {
  voucher_id: string;
  tenant_id: string;
  dag_id: string;
  parent_task_id: string;
  parent_epoch: number;
  parent_turn_seq: number;
  child_task_id: string;
  branch_id: string;
  delegated_tps: number;
  delegated_tokens: number;
  allowance_minor: number;
  period_key: string;
  currency: string;
}

export interface DagBoostVoucher {
  voucher_id: string;
  boost_id: string;
  tenant_id: string;
  dag_id: string;
  source_task_id: string;
  target_task_id: string;
  amount_minor: number;
  delegated_tps: number;
  delegated_tokens: number;
  period_key: string;
  currency: string;
}

export interface DagBoostRecord {
  boost_id: string;
  dag_id: string;
  tenant_id: string;
  source_task_id: string;
  target_task_id: string;
  amount_minor: number;
  delegated_tps: number;
  delegated_tokens: number;
  transfer_id: string;
  status: "PENDING" | "TRANSFERRED";
  voucher_json: string;
  voucher_signature: string;
  created_at: number;
  updated_at: number;
}

export interface DagReturnRecord {
  task_id: string;
  transfer_id: string;
  amount_minor: number;
  status: "PENDING" | "RETURNED";
}

export interface DagRecord {
  dag_id: string;
  tenant_id: string;
  parent_task_id: string;
  parent_epoch: number;
  parent_turn_seq: number;
  period_key: string;
  currency: string;
  race_mode: boolean;
  state: DagState;
  parent_yielded: boolean;
  decision_id: string | null;
  winner_task_id: string | null;
  created_at: number;
  updated_at: number;
  children: DagChildRecord[];
}

export interface DagChildRecord {
  dag_id: string;
  tenant_id: string;
  task_id: string;
  branch_id: string;
  voucher_id: string;
  voucher_json: string;
  voucher_signature: string;
  delegated_tps: number;
  delegated_tokens: number;
  allowance_minor: number;
  transfer_id: string;
  transfer_status: "PENDING" | "TRANSFERRED" | "RETURNED";
  returned_minor: number;
  voucher_status: "ISSUED" | "REDEEMED";
  observed_state: DagChildState;
  observation_version: number;
  winner_notified: number;
  stop_evidence_json: string | null;
  return_transfer_id: string | null;
  created_at: number;
  updated_at: number;
}

interface DagRow extends Omit<DagRecord, "children" | "race_mode" | "parent_yielded"> {
  race_mode: number;
  parent_yielded: number;
}
interface ChildEventRow { event_id: string; dag_id: string; task_id: string; aggregate_version: number; payload_digest: string; payload_json: string; }

export class DagPersistenceError extends Error {
  constructor(public readonly code: string, message: string, public readonly status = 409) {
    super(message);
    this.name = "DagPersistenceError";
  }
}

export const dagMigration: CellMigration = {
  version: 2,
  description: "durable DAG topology, allowance transfers, child observations, barriers and race decisions",
  up(sql) {
    sql.exec(`CREATE TABLE IF NOT EXISTS durable_dags (
      dag_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, parent_task_id TEXT NOT NULL, parent_epoch INTEGER NOT NULL,
      parent_turn_seq INTEGER NOT NULL, period_key TEXT NOT NULL, currency TEXT NOT NULL, race_mode INTEGER NOT NULL,
      state TEXT NOT NULL, parent_yielded INTEGER NOT NULL, decision_id TEXT, winner_task_id TEXT,
      create_digest TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    )`);
    sql.exec("CREATE INDEX IF NOT EXISTS durable_dags_tenant_idx ON durable_dags (tenant_id, created_at)");
    sql.exec(`CREATE TABLE IF NOT EXISTS durable_dag_children (
      dag_id TEXT NOT NULL, tenant_id TEXT NOT NULL, task_id TEXT NOT NULL, branch_id TEXT NOT NULL,
      voucher_id TEXT NOT NULL, voucher_json TEXT NOT NULL, voucher_signature TEXT NOT NULL,
      delegated_tps INTEGER NOT NULL, delegated_tokens INTEGER NOT NULL, allowance_minor INTEGER NOT NULL,
      transfer_id TEXT NOT NULL, transfer_status TEXT NOT NULL, returned_minor INTEGER NOT NULL DEFAULT 0, voucher_status TEXT NOT NULL,
      observed_state TEXT NOT NULL, observation_version INTEGER NOT NULL, winner_notified INTEGER NOT NULL,
      stop_evidence_json TEXT, return_transfer_id TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
      PRIMARY KEY(dag_id, task_id), UNIQUE(voucher_id)
    )`);
    sql.exec("CREATE INDEX IF NOT EXISTS durable_dag_children_voucher_idx ON durable_dag_children (voucher_id, voucher_status)");
    sql.exec(`CREATE TABLE IF NOT EXISTS durable_dag_child_events (
      event_id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, task_id TEXT NOT NULL,
      aggregate_version INTEGER NOT NULL, payload_digest TEXT NOT NULL, payload_json TEXT NOT NULL, created_at INTEGER NOT NULL
    )`);
  },
};

export const dagBoostMigration: CellMigration = {
  version: 3,
  description: "durable straggler boosts backed by TenantLedger allowance transfers",
  up(sql) {
    sql.exec(`CREATE TABLE IF NOT EXISTS durable_dag_boosts (
      boost_id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, tenant_id TEXT NOT NULL, source_task_id TEXT NOT NULL,
      target_task_id TEXT NOT NULL, amount_minor INTEGER NOT NULL, delegated_tps INTEGER NOT NULL,
      delegated_tokens INTEGER NOT NULL, transfer_id TEXT NOT NULL, status TEXT NOT NULL,
      voucher_json TEXT NOT NULL, voucher_signature TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    )`);
    sql.exec("CREATE INDEX IF NOT EXISTS durable_dag_boosts_target_idx ON durable_dag_boosts (dag_id, target_task_id, status)");
  },
};

export const dagReturnMigration: CellMigration = {
  version: 4,
  description: "durable child allowance return identity and amount before cross-Cell transfer",
  up(sql) {
    sql.exec(`CREATE TABLE IF NOT EXISTS durable_dag_returns (
      task_id TEXT PRIMARY KEY, transfer_id TEXT NOT NULL, amount_minor INTEGER NOT NULL, status TEXT NOT NULL
    )`);
  },
};

/** SQL authority for one DagCell. Cross-Cell work is deliberately performed by its adapter. */
export class DagStore {
  constructor(public readonly storage: PersistenceObjectStorage, public readonly dagId: string) {}

  public async create(request: CreateDagRequest, voucherSigner: (voucher: DagVoucher) => string): Promise<DagRecord> {
    validateCreate(request, this.dagId);
    const existing = this.storage.sql.exec<DagRow>("SELECT * FROM durable_dags WHERE dag_id = ?", this.dagId).toArray()[0];
    const canonical = await digestJson(normalizeCreate(request));
    if (existing) {
      const digest = (existing as DagRow & { create_digest: string }).create_digest;
      if (digest !== canonical.sha256) throw new DagPersistenceError("DAG_CREATE_CONFLICT", `DAG ${this.dagId} already exists with different topology.`);
      await confirmCellWrite(this.storage);
      return this.get()!;
    }

    const command = await this.command(`dag:${this.dagId}:create`, "dag.create", request, request.tenant_id);
    await executeCellOperation(this.storage, command, (sql) => {
      const current = sql.exec<DagRow>("SELECT * FROM durable_dags WHERE dag_id = ?", this.dagId).toArray()[0];
      if (current) {
        if ((current as DagRow & { create_digest: string }).create_digest !== canonical.sha256) {
          throw new DagPersistenceError("DAG_CREATE_CONFLICT", `DAG ${this.dagId} already exists with different topology.`);
        }
        return this.dagId;
      }
      const now = Date.now();
      sql.exec(`INSERT INTO durable_dags (
        dag_id, tenant_id, parent_task_id, parent_epoch, parent_turn_seq, period_key, currency, race_mode,
        state, parent_yielded, decision_id, winner_task_id, create_digest, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      this.dagId, request.tenant_id, request.parent_task_id, request.parent_epoch, request.parent_turn_seq,
      request.period_key, request.currency.toUpperCase(), request.race_mode ? 1 : 0,
      "PREPARING", 0, null, null, canonical.sha256, now, now);
      for (const child of normalizeChildren(request.children)) {
        const voucher: DagVoucher = {
          voucher_id: `dag-voucher:${this.dagId}:${encodeURIComponent(child.task_id)}`,
          tenant_id: request.tenant_id,
          dag_id: this.dagId,
          parent_task_id: request.parent_task_id,
          parent_epoch: request.parent_epoch,
          parent_turn_seq: request.parent_turn_seq,
          child_task_id: child.task_id,
          branch_id: child.branch_id ?? child.task_id,
          delegated_tps: child.delegated_tps,
          delegated_tokens: child.delegated_tokens,
          allowance_minor: child.allowance_minor,
          period_key: request.period_key,
          currency: request.currency.toUpperCase(),
        };
        const now = Date.now();
        sql.exec(`INSERT INTO durable_dag_children (
          dag_id, tenant_id, task_id, branch_id, voucher_id, voucher_json, voucher_signature,
          delegated_tps, delegated_tokens, allowance_minor, transfer_id, transfer_status, returned_minor, voucher_status,
          observed_state, observation_version, winner_notified, stop_evidence_json, return_transfer_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        this.dagId, request.tenant_id, child.task_id, voucher.branch_id, voucher.voucher_id,
        JSON.stringify(voucher), voucherSigner(voucher), child.delegated_tps, child.delegated_tokens,
        child.allowance_minor, `dag-transfer:${this.dagId}:${encodeURIComponent(child.task_id)}`,
        "PENDING", 0, "ISSUED", "WAITING", 0, 0, null, null, now, now);
      }
      return this.dagId;
    });
    return this.get()!;
  }

  public get(): DagRecord | null {
    const row = this.storage.sql.exec<DagRow>("SELECT * FROM durable_dags WHERE dag_id = ?", this.dagId).toArray()[0];
    if (!row) return null;
    const children = this.storage.sql.exec<DagChildRecord>("SELECT * FROM durable_dag_children WHERE dag_id = ? ORDER BY created_at", this.dagId).toArray();
    return {
      ...row,
      race_mode: Number(row.race_mode) === 1,
      parent_yielded: Number(row.parent_yielded) === 1,
      parent_epoch: Number(row.parent_epoch),
      parent_turn_seq: Number(row.parent_turn_seq),
      created_at: Number(row.created_at),
      updated_at: Number(row.updated_at),
      children: children.map((child) => ({ ...child,
        delegated_tps: Number(child.delegated_tps), delegated_tokens: Number(child.delegated_tokens),
        allowance_minor: Number(child.allowance_minor), returned_minor: Number(child.returned_minor ?? 0), observation_version: Number(child.observation_version),
        winner_notified: Number(child.winner_notified), created_at: Number(child.created_at), updated_at: Number(child.updated_at),
      })),
    };
  }

  public child(taskId: string): DagChildRecord | null {
    return this.storage.sql.exec<DagChildRecord>("SELECT * FROM durable_dag_children WHERE dag_id = ? AND task_id = ?", this.dagId, taskId).toArray()[0] ?? null;
  }

  public boosts(): DagBoostRecord[] {
    return this.storage.sql.exec<DagBoostRecord>("SELECT * FROM durable_dag_boosts WHERE dag_id = ? ORDER BY created_at", this.dagId).toArray()
      .map((boost) => ({ ...boost, amount_minor: Number(boost.amount_minor), delegated_tps: Number(boost.delegated_tps), delegated_tokens: Number(boost.delegated_tokens), created_at: Number(boost.created_at), updated_at: Number(boost.updated_at) }));
  }

  public allowanceReturn(taskId: string): DagReturnRecord | null {
    const row = this.storage.sql.exec<DagReturnRecord>("SELECT * FROM durable_dag_returns WHERE task_id = ?", taskId).toArray()[0];
    return row ? { ...row, amount_minor: Number(row.amount_minor) } : null;
  }

  public allowanceReturns(): DagReturnRecord[] {
    return this.storage.sql.exec<DagReturnRecord>("SELECT * FROM durable_dag_returns ORDER BY task_id").toArray()
      .map((row) => ({ ...row, amount_minor: Number(row.amount_minor) }));
  }

  public async beginAllowanceReturn(taskId: string, transferId: string, amountMinor: number): Promise<DagReturnRecord> {
    const existing = this.allowanceReturn(taskId);
    if (existing) {
      if (existing.transfer_id !== transferId) throw new DagPersistenceError("DAG_RETURN_CONFLICT", "Return already has a different transfer identity.");
      await confirmCellWrite(this.storage);
      return existing;
    }
    if (!Number.isSafeInteger(amountMinor) || amountMinor < 0) throw new DagPersistenceError("DAG_CHILD_ACCOUNT_INVALID", "Return amount must be a non-negative safe integer.", 503);
    await this.mutate(`dag:${this.dagId}:return-pending:${encodeURIComponent(taskId)}`, "dag.allowance.return_pending", { task_id: taskId, transfer_id: transferId, amount_minor: amountMinor }, (sql) => {
      sql.exec("INSERT INTO durable_dag_returns (task_id, transfer_id, amount_minor, status) VALUES (?, ?, ?, ?)", taskId, transferId, amountMinor, "PENDING");
    });
    return this.allowanceReturn(taskId)!;
  }

  public async beginBoost(input: {
    boost_id: string; source_task_id: string; target_task_id: string; amount_minor: number;
    delegated_tps: number; delegated_tokens: number; voucher: DagBoostVoucher; signature: string;
  }): Promise<DagBoostRecord> {
    const dag = this.requireDag();
    validateText(input.boost_id, "boost_id");
    if (input.source_task_id === input.target_task_id) throw new DagPersistenceError("DAG_BOOST_TASK_CONFLICT", "Boost source and target must differ.", 400);
    for (const [field, value] of Object.entries({ amount_minor: input.amount_minor, delegated_tps: input.delegated_tps, delegated_tokens: input.delegated_tokens })) {
      if (!Number.isSafeInteger(value) || value < 0) throw new DagPersistenceError("DAG_BOOST_INVALID", `${field} must be a non-negative integer.`, 400);
    }
    if (input.amount_minor <= 0 || (input.delegated_tps <= 0 && input.delegated_tokens <= 0)) throw new DagPersistenceError("DAG_BOOST_INVALID", "A boost requires positive transferable money and at least one quota increase.", 400);
    const expectedVoucher = {
      voucher_id: `dag-boost-voucher:${this.dagId}:${encodeURIComponent(input.boost_id)}`,
      boost_id: input.boost_id,
      tenant_id: dag.tenant_id,
      dag_id: this.dagId,
      source_task_id: input.source_task_id,
      target_task_id: input.target_task_id,
      amount_minor: input.amount_minor,
      delegated_tps: input.delegated_tps,
      delegated_tokens: input.delegated_tokens,
      period_key: dag.period_key,
      currency: dag.currency,
    } satisfies DagBoostVoucher;
    if (JSON.stringify(input.voucher) !== JSON.stringify(expectedVoucher)) {
      throw new DagPersistenceError("DAG_BOOST_VOUCHER_INVALID", "Boost voucher does not match the durable DAG and transfer request.", 400);
    }
    const existing = this.storage.sql.exec<DagBoostRecord>("SELECT * FROM durable_dag_boosts WHERE boost_id = ?", input.boost_id).toArray()[0];
    if (existing) {
      if (existing.voucher_json !== JSON.stringify(input.voucher) || existing.voucher_signature !== input.signature) {
        throw new DagPersistenceError("DAG_BOOST_CONFLICT", `Boost ${input.boost_id} already has different content.`);
      }
      await confirmCellWrite(this.storage);
      return { ...existing, amount_minor: Number(existing.amount_minor), delegated_tps: Number(existing.delegated_tps), delegated_tokens: Number(existing.delegated_tokens), created_at: Number(existing.created_at), updated_at: Number(existing.updated_at) };
    }
    const source = this.requireChild(input.source_task_id);
    const target = this.requireChild(input.target_task_id);
    if (dag.race_mode || dag.state !== "ACTIVE") throw new DagPersistenceError("DAG_BOOST_STATE_INVALID", "Only an active join DAG can issue a straggler boost.", 409);
    if (!["SUCCEEDED", "FAILED", "STOPPED"].includes(source.observed_state) || target.observed_state !== "RUNNING") {
      throw new DagPersistenceError("DAG_BOOST_BARRIER_INVALID", "Boost source must be terminal and target must be running.", 409);
    }
    const payload = { ...input, voucher: input.voucher };
    await this.mutate(`dag:${this.dagId}:boost:${encodeURIComponent(input.boost_id)}:begin`, "dag.allowance.boost_pending", payload, (sql) => {
      const freshSource = this.requireChild(input.source_task_id);
      const freshTarget = this.requireChild(input.target_task_id);
      const freshDag = this.requireDag();
      if (freshDag.state !== "ACTIVE" || !["SUCCEEDED", "FAILED", "STOPPED"].includes(freshSource.observed_state) || freshTarget.observed_state !== "RUNNING") {
        throw new DagPersistenceError("DAG_BOOST_BARRIER_INVALID", "Boost source/target changed while preparing the intent.", 409);
      }
      const outgoing = this.boosts().filter((entry) => entry.source_task_id === input.source_task_id);
      for (const field of ["delegated_tokens", "delegated_tps"] as const) {
        const total = outgoing.reduce((sum, entry) => sum + Number(entry[field]), input[field]);
        if (!Number.isSafeInteger(total) || total > Number(freshSource[field]) || !Number.isSafeInteger(Number(freshTarget[field]) + input[field])) {
          throw new DagPersistenceError("DAG_BOOST_QUOTA_EXCEEDED", "Boost exceeds the source's remaining delegated quota.", 409);
        }
      }
      const now = Date.now();
      sql.exec(`INSERT INTO durable_dag_boosts (
        boost_id, dag_id, tenant_id, source_task_id, target_task_id, amount_minor, delegated_tps,
        delegated_tokens, transfer_id, status, voucher_json, voucher_signature, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      input.boost_id, this.dagId, dag.tenant_id, input.source_task_id, input.target_task_id, input.amount_minor,
      input.delegated_tps, input.delegated_tokens, `dag-boost-transfer:${this.dagId}:${encodeURIComponent(input.boost_id)}`,
      "PENDING", JSON.stringify(input.voucher), input.signature, now, now);
    });
    return this.storage.sql.exec<DagBoostRecord>("SELECT * FROM durable_dag_boosts WHERE boost_id = ?", input.boost_id).toArray()[0];
  }

  public async finishBoost(boostId: string): Promise<DagBoostRecord> {
    const boost = this.storage.sql.exec<DagBoostRecord>("SELECT * FROM durable_dag_boosts WHERE boost_id = ?", boostId).toArray()[0];
    if (!boost) throw new DagPersistenceError("DAG_BOOST_NOT_FOUND", `Boost ${boostId} is unknown.`, 404);
    if (boost.status === "TRANSFERRED") { await confirmCellWrite(this.storage); return boost; }
    await this.mutate(`dag:${this.dagId}:boost:${encodeURIComponent(boostId)}:transferred`, "dag.allowance.boost_transferred", { dag_id: this.dagId, boost_id: boostId, transfer_id: boost.transfer_id }, (sql) => {
      const target = sql.exec<DagChildRecord>("SELECT * FROM durable_dag_children WHERE dag_id = ? AND task_id = ?", this.dagId, boost.target_task_id).toArray()[0];
      if (!target) throw new DagPersistenceError("DAG_CHILD_NOT_FOUND", `Boost target ${boost.target_task_id} is unknown.`, 404);
      sql.exec(`UPDATE durable_dag_children SET delegated_tps = ?, delegated_tokens = ?, allowance_minor = ?, updated_at = ? WHERE dag_id = ? AND task_id = ?`,
        Number(target.delegated_tps) + Number(boost.delegated_tps), Number(target.delegated_tokens) + Number(boost.delegated_tokens),
        Number(target.allowance_minor) + Number(boost.amount_minor), Date.now(), this.dagId, boost.target_task_id);
      sql.exec("UPDATE durable_dag_boosts SET status = ?, updated_at = ? WHERE boost_id = ?", "TRANSFERRED", Date.now(), boostId);
    });
    return this.storage.sql.exec<DagBoostRecord>("SELECT * FROM durable_dag_boosts WHERE boost_id = ?", boostId).toArray()[0];
  }

  public event(eventId: string): ChildEventRow | null {
    return this.storage.sql.exec<ChildEventRow>("SELECT * FROM durable_dag_child_events WHERE event_id = ?", eventId).toArray()[0] ?? null;
  }

  public async markParentYielded(): Promise<void> {
    const dag = this.requireDag();
    if (dag.parent_yielded) { await confirmCellWrite(this.storage); return; }
    await this.mutate(`dag:${this.dagId}:parent-yielded`, "dag.parent.yielded", { dag_id: this.dagId, parent_task_id: dag.parent_task_id }, (sql) => {
      sql.exec("UPDATE durable_dags SET parent_yielded = ?, state = ?, updated_at = ? WHERE dag_id = ?", 1, dag.children.length === 0 ? "JOIN_READY" : "TRANSFERRING", Date.now(), this.dagId);
    });
  }

  public async markTransferred(taskId: string): Promise<void> {
    const child = this.requireChild(taskId);
    if (child.transfer_status === "TRANSFERRED" || child.transfer_status === "RETURNED") { await confirmCellWrite(this.storage); return; }
    await this.mutate(`dag:${this.dagId}:transfer:${encodeURIComponent(taskId)}`, "dag.allowance.transferred", { dag_id: this.dagId, task_id: taskId, transfer_id: child.transfer_id }, (sql) => {
      sql.exec("UPDATE durable_dag_children SET transfer_status = ?, updated_at = ? WHERE dag_id = ? AND task_id = ?", "TRANSFERRED", Date.now(), this.dagId, taskId);
    });
  }

  public async activate(): Promise<void> {
    const dag = this.requireDag();
    if (dag.state === "ACTIVE" || ["JOIN_READY", "RESUMING", "COMPLETED"].includes(dag.state)) { await confirmCellWrite(this.storage); return; }
    if (!dag.parent_yielded || dag.children.some((child) => child.transfer_status !== "TRANSFERRED")) {
      throw new DagPersistenceError("DAG_NOT_READY", "DAG allowances cannot be activated before the parent is durably yielded and all transfers are confirmed.", 409);
    }
    await this.mutate(`dag:${this.dagId}:activate`, "dag.activated", { dag_id: this.dagId }, (sql) => {
      sql.exec("UPDATE durable_dags SET state = ?, updated_at = ? WHERE dag_id = ?", "ACTIVE", Date.now(), this.dagId);
    });
  }

  public async redeem(taskId: string, voucherId: string): Promise<DagChildRecord> {
    const dag = this.requireDag();
    const child = this.requireChild(taskId);
    if (child.voucher_id !== voucherId) throw new DagPersistenceError("DAG_VOUCHER_UNKNOWN", "Voucher does not belong to this child.", 404);
    if (child.voucher_status === "REDEEMED") { await confirmCellWrite(this.storage); return child; }
    if (dag.state !== "ACTIVE" || !dag.parent_yielded || child.transfer_status !== "TRANSFERRED") {
      throw new DagPersistenceError("DAG_VOUCHER_NOT_READY", "Voucher is not spendable until parent yield and allowance transfer are confirmed.", 409);
    }
    await this.mutate(`dag:${this.dagId}:redeem:${encodeURIComponent(taskId)}`, "dag.voucher.redeemed", { dag_id: this.dagId, task_id: taskId, voucher_id: voucherId }, (sql) => {
      sql.exec("UPDATE durable_dag_children SET voucher_status = ?, observed_state = ?, updated_at = ? WHERE dag_id = ? AND task_id = ?", "REDEEMED", "READY", Date.now(), this.dagId, taskId);
    });
    return this.requireChild(taskId);
  }

  public async observe(input: { event_id: string; task_id: string; aggregate_version: number; state: DagChildState; evidence?: unknown }): Promise<{ duplicate: boolean; child: DagChildRecord }> {
    const dag = this.requireDag();
    const child = this.requireChild(input.task_id);
    validateText(input.event_id, "event_id");
    if (!Number.isSafeInteger(input.aggregate_version) || input.aggregate_version < 1) throw new DagPersistenceError("DAG_EVENT_VERSION_INVALID", "aggregate_version must be a positive integer.", 400);
    if (!isChildState(input.state)) throw new DagPersistenceError("DAG_CHILD_STATE_INVALID", `Unsupported child state ${String(input.state)}.`, 400);
    if (child.voucher_status !== "REDEEMED" && input.state !== "STOPPED") throw new DagPersistenceError("DAG_CHILD_NOT_REDEEMED", "Child observations require a redeemed voucher.");
    const payload = { ...input, dag_id: this.dagId };
    const digest = (await digestJson(payload)).sha256;
    const priorEvent = this.event(input.event_id);
    if (priorEvent) {
      if (priorEvent.payload_digest !== digest || priorEvent.task_id !== input.task_id || Number(priorEvent.aggregate_version) !== input.aggregate_version) {
        throw new DagPersistenceError("DAG_EVENT_CONFLICT", `Event ${input.event_id} was reused with different content.`);
      }
      await confirmCellWrite(this.storage);
      return { duplicate: true, child };
    }
    if (input.aggregate_version !== child.observation_version + 1) {
      throw new DagPersistenceError("DAG_EVENT_VERSION_GAP", `Expected child observation ${child.observation_version + 1}; received ${input.aggregate_version}.`, 409);
    }
    if (!canTransition(child.observed_state, input.state, dag.race_mode)) {
      throw new DagPersistenceError("DAG_CHILD_STATE_CONFLICT", `Child cannot transition from ${child.observed_state} to ${input.state}.`);
    }
    await this.mutate(`dag:${this.dagId}:event:${encodeURIComponent(input.event_id)}`, "dag.child.observed", payload, (sql) => {
      sql.exec(`INSERT INTO durable_dag_child_events (event_id, dag_id, task_id, aggregate_version, payload_digest, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)`, input.event_id, this.dagId, input.task_id, input.aggregate_version, digest, JSON.stringify(payload), Date.now());
      sql.exec("UPDATE durable_dag_children SET observed_state = ?, observation_version = ?, stop_evidence_json = ?, updated_at = ? WHERE dag_id = ? AND task_id = ?",
        input.state, input.aggregate_version, input.evidence === undefined ? null : JSON.stringify(input.evidence), Date.now(), this.dagId, input.task_id);
    });
    return { duplicate: false, child: this.requireChild(input.task_id) };
  }

  public async selectWinner(winnerTaskId: string): Promise<DagRecord> {
    const dag = this.requireDag();
    if (!dag.race_mode) throw new DagPersistenceError("DAG_NOT_RACE", "Winner selection applies only to speculative race DAGs.");
    const child = this.requireChild(winnerTaskId);
    if (child.observed_state !== "SUCCEEDED") throw new DagPersistenceError("DAG_WINNER_NOT_SUCCEEDED", "Only an observed successful child can win.");
    if (dag.winner_task_id) {
      if (dag.winner_task_id !== winnerTaskId) throw new DagPersistenceError("DAG_RACE_WINNER_CONFLICT", `Race ${dag.dag_id} already selected ${dag.winner_task_id}.`);
      await confirmCellWrite(this.storage);
      return dag;
    }
    const decisionId = `dag-decision:${this.dagId}`;
    await this.mutate(`dag:${this.dagId}:winner`, "dag.race.winner_selected", { dag_id: this.dagId, decision_id: decisionId, winner_task_id: winnerTaskId }, (sql) => {
      const fresh = sql.exec<DagRow>("SELECT * FROM durable_dags WHERE dag_id = ?", this.dagId).toArray()[0];
      if (fresh?.winner_task_id && fresh.winner_task_id !== winnerTaskId) throw new DagPersistenceError("DAG_RACE_WINNER_CONFLICT", `Race ${this.dagId} already selected another child.`);
      sql.exec("UPDATE durable_dags SET decision_id = ?, winner_task_id = ?, state = ?, updated_at = ? WHERE dag_id = ?", decisionId, winnerTaskId, "RESUMING", Date.now(), this.dagId);
    });
    return this.get()!;
  }

  public async markWinnerNotified(taskId: string): Promise<void> {
    const child = this.requireChild(taskId);
    if (Number(child.winner_notified) === 1) { await confirmCellWrite(this.storage); return; }
    await this.mutate(`dag:${this.dagId}:winner-notified:${encodeURIComponent(taskId)}`, "dag.race.winner_delivered", { dag_id: this.dagId, task_id: taskId }, (sql) => {
      sql.exec("UPDATE durable_dag_children SET winner_notified = ?, updated_at = ? WHERE dag_id = ? AND task_id = ?", 1, Date.now(), this.dagId, taskId);
    });
  }

  public async setState(state: DagState, operationSuffix: string, payload: unknown = {}): Promise<void> {
    const dag = this.requireDag();
    if (dag.state === state) { await confirmCellWrite(this.storage); return; }
    await this.mutate(`dag:${this.dagId}:${operationSuffix}`, `dag.${operationSuffix.replaceAll(":", ".")}`, { dag_id: this.dagId, ...asRecord(payload), state }, (sql) => {
      sql.exec("UPDATE durable_dags SET state = ?, updated_at = ? WHERE dag_id = ?", state, Date.now(), this.dagId);
    });
  }

  public async markAllowanceReturned(taskId: string, transferId: string, amountMinor: number): Promise<void> {
    const child = this.requireChild(taskId);
    if (child.transfer_status === "RETURNED") {
      if (child.return_transfer_id !== transferId) throw new DagPersistenceError("DAG_RETURN_CONFLICT", "Child allowance was already returned under a different transfer.");
      await confirmCellWrite(this.storage);
      return;
    }
    await this.mutate(`dag:${this.dagId}:return:${encodeURIComponent(taskId)}`, "dag.allowance.returned", { dag_id: this.dagId, task_id: taskId, transfer_id: transferId, amount_minor: amountMinor }, (sql) => {
      sql.exec("UPDATE durable_dag_children SET transfer_status = ?, returned_minor = ?, return_transfer_id = ?, updated_at = ? WHERE dag_id = ? AND task_id = ?", "RETURNED", amountMinor, transferId, Date.now(), this.dagId, taskId);
      sql.exec("UPDATE durable_dag_returns SET status = ? WHERE task_id = ?", "RETURNED", taskId);
    });
  }

  public async markStopped(taskId: string, evidence: unknown): Promise<void> {
    await this.mutate(`dag:${this.dagId}:stopped:${encodeURIComponent(taskId)}`, "dag.child.stopped", { dag_id: this.dagId, task_id: taskId, evidence }, (sql) => {
      sql.exec("UPDATE durable_dag_children SET observed_state = ?, stop_evidence_json = ?, updated_at = ? WHERE dag_id = ? AND task_id = ?", "STOPPED", JSON.stringify(evidence), Date.now(), this.dagId, taskId);
    });
  }

  public async isDispatchAuthorized(taskId: string, voucherId: string): Promise<boolean> {
    const dag = this.get();
    const child = this.child(taskId);
    const authorized = Boolean(dag && child && dag.state === "ACTIVE" && (!dag.race_mode || !dag.winner_task_id || dag.winner_task_id === taskId) &&
      child.voucher_status === "REDEEMED" && child.transfer_status === "TRANSFERRED" && child.voucher_id === voucherId);
    if (authorized) await confirmCellWrite(this.storage);
    return authorized;
  }

  private async mutate(operationId: string, operationType: string, payload: unknown, apply: (sql: PersistenceSqlStorage) => void): Promise<void> {
    const command = await this.command(operationId, operationType, payload);
    await executeCellOperation(this.storage, command, (sql) => {
      apply(sql);
      return true;
    });
  }

  private async command(operationId: string, operationType: string, payload: unknown, tenantOverride?: string): Promise<CellOperationCommand> {
    const prior = this.storage.sql.exec<{ expected_version: number }>("SELECT expected_version FROM cell_operations WHERE operation_id = ?", operationId).toArray()[0];
    const version = prior ? Number(prior.expected_version) : Number(this.storage.sql.exec<{ version: number }>("SELECT version FROM cell_aggregate_versions WHERE aggregate_type = ? AND aggregate_id = ?", "dag", this.dagId).toArray()[0]?.version ?? 0);
    const tenantId = tenantOverride ?? this.requireDag().tenant_id;
    return { operationId, operationType, resourceId: this.dagId, tenantId, payload,
      aggregateType: "dag", aggregateId: this.dagId, expectedVersion: version };
  }

  private requireDag(): DagRecord {
    const dag = this.get();
    if (!dag) throw new DagPersistenceError("DAG_NOT_FOUND", `DAG ${this.dagId} is unknown.`, 404);
    return dag;
  }
  private requireChild(taskId: string): DagChildRecord {
    const child = this.child(taskId);
    if (!child) throw new DagPersistenceError("DAG_CHILD_NOT_FOUND", `Task ${taskId} is not a child of DAG ${this.dagId}.`, 404);
    return child;
  }
}

function validateCreate(request: CreateDagRequest, cellDagId: string): void {
  for (const [name, value] of Object.entries({ dag_id: request.dag_id, tenant_id: request.tenant_id, parent_task_id: request.parent_task_id, period_key: request.period_key, currency: request.currency })) validateText(value, name);
  if (request.dag_id !== cellDagId) throw new DagPersistenceError("DAG_ID_MISMATCH", "Request DAG id does not match its DagCell identity.", 400);
  for (const [name, value] of Object.entries({ parent_epoch: request.parent_epoch, parent_turn_seq: request.parent_turn_seq })) if (!Number.isSafeInteger(value) || value < 0) throw new DagPersistenceError("DAG_FENCE_INVALID", `${name} must be a non-negative safe integer.`, 400);
  if (!Array.isArray(request.children)) throw new DagPersistenceError("DAG_CHILDREN_INVALID", "children must be an array.", 400);
  const children = normalizeChildren(request.children);
  if (children.some((child) => child.task_id === request.parent_task_id)) throw new DagPersistenceError("DAG_PARENT_CHILD_CONFLICT", "Parent cannot also be a child.", 400);
}
function normalizeChildren(children: DagChildSpec[]): DagChildSpec[] {
  const seen = new Set<string>();
  return children.map((child) => {
    validateText(child.task_id, "child.task_id");
    if (seen.has(child.task_id)) throw new DagPersistenceError("DAG_CHILD_DUPLICATE", `Child ${child.task_id} appears more than once.`, 400);
    seen.add(child.task_id);
    for (const [field, value] of Object.entries({ delegated_tps: child.delegated_tps, delegated_tokens: child.delegated_tokens, allowance_minor: child.allowance_minor })) {
      if (!Number.isSafeInteger(value) || value < 0) throw new DagPersistenceError("DAG_CHILD_ALLOWANCE_INVALID", `${field} must be a non-negative integer.`, 400);
    }
    if (child.allowance_minor <= 0) throw new DagPersistenceError("DAG_CHILD_ALLOWANCE_INVALID", "allowance_minor must be greater than zero.", 400);
    return { ...child, branch_id: child.branch_id ?? child.task_id };
  });
}
function normalizeCreate(request: CreateDagRequest): unknown { return { ...request, currency: request.currency.toUpperCase(), children: normalizeChildren(request.children) }; }
function validateText(value: string, field: string): void { if (typeof value !== "string" || !value.trim() || value.length > 512) throw new DagPersistenceError("DAG_INPUT_INVALID", `${field} must be a non-empty string of at most 512 characters.`, 400); }
function isChildState(state: string): state is DagChildState { return ["WAITING", "READY", "RUNNING", "SUCCEEDED", "FAILED", "STOPPING", "STOPPED"].includes(state); }
function canTransition(from: DagChildState, to: DagChildState, raceMode: boolean): boolean {
  if (from === to) return true;
  if (["SUCCEEDED", "FAILED", "STOPPED"].includes(from)) return false;
  if (to === "WAITING" || to === "READY") return false;
  if (to === "STOPPING") return raceMode && from !== "SUCCEEDED";
  if (to === "STOPPED") return raceMode && ["WAITING", "READY", "STOPPING", "FAILED"].includes(from);
  if (to === "RUNNING") return from === "READY";
  if (to === "SUCCEEDED" || to === "FAILED") return from === "RUNNING" || from === "READY";
  return false;
}
function asRecord(value: unknown): Record<string, unknown> { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {}; }
