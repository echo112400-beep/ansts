import { CellOperationCommand, PersistenceObjectStorage, PersistenceSqlStorage } from "../contracts";
import { CellVersionConflictError, executeCellOperation } from "../operations";
import { NodeResourceVector } from "../node-capacity";
import { TaskDispatchKind, TaskDispatchStage } from "../task-core";

export interface SchedulerCandidate {
  task_id: string;
  tenant_id: string;
  dispatch_id?: string;
  node_id: string;
  node_incarnation?: string;
  region?: string;
  resource_pool?: string;
  tenant_weight?: number;
  kind?: TaskDispatchKind;
  dag_id?: string;
  voucher_id?: string;
  resources: NodeResourceVector;
  task_spec?: unknown;
  execution_policy?: unknown;
  expected_task_version?: number;
}

/** Stable Durable Object name for exactly one regional resource pool. */
export function schedulerPoolCellKey(region = "default", resourcePool = "default"): string {
  const normalizedRegion = normalizePoolPart(region, "region");
  const normalizedPool = normalizePoolPart(resourcePool, "resource_pool");
  if (normalizedRegion === "default" && normalizedPool === "default") return "global-5d-scheduler";
  return `scheduler:${encodeURIComponent(normalizedRegion)}:${encodeURIComponent(normalizedPool)}`;
}

export interface SchedulerDispatchRecord {
  dispatch_id: string;
  task_id: string;
  tenant_id: string;
  region: string;
  resource_pool: string;
  tenant_weight: number;
  node_id: string;
  node_incarnation: string;
  kind: TaskDispatchKind;
  stage: TaskDispatchStage;
  resources_json: string;
  reservation_id: string | null;
  reservation_token: string | null;
  authorization_token: string | null;
  business_epoch: number | null;
  request_json: string;
  dispatch_version: number;
  created_at: number;
  updated_at: number;
}

export const schedulerPoolMigration = {
  version: 2,
  description: "regional resource-pool candidates, fairness usage and dispatch stages",
  up(sql: PersistenceSqlStorage): void {
    sql.exec(`
      CREATE TABLE IF NOT EXISTS scheduler_dispatches (
        dispatch_id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        region TEXT NOT NULL,
        resource_pool TEXT NOT NULL,
        tenant_weight REAL NOT NULL,
        node_id TEXT NOT NULL,
        node_incarnation TEXT NOT NULL,
        kind TEXT NOT NULL,
        stage TEXT NOT NULL,
        resources_json TEXT NOT NULL,
        reservation_id TEXT,
        reservation_token TEXT,
        authorization_token TEXT,
        business_epoch INTEGER,
        request_json TEXT NOT NULL,
        dispatch_version INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);
    sql.exec("CREATE INDEX IF NOT EXISTS scheduler_dispatch_pool_idx ON scheduler_dispatches (region, resource_pool, stage, tenant_id)");
    sql.exec("CREATE INDEX IF NOT EXISTS scheduler_dispatch_task_idx ON scheduler_dispatches (task_id, created_at)");
  },
};

export function readSchedulerDispatch(sql: PersistenceSqlStorage, dispatchId: string): SchedulerDispatchRecord | null {
  return sql.exec<SchedulerDispatchRecord>("SELECT * FROM scheduler_dispatches WHERE dispatch_id = ?", dispatchId).toArray()[0] ?? null;
}

export class SchedulerPool {
  constructor(public readonly storage: PersistenceObjectStorage, public readonly poolKey: string) {}

  public getDispatch(dispatchId: string): SchedulerDispatchRecord | null {
    return readSchedulerDispatch(this.storage.sql, dispatchId);
  }

  public listDispatches(): SchedulerDispatchRecord[] {
    return this.storage.sql.exec<SchedulerDispatchRecord>("SELECT * FROM scheduler_dispatches ORDER BY created_at").toArray();
  }

  public validateCandidates(candidates: SchedulerCandidate[]): void {
    for (const candidate of candidates.map(normalizedCandidate)) assertCandidatePool(this.poolKey, candidate);
  }

  public async ensureIntent(candidate: SchedulerCandidate): Promise<SchedulerDispatchRecord> {
    validateCandidate(candidate);
    const request = normalizedCandidate(candidate);
    assertCandidatePool(this.poolKey, request);
    const dispatchId = request.dispatch_id!;
    const existing = this.getDispatch(dispatchId);
    if (existing) {
      assertSameCandidate(existing, request);
      return existing;
    }
    const command = dispatchCommand(dispatchId, "scheduler.dispatch.prepare", request, 0);
    const result = await executeCellOperation(this.storage, command, (sql) => {
      const duplicate = readSchedulerDispatch(sql, dispatchId);
      if (duplicate) {
        assertSameCandidate(duplicate, request);
        return duplicate;
      }
      const now = Date.now();
      sql.exec(
        `INSERT INTO scheduler_dispatches (
          dispatch_id, task_id, tenant_id, region, resource_pool, tenant_weight, node_id, node_incarnation,
          kind, stage, resources_json, reservation_id, reservation_token, authorization_token, business_epoch,
          request_json, dispatch_version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        dispatchId, request.task_id, request.tenant_id, request.region!, request.resource_pool!, request.tenant_weight!,
        request.node_id, request.node_incarnation!, request.kind!, "PREPARING", JSON.stringify(request.resources),
        null, null, null, null, JSON.stringify(request), 1, now, now,
      );
      return readSchedulerDispatch(sql, dispatchId)!;
    });
    return result.result;
  }

  public async advance(params: {
    dispatch_id: string;
    stage: TaskDispatchStage;
    reservation_id?: string;
    reservation_token?: string;
    authorization_token?: string;
    business_epoch?: number;
  }): Promise<SchedulerDispatchRecord> {
    const current = this.getDispatch(params.dispatch_id);
    if (!current) throw new Error(`Dispatch ${params.dispatch_id} has no SchedulerPool intent.`);
    if (stageRank(current.stage) >= stageRank(params.stage)) {
      assertStageFields(current, params);
      return current;
    }
    if (stageRank(params.stage) !== stageRank(current.stage) + 1) {
      throw new Error(`Dispatch ${params.dispatch_id} cannot move from ${current.stage} to ${params.stage}.`);
    }
    const expected = Number(current.dispatch_version);
    const command = dispatchCommand(
      params.dispatch_id,
      `scheduler.dispatch.${params.stage.toLowerCase()}`,
      params,
      expected,
    );
    const result = await retryVersionConflict(async () => executeCellOperation(this.storage, command, (sql) => {
      const row = readSchedulerDispatch(sql, params.dispatch_id);
      if (!row) throw new Error(`Dispatch ${params.dispatch_id} disappeared.`);
      if (stageRank(row.stage) >= stageRank(params.stage)) {
        assertStageFields(row, params);
        return row;
      }
      if (stageRank(params.stage) !== stageRank(row.stage) + 1) {
        throw new Error(`Dispatch ${params.dispatch_id} cannot move from ${row.stage} to ${params.stage}.`);
      }
      sql.exec(
        "UPDATE scheduler_dispatches SET stage = ?, reservation_id = ?, reservation_token = ?, authorization_token = ?, business_epoch = ?, dispatch_version = ?, updated_at = ? WHERE dispatch_id = ?",
        params.stage, params.reservation_id ?? row.reservation_id, params.reservation_token ?? row.reservation_token,
        params.authorization_token ?? row.authorization_token, params.business_epoch ?? row.business_epoch,
        expected + 1, Date.now(), params.dispatch_id,
      );
      return readSchedulerDispatch(sql, params.dispatch_id)!;
    }));
    return result.result;
  }

  public fairOrder(candidates: SchedulerCandidate[], poolCapacity: NodeResourceVector): SchedulerCandidate[] {
    const rows = this.listDispatches().filter((row) => ["RESERVED", "AUTHORIZED", "STARTED"].includes(row.stage));
    const usage = new Map<string, NodeResourceVector>();
    const weights = new Map<string, number>();
    for (const row of rows) {
      const resource = JSON.parse(row.resources_json) as NodeResourceVector;
      usage.set(row.tenant_id, addVector(usage.get(row.tenant_id) ?? zeroVector(), resource));
      weights.set(row.tenant_id, Number(row.tenant_weight) || 1);
    }
    const normalized = candidates.map(normalizedCandidate);
    this.validateCandidates(normalized);
    const byTenantWeights = new Map<string, number>();
    for (const row of normalized) {
      const weight = Number(row.tenant_weight) || 1;
      const previous = byTenantWeights.get(row.tenant_id) ?? weights.get(row.tenant_id);
      if (previous !== undefined && Math.abs(previous - weight) > 1e-9) {
        throw new Error(`Tenant ${row.tenant_id} changed its weight within scheduler pool ${this.poolKey}.`);
      }
      byTenantWeights.set(row.tenant_id, weight);
    }
    const available = validatePoolCapacity(poolCapacity);
    const projectedShare = (candidate: SchedulerCandidate): number => {
      const used = usage.get(candidate.tenant_id) ?? zeroVector();
      return dominantShare(addVector(used, candidate.resources), available) / (Number(candidate.tenant_weight) || 1);
    };
    const pending = [...normalized];
    const ordered: SchedulerCandidate[] = [];
    while (pending.length > 0) {
      pending.sort((left, right) => {
        const shareDelta = projectedShare(left) - projectedShare(right);
        if (Math.abs(shareDelta) > 1e-9) return shareDelta;
        return String(left.dispatch_id).localeCompare(String(right.dispatch_id));
      });
      const next = pending.shift()!;
      ordered.push(next);
      usage.set(next.tenant_id, addVector(usage.get(next.tenant_id) ?? zeroVector(), next.resources));
    }
    return ordered;
  }
}

function dispatchCommand(dispatchId: string, operationType: string, payload: unknown, expectedVersion: number): CellOperationCommand {
  return {
    operationId: `scheduler:${dispatchId}:${operationType}`,
    operationType,
    resourceId: dispatchId,
    tenantId: "system",
    payload: jsonCompatible(payload),
    aggregateType: "scheduler-dispatch",
    aggregateId: dispatchId,
    expectedVersion,
  };
}

async function retryVersionConflict<T>(apply: () => Promise<T>): Promise<T> {
  let last: unknown;
  for (let attempt = 0; attempt < 4; attempt += 1) {
    try {
      return await apply();
    } catch (error) {
      if (!(error instanceof CellVersionConflictError)) throw error;
      last = error;
    }
  }
  throw last;
}

function validateCandidate(candidate: SchedulerCandidate): void {
  for (const key of ["task_id", "tenant_id", "node_id"] as const) {
    if (typeof candidate[key] !== "string" || !candidate[key]!.trim()) throw new TypeError(`${key} is required.`);
  }
  if (!candidate.resources || ["cpu", "memory_mb", "burst_cpu", "sessions", "tps"].some((key) => {
    const value = candidate.resources[key as keyof NodeResourceVector];
    return typeof value !== "number" || !Number.isFinite(value) || value < 0;
  })) {
    throw new TypeError("Candidate requires a finite non-negative resource vector.");
  }
  if (candidate.kind && !["ordinary", "recovery", "dag"].includes(candidate.kind)) throw new TypeError("Candidate kind must be ordinary, recovery or dag.");
  if (candidate.kind === "dag" && (!candidate.dag_id?.trim() || !candidate.voucher_id?.trim())) {
    throw new TypeError("DAG candidates require a durable dag_id and redeemed voucher_id.");
  }
  if (candidate.tenant_weight !== undefined && (!Number.isFinite(candidate.tenant_weight) || candidate.tenant_weight <= 0)) {
    throw new TypeError("tenant_weight must be finite and positive.");
  }
}

function assertCandidatePool(poolKey: string, candidate: SchedulerCandidate & Required<Pick<SchedulerCandidate, "region" | "resource_pool">>): void {
  const expected = schedulerPoolCellKey(candidate.region, candidate.resource_pool);
  if (poolKey !== expected) {
    throw new Error(`Candidate ${candidate.task_id} belongs to ${expected}, not SchedulerPool ${poolKey}.`);
  }
}

function normalizePoolPart(value: string, name: string): string {
  if (typeof value !== "string" || !value.trim() || /[\u0000-\u001f\u007f]/.test(value)) {
    throw new TypeError(`${name} must be a non-empty string without control characters.`);
  }
  return value.trim();
}

function validatePoolCapacity(capacity: NodeResourceVector): NodeResourceVector {
  if (!capacity || ["cpu", "memory_mb", "burst_cpu", "sessions", "tps"].some((key) => {
    const value = capacity[key as keyof NodeResourceVector];
    return typeof value !== "number" || !Number.isFinite(value) || value < 0;
  })) {
    throw new TypeError("SchedulerPool requires configured non-negative capacity for every resource dimension.");
  }
  return capacity;
}

function normalizedCandidate(candidate: SchedulerCandidate): SchedulerCandidate & Required<Pick<SchedulerCandidate,
  "dispatch_id" | "region" | "resource_pool" | "tenant_weight" | "kind" | "node_incarnation">> {
  return {
    ...candidate,
    dispatch_id: candidate.dispatch_id ?? `${candidate.kind ?? "ordinary"}:${candidate.task_id}:${candidate.expected_task_version ?? 0}`,
    region: candidate.region ?? "default",
    resource_pool: candidate.resource_pool ?? "default",
    node_incarnation: candidate.node_incarnation ?? "",
    tenant_weight: candidate.tenant_weight ?? 1,
    kind: candidate.kind ?? "ordinary",
  };
}

function assertSameCandidate(existing: SchedulerDispatchRecord, candidate: SchedulerCandidate & Required<Pick<SchedulerCandidate,
  "dispatch_id" | "region" | "resource_pool" | "tenant_weight" | "kind" | "node_incarnation">>): void {
  const old = JSON.parse(existing.request_json) as SchedulerCandidate;
  if (
    existing.task_id !== candidate.task_id || existing.tenant_id !== candidate.tenant_id ||
    existing.node_id !== candidate.node_id || existing.kind !== candidate.kind ||
    existing.region !== candidate.region || existing.resource_pool !== candidate.resource_pool ||
    existing.node_incarnation !== candidate.node_incarnation ||
    old.dag_id !== candidate.dag_id || old.voucher_id !== candidate.voucher_id ||
    JSON.stringify(JSON.parse(existing.resources_json)) !== JSON.stringify(candidate.resources) ||
    JSON.stringify(old.task_spec) !== JSON.stringify(candidate.task_spec)
  ) throw new Error(`Dispatch ${candidate.dispatch_id} was reused with different request content.`);
}

function stageRank(stage: TaskDispatchStage): number {
  return { PREPARING: 0, RESERVED: 1, AUTHORIZED: 2, STARTED: 3 }[stage];
}

function assertStageFields(row: SchedulerDispatchRecord, params: {
  reservation_id?: string; reservation_token?: string; authorization_token?: string; business_epoch?: number;
}): void {
  if ((params.reservation_id && row.reservation_id !== params.reservation_id) ||
      (params.reservation_token && row.reservation_token !== params.reservation_token) ||
      (params.authorization_token && row.authorization_token !== params.authorization_token) ||
      (params.business_epoch !== undefined && Number(row.business_epoch) !== params.business_epoch)) {
    throw new Error(`Dispatch ${row.dispatch_id} stage replay changed its reservation or authorization.`);
  }
}

function zeroVector(): NodeResourceVector {
  return { cpu: 0, memory_mb: 0, burst_cpu: 0, sessions: 0, tps: 0 };
}

function addVector(left: NodeResourceVector, right: NodeResourceVector): NodeResourceVector {
  return { cpu: left.cpu + right.cpu, memory_mb: left.memory_mb + right.memory_mb,
    burst_cpu: left.burst_cpu + right.burst_cpu, sessions: left.sessions + right.sessions, tps: left.tps + right.tps };
}

function dominantShare(used: NodeResourceVector, capacity: NodeResourceVector): number {
  const share = (amount: number, limit: number) => limit === 0 ? (amount === 0 ? 0 : Number.POSITIVE_INFINITY) : amount / limit;
  return Math.max(
    share(used.cpu, capacity.cpu),
    share(used.memory_mb, capacity.memory_mb),
    share(used.burst_cpu, capacity.burst_cpu),
    share(used.sessions, capacity.sessions),
    share(used.tps, capacity.tps),
  );
}

function jsonCompatible<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}
