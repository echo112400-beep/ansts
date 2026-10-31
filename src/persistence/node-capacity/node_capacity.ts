import { CellOperationCommand, PersistenceObjectStorage, PersistenceSqlStorage } from "../contracts";
import { CellOperationConflictError, CellVersionConflictError, executeCellOperation } from "../operations";

export type NodeReservationStatus = "RESERVED" | "STARTING" | "RUNNING" | "SUSPECT" | "RELEASED" | "ISOLATED";

export interface NodeResourceVector {
  cpu: number;
  memory_mb: number;
  burst_cpu: number;
  sessions: number;
  tps: number;
}

export interface NodeReservation {
  dispatch_id: string;
  task_id: string;
  tenant_id: string;
  kind: "ordinary" | "recovery" | "dag";
  node_id: string;
  node_incarnation: string;
  resources: NodeResourceVector;
  status: NodeReservationStatus;
  reservation_token: string;
  authorization_token: string | null;
  business_epoch: number | null;
  start_receipt: string | null;
  release_evidence_id: string | null;
  release_evidence_type: "STOPPED" | "ISOLATED" | null;
  updated_at: number;
}

export interface NodeState {
  node_id: string;
  incarnation: string;
  capacity: NodeResourceVector;
  version: number;
  updated_at: number;
}

export class NodeCapacityError extends Error {
  constructor(public readonly code: string, message: string, public readonly status = 409) {
    super(message);
    this.name = "NodeCapacityError";
  }
}

export const nodeCapacityMigration = {
  version: 2,
  description: "node incarnation, exclusive reservations and release evidence",
  up(sql: PersistenceSqlStorage): void {
    sql.exec(`
      CREATE TABLE IF NOT EXISTS node_state (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        node_id TEXT NOT NULL,
        incarnation TEXT NOT NULL,
        capacity_json TEXT NOT NULL,
        version INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS node_reservations (
        dispatch_id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        node_id TEXT NOT NULL,
        node_incarnation TEXT NOT NULL,
        resources_json TEXT NOT NULL,
        status TEXT NOT NULL,
        reservation_token TEXT NOT NULL,
        authorization_token TEXT,
        business_epoch INTEGER,
        start_receipt TEXT,
        release_evidence_id TEXT,
        release_evidence_type TEXT,
        updated_at INTEGER NOT NULL
      )
    `);
    sql.exec("CREATE INDEX IF NOT EXISTS node_reservations_capacity_idx ON node_reservations (status, tenant_id)");
  },
};

export function readNodeState(sql: PersistenceSqlStorage): NodeState | null {
  const row = sql.exec<{ node_id: string; incarnation: string; capacity_json: string; version: number; updated_at: number }>(
    "SELECT * FROM node_state WHERE singleton = ?", 1,
  ).toArray()[0];
  return row ? {
    node_id: row.node_id,
    incarnation: row.incarnation,
    capacity: JSON.parse(row.capacity_json),
    version: Number(row.version),
    updated_at: Number(row.updated_at),
  } : null;
}

export function readReservation(sql: PersistenceSqlStorage, dispatchId: string): NodeReservation | null {
  const row = sql.exec<any>("SELECT * FROM node_reservations WHERE dispatch_id = ?", dispatchId).toArray()[0];
  return row ? {
    ...row,
    resources: JSON.parse(row.resources_json),
    business_epoch: row.business_epoch === null ? null : Number(row.business_epoch),
  } as NodeReservation : null;
}

export function activeNodeReservations(sql: PersistenceSqlStorage): NodeReservation[] {
  return sql.exec<any>("SELECT * FROM node_reservations").toArray()
    .map((row) => ({
      ...row,
      resources: JSON.parse(row.resources_json),
      business_epoch: row.business_epoch === null ? null : Number(row.business_epoch),
    } as NodeReservation))
    .filter((row) => isCapacityHoldingStatus(row.status));
}

export class NodeCapacity {
  constructor(public readonly storage: PersistenceObjectStorage, public readonly nodeId: string) {}

  public getState(): NodeState | null {
    return readNodeState(this.storage.sql);
  }

  public getReservation(dispatchId: string): NodeReservation | null {
    return readReservation(this.storage.sql, dispatchId);
  }

  public getReservations(includeReleased = false): NodeReservation[] {
    const all = this.storage.sql.exec<any>("SELECT * FROM node_reservations").toArray().map((row) => ({
      ...row,
      resources: JSON.parse(row.resources_json),
      business_epoch: row.business_epoch === null ? null : Number(row.business_epoch),
    } as NodeReservation));
    return includeReleased ? all : all.filter((row) => isCapacityHoldingStatus(row.status));
  }

  public async register(params: {
    incarnation: string;
    capacity: NodeResourceVector;
    reboot_evidence_id?: string;
  }): Promise<NodeState> {
    validateText(params.incarnation, "incarnation");
    const capacity = validateVector(params.capacity);
    const current = this.getState();
    if (current && current.incarnation === params.incarnation) {
      if (!sameVector(current.capacity, capacity)) throw new NodeCapacityError("NODE_CAPACITY_CONFLICT", "Node capacity cannot change within one incarnation.");
      return current;
    }
    if (current && !params.reboot_evidence_id?.trim()) {
      throw new NodeCapacityError("NODE_REINCARNATION_EVIDENCE_REQUIRED", "A new node incarnation requires reboot/isolation evidence.");
    }
    const operationId = `node:${this.nodeId}:incarnation:${params.incarnation}`;
    const payload = jsonCompatible({
      node_id: this.nodeId,
      incarnation: params.incarnation,
      capacity,
      ...(params.reboot_evidence_id ? { reboot_evidence_id: params.reboot_evidence_id } : {}),
    });
    return executeNodeOperation(this.storage, (expected) => nodeCommand(this.nodeId, operationId, "node.capacity.register", payload, expected), (sql) => {
      const old = readNodeState(sql);
      if (old && old.incarnation !== params.incarnation) {
        const oldRows = activeNodeReservations(sql);
        for (const reservation of oldRows) {
          sql.exec(
            "UPDATE node_reservations SET status = ?, release_evidence_id = ?, release_evidence_type = ?, updated_at = ? WHERE dispatch_id = ?",
            "ISOLATED", `incarnation:${old.incarnation}->${params.incarnation}:${params.reboot_evidence_id}`, "ISOLATED", Date.now(), reservation.dispatch_id,
          );
        }
      }
      const nextVersion = (old?.version ?? 0) + 1;
      if (old) {
        sql.exec(
          "UPDATE node_state SET incarnation = ?, capacity_json = ?, version = ?, updated_at = ? WHERE singleton = ?",
          params.incarnation, JSON.stringify(capacity), nextVersion, Date.now(), 1,
        );
      } else {
        sql.exec(
          `INSERT INTO node_state (singleton, node_id, incarnation, capacity_json, version, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)`,
          1, this.nodeId, params.incarnation, JSON.stringify(capacity), nextVersion, Date.now(),
        );
      }
      return readNodeState(sql)!;
    }, () => {
      const latest = this.getState();
      if (latest?.incarnation === params.incarnation && sameVector(latest.capacity, capacity)) return latest;
      return null;
    });
  }

  /** Record a node-agent heartbeat; it never releases reservations or assumes a task stopped. */
  public async heartbeat(incarnation: string, observedAt = Date.now()): Promise<NodeState> {
    validateText(incarnation, "incarnation");
    if (!Number.isSafeInteger(observedAt) || observedAt < 0) throw new NodeCapacityError("INVALID_HEARTBEAT", "heartbeat_at must be a non-negative safe integer.", 400);
    const current = this.getState();
    if (!current) throw new NodeCapacityError("NODE_NOT_REGISTERED", `Node ${this.nodeId} has no registered incarnation.`, 404);
    if (current.incarnation !== incarnation) throw new NodeCapacityError("NODE_INCARNATION_MISMATCH", "Heartbeat belongs to a stale node incarnation.");
    const heartbeatAt = Math.floor(observedAt / 5000) * 5000;
    if (Number(current.updated_at) >= heartbeatAt) return current;
    const operationId = `node:${this.nodeId}:heartbeat:${heartbeatAt}`;
    return executeNodeOperation(
      this.storage,
      (expected) => nodeCommand(this.nodeId, operationId, "node.capacity.heartbeat", { incarnation, heartbeat_at: heartbeatAt }, expected),
      (sql) => {
        const latest = readNodeState(sql);
        if (!latest || latest.incarnation !== incarnation) throw new NodeCapacityError("NODE_INCARNATION_MISMATCH", "Node incarnation changed before heartbeat commit.");
        if (latest.updated_at < heartbeatAt) {
          sql.exec("UPDATE node_state SET updated_at = ? WHERE singleton = ?", heartbeatAt, 1);
        }
        return readNodeState(sql)!;
      },
      () => {
        const latest = this.getState();
        return latest?.incarnation === incarnation && latest.updated_at >= heartbeatAt ? latest : null;
      },
    );
  }

  public async reserve(params: {
    dispatch_id: string;
    task_id: string;
    tenant_id: string;
    kind: NodeReservation["kind"];
    node_incarnation: string;
    resources: NodeResourceVector;
  }): Promise<NodeReservation> {
    for (const [name, value] of Object.entries(params)) {
      if (name === "resources") continue;
      validateText(String(value), name);
    }
    const resources = validateVector(params.resources);
    const existing = this.getReservation(params.dispatch_id);
    if (existing) {
      assertSameReservation(existing, { ...params, node_id: this.nodeId, resources });
      if (["RELEASED", "ISOLATED"].includes(existing.status)) throw new NodeCapacityError("NODE_RESERVATION_FINAL", `Dispatch ${params.dispatch_id} is already ${existing.status}.`);
      return existing;
    }
    const current = this.getState();
    if (!current) throw new NodeCapacityError("NODE_NOT_REGISTERED", `Node ${this.nodeId} has no registered incarnation.`, 404);
    if (current.incarnation !== params.node_incarnation) throw new NodeCapacityError("NODE_INCARNATION_MISMATCH", "Reservation targets a stale node incarnation.");
    const payload = { ...params, node_id: this.nodeId, resources };
    const operationId = `node:${this.nodeId}:reserve:${params.dispatch_id}`;
    return executeNodeOperation(this.storage, (expected) => nodeCommand(this.nodeId, operationId, "node.capacity.reserve", payload, expected), (sql) => {
      const node = readNodeState(sql);
      if (!node || node.incarnation !== params.node_incarnation) throw new NodeCapacityError("NODE_INCARNATION_MISMATCH", "Node incarnation changed before reservation.");
      const duplicate = readReservation(sql, params.dispatch_id);
      if (duplicate) {
        assertSameReservation(duplicate, { ...params, node_id: this.nodeId, resources });
        return duplicate;
      }
      const used = activeNodeReservations(sql).reduce(addVector, zeroVector());
      if (!fits(used, resources, node.capacity)) throw new NodeCapacityError("NODE_CAPACITY_EXCEEDED", `Node ${this.nodeId} does not have enough unreserved capacity.`, 409);
      const token = `reservation:${params.dispatch_id}:${params.node_incarnation}`;
      sql.exec(
        `INSERT INTO node_reservations (
          dispatch_id, task_id, tenant_id, kind, node_id, node_incarnation, resources_json, status,
          reservation_token, authorization_token, business_epoch, start_receipt, release_evidence_id,
          release_evidence_type, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        params.dispatch_id, params.task_id, params.tenant_id, params.kind, this.nodeId, params.node_incarnation,
        JSON.stringify(resources), "RESERVED", token, null, null, null, null, null, Date.now(),
      );
      return readReservation(sql, params.dispatch_id)!;
    }, () => {
      const latest = this.getReservation(params.dispatch_id);
      if (!latest) return null;
      assertSameReservation(latest, { ...params, node_id: this.nodeId, resources });
      if (["RELEASED", "ISOLATED"].includes(latest.status)) throw new NodeCapacityError("NODE_RESERVATION_FINAL", `Dispatch ${params.dispatch_id} is already ${latest.status}.`);
      return latest;
    });
  }

  public async authorize(params: {
    dispatch_id: string;
    reservation_token: string;
    authorization_token: string;
    business_epoch: number;
    node_incarnation: string;
  }): Promise<NodeReservation> {
    const current = this.requireReservation(params.dispatch_id);
    if (current.reservation_token !== params.reservation_token || current.node_incarnation !== params.node_incarnation) {
      throw new NodeCapacityError("NODE_RESERVATION_INVALID", "Authorization does not match the current reservation.");
    }
    if (current.authorization_token) {
      if (current.authorization_token !== params.authorization_token || current.business_epoch !== params.business_epoch) {
        throw new NodeCapacityError("NODE_AUTHORIZATION_CONFLICT", "Reservation already has a different TaskCell authorization.");
      }
      return current;
    }
    if (current.status !== "RESERVED") throw new NodeCapacityError("NODE_RESERVATION_STAGE_CONFLICT", `Reservation is ${current.status}.`);
    return this.updateReservation(params.dispatch_id, `node:reservation:${params.dispatch_id}:authorize`, "node.capacity.authorize", {
      ...params,
    }, (sql) => {
      sql.exec(
        "UPDATE node_reservations SET status = ?, authorization_token = ?, business_epoch = ?, updated_at = ? WHERE dispatch_id = ?",
        "STARTING", params.authorization_token, params.business_epoch, Date.now(), params.dispatch_id,
      );
      return readReservation(sql, params.dispatch_id)!;
    }, () => {
      const latest = this.getReservation(params.dispatch_id);
      if (latest?.authorization_token === params.authorization_token && latest.business_epoch === params.business_epoch) return latest;
      return null;
    });
  }

  public async markStarted(params: {
    dispatch_id: string;
    authorization_token: string;
    business_epoch: number;
    start_receipt: string;
  }): Promise<NodeReservation> {
    validateText(params.start_receipt, "start_receipt");
    const current = this.requireReservation(params.dispatch_id);
    if (current.authorization_token !== params.authorization_token || current.business_epoch !== params.business_epoch) {
      throw new NodeCapacityError("NODE_AUTHORIZATION_INVALID", "Start receipt does not match TaskCell authorization.");
    }
    if (current.status === "RUNNING") {
      if (current.start_receipt !== params.start_receipt) throw new NodeCapacityError("NODE_START_RECEIPT_CONFLICT", "Dispatch already started with a different receipt.");
      return current;
    }
    if (current.status !== "STARTING") throw new NodeCapacityError("NODE_RESERVATION_STAGE_CONFLICT", `Reservation is ${current.status}.`);
    return this.updateReservation(params.dispatch_id, `node:reservation:${params.dispatch_id}:started`, "node.capacity.started", params, (sql) => {
      sql.exec("UPDATE node_reservations SET status = ?, start_receipt = ?, updated_at = ? WHERE dispatch_id = ?", "RUNNING", params.start_receipt, Date.now(), params.dispatch_id);
      return readReservation(sql, params.dispatch_id)!;
    }, () => {
      const latest = this.getReservation(params.dispatch_id);
      if (latest?.status === "RUNNING" && latest.start_receipt === params.start_receipt) return latest;
      return null;
    });
  }

  public async markSuspect(dispatchId: string, reason: string): Promise<NodeReservation> {
    validateText(reason, "reason");
    const current = this.requireReservation(dispatchId);
    if (current.status === "SUSPECT") return current;
    if (current.status === "RELEASED" || current.status === "ISOLATED") return current;
    return this.updateReservation(dispatchId, `node:reservation:${dispatchId}:suspect`, "node.capacity.suspect", { reason }, (sql) => {
      sql.exec("UPDATE node_reservations SET status = ?, updated_at = ? WHERE dispatch_id = ?", "SUSPECT", Date.now(), dispatchId);
      return readReservation(sql, dispatchId)!;
    }, () => this.getReservation(dispatchId)?.status === "SUSPECT" ? this.getReservation(dispatchId) : null);
  }

  public async release(params: {
    dispatch_id: string;
    node_incarnation: string;
    evidence_type: "STOPPED" | "ISOLATED";
    evidence_id: string;
  }): Promise<NodeReservation> {
    validateText(params.evidence_id, "evidence_id");
    const current = this.requireReservation(params.dispatch_id);
    if (current.status === "RELEASED" || current.status === "ISOLATED") {
      if (current.release_evidence_type !== params.evidence_type || current.release_evidence_id !== params.evidence_id) {
        throw new NodeCapacityError("NODE_RELEASE_EVIDENCE_CONFLICT", "Reservation was finalized with different stop/isolation evidence.");
      }
      return current;
    }
    if (current.node_incarnation !== params.node_incarnation) throw new NodeCapacityError("NODE_INCARNATION_MISMATCH", "Release evidence is for a different node incarnation.");
    const terminalStatus = params.evidence_type === "ISOLATED" ? "ISOLATED" : "RELEASED";
    return this.updateReservation(params.dispatch_id, `node:reservation:${params.dispatch_id}:release`, "node.capacity.release", params, (sql) => {
      sql.exec(
        "UPDATE node_reservations SET status = ?, release_evidence_id = ?, release_evidence_type = ?, updated_at = ? WHERE dispatch_id = ?",
        terminalStatus, params.evidence_id, params.evidence_type, Date.now(), params.dispatch_id,
      );
      return readReservation(sql, params.dispatch_id)!;
    }, () => {
      const latest = this.getReservation(params.dispatch_id);
      if (latest && latest.release_evidence_type === params.evidence_type && latest.release_evidence_id === params.evidence_id) return latest;
      return null;
    });
  }

  private requireReservation(dispatchId: string): NodeReservation {
    const reservation = this.getReservation(dispatchId);
    if (!reservation) throw new NodeCapacityError("NODE_RESERVATION_NOT_FOUND", `Dispatch ${dispatchId} has no reservation.`, 404);
    return reservation;
  }

  private async updateReservation<T extends NodeReservation>(
    dispatchId: string,
    operationId: string,
    operationType: string,
    payload: unknown,
    apply: (sql: PersistenceSqlStorage) => T,
    replay: () => T | null,
  ): Promise<T> {
    return executeNodeOperation(
      this.storage,
      (expected) => nodeCommand(this.nodeId, operationId, operationType, { dispatch_id: dispatchId, payload }, expected),
      apply,
      replay,
    );
  }
}

function nodeCommand(nodeId: string, operationId: string, operationType: string, payload: unknown, expectedVersion: number): CellOperationCommand {
  return {
    operationId,
    operationType,
    resourceId: nodeId,
    tenantId: "system",
    payload: jsonCompatible(payload),
    aggregateType: "node-capacity",
    aggregateId: nodeId,
    expectedVersion,
  };
}

function readAggregateVersion(sql: PersistenceSqlStorage, aggregateType: string, aggregateId: string): number {
  const row = sql.exec<{ version: number }>(
    "SELECT version FROM cell_aggregate_versions WHERE aggregate_type = ? AND aggregate_id = ?",
    aggregateType,
    aggregateId,
  ).toArray()[0];
  return row ? Number(row.version) : 0;
}

async function executeNodeOperation<T>(
  storage: PersistenceObjectStorage,
  commandForVersion: (expectedVersion: number) => CellOperationCommand,
  apply: (sql: PersistenceSqlStorage) => T,
  replay: () => T | null,
): Promise<T> {
  let last: unknown;
  for (let attempt = 0; attempt < 4; attempt += 1) {
    try {
      const command = commandForVersion(readAggregateVersion(storage.sql, "node-capacity", commandForVersion(0).aggregateId));
      return (await executeCellOperation(storage, command, apply)).result;
    } catch (error) {
      if (error instanceof CellOperationConflictError) {
        const existing = replay();
        if (existing !== null) return existing;
        throw error;
      }
      if (!(error instanceof CellVersionConflictError)) throw error;
      last = error;
    }
  }
  throw last;
}

function validateVector(value: NodeResourceVector): NodeResourceVector {
  if (!value || typeof value !== "object") throw new NodeCapacityError("INVALID_RESOURCE_VECTOR", "Resource vector is required.", 400);
  const result = {} as NodeResourceVector;
  for (const key of ["cpu", "memory_mb", "burst_cpu", "sessions", "tps"] as const) {
    const n = value[key];
    if (typeof n !== "number" || !Number.isFinite(n) || n < 0) throw new NodeCapacityError("INVALID_RESOURCE_VECTOR", `${key} must be a finite non-negative number.`, 400);
    result[key] = n;
  }
  return result;
}

function validateText(value: string, name: string): void {
  if (typeof value !== "string" || !value.trim()) throw new NodeCapacityError("INVALID_NODE_COMMAND", `${name} is required.`, 400);
}

function sameVector(left: NodeResourceVector, right: NodeResourceVector): boolean {
  return ["cpu", "memory_mb", "burst_cpu", "sessions", "tps"].every((key) => left[key as keyof NodeResourceVector] === right[key as keyof NodeResourceVector]);
}

function assertSameReservation(existing: NodeReservation, request: {
  dispatch_id: string;
  task_id: string;
  tenant_id: string;
  kind: NodeReservation["kind"];
  node_id: string;
  node_incarnation: string;
  resources: NodeResourceVector;
}): void {
  if (
    existing.task_id !== request.task_id || existing.tenant_id !== request.tenant_id || existing.kind !== request.kind ||
    existing.node_id !== request.node_id || existing.node_incarnation !== request.node_incarnation ||
    !sameVector(existing.resources, request.resources)
  ) throw new NodeCapacityError("NODE_DISPATCH_CONFLICT", `Dispatch ${request.dispatch_id} was reused for different node capacity.`);
}

function isCapacityHoldingStatus(status: string): boolean {
  return ["RESERVED", "STARTING", "RUNNING", "SUSPECT"].includes(status);
}

function jsonCompatible<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function zeroVector(): NodeResourceVector {
  return { cpu: 0, memory_mb: 0, burst_cpu: 0, sessions: 0, tps: 0 };
}

function addVector(sum: NodeResourceVector, reservation: NodeReservation): NodeResourceVector {
  return {
    cpu: sum.cpu + reservation.resources.cpu,
    memory_mb: sum.memory_mb + reservation.resources.memory_mb,
    burst_cpu: sum.burst_cpu + reservation.resources.burst_cpu,
    sessions: sum.sessions + reservation.resources.sessions,
    tps: sum.tps + reservation.resources.tps,
  };
}

function fits(used: NodeResourceVector, requested: NodeResourceVector, capacity: NodeResourceVector): boolean {
  return used.cpu + requested.cpu <= capacity.cpu &&
    used.memory_mb + requested.memory_mb <= capacity.memory_mb &&
    used.burst_cpu + requested.burst_cpu <= capacity.burst_cpu &&
    used.sessions + requested.sessions <= capacity.sessions &&
    used.tps + requested.tps <= capacity.tps;
}
