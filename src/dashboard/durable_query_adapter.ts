import type { D1DatabaseBinding } from "../cloudflare/env";

export interface TenantTaskProjection {
  tenant_id: string;
  task_id: string;
  admission_status: string;
  runnable_status: string;
  accepted_at: number;
  lifecycle_state: string;
  business_epoch: number;
  last_turn_seq: number;
  execution_version: number;
}

export interface ProjectedTaskEvent {
  event_id: string;
  tenant_id: string;
  task_id: string;
  event_type: string;
  aggregate_version: number;
  payload_digest: string;
  payload: unknown;
  created_at: number;
}

export class ProjectionConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProjectionConflictError";
  }
}

/** Rebuildable dashboard state; the tenant and task Cells remain authoritative. */
export async function projectTenantTasks(db: D1DatabaseBinding, tasks: TenantTaskProjection[]): Promise<void> {
  for (const task of tasks) {
    validateProjection(task);
    await db.prepare(`INSERT INTO tenant_task_projection (
      tenant_id, task_id, admission_status, runnable_status, accepted_at, lifecycle_state,
      business_epoch, last_turn_seq, execution_version, projected_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(tenant_id, task_id) DO UPDATE SET
      admission_status = excluded.admission_status,
      runnable_status = excluded.runnable_status,
      accepted_at = excluded.accepted_at,
      lifecycle_state = excluded.lifecycle_state,
      business_epoch = excluded.business_epoch,
      last_turn_seq = excluded.last_turn_seq,
      execution_version = excluded.execution_version,
      projected_at = excluded.projected_at
    WHERE excluded.execution_version >= tenant_task_projection.execution_version`)
      .bind(task.tenant_id, task.task_id, task.admission_status, task.runnable_status, task.accepted_at,
        task.lifecycle_state, task.business_epoch, task.last_turn_seq, task.execution_version, Date.now())
      .run();
  }
}

/** Event rows are immutable and idempotent; a reused event ID with different content is rejected. */
export async function projectTaskEvents(db: D1DatabaseBinding, events: ProjectedTaskEvent[]): Promise<void> {
  for (const event of events) {
    validateEvent(event);
    // The insert arbitrates concurrent deliveries. Always verify its winning row, even
    // when this delivery lost the insert race, before acknowledging or advancing offsets.
    await db.prepare(`INSERT OR IGNORE INTO task_event_projection (
      event_id, tenant_id, task_id, event_type, aggregate_version, payload_digest, payload_json, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
      .bind(event.event_id, event.tenant_id, event.task_id, event.event_type, event.aggregate_version,
        event.payload_digest, JSON.stringify(event.payload), event.created_at)
      .run();
    const existingResult = await db.prepare(
      "SELECT tenant_id, task_id, event_type, aggregate_version, payload_digest FROM task_event_projection WHERE event_id = ?",
    ).bind(event.event_id).all<Record<string, unknown>>();
    const existing = existingResult.results[0];
    if (!existing || existing.tenant_id !== event.tenant_id || existing.task_id !== event.task_id
      || existing.event_type !== event.event_type || Number(existing.aggregate_version) !== event.aggregate_version
      || existing.payload_digest !== event.payload_digest) {
      throw new ProjectionConflictError(`Task event ${event.event_id} was projected with different identity or content.`);
    }
    // A prior attempt may have inserted the event and failed before this write.
    // Replays must repair the offset rather than returning early on an existing event.
    await db.prepare(`INSERT INTO task_projection_offsets (tenant_id, task_id, latest_aggregate_version, updated_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(tenant_id, task_id) DO UPDATE SET
        latest_aggregate_version = MAX(task_projection_offsets.latest_aggregate_version, excluded.latest_aggregate_version),
        updated_at = excluded.updated_at`)
      .bind(event.tenant_id, event.task_id, event.aggregate_version, Date.now())
      .run();
  }
}

export async function readTenantTaskProjection(db: D1DatabaseBinding, tenantId: string, limit = 500): Promise<TenantTaskProjection[]> {
  validateTenant(tenantId);
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 500) throw new RangeError("Projection limit must be 1..500.");
  const result = await db.prepare(`SELECT tenant_id, task_id, admission_status, runnable_status, accepted_at,
    lifecycle_state, business_epoch, last_turn_seq, execution_version
    FROM tenant_task_projection WHERE tenant_id = ? ORDER BY accepted_at DESC LIMIT ?`)
    .bind(tenantId, limit).all<TenantTaskProjection>();
  return result.results.map((task) => ({ ...task, accepted_at: Number(task.accepted_at),
    business_epoch: Number(task.business_epoch), last_turn_seq: Number(task.last_turn_seq),
    execution_version: Number(task.execution_version) }));
}

/** D1's tenant index is a discovery hint for refresh; task Cells remain the recovery source. */
export async function listProjectedTenants(db: D1DatabaseBinding, limit = 20): Promise<string[]> {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new RangeError("Tenant projection scan limit must be 1..100.");
  const result = await db.prepare(`SELECT tenant_id FROM tenant_task_projection
    GROUP BY tenant_id ORDER BY MIN(projected_at) ASC LIMIT ?`).bind(limit).all<{ tenant_id: string }>();
  return result.results.map((row) => row.tenant_id);
}

/** Retain the D1 business-event replay window; source events remain in TaskCell for rebuild. */
export async function pruneTaskEventProjection(db: D1DatabaseBinding, now = Date.now()): Promise<void> {
  if (!Number.isSafeInteger(now) || now < 0) throw new TypeError("Retention time must be a non-negative integer.");
  await db.prepare("DELETE FROM task_event_projection WHERE created_at < ?")
    .bind(now - 7 * 24 * 60 * 60 * 1000).run();
}

function validateProjection(task: TenantTaskProjection): void {
  validateTenant(task.tenant_id);
  if (!task.task_id || !task.admission_status || !task.runnable_status || !task.lifecycle_state) throw new TypeError("Task projection identity and state are required.");
  for (const value of [task.accepted_at, task.business_epoch, task.last_turn_seq, task.execution_version]) {
    if (!Number.isSafeInteger(value) || value < 0) throw new TypeError("Task projection versions and timestamps must be non-negative integers.");
  }
}

function validateEvent(event: ProjectedTaskEvent): void {
  validateTenant(event.tenant_id);
  if (!event.event_id || !event.task_id || !event.event_type || !/^[a-f0-9]{64}$/i.test(event.payload_digest)) throw new TypeError("Task event projection identity or digest is invalid.");
  if (!Number.isSafeInteger(event.aggregate_version) || event.aggregate_version < 1 || !Number.isSafeInteger(event.created_at) || event.created_at < 0) {
    throw new TypeError("Task event version and timestamp are invalid.");
  }
}

function validateTenant(tenantId: string): void {
  if (typeof tenantId !== "string" || !tenantId.trim() || tenantId.length > 512) throw new TypeError("tenant_id must be non-empty and at most 512 characters.");
}
