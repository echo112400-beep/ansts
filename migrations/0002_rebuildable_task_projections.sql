-- Rebuildable tenant task and committed-event projections. TenantLedgerCell and TaskCell remain authoritative.

CREATE TABLE IF NOT EXISTS tenant_task_projection (
  tenant_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  admission_status TEXT NOT NULL,
  runnable_status TEXT NOT NULL,
  accepted_at INTEGER NOT NULL,
  lifecycle_state TEXT NOT NULL,
  business_epoch INTEGER NOT NULL DEFAULT 0,
  last_turn_seq INTEGER NOT NULL DEFAULT 0,
  execution_version INTEGER NOT NULL DEFAULT 0,
  projected_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, task_id)
);

CREATE INDEX IF NOT EXISTS idx_tenant_task_projection_order
  ON tenant_task_projection (tenant_id, accepted_at DESC, task_id);

CREATE TABLE IF NOT EXISTS task_event_projection (
  event_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  aggregate_version INTEGER NOT NULL,
  payload_digest TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_event_projection_replay
  ON task_event_projection (tenant_id, task_id, aggregate_version, created_at, event_id);

CREATE TABLE IF NOT EXISTS task_projection_offsets (
  tenant_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  latest_aggregate_version INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, task_id)
);
