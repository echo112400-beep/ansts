-- ANSTS v2.2 Cloudflare D1 & celld SQLite Audit Schema
CREATE TABLE IF NOT EXISTS task_audit_events (
  event_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  epoch INTEGER NOT NULL DEFAULT 0,
  turn_seq INTEGER NOT NULL DEFAULT 0,
  routed_model TEXT NOT NULL DEFAULT 'DeepSeek',
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_task_audit_tenant ON task_audit_events(tenant_id, created_at);
