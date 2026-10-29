-- D1 Migration 0001: ANSTS v2.0 Multi-Tenant Quota, Bayesian Profiles & Audit Ledger
-- Compatible with `celld d1 migrations apply ansts-audit-ledger` and `wrangler d1 migrations apply`

CREATE TABLE IF NOT EXISTS tenant_quotas (
  tenant_id TEXT PRIMARY KEY,
  tier_group TEXT NOT NULL DEFAULT 'Enterprise-Dedicated',
  weight REAL NOT NULL DEFAULT 1.0,
  guaranteed_cpu REAL NOT NULL DEFAULT 4.0,
  burst_ceiling_cpu REAL NOT NULL DEFAULT 32.0,
  burst_ceiling_tps INTEGER NOT NULL DEFAULT 2400,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bayesian_profiles (
  fingerprint TEXT PRIMARY KEY,
  sample_count INTEGER NOT NULL DEFAULT 0,
  mu_log_tokens REAL NOT NULL,
  sigma_log_tokens REAL NOT NULL,
  alpha_cog REAL NOT NULL,
  beta_exec REAL NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_audit_events (
  event_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  epoch INTEGER NOT NULL,
  turn_seq INTEGER NOT NULL,
  routed_model TEXT,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_task_id ON task_audit_events(task_id);
CREATE INDEX IF NOT EXISTS idx_audit_tenant_id ON task_audit_events(tenant_id);
