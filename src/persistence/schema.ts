import { CellMigration, PERSISTENCE_CELL_TYPES, PersistenceCellIdentity, PersistenceSqlStorage } from "./contracts";
import { withCellTransaction } from "./transactions";

export class CellIdentityMismatchError extends Error {
  public readonly code = "CELL_IDENTITY_MISMATCH";

  constructor(expected: PersistenceCellIdentity, stored: PersistenceCellIdentity) {
    super(
      `Cell database identity mismatch: expected ${expected.type}/${expected.key}, found ${stored.type}/${stored.key}.`,
    );
    this.name = "CellIdentityMismatchError";
  }
}

export class CellSchemaError extends Error {
  public readonly code = "CELL_SCHEMA_INVALID";

  constructor(message: string) {
    super(message);
    this.name = "CellSchemaError";
  }
}

const protocolMigration: CellMigration = {
  version: 1,
  description: "durable cell protocol tables",
  up(sql) {
    sql.exec(`
      CREATE TABLE IF NOT EXISTS cell_operations (
        operation_id TEXT PRIMARY KEY,
        operation_type TEXT NOT NULL,
        resource_id TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        payload_digest TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        expected_version INTEGER NOT NULL,
        aggregate_version INTEGER NOT NULL,
        result_json TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS cell_aggregate_versions (
        aggregate_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        version INTEGER NOT NULL,
        PRIMARY KEY (aggregate_type, aggregate_id)
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS cell_inbox (
        event_id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        aggregate_version INTEGER NOT NULL,
        payload_digest TEXT NOT NULL,
        received_at INTEGER NOT NULL,
        processed_at INTEGER NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS cell_outbox (
        event_id TEXT PRIMARY KEY,
        operation_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        aggregate_id TEXT NOT NULL,
        aggregate_version INTEGER NOT NULL,
        payload_digest TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL,
        next_attempt_at INTEGER NOT NULL,
        created_at INTEGER NOT NULL,
        delivered_at INTEGER,
        last_error TEXT
      )
    `);
    sql.exec("CREATE INDEX IF NOT EXISTS cell_outbox_due_idx ON cell_outbox (status, next_attempt_at, created_at)");
  },
};

/** Create the shared tables and apply contiguous, versioned owner migrations. */
export function initializeCellProtocol(
  sql: PersistenceSqlStorage,
  identity: PersistenceCellIdentity,
  ownerMigrations: CellMigration[] = [],
): number {
  validateIdentity(identity);
  const migrations = [protocolMigration, ...ownerMigrations].sort((a, b) => a.version - b.version);
  const versions = new Set<number>();
  for (const migration of migrations) {
    if (!Number.isSafeInteger(migration.version) || migration.version < 1 || versions.has(migration.version)) {
      throw new CellSchemaError(`Invalid or duplicate Cell migration version: ${migration.version}.`);
    }
    versions.add(migration.version);
  }
  migrations.forEach((migration, index) => {
    if (migration.version !== index + 1) {
      throw new CellSchemaError(`Cell migrations must be contiguous from version 1; missing ${index + 1}.`);
    }
  });

  withCellTransaction(sql, () => {
    sql.exec(`
      CREATE TABLE IF NOT EXISTS cell_registry (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        cell_type TEXT NOT NULL,
        cell_key TEXT NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS cell_schema_version (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        version INTEGER NOT NULL
      )
    `);
    const storedRows = sql.exec<{ cell_type: PersistenceCellIdentity["type"]; cell_key: string }>(
      "SELECT cell_type, cell_key FROM cell_registry",
    ).toArray();
    if (
      storedRows.length > 0 &&
      (storedRows[0].cell_type !== identity.type || storedRows[0].cell_key !== identity.key)
    ) {
      throw new CellIdentityMismatchError(identity, { type: storedRows[0].cell_type, key: storedRows[0].cell_key });
    }
    if (storedRows.length === 0) {
      sql.exec("INSERT INTO cell_registry (singleton, cell_type, cell_key) VALUES (?, ?, ?)", 1, identity.type, identity.key);
    }
  });

  const schemaRows = sql.exec<{ version: number }>("SELECT version FROM cell_schema_version WHERE singleton = ?", 1).toArray();
  let currentVersion = schemaRows.length > 0 ? Number(schemaRows[0].version) : 0;
  const latestVersion = migrations[migrations.length - 1]?.version ?? 0;
  if (currentVersion > latestVersion) {
    throw new CellSchemaError(`Cell schema ${currentVersion} is newer than this code supports (${latestVersion}).`);
  }

  for (const migration of migrations) {
    if (migration.version <= currentVersion) continue;
    if (migration.version !== currentVersion + 1) {
      throw new CellSchemaError(`Cannot apply Cell schema ${migration.version} after ${currentVersion}.`);
    }
    withCellTransaction(sql, () => {
      migration.up(sql);
      if (currentVersion === 0 && migration.version === 1) {
        sql.exec("INSERT INTO cell_schema_version (singleton, version) VALUES (?, ?)", 1, migration.version);
      } else {
        sql.exec("UPDATE cell_schema_version SET version = ? WHERE singleton = ?", migration.version, 1);
      }
    });
    currentVersion = migration.version;
  }

  return latestVersion;
}

function validateIdentity(identity: PersistenceCellIdentity): void {
  if (
    !identity ||
    !PERSISTENCE_CELL_TYPES.includes(identity.type) ||
    typeof identity.key !== "string" ||
    identity.key.trim().length === 0
  ) {
    throw new CellSchemaError("Cell identity requires a known type and non-empty key.");
  }
  if (/[\u0000-\u001f\u007f]/.test(identity.key)) {
    throw new CellSchemaError("Cell identity keys cannot contain control characters.");
  }
}

export const CELL_PROTOCOL_SCHEMA_VERSION = 1;
