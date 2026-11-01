import { digestJson } from "../digest";
import { CellOperationCommand, PersistenceSqlStorage } from "../contracts";
import { executeCellOperation } from "../operations";
import { initializeCellProtocol } from "../schema";
import { confirmCellWrite, withCellTransaction } from "../transactions";
import { DurableObjectState, CloudflareCelldEnv } from "../../cloudflare/env";

export interface ProviderQuotaPolicy {
  requestsPerMinute: number;
  tokensPerSecond: number;
  grantTtlMs: number;
}

export interface ConfigureProviderQuotaPolicyInput {
  operationId: string;
  expectedVersion: number;
  policy: ProviderQuotaPolicy;
}

export interface AcquireProviderQuotaGrantInput {
  requestId: string;
  tenantId: string;
  provider: string;
  routeVersion: number;
  requests: number;
  tokensPerSecond: number;
}

export type ProviderQuotaGrantStatus = "GRANTED" | "DENIED" | "STARTED" | "UNKNOWN" | "COMPLETED";

export interface ProviderUsageEvidence {
  verified: true;
  reference: string;
  inputTokens: number;
  outputTokens: number;
}

export interface ProviderQuotaGrant {
  grantId: string;
  requestId: string;
  tenantId: string;
  provider: string;
  routeVersion: number;
  requests: number;
  tokensPerSecond: number;
  policyVersion: number;
  expiresAt: number;
  status: ProviderQuotaGrantStatus;
  usage?: ProviderUsageEvidence;
  unknownReason?: string;
  denialReason?: string;
}

export type BeginProviderCallResult =
  | { state: "STARTED" }
  | { state: "OUTCOME_UNKNOWN" }
  | { state: "EXPIRED" }
  | { state: "COMPLETED"; usage: ProviderUsageEvidence };

interface PolicyVersionRow {
  version: number;
  requests_per_minute: number;
  tokens_per_second: number;
  grant_ttl_ms: number;
}

interface PolicyHeadRow {
  current_version: number;
}

interface RateGrantRow {
  requests: number;
  tokens_per_second: number;
}

interface GrantRow {
  grant_id: string;
  request_id: string;
  request_digest: string;
  tenant_id: string;
  provider: string;
  route_version: number;
  requests: number;
  tokens_per_second: number;
  policy_version: number;
  rpm_window_start: number;
  tps_window_start: number;
  expires_at: number;
  status: ProviderQuotaGrantStatus;
  usage_json: string | null;
  unknown_reason: string | null;
  denial_reason: string | null;
  created_at: number;
  updated_at: number;
}

export class ProviderQuotaError extends Error {
  constructor(public readonly code: string, message: string, public readonly status = 409) {
    super(message);
    this.name = "ProviderQuotaError";
  }
}

export const providerQuotaMigration = {
  version: 2,
  description: "versioned provider quota policy, rate windows and durable grants",
  up(sql: PersistenceSqlStorage): void {
    sql.exec(`
      CREATE TABLE IF NOT EXISTS provider_quota_policy_versions (
        version INTEGER PRIMARY KEY,
        requests_per_minute INTEGER NOT NULL,
        tokens_per_second INTEGER NOT NULL,
        grant_ttl_ms INTEGER NOT NULL,
        operation_id TEXT NOT NULL UNIQUE
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS provider_quota_policy_head (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        current_version INTEGER NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS provider_quota_grants (
        grant_id TEXT PRIMARY KEY,
        request_id TEXT NOT NULL UNIQUE,
        request_digest TEXT NOT NULL,
        tenant_id TEXT NOT NULL,
        provider TEXT NOT NULL,
        route_version INTEGER NOT NULL,
        requests INTEGER NOT NULL,
        tokens_per_second INTEGER NOT NULL,
        policy_version INTEGER NOT NULL,
        rpm_window_start INTEGER NOT NULL,
        tps_window_start INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        status TEXT NOT NULL,
        usage_json TEXT,
        unknown_reason TEXT,
        denial_reason TEXT,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);
    sql.exec("CREATE INDEX IF NOT EXISTS provider_quota_grants_status_idx ON provider_quota_grants (status, created_at)");
  },
};

/** The named Durable Object is the only writer for one shared provider quota domain. */
export class ProviderQuotaCell {
  private readonly quotaDomainKey: string;

  constructor(
    public readonly ctx: DurableObjectState,
    public readonly env: CloudflareCelldEnv,
    private readonly now: () => number = Date.now,
  ) {
    this.quotaDomainKey = requireText(ctx.id.name ?? ctx.id.toString(), "provider quota domain key");
    initializeCellProtocol(ctx.storage, { type: "provider-quota", key: this.quotaDomainKey }, [providerQuotaMigration]);
  }

  public async configurePolicy(input: ConfigureProviderQuotaPolicyInput): Promise<{ version: number; policy: ProviderQuotaPolicy }> {
    validateText(input.operationId, "operationId");
    requireVersion(input.expectedVersion, "expectedVersion", true);
    const policy = validatePolicy(input.policy);
    const command: CellOperationCommand = {
      operationId: input.operationId,
      operationType: "provider-quota.configure-policy",
      resourceId: this.quotaDomainKey,
      tenantId: "provider-platform",
      payload: { expectedVersion: input.expectedVersion, policy },
      aggregateType: "provider-quota-policy",
      aggregateId: this.quotaDomainKey,
      expectedVersion: input.expectedVersion,
    };
    const result = await executeCellOperation(this.ctx.storage, command, (sql) => {
      const currentVersion = readPolicyVersion(sql);
      if (currentVersion !== input.expectedVersion) {
        throw new ProviderQuotaError("PROVIDER_QUOTA_POLICY_VERSION_CONFLICT", `Provider quota policy is at version ${currentVersion}; expected ${input.expectedVersion}.`);
      }
      const version = currentVersion + 1;
      sql.exec(
        `INSERT INTO provider_quota_policy_versions (version, requests_per_minute, tokens_per_second, grant_ttl_ms, operation_id)
         VALUES (?, ?, ?, ?, ?)`,
        version, policy.requestsPerMinute, policy.tokensPerSecond, policy.grantTtlMs, input.operationId,
      );
      if (currentVersion === 0) sql.exec("INSERT INTO provider_quota_policy_head (singleton, current_version) VALUES (?, ?)", 1, version);
      else sql.exec("UPDATE provider_quota_policy_head SET current_version = ? WHERE singleton = ?", version, 1);
      return { version, policy };
    });
    return result.result;
  }

  public getPolicy(version?: number): { version: number; policy: ProviderQuotaPolicy } | null {
    const policyVersion = version ?? readPolicyVersion(this.ctx.storage.sql);
    if (policyVersion === 0) return null;
    requireVersion(policyVersion, "version", false);
    const row = this.ctx.storage.sql
      .exec<PolicyVersionRow>("SELECT * FROM provider_quota_policy_versions WHERE version = ?", policyVersion)
      .toArray()[0];
    if (!row) return null;
    return {
      version: Number(row.version),
      policy: {
        requestsPerMinute: Number(row.requests_per_minute),
        tokensPerSecond: Number(row.tokens_per_second),
        grantTtlMs: Number(row.grant_ttl_ms),
      },
    };
  }

  public async acquireGrant(input: AcquireProviderQuotaGrantInput): Promise<ProviderQuotaGrant> {
    const normalized = validateGrantRequest(input);
    const digest = (await digestJson(normalized)).sha256;
    const now = this.readNow();
    const decision = withCellTransaction(this.ctx.storage, () => {
      const existing = this.ctx.storage.sql
        .exec<GrantRow>("SELECT * FROM provider_quota_grants WHERE request_id = ?", normalized.requestId)
        .toArray()[0];
      if (existing) {
        if (existing.request_digest !== digest) {
          throw new ProviderQuotaError("PROVIDER_QUOTA_GRANT_CONFLICT", `Quota request ${normalized.requestId} was already used with different content.`);
        }
        return { grant: grantFromRow(existing), denied: existing.status === "DENIED" };
      }

      const currentPolicy = this.getPolicy();
      if (!currentPolicy) throw new ProviderQuotaError("PROVIDER_QUOTA_POLICY_MISSING", "Configure provider quota limits before issuing grants.", 503);
      const policy = currentPolicy.policy;
      const rpmWindowStart = Math.floor(now / 60_000) * 60_000;
      const tpsWindowStart = Math.floor(now / 1000) * 1000;
      const rpmRows = this.ctx.storage.sql
        .exec<RateGrantRow>("SELECT requests FROM provider_quota_grants WHERE status <> ? AND created_at > ?", "DENIED", now - 60_000)
        .toArray();
      const rpmUsed = rpmRows.reduce((sum, row) => sum + Number(row.requests), 0);
      const tpsUsed = readTpsUsage(this.ctx.storage.sql, now);
      const denialReason = normalized.requests > policy.requestsPerMinute || normalized.tokensPerSecond > policy.tokensPerSecond
        ? "The requested grant exceeds the provider's configured rate limits."
        : rpmUsed + normalized.requests > policy.requestsPerMinute || tpsUsed + normalized.tokensPerSecond > policy.tokensPerSecond
          ? "Provider quota is already allocated for this rate window."
          : null;
      const grantId = denialReason ? `denied:${normalized.requestId}` : `grant:${normalized.requestId}`;
      const expiresAt = Math.min(now + policy.grantTtlMs, rpmWindowStart + 60_000, tpsWindowStart + 1000);
      if (denialReason) {
        this.ctx.storage.sql.exec(
          `INSERT INTO provider_quota_grants (
            grant_id, request_id, request_digest, tenant_id, provider, route_version, requests,
            tokens_per_second, policy_version, rpm_window_start, tps_window_start, expires_at,
            status, usage_json, unknown_reason, denial_reason, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          grantId, normalized.requestId, digest, normalized.tenantId, normalized.provider, normalized.routeVersion,
          normalized.requests, normalized.tokensPerSecond, currentPolicy.version, rpmWindowStart, tpsWindowStart,
          expiresAt, "DENIED", null, null, denialReason, now, now,
        );
        return {
          grant: {
            grantId,
            requestId: normalized.requestId,
            tenantId: normalized.tenantId,
            provider: normalized.provider,
            routeVersion: normalized.routeVersion,
            requests: normalized.requests,
            tokensPerSecond: normalized.tokensPerSecond,
            policyVersion: currentPolicy.version,
            expiresAt,
            status: "DENIED" as const,
            denialReason,
          },
          denied: true,
        };
      }
      this.ctx.storage.sql.exec(
        `INSERT INTO provider_quota_grants (
          grant_id, request_id, request_digest, tenant_id, provider, route_version, requests,
          tokens_per_second, policy_version, rpm_window_start, tps_window_start, expires_at,
          status, usage_json, unknown_reason, denial_reason, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        grantId, normalized.requestId, digest, normalized.tenantId, normalized.provider, normalized.routeVersion,
        normalized.requests, normalized.tokensPerSecond, currentPolicy.version, rpmWindowStart, tpsWindowStart,
        expiresAt, "GRANTED", null, null, null, now, now,
      );
      return { grant: {
          grantId,
          requestId: normalized.requestId,
          tenantId: normalized.tenantId,
          provider: normalized.provider,
          routeVersion: normalized.routeVersion,
          requests: normalized.requests,
          tokensPerSecond: normalized.tokensPerSecond,
          policyVersion: currentPolicy.version,
          expiresAt,
          status: "GRANTED" as const,
        },
        denied: false,
      };
    });
    await confirmCellWrite(this.ctx.storage);
    if (decision.denied) {
      throw new ProviderQuotaError("PROVIDER_QUOTA_EXCEEDED", decision.grant.denialReason ?? "Provider quota is already allocated for this request.", 429);
    }
    return decision.grant;
  }

  public async beginCall(grantId: string, requestId: string): Promise<BeginProviderCallResult> {
    validateText(grantId, "grantId");
    validateText(requestId, "requestId");
    const now = this.readNow();
    const outcome = withCellTransaction(this.ctx.storage, () => {
      const row = requireGrant(this.ctx.storage.sql, grantId, requestId);
      if (row.status === "DENIED") throw new ProviderQuotaError("PROVIDER_QUOTA_EXCEEDED", row.denial_reason ?? "Provider quota request was denied.", 429);
      if (row.status === "COMPLETED") return { state: "COMPLETED" as const, usage: parseUsage(row.usage_json) };
      if (row.status === "STARTED" || row.status === "UNKNOWN") return { state: "OUTCOME_UNKNOWN" as const };
      if (Number(row.expires_at) <= now) return { state: "EXPIRED" as const };
      this.ctx.storage.sql.exec("UPDATE provider_quota_grants SET status = ?, updated_at = ? WHERE grant_id = ?", "STARTED", now, grantId);
      return { state: "STARTED" as const };
    });
    if (outcome.state === "STARTED") await confirmCellWrite(this.ctx.storage);
    return outcome;
  }

  public async completeCall(grantId: string, requestId: string, usage: ProviderUsageEvidence): Promise<ProviderQuotaGrant> {
    validateText(grantId, "grantId");
    validateText(requestId, "requestId");
    const normalized = validateUsage(usage);
    const now = this.readNow();
    const grant = withCellTransaction(this.ctx.storage, () => {
      const row = requireGrant(this.ctx.storage.sql, grantId, requestId);
      if (row.status === "COMPLETED") {
        if (!row.usage_json || JSON.stringify(validateUsage(JSON.parse(row.usage_json) as ProviderUsageEvidence)) !== JSON.stringify(normalized)) {
          throw new ProviderQuotaError("PROVIDER_QUOTA_COMPLETION_CONFLICT", `Grant ${grantId} already has different usage evidence.`);
        }
        return grantFromRow(row);
      }
      if (row.status !== "STARTED" && row.status !== "UNKNOWN") {
        throw new ProviderQuotaError("PROVIDER_QUOTA_CALL_NOT_STARTED", `Grant ${grantId} was not started.`);
      }
      this.ctx.storage.sql.exec(
        "UPDATE provider_quota_grants SET status = ?, usage_json = ?, unknown_reason = ?, updated_at = ? WHERE grant_id = ?",
        "COMPLETED", JSON.stringify(normalized), null, now, grantId,
      );
      return { ...grantFromRow(row), status: "COMPLETED" as const, usage: normalized, unknownReason: undefined };
    });
    await confirmCellWrite(this.ctx.storage);
    return grant;
  }

  public async markUnknown(grantId: string, requestId: string, reason: string): Promise<ProviderQuotaGrant> {
    validateText(grantId, "grantId");
    validateText(requestId, "requestId");
    const unknownReason = requireText(reason, "reason");
    const now = this.readNow();
    const result = withCellTransaction(this.ctx.storage, () => {
      const row = requireGrant(this.ctx.storage.sql, grantId, requestId);
      if (row.status === "COMPLETED") return grantFromRow(row);
      if (row.status === "UNKNOWN") return grantFromRow(row);
      if (row.status !== "STARTED") throw new ProviderQuotaError("PROVIDER_QUOTA_CALL_NOT_STARTED", `Grant ${grantId} has not entered provider execution.`);
      this.ctx.storage.sql.exec(
        "UPDATE provider_quota_grants SET status = ?, unknown_reason = ?, updated_at = ? WHERE grant_id = ?",
        "UNKNOWN", unknownReason, now, grantId,
      );
      return { ...grantFromRow(row), status: "UNKNOWN" as const, unknownReason };
    });
    if (result.status !== "COMPLETED") await confirmCellWrite(this.ctx.storage);
    return result;
  }

  public getGrant(grantId: string): ProviderQuotaGrant | null {
    validateText(grantId, "grantId");
    const row = this.ctx.storage.sql
      .exec<GrantRow>("SELECT * FROM provider_quota_grants WHERE grant_id = ?", grantId)
      .toArray()[0];
    return row ? grantFromRow(row) : null;
  }

  public getGrantByRequestId(requestId: string): ProviderQuotaGrant | null {
    validateText(requestId, "requestId");
    const row = this.ctx.storage.sql
      .exec<GrantRow>("SELECT * FROM provider_quota_grants WHERE request_id = ?", requestId)
      .toArray()[0];
    return row ? grantFromRow(row) : null;
  }

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (request.method === "PUT" && url.pathname === "/provider-quota/policy") {
        const body = (await request.json()) as ConfigureProviderQuotaPolicyInput;
        return json(await this.configurePolicy(body), 201);
      }
      if (request.method === "POST" && url.pathname === "/provider-quota/grants") {
        return json(await this.acquireGrant((await request.json()) as AcquireProviderQuotaGrantInput), 201);
      }
      const grantMatch = url.pathname.match(/^\/provider-quota\/grants\/([^/]+)$/);
      if (request.method === "GET" && grantMatch) {
        const grant = this.getGrant(decodeURIComponent(grantMatch[1]));
        return grant ? json(grant) : json({ error: "PROVIDER_QUOTA_GRANT_NOT_FOUND" }, 404);
      }
      if (request.method === "GET" && url.pathname === "/provider-quota/grants") {
        const requestId = url.searchParams.get("request_id");
        if (!requestId) return json({ error: "REQUEST_ID_REQUIRED" }, 400);
        const grant = this.getGrantByRequestId(requestId);
        return grant ? json(grant) : json({ error: "PROVIDER_QUOTA_GRANT_NOT_FOUND" }, 404);
      }
      const startMatch = url.pathname.match(/^\/provider-quota\/grants\/([^/]+)\/start$/);
      if (request.method === "POST" && startMatch) {
        const body = (await request.json()) as { requestId: string };
        return json(await this.beginCall(decodeURIComponent(startMatch[1]), body.requestId));
      }
      const completeMatch = url.pathname.match(/^\/provider-quota\/grants\/([^/]+)\/complete$/);
      if (request.method === "POST" && completeMatch) {
        const body = (await request.json()) as { requestId: string; usage: ProviderUsageEvidence };
        return json(await this.completeCall(decodeURIComponent(completeMatch[1]), body.requestId, body.usage));
      }
      const unknownMatch = url.pathname.match(/^\/provider-quota\/grants\/([^/]+)\/unknown$/);
      if (request.method === "POST" && unknownMatch) {
        const body = (await request.json()) as { requestId: string; reason: string };
        return json(await this.markUnknown(decodeURIComponent(unknownMatch[1]), body.requestId, body.reason));
      }
      if (request.method === "GET" && url.pathname === "/provider-quota/policy") {
        const version = url.searchParams.has("version") ? Number(url.searchParams.get("version")) : undefined;
        const policy = this.getPolicy(version);
        return policy ? json(policy) : json({ error: "PROVIDER_QUOTA_POLICY_NOT_FOUND" }, 404);
      }
      return json({ error: "NotFound", path: url.pathname }, 404);
    } catch (error) {
      if (error instanceof ProviderQuotaError) return json({ error: error.code, message: error.message }, error.status);
      if (error instanceof TypeError) return json({ error: "INVALID_PROVIDER_QUOTA_REQUEST", message: error.message }, 400);
      if (error && typeof error === "object" && "code" in error && (error as { code: string }).code === "CELL_VERSION_CONFLICT") {
        return json({ error: "PROVIDER_QUOTA_POLICY_VERSION_CONFLICT", message: error instanceof Error ? error.message : String(error) }, 409);
      }
      throw error;
    }
  }

  private readNow(): number {
    const now = this.now();
    if (!Number.isSafeInteger(now) || now < 0) throw new TypeError("clock must return a non-negative safe integer timestamp.");
    return now;
  }
}

export function providerQuotaCellKey(provider: string, quotaDomain: string): string {
  // Use an opaque credential-domain label here; never pass the credential secret itself as a Cell name.
  return `${encodeURIComponent(requireText(provider, "provider"))}:${encodeURIComponent(requireText(quotaDomain, "quotaDomain"))}`;
}

function validateGrantRequest(input: AcquireProviderQuotaGrantInput): AcquireProviderQuotaGrantInput {
  if (!input || typeof input !== "object") throw new TypeError("grant request must be an object.");
  const normalized = {
    requestId: requireText(input.requestId, "requestId"),
    tenantId: requireText(input.tenantId, "tenantId"),
    provider: requireText(input.provider, "provider"),
    routeVersion: requirePositiveInteger(input.routeVersion, "routeVersion"),
    requests: requirePositiveInteger(input.requests, "requests"),
    tokensPerSecond: requirePositiveInteger(input.tokensPerSecond, "tokensPerSecond"),
  };
  if (normalized.requests !== 1) throw new TypeError("Each provider call must receive exactly one request grant.");
  return normalized;
}

function validatePolicy(policy: ProviderQuotaPolicy): ProviderQuotaPolicy {
  if (!policy || typeof policy !== "object") throw new TypeError("policy must be an object.");
  return {
    requestsPerMinute: requirePositiveInteger(policy.requestsPerMinute, "requestsPerMinute"),
    tokensPerSecond: requirePositiveInteger(policy.tokensPerSecond, "tokensPerSecond"),
    grantTtlMs: requirePositiveInteger(policy.grantTtlMs, "grantTtlMs"),
  };
}

function validateUsage(usage: ProviderUsageEvidence): ProviderUsageEvidence {
  if (!usage || typeof usage !== "object" || usage.verified !== true) throw new TypeError("Provider usage must be marked verified.");
  return {
    verified: true,
    reference: requireText(usage.reference, "usage.reference"),
    inputTokens: requireNonNegativeInteger(usage.inputTokens, "usage.inputTokens"),
    outputTokens: requireNonNegativeInteger(usage.outputTokens, "usage.outputTokens"),
  };
}

function parseUsage(value: string | null): ProviderUsageEvidence {
  if (!value) throw new ProviderQuotaError("PROVIDER_QUOTA_USAGE_MISSING", "Completed grant has no recorded usage evidence.", 500);
  return validateUsage(JSON.parse(value) as ProviderUsageEvidence);
}

function grantFromRow(row: GrantRow): ProviderQuotaGrant {
  return {
    grantId: row.grant_id,
    requestId: row.request_id,
    tenantId: row.tenant_id,
    provider: row.provider,
    routeVersion: Number(row.route_version),
    requests: Number(row.requests),
    tokensPerSecond: Number(row.tokens_per_second),
    policyVersion: Number(row.policy_version),
    expiresAt: Number(row.expires_at),
    status: row.status,
    usage: row.usage_json ? parseUsage(row.usage_json) : undefined,
    unknownReason: row.unknown_reason ?? undefined,
    denialReason: row.denial_reason ?? undefined,
  };
}

function requireGrant(sql: PersistenceSqlStorage, grantId: string, requestId: string): GrantRow {
  const row = sql.exec<GrantRow>("SELECT * FROM provider_quota_grants WHERE grant_id = ?", grantId).toArray()[0];
  if (!row) throw new ProviderQuotaError("PROVIDER_QUOTA_GRANT_NOT_FOUND", `Provider quota grant ${grantId} was not found.`, 404);
  if (row.request_id !== requestId) throw new ProviderQuotaError("PROVIDER_QUOTA_GRANT_CONFLICT", `Grant ${grantId} does not belong to request ${requestId}.`);
  return row;
}

function readPolicyVersion(sql: PersistenceSqlStorage): number {
  const row = sql.exec<PolicyHeadRow>("SELECT current_version FROM provider_quota_policy_head WHERE singleton = ?", 1).toArray()[0];
  return row ? Number(row.current_version) : 0;
}

function readTpsUsage(sql: PersistenceSqlStorage, now: number): number {
  const pending = sql.exec<RateGrantRow>(
    "SELECT tokens_per_second FROM provider_quota_grants WHERE status = ? AND expires_at > ?",
    "GRANTED", now,
  ).toArray();
  const active = sql.exec<RateGrantRow>(
    "SELECT tokens_per_second FROM provider_quota_grants WHERE status = ?",
    "STARTED",
  ).toArray();
  const unknown = sql.exec<RateGrantRow>(
    "SELECT tokens_per_second FROM provider_quota_grants WHERE status = ?",
    "UNKNOWN",
  ).toArray();
  const recentlyCompleted = sql.exec<RateGrantRow>(
    "SELECT tokens_per_second FROM provider_quota_grants WHERE status = ? AND updated_at > ?",
    "COMPLETED", now - 1000,
  ).toArray();
  return [...pending, ...active, ...unknown, ...recentlyCompleted]
    .reduce((sum, row) => sum + Number(row.tokens_per_second), 0);
}

function requireVersion(value: number, label: string, allowZero: boolean): void {
  if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1)) throw new TypeError(`${label} must be a ${allowZero ? "non-negative" : "positive"} safe integer.`);
}

function requirePositiveInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) <= 0) throw new TypeError(`${label} must be a positive safe integer.`);
  return Number(value);
}

function requireNonNegativeInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) throw new TypeError(`${label} must be a non-negative safe integer.`);
  return Number(value);
}

function requireText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0 || /[\u0000-\u001f\u007f]/.test(value)) throw new TypeError(`${label} must be a non-empty string without control characters.`);
  return value.trim();
}

function validateText(value: string, label: string): void {
  requireText(value, label);
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}
