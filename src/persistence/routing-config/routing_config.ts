import { CellOperationCommand, PersistenceSqlStorage } from "../contracts";
import { executeCellOperation } from "../operations";
import { initializeCellProtocol } from "../schema";
import { confirmCellWrite, withCellTransaction } from "../transactions";
import { DurableObjectState, CloudflareCelldEnv } from "../../cloudflare/env";

export interface ModelPricing {
  priceVersion: string;
  currency: string;
  inputMinorPerMillionTokens: number;
  outputMinorPerMillionTokens: number;
}

export interface ModelRouteConfig {
  poolName: string;
  provider: string;
  modelId: string;
  quotaDomain: string;
  quotaDemandTokensPerSecond: number;
  maxInputTokens?: number;
  maxOutputTokens?: number;
  pricing?: ModelPricing;
}

export interface RoutingConfiguration {
  tenantId: string;
  policyVersion: string;
  routes: ModelRouteConfig[];
}

export interface RoutingConfigSnapshot extends RoutingConfiguration {
  version: number;
  publishedAt: number;
}

export interface PublishRoutingConfigInput {
  operationId: string;
  expectedVersion: number;
  configuration: RoutingConfiguration;
  publishedAt?: number;
}

export interface RoutingCallPin {
  callId: string;
  poolName: string;
  routingVersion: number;
  priceVersion: string | null;
  route: ModelRouteConfig;
}

export interface PinRoutingCallInput {
  callId: string;
  poolName: string;
  routingVersion?: number;
}

interface RoutingVersionRow {
  version: number;
  configuration_json: string;
  published_at: number;
}

interface RoutingHeadRow {
  current_version: number;
}

interface RoutingCallPinRow {
  call_id: string;
  pool_name: string;
  routing_version: number;
  price_version: string | null;
}

export class RoutingConfigError extends Error {
  constructor(public readonly code: string, message: string, public readonly status = 409) {
    super(message);
    this.name = "RoutingConfigError";
  }
}

export const routingConfigMigration = {
  version: 2,
  description: "immutable routing, policy and price configuration versions",
  up(sql: PersistenceSqlStorage): void {
    sql.exec(`
      CREATE TABLE IF NOT EXISTS routing_config_versions (
        version INTEGER PRIMARY KEY,
        configuration_json TEXT NOT NULL,
        published_at INTEGER NOT NULL,
        operation_id TEXT NOT NULL UNIQUE
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS routing_config_head (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        current_version INTEGER NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS routing_config_call_pins (
        call_id TEXT PRIMARY KEY,
        pool_name TEXT NOT NULL,
        routing_version INTEGER NOT NULL,
        price_version TEXT,
        pinned_at INTEGER NOT NULL
      )
    `);
  },
};

/** A versioned configuration authority. One Cell name owns one tenant's route history. */
export class RoutingConfigCell {
  private readonly tenantId: string;

  constructor(
    public readonly ctx: DurableObjectState,
    public readonly env: CloudflareCelldEnv,
    private readonly now: () => number = Date.now,
  ) {
    this.tenantId = requireText(ctx.id.name ?? ctx.id.toString(), "routing config key");
    initializeCellProtocol(ctx.storage.sql, { type: "routing-config", key: this.tenantId }, [routingConfigMigration]);
  }

  public async publish(input: PublishRoutingConfigInput): Promise<RoutingConfigSnapshot> {
    validateText(input.operationId, "operationId");
    validateVersion(input.expectedVersion, "expectedVersion");
    const configuration = validateConfiguration(input.configuration, this.tenantId);
    const command: CellOperationCommand = {
      operationId: input.operationId,
      operationType: "routing-config.publish",
      resourceId: this.tenantId,
      tenantId: this.tenantId,
      payload: { expectedVersion: input.expectedVersion, configuration },
      aggregateType: "routing-config",
      aggregateId: this.tenantId,
      expectedVersion: input.expectedVersion,
    };

    const result = await executeCellOperation(this.ctx.storage, command, (sql) => {
      const current = readRoutingHead(sql);
      if (current !== input.expectedVersion) {
        throw new RoutingConfigError("ROUTING_CONFIG_VERSION_CONFLICT", `Routing config is at version ${current}; expected ${input.expectedVersion}.`);
      }
      const version = current + 1;
      const publishedAt = input.publishedAt ?? this.now();
      validateTimestamp(publishedAt);
      const snapshot: RoutingConfigSnapshot = { ...configuration, version, publishedAt };
      sql.exec(
        "INSERT INTO routing_config_versions (version, configuration_json, published_at, operation_id) VALUES (?, ?, ?, ?)",
        version,
        JSON.stringify(configuration),
        publishedAt,
        input.operationId,
      );
      if (current === 0) sql.exec("INSERT INTO routing_config_head (singleton, current_version) VALUES (?, ?)", 1, version);
      else sql.exec("UPDATE routing_config_head SET current_version = ? WHERE singleton = ?", version, 1);
      return snapshot;
    });
    return result.result;
  }

  public getCurrent(): RoutingConfigSnapshot | null {
    const current = readRoutingHead(this.ctx.storage.sql);
    return current === 0 ? null : this.getVersion(current);
  }

  public getVersion(version: number): RoutingConfigSnapshot | null {
    validateVersion(version, "version");
    const row = this.ctx.storage.sql
      .exec<RoutingVersionRow>("SELECT * FROM routing_config_versions WHERE version = ?", version)
      .toArray()[0];
    if (!row) return null;
    return {
      ...(JSON.parse(row.configuration_json) as RoutingConfiguration),
      version: Number(row.version),
      publishedAt: Number(row.published_at),
    };
  }

  public listVersions(fromVersion = 1, limit = 100): RoutingConfigSnapshot[] {
    validateVersion(fromVersion, "fromVersion");
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new TypeError("limit must be between 1 and 1000.");
    return this.ctx.storage.sql
      .exec<RoutingVersionRow>("SELECT * FROM routing_config_versions WHERE version >= ? ORDER BY version LIMIT ?", fromVersion, limit)
      .toArray()
      .map((row) => ({
        ...(JSON.parse(row.configuration_json) as RoutingConfiguration),
        version: Number(row.version),
        publishedAt: Number(row.published_at),
      }));
  }

  /** Persist the route snapshot for a stable provider-call identity before any external effect. */
  public async pinCall(input: PinRoutingCallInput): Promise<RoutingCallPin> {
    const callId = requireText(input.callId, "callId");
    const poolName = requireText(input.poolName, "poolName");
    if (input.routingVersion !== undefined) validateVersion(input.routingVersion, "routingVersion");
    const now = this.now();
    validateTimestamp(now);
    const pinned = withCellTransaction(this.ctx.storage.sql, () => {
      const existing = this.ctx.storage.sql
        .exec<RoutingCallPinRow>("SELECT * FROM routing_config_call_pins WHERE call_id = ?", callId)
        .toArray()[0];
      if (existing) {
        if (existing.pool_name !== poolName || (input.routingVersion !== undefined && Number(existing.routing_version) !== input.routingVersion)) {
          throw new RoutingConfigError("ROUTING_CALL_PIN_CONFLICT", `Call ${callId} is already pinned to another route version or pool.`);
        }
        return this.resolvePinnedRoute(existing);
      }
      const version = input.routingVersion ?? readRoutingHead(this.ctx.storage.sql);
      if (version === 0) throw new RoutingConfigError("ROUTING_CONFIG_NOT_FOUND", "Publish a routing configuration before pinning a provider call.", 404);
      const snapshot = this.getVersion(version);
      if (!snapshot) throw new RoutingConfigError("ROUTING_CONFIG_VERSION_NOT_FOUND", `Routing config version ${version} was not found.`, 404);
      const route = snapshot.routes.find((candidate) => candidate.poolName === poolName);
      if (!route) throw new RoutingConfigError("MODEL_ROUTE_NOT_FOUND", `No route is configured for model pool ${poolName}.`, 404);
      const pin: RoutingCallPin = {
        callId,
        poolName,
        routingVersion: version,
        priceVersion: route.pricing?.priceVersion ?? null,
        route,
      };
      this.ctx.storage.sql.exec(
        "INSERT INTO routing_config_call_pins (call_id, pool_name, routing_version, price_version, pinned_at) VALUES (?, ?, ?, ?, ?)",
        callId, poolName, version, pin.priceVersion, now,
      );
      return pin;
    });
    await confirmCellWrite(this.ctx.storage);
    return pinned;
  }

  public getCallPin(callId: string): RoutingCallPin | null {
    const row = this.ctx.storage.sql
      .exec<RoutingCallPinRow>("SELECT * FROM routing_config_call_pins WHERE call_id = ?", requireText(callId, "callId"))
      .toArray()[0];
    return row ? this.resolvePinnedRoute(row) : null;
  }

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    try {
      if (request.method === "POST" && url.pathname === "/routing-config/versions") {
        const body = (await request.json()) as PublishRoutingConfigInput;
        const snapshot = await this.publish(body);
        return json(snapshot, 201);
      }
      if (request.method === "GET" && url.pathname === "/routing-config/current") {
        const snapshot = this.getCurrent();
        return snapshot ? json(snapshot) : json({ error: "ROUTING_CONFIG_NOT_FOUND" }, 404);
      }
      const versionMatch = url.pathname.match(/^\/routing-config\/versions\/(\d+)$/);
      if (request.method === "GET" && versionMatch) {
        const snapshot = this.getVersion(Number(versionMatch[1]));
        return snapshot ? json(snapshot) : json({ error: "ROUTING_CONFIG_VERSION_NOT_FOUND" }, 404);
      }
      if (request.method === "GET" && url.pathname === "/routing-config/versions") {
        const from = Number(url.searchParams.get("from") ?? 1);
        const limit = Number(url.searchParams.get("limit") ?? 100);
        return json({ versions: this.listVersions(from, limit) });
      }
      if (request.method === "POST" && url.pathname === "/routing-config/call-pins") {
        return json(await this.pinCall((await request.json()) as PinRoutingCallInput), 201);
      }
      const pinMatch = url.pathname.match(/^\/routing-config\/call-pins\/([^/]+)$/);
      if (request.method === "GET" && pinMatch) {
        const pin = this.getCallPin(decodeURIComponent(pinMatch[1]));
        return pin ? json(pin) : json({ error: "ROUTING_CALL_PIN_NOT_FOUND" }, 404);
      }
      return json({ error: "NotFound", path: url.pathname }, 404);
    } catch (error) {
      if (error instanceof RoutingConfigError) return json({ error: error.code, message: error.message }, error.status);
      if (error instanceof TypeError) return json({ error: "INVALID_ROUTING_CONFIG", message: error.message }, 400);
      if (error && typeof error === "object" && "code" in error && (error as { code: string }).code === "CELL_VERSION_CONFLICT") {
        return json({ error: "ROUTING_CONFIG_VERSION_CONFLICT", message: error instanceof Error ? error.message : String(error) }, 409);
      }
      throw error;
    }
  }

  private resolvePinnedRoute(row: RoutingCallPinRow): RoutingCallPin {
    const snapshot = this.getVersion(Number(row.routing_version));
    const route = snapshot?.routes.find((candidate) => candidate.poolName === row.pool_name);
    if (!snapshot || !route) throw new RoutingConfigError("ROUTING_CALL_PIN_CORRUPT", `Pinned route ${row.pool_name}@${row.routing_version} is unavailable.`, 500);
    return {
      callId: row.call_id,
      poolName: row.pool_name,
      routingVersion: Number(row.routing_version),
      priceVersion: row.price_version,
      route,
    };
  }
}

export function routingConfigCellKey(tenantId: string): string {
  return requireText(tenantId, "tenantId");
}

export function validateRoutingConfiguration(configuration: RoutingConfiguration, tenantId: string): RoutingConfiguration {
  return validateConfiguration(configuration, tenantId);
}

function validateConfiguration(configuration: RoutingConfiguration, tenantId: string): RoutingConfiguration {
  if (!configuration || typeof configuration !== "object" || Array.isArray(configuration)) throw new TypeError("configuration must be an object.");
  const normalizedTenant = requireText(configuration.tenantId, "configuration.tenantId");
  if (normalizedTenant !== tenantId) throw new RoutingConfigError("ROUTING_CONFIG_TENANT_MISMATCH", "Configuration tenantId must match the routing Cell key.", 403);
  const policyVersion = requireText(configuration.policyVersion, "configuration.policyVersion");
  if (!Array.isArray(configuration.routes) || configuration.routes.length === 0 || configuration.routes.length > 100) {
    throw new TypeError("configuration.routes must contain between 1 and 100 routes.");
  }
  const names = new Set<string>();
  const routes = configuration.routes.map((route) => {
    if (!route || typeof route !== "object") throw new TypeError("Each route must be an object.");
    const poolName = requireText(route.poolName, "route.poolName");
    if (names.has(poolName)) throw new TypeError(`Duplicate route poolName: ${poolName}.`);
    names.add(poolName);
    const normalized: ModelRouteConfig = {
      poolName,
      provider: requireText(route.provider, "route.provider"),
      modelId: requireText(route.modelId, "route.modelId"),
      quotaDomain: requireText(route.quotaDomain, "route.quotaDomain"),
      quotaDemandTokensPerSecond: requirePositiveInteger(route.quotaDemandTokensPerSecond, "route.quotaDemandTokensPerSecond"),
    };
    if (route.maxInputTokens !== undefined) normalized.maxInputTokens = requirePositiveInteger(route.maxInputTokens, "route.maxInputTokens");
    if (route.maxOutputTokens !== undefined) normalized.maxOutputTokens = requirePositiveInteger(route.maxOutputTokens, "route.maxOutputTokens");
    if (route.pricing !== undefined) {
      const pricing = route.pricing;
      normalized.pricing = {
        priceVersion: requireText(pricing.priceVersion, "route.pricing.priceVersion"),
        currency: requireCurrency(pricing.currency),
        inputMinorPerMillionTokens: requireMinorRate(pricing.inputMinorPerMillionTokens, "route.pricing.inputMinorPerMillionTokens"),
        outputMinorPerMillionTokens: requireMinorRate(pricing.outputMinorPerMillionTokens, "route.pricing.outputMinorPerMillionTokens"),
      };
    }
    return normalized;
  });
  return { tenantId: normalizedTenant, policyVersion, routes };
}

function readRoutingHead(sql: PersistenceSqlStorage): number {
  const row = sql.exec<RoutingHeadRow>("SELECT current_version FROM routing_config_head WHERE singleton = ?", 1).toArray()[0];
  return row ? Number(row.current_version) : 0;
}

function requireText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0 || /[\u0000-\u001f\u007f]/.test(value)) throw new TypeError(`${label} must be a non-empty string without control characters.`);
  return value.trim();
}

function validateText(value: string, label: string): void {
  requireText(value, label);
}

function validateVersion(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0 || (label === "version" && value === 0)) throw new TypeError(`${label} must be a positive safe integer.`);
}

function validateTimestamp(value: number): void {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError("publishedAt must be a non-negative safe integer timestamp.");
}

function requirePositiveInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) <= 0) throw new TypeError(`${label} must be a positive safe integer.`);
  return Number(value);
}

function requireMinorRate(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) throw new TypeError(`${label} must be a non-negative safe integer.`);
  return Number(value);
}

function requireCurrency(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Z]{3}$/.test(value)) throw new TypeError("currency must be a three-letter uppercase code.");
  return value;
}

function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { "content-type": "application/json" } });
}
