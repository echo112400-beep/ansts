import { PersistenceCellNamespace } from "../persistence/contracts";
import { providerQuotaCellKey } from "../persistence/provider-quota";
import { routingConfigCellKey, RoutingCallPin, RoutingConfigSnapshot } from "../persistence/routing-config";
import {
  BeginProviderCallResult,
  ProviderQuotaGrant,
  ProviderUsageEvidence,
} from "../persistence/provider-quota";
import {
  ProviderQuotaAuthority,
  RoutingConfigAuthority,
  TenantBudgetAuthority,
} from "./provider_call";

/** Thin RPC clients for the three durable owners used by MeteredProviderCallExecutor. */
export class RoutingConfigCellClient implements RoutingConfigAuthority {
  private readonly stub;

  constructor(namespace: PersistenceCellNamespace, tenantId: string) {
    const key = routingConfigCellKey(tenantId);
    this.stub = namespace.get(namespace.idFromName(key));
  }

  public async getCurrent(tenantId: string): Promise<RoutingConfigSnapshot | null> {
    const response = await this.stub.fetch(cellRequest("/routing-config/current"));
    if (response.status === 404) return null;
    const body = await readBody(response);
    assertTenant(body.tenantId, tenantId);
    return body as unknown as RoutingConfigSnapshot;
  }

  public async getVersion(tenantId: string, version: number): Promise<RoutingConfigSnapshot | null> {
    const response = await this.stub.fetch(cellRequest(`/routing-config/versions/${encodeURIComponent(String(version))}`));
    if (response.status === 404) return null;
    const body = await readBody(response);
    assertTenant(body.tenantId, tenantId);
    return body as unknown as RoutingConfigSnapshot;
  }

  public async pinCall(tenantId: string, callId: string, poolName: string, routingVersion?: number): Promise<RoutingCallPin> {
    const body = await readBody(await this.stub.fetch(cellRequest("/routing-config/call-pins", "POST", {
      callId,
      poolName,
      routingVersion,
    })));
    return body as unknown as RoutingCallPin;
  }
}

export class ProviderQuotaCellClient implements ProviderQuotaAuthority {
  private readonly stub;

  constructor(namespace: PersistenceCellNamespace, provider: string, quotaDomain: string) {
    const key = providerQuotaCellKey(provider, quotaDomain);
    this.stub = namespace.get(namespace.idFromName(key));
  }

  public async configurePolicy(input: { operationId: string; expectedVersion: number; policy: { requestsPerMinute: number; tokensPerSecond: number; grantTtlMs: number } }): Promise<unknown> {
    return readBody(await this.stub.fetch(cellRequest("/provider-quota/policy", "PUT", input)));
  }

  public async acquireGrant(input: Parameters<ProviderQuotaAuthority["acquireGrant"]>[0]): Promise<ProviderQuotaGrant> {
    return (await readBody(await this.stub.fetch(cellRequest("/provider-quota/grants", "POST", input)))) as unknown as ProviderQuotaGrant;
  }

  public async getGrantByRequestId(requestId: string): Promise<ProviderQuotaGrant | null> {
    const response = await this.stub.fetch(cellRequest(`/provider-quota/grants?request_id=${encodeURIComponent(requestId)}`));
    if (response.status === 404) return null;
    return (await readBody(response)) as unknown as ProviderQuotaGrant;
  }

  public async beginCall(grantId: string, requestId: string): Promise<BeginProviderCallResult> {
    return (await readBody(await this.stub.fetch(cellRequest(`/provider-quota/grants/${encodeURIComponent(grantId)}/start`, "POST", { requestId })))) as unknown as BeginProviderCallResult;
  }

  public async completeCall(grantId: string, requestId: string, usage: ProviderUsageEvidence): Promise<ProviderQuotaGrant> {
    return (await readBody(await this.stub.fetch(cellRequest(`/provider-quota/grants/${encodeURIComponent(grantId)}/complete`, "POST", { requestId, usage })))) as unknown as ProviderQuotaGrant;
  }

  public async markUnknown(grantId: string, requestId: string, reason: string): Promise<ProviderQuotaGrant> {
    return (await readBody(await this.stub.fetch(cellRequest(`/provider-quota/grants/${encodeURIComponent(grantId)}/unknown`, "POST", { requestId, reason })))) as unknown as ProviderQuotaGrant;
  }
}

export class ProviderQuotaCellResolver {
  private readonly clients = new Map<string, ProviderQuotaCellClient>();

  constructor(private readonly namespace: PersistenceCellNamespace) {}

  public forRoute(route: { provider: string; quotaDomain: string }): ProviderQuotaCellClient {
    const key = providerQuotaCellKey(route.provider, route.quotaDomain);
    let client = this.clients.get(key);
    if (!client) {
      client = new ProviderQuotaCellClient(this.namespace, route.provider, route.quotaDomain);
      this.clients.set(key, client);
    }
    return client;
  }
}

/** Adapts the TenantLedger's existing HTTP protocol and verifies the period price before creating a hold. */
export class TenantLedgerBudgetAuthorityClient implements TenantBudgetAuthority {
  private readonly stub;

  constructor(namespace: PersistenceCellNamespace, private readonly tenantId: string) {
    this.stub = namespace.get(namespace.idFromName(tenantId));
  }

  public async reserve(input: Parameters<TenantBudgetAuthority["reserve"]>[0]): Promise<Awaited<ReturnType<TenantBudgetAuthority["reserve"]>>> {
    const periodPath = `/budget-periods/${encodeURIComponent(input.periodKey)}?tenant_id=${encodeURIComponent(input.tenantId)}`;
    const period = await readBody(await this.stub.fetch(cellRequest(periodPath)));
    if (input.tenantId !== this.tenantId) throw new CellAuthorityError("TENANT_LEDGER_TENANT_MISMATCH", "Budget request tenant does not match the TenantLedger Cell.", 403);
    if (period.currency !== input.currency || period.price_version !== input.priceVersion) {
      throw new CellAuthorityError("BUDGET_PRICE_VERSION_MISMATCH", "Tenant billing period currency and price version must match the selected immutable route price before reserving.", 409);
    }
    const response = await readBody(await this.stub.fetch(cellRequest("/budget-holds", "POST", {
      tenant_id: input.tenantId,
      period_key: input.periodKey,
      task_id: input.taskId,
      hold_id: input.holdId,
      amount_minor: input.amountMinor,
      cost_bound_minor: input.costBoundMinor,
      currency: input.currency,
    })));
    return {
      holdId: String(response.hold_id),
      amountMinor: Number(response.amount_minor),
      currency: String(response.currency),
      priceVersion: String(response.price_version),
      status: String(response.status),
    };
  }

  public async settle(input: Parameters<TenantBudgetAuthority["settle"]>[0]): Promise<unknown> {
    const usage = parseUsageReference(input.usageReference);
    const submit = async (settlementId: string) => readBody(await this.stub.fetch(cellRequest(`/budget-holds/${encodeURIComponent(input.holdId)}/settlements`, "POST", {
      tenant_id: this.tenantId,
      settlement_id: settlementId,
      actual_minor: input.actualMinor,
      currency: input.currency,
      usage_reference: JSON.stringify(usage),
    })));
    try { return await submit(input.settlementId); }
    catch (error) {
      if (!(error instanceof CellAuthorityError) || error.code !== "BUDGET_HOLD_ALREADY_SETTLED") throw error;
      if (!input.periodKey) throw settlementReconciliationRequired();
      const query = `/budget-holds?tenant_id=${encodeURIComponent(this.tenantId)}&period_key=${encodeURIComponent(input.periodKey)}&status=SETTLED`;
      const ledger = await readBody(await this.stub.fetch(cellRequest(query)));
      if (ledger.tenant_id !== this.tenantId || ledger.period_key !== input.periodKey || !Array.isArray(ledger.holds)) {
        throw settlementReconciliationRequired();
      }
      const hold = ledger.holds.find((candidate: Record<string, unknown>) => candidate.hold_id === input.holdId);
      if (!hold || hold.tenant_id !== this.tenantId || hold.period_key !== input.periodKey || hold.status !== "SETTLED"
        || hold.currency !== input.currency || typeof hold.settlement_id !== "string" || !hold.settlement_id.trim()
        || hold.settlement_id === input.settlementId) throw settlementReconciliationRequired();
      // Reuse only the owner's recorded identity. The immutable operation replay
      // checks actual amount, currency, hold and usage evidence before returning.
      return submit(hold.settlement_id);
    }
  }

  public async markUnknown(input: Parameters<TenantBudgetAuthority["markUnknown"]>[0]): Promise<unknown> {
    return readBody(await this.stub.fetch(cellRequest(`/budget-holds/${encodeURIComponent(input.holdId)}/unknown`, "POST", {
      tenant_id: this.tenantId,
      reason: input.reason,
    })));
  }
}

function settlementReconciliationRequired(): CellAuthorityError {
  return new CellAuthorityError("BUDGET_SETTLEMENT_RECONCILIATION_REQUIRED",
    "The hold is settled but its original settlement identity cannot be verified; reconcile the recorded evidence before retrying.", 409);
}

export class CellAuthorityError extends Error {
  constructor(public readonly code: string, message: string, public readonly status: number) {
    super(message);
    this.name = "CellAuthorityError";
  }
}

function cellRequest(path: string, method = "GET", body?: unknown): Request {
  return new Request(`https://celld.internal${path}`, {
    method,
    headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

async function readBody(response: Response): Promise<Record<string, unknown>> {
  let body: Record<string, unknown>;
  try {
    body = await response.json() as Record<string, unknown>;
  } catch {
    throw new CellAuthorityError("CELL_AUTHORITY_INVALID_RESPONSE", "A durable authority returned invalid JSON.", 502);
  }
  if (!response.ok) {
    throw new CellAuthorityError(
      typeof body.error === "string" ? body.error : "CELL_AUTHORITY_REJECTED",
      typeof body.message === "string" ? body.message : `Durable authority returned HTTP ${response.status}.`,
      response.status,
    );
  }
  return body;
}

function assertTenant(actual: unknown, expected: string): void {
  if (actual !== expected) throw new CellAuthorityError("ROUTING_CONFIG_TENANT_MISMATCH", "Routing config authority returned another tenant's configuration.", 403);
}

function parseUsageReference(value: string): Record<string, unknown> {
  try {
    const parsed = JSON.parse(value) as Record<string, unknown>;
    if (parsed && typeof parsed === "object") return parsed;
  } catch {
    // Zero-usage releases use a direct audit identity.
  }
  return { providerUsageReference: value };
}
