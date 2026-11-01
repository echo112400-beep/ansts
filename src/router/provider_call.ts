import {
  BeginProviderCallResult,
  ProviderQuotaGrant,
  ProviderUsageEvidence,
} from "../persistence/provider-quota";
import {
  ModelRouteConfig,
  RoutingCallPin,
  RoutingConfigSnapshot,
} from "../persistence/routing-config";
import { digestJson } from "../persistence/digest";

export interface RoutingConfigAuthority {
  getCurrent(tenantId: string): Promise<RoutingConfigSnapshot | null> | RoutingConfigSnapshot | null;
  getVersion(tenantId: string, version: number): Promise<RoutingConfigSnapshot | null> | RoutingConfigSnapshot | null;
  pinCall(tenantId: string, callId: string, poolName: string, routingVersion?: number): Promise<RoutingCallPin> | RoutingCallPin;
}

export interface ProviderQuotaAuthority {
  acquireGrant(input: {
    requestId: string;
    tenantId: string;
    provider: string;
    routeVersion: number;
    requests: number;
    tokensPerSecond: number;
  }): Promise<ProviderQuotaGrant>;
  getGrantByRequestId(requestId: string): Promise<ProviderQuotaGrant | null> | ProviderQuotaGrant | null;
  beginCall(grantId: string, requestId: string): Promise<BeginProviderCallResult>;
  completeCall(grantId: string, requestId: string, usage: ProviderUsageEvidence): Promise<ProviderQuotaGrant>;
  markUnknown(grantId: string, requestId: string, reason: string): Promise<ProviderQuotaGrant>;
}

export interface TenantBudgetAuthority {
  reserve(input: {
    holdId: string;
    tenantId: string;
    taskId: string;
    periodKey: string;
    amountMinor: number;
    costBoundMinor: number;
    currency: string;
    priceVersion: string;
    routingVersion: number;
  }): Promise<{
    holdId: string;
    amountMinor: number;
    currency: string;
    priceVersion: string;
    status: string;
  }>;
  settle(input: {
    holdId: string;
    settlementId: string;
    actualMinor: number;
    currency: string;
    usageReference: string;
    periodKey?: string;
  }): Promise<unknown>;
  markUnknown(input: { holdId: string; reason: string }): Promise<unknown>;
}

export interface ProviderCallAdapter {
  preflight?(input: { tenantId: string; taskId: string; route: ModelRouteConfig; payload: unknown }): Promise<void> | void;
  call(input: {
    callId: string;
    idempotencyKey: string;
    tenantId: string;
    taskId: string;
    route: ModelRouteConfig;
    payload: unknown;
    maxOutputTokens: number;
    reasoningEffort?: string;
  }): Promise<{ response?: unknown; usage?: ProviderUsageEvidence }>;
}

/** A server-side tokenizer or equivalent conservative estimator; never accept a caller's claimed token count. */
export interface TrustedInputTokenEstimator {
  estimateUpperBound(payload: unknown, route: ModelRouteConfig): TrustedInputTokenEstimate | Promise<TrustedInputTokenEstimate>;
}

export interface TrustedInputTokenEstimate {
  /** Server-side estimate; conservative byte bounds may be used when no tokenizer is installed. */
  estimatedTokens: number;
  /** Conservative upper bound after applying any tokenizer/provider safety margin. */
  upperBoundTokens: number;
}

export interface ProviderCallResultJournal {
  getModelCallResult(input: { taskId: string; tenantId: string; callId: string }): Promise<{
    response: unknown;
    usage: ProviderUsageEvidence;
    routeRefs?: Record<string, unknown>;
  } | null>;
  recordModelCallResult(input: {
    taskId: string;
    tenantId: string;
    callId: string;
    response: unknown;
    usage: ProviderUsageEvidence;
    routeRefs: Record<string, unknown>;
  }): Promise<unknown>;
}

export interface MeteredProviderCallDependencies {
  routingConfig: RoutingConfigAuthority;
  providerQuota?: ProviderQuotaAuthority;
  providerQuotaForRoute?: (route: ModelRouteConfig) => ProviderQuotaAuthority;
  budget: TenantBudgetAuthority;
  provider: ProviderCallAdapter;
  inputTokenEstimator?: TrustedInputTokenEstimator;
  /** Persist a completed provider response before quota completion and budget settlement. */
  resultJournal?: ProviderCallResultJournal;
}

export interface MeteredProviderCallRequest {
  callId: string;
  tenantId: string;
  taskId: string;
  poolName: string;
  periodKey: string;
  requestedMaxOutputTokens?: number;
  reasoningEffort?: string;
  payload: unknown;
  fallbackOfCallId?: string;
  routingVersion?: number;
}

export type MeteredProviderCallResult =
  | {
      status: "SETTLED";
      callId: string;
      holdId: string;
      grantId: string;
      routingVersion: number;
      priceVersion: string;
      currency: string;
      reservedMinor: number;
      actualMinor: number;
      releasedMinor: number;
      usage: ProviderUsageEvidence;
      response?: unknown;
      recovered: boolean;
    }
  | {
      status: "OUTCOME_UNKNOWN";
      callId: string;
      holdId: string;
      grantId: string;
      routingVersion: number;
      priceVersion: string;
      reservedMinor: number;
      reason: string;
    }
  | {
      status: "NOT_STARTED";
      callId: string;
      holdId: string;
      grantId: string;
      routingVersion: number;
      priceVersion: string;
      reservedMinor: number;
    };

export class MeteredProviderCallError extends Error {
  constructor(public readonly code: string, message: string, public readonly status = 422) {
    super(message);
    this.name = "MeteredProviderCallError";
  }
}

/** One call identity owns one quote, one budget hold, one provider grant and at most one provider attempt. */
export class MeteredProviderCallExecutor {
  constructor(private readonly dependencies: MeteredProviderCallDependencies) {}

  public async execute(request: MeteredProviderCallRequest): Promise<MeteredProviderCallResult> {
    validateCallRequest(request);
    const callId = normalizeText(request.callId, "callId");
    const tenantId = normalizeText(request.tenantId, "tenantId");
    const taskId = normalizeText(request.taskId, "taskId");
    const providerIdempotencyKey = `provider-call:${(await digestJson({ tenantId, taskId, callId })).sha256}`;
    const requestDigest = (await digestJson({
      payload: request.payload,
      poolName: request.poolName,
      reasoningEffort: request.reasoningEffort ?? null,
      requestedMaxOutputTokens: request.requestedMaxOutputTokens ?? null,
      routingVersion: request.routingVersion ?? null,
    })).sha256;
    const requestId = `${encodeURIComponent(tenantId)}:${encodeURIComponent(taskId)}:${encodeURIComponent(callId)}`;

    const pin = await this.dependencies.routingConfig.pinCall(tenantId, requestId, request.poolName, request.routingVersion);
    const snapshot = await this.dependencies.routingConfig.getVersion(tenantId, pin.routingVersion);
    if (!snapshot) throw new MeteredProviderCallError("ROUTING_CONFIG_NOT_FOUND", `Pinned routing version ${pin.routingVersion} is unavailable for tenant ${tenantId}.`, 404);
    if (snapshot.tenantId !== tenantId) throw new MeteredProviderCallError("ROUTING_CONFIG_TENANT_MISMATCH", "Routing config authority returned another tenant's configuration.", 403);
    const route = snapshot.routes.find((candidate) => candidate.poolName === request.poolName);
    if (!route || pin.route.poolName !== route.poolName || pin.routingVersion !== snapshot.version ||
        pin.priceVersion !== (route.pricing?.priceVersion ?? null) || JSON.stringify(pin.route) !== JSON.stringify(route)) {
      throw new MeteredProviderCallError("ROUTING_CALL_PIN_CONFLICT", "The pinned route does not match its immutable configuration snapshot.", 409);
    }
    await this.dependencies.provider.preflight?.({ tenantId, taskId, route, payload: request.payload });
    const providerQuota = this.dependencies.providerQuotaForRoute?.(route) ?? this.dependencies.providerQuota;
    if (!providerQuota) throw new MeteredProviderCallError("PROVIDER_QUOTA_AUTHORITY_REQUIRED", "A quota authority for the selected provider domain must be injected.", 500);
    const existingGrant = await providerQuota.getGrantByRequestId(requestId);
    const legacyRequestId = `${tenantId}:${taskId}:${callId}`;
    if (!existingGrant && legacyRequestId !== requestId && await providerQuota.getGrantByRequestId(legacyRequestId)) {
      throw new MeteredProviderCallError("PROVIDER_CALL_IDENTITY_RECONCILIATION_REQUIRED",
        "A legacy provider quota identity exists. Reconcile its recorded outcome before assigning the canonical call identity.", 409);
    }
    if (existingGrant && existingGrant.routeVersion !== snapshot.version) {
      throw new MeteredProviderCallError("PROVIDER_QUOTA_ROUTE_VERSION_CONFLICT", "The durable quota grant is pinned to another routing version.", 409);
    }

    if (!this.dependencies.inputTokenEstimator) {
      throw new MeteredProviderCallError("COST_BOUND_UNAVAILABLE", "A trusted server-side input token estimator is required for strict hard-budget calls.");
    }
    const inputEstimate = await this.dependencies.inputTokenEstimator.estimateUpperBound(request.payload, route);
    const quote = buildCostBound(route, inputEstimate, request.requestedMaxOutputTokens);
    const holdId = stableHoldId(tenantId, taskId, callId);
    const reservation = await this.dependencies.budget.reserve({
      holdId,
      tenantId,
      taskId,
      periodKey: normalizeText(request.periodKey, "periodKey"),
      amountMinor: quote.amountMinor,
      costBoundMinor: quote.amountMinor,
      currency: quote.currency,
      priceVersion: quote.priceVersion,
      routingVersion: snapshot.version,
    });
    assertReservationMatches(reservation, holdId, quote.amountMinor, quote.currency, quote.priceVersion);

    let grant: ProviderQuotaGrant;
    try {
      grant = await providerQuota.acquireGrant({
        requestId,
        tenantId,
        provider: route.provider,
        routeVersion: snapshot.version,
        requests: 1,
        tokensPerSecond: route.quotaDemandTokensPerSecond,
      });
    } catch (error) {
      const reason = errorMessage(error);
      if (hasCode(error, "PROVIDER_QUOTA_EXCEEDED")) {
        await this.dependencies.budget.settle({
          holdId,
          settlementId: `release:${holdId}`,
          periodKey: request.periodKey,
          actualMinor: 0,
          currency: quote.currency,
          usageReference: JSON.stringify({
            outcome: "PROVIDER_CALL_NOT_STARTED",
            reason: "quota-denied",
            callId,
            routingVersion: snapshot.version,
            priceVersion: quote.priceVersion,
          }),
        });
        throw error;
      }
      await safelyMarkBudgetUnknown(this.dependencies.budget, holdId, `quota grant result unknown: ${reason}`);
      return unknownResult(callId, holdId, existingGrant?.grantId ?? `grant:${requestId}`, snapshot.version, quote, reason);
    }

    const stagedResult = await this.dependencies.resultJournal?.getModelCallResult({ taskId, tenantId, callId });
    if (stagedResult) {
      const usage = validateUsageWithinBound(stagedResult.usage, quote);
      const refs = stagedResult.routeRefs ?? {};
      if (!usage ||
          (refs.routingVersion !== undefined && refs.routingVersion !== snapshot.version) ||
          (refs.priceVersion !== undefined && refs.priceVersion !== quote.priceVersion) ||
          (refs.holdId !== undefined && refs.holdId !== holdId) ||
          (refs.grantId !== undefined && refs.grantId !== grant.grantId) ||
          (refs.requestDigest !== undefined && refs.requestDigest !== requestDigest)) {
        const reason = "A staged provider result does not match the pinned route, hold, grant, or token bound.";
        await safelyMarkBudgetUnknown(this.dependencies.budget, holdId, reason);
        await safelyMarkQuotaUnknown(providerQuota, grant.grantId, requestId, reason);
        return unknownResult(callId, holdId, grant.grantId, snapshot.version, quote, reason);
      }
      await providerQuota.completeCall(grant.grantId, requestId, usage);
      return this.settleUsage({
        callId,
        holdId,
        grant,
        snapshot,
        quote,
        usage,
        response: stagedResult.response,
        recovered: true,
        periodKey: request.periodKey,
        providerQuota,
      });
    }

    const begin = await providerQuota.beginCall(grant.grantId, requestId);
    if (begin.state === "EXPIRED") {
      await this.dependencies.budget.settle({
        holdId,
        settlementId: `release:${holdId}`,
        periodKey: request.periodKey,
        actualMinor: 0,
        currency: quote.currency,
        usageReference: JSON.stringify({
          outcome: "PROVIDER_CALL_NOT_STARTED",
          reason: "expired-quota-grant",
          grantId: grant.grantId,
          routingVersion: snapshot.version,
          priceVersion: quote.priceVersion,
        }),
      });
      return { status: "NOT_STARTED", callId, holdId, grantId: grant.grantId, routingVersion: snapshot.version, priceVersion: quote.priceVersion, reservedMinor: quote.amountMinor };
    }
    if (begin.state === "OUTCOME_UNKNOWN") {
      const reason = "A provider attempt was already started for this call identity; duplicate execution is blocked.";
      await safelyMarkBudgetUnknown(this.dependencies.budget, holdId, reason);
      await safelyMarkQuotaUnknown(providerQuota, grant.grantId, requestId, reason);
      return unknownResult(callId, holdId, grant.grantId, snapshot.version, quote, reason);
    }
    if (begin.state === "COMPLETED") {
      return this.settleUsage({ callId, holdId, grant, snapshot, quote, usage: begin.usage, recovered: true, providerQuota, periodKey: request.periodKey });
    }

    let providerResult: { response?: unknown; usage?: ProviderUsageEvidence };
    try {
      providerResult = await this.dependencies.provider.call({
        callId,
        idempotencyKey: providerIdempotencyKey,
        tenantId,
        taskId,
        route,
        payload: request.payload,
        maxOutputTokens: quote.maxOutputTokens,
        reasoningEffort: request.reasoningEffort,
      });
    } catch (error) {
      const reason = errorMessage(error);
      await safelyMarkBudgetUnknown(this.dependencies.budget, holdId, `provider outcome unknown: ${reason}`);
      await safelyMarkQuotaUnknown(providerQuota, grant.grantId, requestId, reason);
      return unknownResult(callId, holdId, grant.grantId, snapshot.version, quote, reason);
    }

    const usage = validateUsageWithinBound(providerResult.usage, quote);
    if (!usage) {
      const reason = "Provider returned no verified usage evidence within the reserved token bound.";
      await safelyMarkBudgetUnknown(this.dependencies.budget, holdId, reason);
      await safelyMarkQuotaUnknown(providerQuota, grant.grantId, requestId, reason);
      return unknownResult(callId, holdId, grant.grantId, snapshot.version, quote, reason);
    }
    if (this.dependencies.resultJournal) {
      try {
        await this.dependencies.resultJournal.recordModelCallResult({
          taskId,
          tenantId,
          callId,
          response: providerResult.response,
          usage,
          routeRefs: {
            holdId,
            grantId: grant.grantId,
            routingVersion: snapshot.version,
            priceVersion: quote.priceVersion,
            provider: route.provider,
            modelId: route.modelId,
            requestDigest,
          },
        });
      } catch (error) {
        const reason = `Provider result could not be durably journaled: ${errorMessage(error)}`;
        await safelyMarkBudgetUnknown(this.dependencies.budget, holdId, reason);
        await safelyMarkQuotaUnknown(providerQuota, grant.grantId, requestId, reason);
        return unknownResult(callId, holdId, grant.grantId, snapshot.version, quote, reason);
      }
    }
    await providerQuota.completeCall(grant.grantId, requestId, usage);
    return this.settleUsage({ callId, holdId, grant, snapshot, quote, usage, response: providerResult.response, recovered: false, providerQuota, periodKey: request.periodKey });
  }

  private async settleUsage(params: {
    callId: string;
    holdId: string;
    grant: ProviderQuotaGrant;
    snapshot: RoutingConfigSnapshot;
    quote: CostBound;
    usage: ProviderUsageEvidence;
    response?: unknown;
    recovered: boolean;
    providerQuota: ProviderQuotaAuthority;
    periodKey: string;
  }): Promise<MeteredProviderCallResult> {
    const usage = validateUsageWithinBound(params.usage, params.quote);
    if (!usage) {
      const reason = "Recorded provider usage exceeds the original reservation bound.";
      await safelyMarkBudgetUnknown(this.dependencies.budget, params.holdId, reason);
      await safelyMarkQuotaUnknown(params.providerQuota, params.grant.grantId, params.grant.requestId, reason);
      return unknownResult(params.callId, params.holdId, params.grant.grantId, params.snapshot.version, params.quote, reason);
    }
    const actualMinor = computeMinorCost(usage.inputTokens, usage.outputTokens, params.quote.pricing);
    await this.dependencies.budget.settle({
      holdId: params.holdId,
      settlementId: `settle:${params.holdId}`,
      periodKey: params.periodKey,
      actualMinor,
      currency: params.quote.currency,
      usageReference: JSON.stringify({
        providerUsageReference: usage.reference,
        routingVersion: params.snapshot.version,
        priceVersion: params.quote.priceVersion,
      }),
    });
    return {
      status: "SETTLED",
      callId: params.callId,
      holdId: params.holdId,
      grantId: params.grant.grantId,
      routingVersion: params.snapshot.version,
      priceVersion: params.quote.priceVersion,
      currency: params.quote.currency,
      reservedMinor: params.quote.amountMinor,
      actualMinor,
      releasedMinor: params.quote.amountMinor - actualMinor,
      usage,
      response: params.response,
      recovered: params.recovered,
    };
  }
}

interface CostBound {
  amountMinor: number;
  currency: string;
  priceVersion: string;
  maxInputTokens: number;
  maxOutputTokens: number;
  pricing: NonNullable<ModelRouteConfig["pricing"]>;
}

function buildCostBound(route: ModelRouteConfig, estimate: TrustedInputTokenEstimate, requestedMaxOutputTokens?: number): CostBound {
  const maxInputTokens = route.maxInputTokens;
  const routeMaxOutputTokens = route.maxOutputTokens;
  const inputTokenBound = estimate?.upperBoundTokens;
  if (
    !estimate || !Number.isSafeInteger(estimate.estimatedTokens) || estimate.estimatedTokens < 0 ||
    !Number.isSafeInteger(inputTokenBound) || inputTokenBound < estimate.estimatedTokens ||
    typeof maxInputTokens !== "number" || !Number.isSafeInteger(maxInputTokens) || maxInputTokens <= 0 ||
    inputTokenBound > maxInputTokens ||
    typeof routeMaxOutputTokens !== "number" || !Number.isSafeInteger(routeMaxOutputTokens) || routeMaxOutputTokens <= 0 ||
    !route.pricing
  ) {
    throw new MeteredProviderCallError("COST_BOUND_UNAVAILABLE", "Strict hard-budget calls require a trusted input estimate within the route cap, an output token ceiling and versioned integer pricing.");
  }
  const maxOutputTokens = requestedMaxOutputTokens ?? routeMaxOutputTokens;
  if (typeof maxOutputTokens !== "number" || !Number.isSafeInteger(maxOutputTokens) || maxOutputTokens <= 0 || maxOutputTokens > routeMaxOutputTokens) {
    throw new MeteredProviderCallError("COST_BOUND_UNAVAILABLE", "Requested output tokens exceed the route's credible maximum bound.");
  }
  const pricing = route.pricing;
  if (!pricing.priceVersion || !/^[A-Z]{3}$/.test(pricing.currency) ||
      !Number.isSafeInteger(pricing.inputMinorPerMillionTokens) || pricing.inputMinorPerMillionTokens < 0 ||
      !Number.isSafeInteger(pricing.outputMinorPerMillionTokens) || pricing.outputMinorPerMillionTokens < 0) {
    throw new MeteredProviderCallError("COST_BOUND_UNAVAILABLE", "Route pricing must use a currency, price version, and non-negative integer minor-unit rates.");
  }
  const amountMinor = computeMinorCost(inputTokenBound, maxOutputTokens, pricing);
  return {
    amountMinor,
    currency: pricing.currency,
    priceVersion: pricing.priceVersion,
    maxInputTokens: inputTokenBound,
    maxOutputTokens,
    pricing,
  };
}

function computeMinorCost(
  inputTokens: number,
  outputTokens: number,
  pricing: NonNullable<ModelRouteConfig["pricing"]>,
): number {
  const numerator = BigInt(inputTokens) * BigInt(pricing.inputMinorPerMillionTokens) +
    BigInt(outputTokens) * BigInt(pricing.outputMinorPerMillionTokens);
  const amount = (numerator + 999_999n) / 1_000_000n;
  if (amount > BigInt(Number.MAX_SAFE_INTEGER)) throw new MeteredProviderCallError("COST_BOUND_UNAVAILABLE", "Maximum cost exceeds supported integer minor units.");
  return Number(amount);
}

function validateUsageWithinBound(usage: ProviderUsageEvidence | undefined, bound: CostBound): ProviderUsageEvidence | null {
  if (!usage || usage.verified !== true || typeof usage.reference !== "string" || usage.reference.trim().length === 0 ||
      !Number.isSafeInteger(usage.inputTokens) || usage.inputTokens < 0 || usage.inputTokens > bound.maxInputTokens ||
      !Number.isSafeInteger(usage.outputTokens) || usage.outputTokens < 0 || usage.outputTokens > bound.maxOutputTokens) return null;
  return usage;
}

function assertReservationMatches(
  result: Awaited<ReturnType<TenantBudgetAuthority["reserve"]>>,
  holdId: string,
  amountMinor: number,
  currency: string,
  priceVersion: string,
): void {
  if (result.holdId !== holdId || result.amountMinor !== amountMinor || result.currency !== currency || result.priceVersion !== priceVersion || result.status === "REJECTED") {
    throw new MeteredProviderCallError("BUDGET_RESERVATION_MISMATCH", "Tenant budget authority did not confirm the exact versioned cost bound.", 409);
  }
}

function validateCallRequest(request: MeteredProviderCallRequest): void {
  if (!request || typeof request !== "object") throw new TypeError("call request must be an object.");
  normalizeText(request.callId, "callId");
  normalizeText(request.tenantId, "tenantId");
  normalizeText(request.taskId, "taskId");
  normalizeText(request.poolName, "poolName");
  normalizeText(request.periodKey, "periodKey");
  if (request.fallbackOfCallId !== undefined && request.fallbackOfCallId === request.callId) {
    throw new MeteredProviderCallError("FALLBACK_CALL_ID_REQUIRED", "A fallback call must have a distinct call identity and budget hold.");
  }
}

function stableHoldId(tenantId: string, taskId: string, callId: string): string {
  return `provider-hold:${encodeURIComponent(tenantId)}:${encodeURIComponent(taskId)}:${encodeURIComponent(callId)}`;
}

function unknownResult(callId: string, holdId: string, grantId: string, routingVersion: number, quote: CostBound, reason: string): MeteredProviderCallResult {
  return { status: "OUTCOME_UNKNOWN", callId, holdId, grantId, routingVersion, priceVersion: quote.priceVersion, reservedMinor: quote.amountMinor, reason };
}

async function safelyMarkBudgetUnknown(budget: TenantBudgetAuthority, holdId: string, reason: string): Promise<void> {
  try {
    await budget.markUnknown({ holdId, reason });
  } catch {
    // The reservation remains held even if the status annotation needs reconciliation.
  }
}

async function safelyMarkQuotaUnknown(authority: ProviderQuotaAuthority, grantId: string, requestId: string, reason: string): Promise<void> {
  try {
    await authority.markUnknown(grantId, requestId, reason);
  } catch {
    // STARTED grants remain allocated and prevent a second attempt after restart.
  }
}

function hasCode(error: unknown, code: string): boolean {
  return Boolean(error && typeof error === "object" && "code" in error && (error as { code: unknown }).code === code);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function normalizeText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim().length === 0) throw new TypeError(`${label} must be a non-empty string.`);
  return value.trim();
}
