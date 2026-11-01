import { MeteredProviderCallExecutor, MeteredProviderCallError, ProviderCallResultJournal } from "../router/provider_call";
import {
  ProviderQuotaCellResolver,
  RoutingConfigCellClient,
  TenantLedgerBudgetAuthorityClient,
} from "../router/cell_authorities";
import { CloudflareCelldEnv } from "./env";
import { ProviderCallAdapter } from "../router/provider_call";
import {
  FetchOpenAiCompatibleProviderCallAdapter,
  SerializedPayloadTokenUpperBoundEstimator,
} from "./metered_provider";

export function createTaskMeteredProviderCallExecutor(
  env: CloudflareCelldEnv,
  tenantId: string,
  resultJournal: ProviderCallResultJournal,
  guard?: {
    preflight(input: Parameters<NonNullable<ProviderCallAdapter["preflight"]>>[0]): Promise<void>;
    beforeCall(input: Parameters<ProviderCallAdapter["call"]>[0]): Promise<void>;
    afterCall(input: Parameters<ProviderCallAdapter["call"]>[0], result: Awaited<ReturnType<ProviderCallAdapter["call"]>>): Promise<void>;
  },
): MeteredProviderCallExecutor {
  if (!env.ROUTING_CONFIG_CELL || !env.PROVIDER_QUOTA_CELL || !env.TENANT_LEDGER_CELL) {
    throw new MeteredProviderCallError(
      "METERED_CALL_AUTHORITIES_UNAVAILABLE",
      "RoutingConfigCell, ProviderQuotaCell, and TenantLedgerCell bindings are required for model calls.",
      503,
    );
  }
  const provider = new FetchOpenAiCompatibleProviderCallAdapter(env);
  const quotaResolver = new ProviderQuotaCellResolver(env.PROVIDER_QUOTA_CELL);
  return new MeteredProviderCallExecutor({
    routingConfig: new RoutingConfigCellClient(env.ROUTING_CONFIG_CELL, tenantId),
    providerQuotaForRoute: (route) => quotaResolver.forRoute(route),
    budget: new TenantLedgerBudgetAuthorityClient(env.TENANT_LEDGER_CELL, tenantId),
    provider: guard ? {
      async preflight(input) { provider.preflight(input); await guard.preflight(input); },
      async call(input) { await guard.beforeCall(input); const result = await provider.call(input); await guard.afterCall(input, result); return result; },
    } : provider,
    inputTokenEstimator: new SerializedPayloadTokenUpperBoundEstimator(),
    resultJournal,
  });
}
