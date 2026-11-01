import { MeteredProviderCallExecutor, MeteredProviderCallError, ProviderCallResultJournal } from "../router/provider_call";
import {
  ProviderQuotaCellResolver,
  RoutingConfigCellClient,
  TenantLedgerBudgetAuthorityClient,
} from "../router/cell_authorities";
import { CloudflareCelldEnv } from "./env";
import {
  FetchOpenAiCompatibleProviderCallAdapter,
  SerializedPayloadTokenUpperBoundEstimator,
} from "./metered_provider";

export function createTaskMeteredProviderCallExecutor(
  env: CloudflareCelldEnv,
  tenantId: string,
  resultJournal: ProviderCallResultJournal,
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
    provider,
    inputTokenEstimator: new SerializedPayloadTokenUpperBoundEstimator(),
    resultJournal,
  });
}
