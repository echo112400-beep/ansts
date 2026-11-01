import { strict as assert } from "node:assert";
import { test } from "node:test";
import { MockDurableObjectState } from "../src/cloudflare/env";
import { ProviderQuotaCell } from "../src/cloudflare/provider_quota_cell";
import { RoutingConfigCell } from "../src/cloudflare/routing_config_cell";
import { TenantLedgerCell } from "../src/persistence/tenant";
import { MeteredProviderCallExecutor } from "../src/router/provider_call";
import { ProviderQuotaCellResolver, RoutingConfigCellClient, TenantLedgerBudgetAuthorityClient } from "../src/router/cell_authorities";
import { FetchOpenAiCompatibleProviderCallAdapter, SerializedPayloadTokenUpperBoundEstimator } from "../src/cloudflare/metered_provider";

async function fixture(identities: Array<{ tenantId: string; taskId: string }>) {
  const providerRecords = new Map<string, any>();
  const env: any = {
    DEEPSEEK_CHAT_COMPLETIONS_URL: "https://provider.test/chat/completions",
    DEEPSEEK_API_KEY: "test-provider-key",
    MODEL_PROVIDER_PROXY: { async fetch(request: Request) {
      const key = request.headers.get("idempotency-key")!;
      if (!providerRecords.has(key)) {
        const body = await request.json() as any;
        providerRecords.set(key, { id: `response-${providerRecords.size + 1}`, model: body.model,
          choices: [{ message: { content: body.messages[0].content } }], usage: { prompt_tokens: 7, completion_tokens: 3 } });
      }
      return new Response(JSON.stringify(providerRecords.get(key)), { headers: { "content-type": "application/json" } });
    } },
  };
  function namespace(Cell: any) {
    const cells = new Map<string, any>();
    return { idFromName: (name: string) => ({ name, toString: () => name }), get(id: any) {
      if (!cells.has(id.name)) cells.set(id.name, new Cell(new MockDurableObjectState(id.name), env));
      return { fetch: (input: RequestInfo, init?: RequestInit) => cells.get(id.name).fetch(new Request(input, init)) };
    } };
  }
  const ledger = namespace(TenantLedgerCell);
  const routing = namespace(RoutingConfigCell);
  const quota = namespace(ProviderQuotaCell);
  async function command(stub: any, pathname: string, method: string, body: any) {
    const response = await stub.fetch(new Request(`https://owner${pathname}`, { method, body: JSON.stringify(body) }));
    assert.ok(response.ok, await response.clone().text());
    return response;
  }
  const quotaStub = quota.get(quota.idFromName("openai-compatible:shared-account"));
  await command(quotaStub, "/provider-quota/policy", "PUT", {
    operationId: "quota-policy", expectedVersion: 0,
    policy: { requestsPerMinute: 1000, tokensPerSecond: 100000, grantTtlMs: 1000 },
  });
  for (const tenantId of new Set(identities.map((identity) => identity.tenantId))) {
    await command(routing.get(routing.idFromName(tenantId)), "/routing-config/versions", "POST", {
      operationId: "route-policy", expectedVersion: 0,
      configuration: { tenantId, policyVersion: "routing-v1", routes: [{ poolName: "DeepSeek", provider: "openai-compatible",
        modelId: "test-model", quotaDomain: "shared-account", quotaDemandTokensPerSecond: 10,
        maxInputTokens: 10000, maxOutputTokens: 100,
        pricing: { priceVersion: "price-v1", currency: "USD", inputMinorPerMillionTokens: 10000, outputMinorPerMillionTokens: 20000 } }] },
    });
    await command(ledger.get(ledger.idFromName(tenantId)), "/budget-periods/2026-11", "PUT", {
      tenant_id: tenantId, hard_limit_minor: 1000, currency: "USD", price_version: "price-v1",
    });
  }
  for (const { tenantId, taskId } of identities) {
    await command(ledger.get(ledger.idFromName(tenantId)), "/budget-grants", "POST", {
      tenant_id: tenantId, task_id: taskId, period_key: "2026-11", grant_id: `grant:${taskId}`, amount_minor: 100, currency: "USD",
    });
  }
  const resolver = new ProviderQuotaCellResolver(quota);
  const executor = (tenantId: string) => new MeteredProviderCallExecutor({
    routingConfig: new RoutingConfigCellClient(routing, tenantId),
    providerQuotaForRoute: (route) => resolver.forRoute(route),
    budget: new TenantLedgerBudgetAuthorityClient(ledger, tenantId),
    provider: new FetchOpenAiCompatibleProviderCallAdapter(env),
    inputTokenEstimator: new SerializedPayloadTokenUpperBoundEstimator(),
  });
  const execute = (identity: { tenantId: string; taskId: string }, content: string, callId = "turn:1:model:0") => executor(identity.tenantId).execute({
    ...identity, callId, poolName: "DeepSeek", periodKey: "2026-11", requestedMaxOutputTokens: 50,
    payload: [{ role: "user", content }],
  });
  const account = async (identity: { tenantId: string; taskId: string }) => {
    const response = await ledger.get(ledger.idFromName(identity.tenantId)).fetch(new Request(`https://owner/budget-accounts/${encodeURIComponent(identity.taskId)}?tenant_id=${encodeURIComponent(identity.tenantId)}&period_key=2026-11`));
    assert.ok(response.ok, await response.clone().text());
    return response.json() as Promise<any>;
  };
  const reprice = (tenantId: string) => ledger.get(ledger.idFromName(tenantId)).fetch(new Request("https://owner/budget-periods/2026-11", {
    method: "PUT", body: JSON.stringify({ tenant_id: tenantId, expected_policy_version: 1,
      hard_limit_minor: 1000, currency: "USD", price_version: "price-v2" }),
  }));
  const legacyStart = async (identity: { tenantId: string; taskId: string }, callId = "turn:1:model:0") => {
    const requestId = `${identity.tenantId}:${identity.taskId}:${callId}`;
    const granted = await command(quotaStub, "/provider-quota/grants", "POST", {
      requestId, tenantId: identity.tenantId, provider: "openai-compatible", routeVersion: 1, requests: 1, tokensPerSecond: 10,
    });
    const grant = await granted.json() as any;
    await command(quotaStub, `/provider-quota/grants/${encodeURIComponent(grant.grantId)}/start`, "POST", { requestId });
  };
  const legacySettlement = async (identity: { tenantId: string; taskId: string }) => {
    const budget = new TenantLedgerBudgetAuthorityClient(ledger, identity.tenantId);
    const holdId = "legacy-settled-hold";
    await budget.reserve({ ...identity, holdId, periodKey: "2026-11", amountMinor: 10, costBoundMinor: 10,
      currency: "USD", priceVersion: "price-v1", routingVersion: 1 });
    const input = { holdId, settlementId: `settle:${holdId}`, periodKey: "2026-11", actualMinor: 1,
      currency: "USD", usageReference: JSON.stringify({ providerUsageReference: "legacy-provider-receipt", routingVersion: 1, priceVersion: "price-v1" }) };
    await command(ledger.get(ledger.idFromName(identity.tenantId)), `/budget-holds/${holdId}/settlements`, "POST", {
      tenant_id: identity.tenantId, settlement_id: "settle:old-local-call", actual_minor: input.actualMinor,
      currency: input.currency, usage_reference: input.usageReference,
    });
    return { budget, input };
  };
  return { execute, account, providerRecords, reprice, legacyStart, legacySettlement };
}

test("different tenants using the same local model call identity receive their own provider result", async () => {
  const first = { tenantId: "tenant-first", taskId: "task-first" };
  const second = { tenantId: "tenant-second", taskId: "task-second" };
  const { execute, providerRecords } = await fixture([first, second]);
  const a = await execute(first, "answer for first tenant");
  const b = await execute(second, "answer for second tenant");
  assert.equal(a.status, "SETTLED");
  assert.equal(b.status, "SETTLED");
  assert.equal((b as any).response.choices[0].message.content, "answer for second tenant");
  assert.equal(providerRecords.size, 2);
});

test("two tasks in one tenant settle their same local call independently without leaving held budget", async () => {
  const first = { tenantId: "tenant-shared", taskId: "task-first" };
  const second = { tenantId: "tenant-shared", taskId: "task-second" };
  const { execute, account } = await fixture([first, second]);
  assert.equal((await execute(first, "first task")).status, "SETTLED");
  assert.equal((await execute(second, "second task")).status, "SETTLED");
  assert.equal((await account(first)).reserved_minor, 0);
  assert.equal((await account(second)).reserved_minor, 0);
  assert.equal((await account(first)).spent_minor, 1);
  assert.equal((await account(second)).spent_minor, 1);
});

test("separator-containing tenant and task identities cannot alias the shared provider quota request", async () => {
  const first = { tenantId: "tenant:division", taskId: "task" };
  const second = { tenantId: "tenant", taskId: "division:task" };
  const { execute, providerRecords } = await fixture([first, second]);
  assert.equal((await execute(first, "first identity")).status, "SETTLED");
  const result = await execute(second, "second identity");
  assert.equal(result.status, "SETTLED");
  assert.equal((result as any).response.choices[0].message.content, "second identity");
  assert.equal(providerRecords.size, 2);
});

test("allocated billing periods reject price rewrites and settled call retries keep their original charge", async () => {
  const identity = { tenantId: "tenant-reprice", taskId: "task-reprice" };
  const { execute, reprice, providerRecords, account } = await fixture([identity]);
  const original = await execute(identity, "original call");
  assert.equal(original.status, "SETTLED");
  const rewrite = await reprice(identity.tenantId);
  assert.equal(rewrite.status, 409);
  assert.equal(((await rewrite.json()) as any).error, "PRICE_VERSION_IMMUTABLE");
  const retry = await execute(identity, "original call");
  assert.equal(retry.status, "SETTLED");
  assert.equal(retry.priceVersion, "price-v1");
  assert.equal((retry as any).recovered, true);
  assert.equal(providerRecords.size, 1);
  assert.equal((await account(identity)).spent_minor, 1);
});

test("a legacy started quota identity requires reconciliation before an upgrade may create another provider attempt", async () => {
  const identity = { tenantId: "tenant-legacy", taskId: "task-legacy" };
  const { execute, legacyStart, providerRecords, account } = await fixture([identity]);
  await legacyStart(identity);
  await assert.rejects(execute(identity, "uncertain legacy call"), { code: "PROVIDER_CALL_IDENTITY_RECONCILIATION_REQUIRED" });
  assert.equal(providerRecords.size, 0);
  assert.equal((await account(identity)).reserved_minor, 0);
});

test("a settled legacy hold reuses its recorded settlement identity only for the same billing evidence", async () => {
  const identity = { tenantId: "tenant-old-settlement", taskId: "task-old-settlement" };
  const { legacySettlement, account } = await fixture([identity]);
  const { budget, input } = await legacySettlement(identity);
  const replay = await budget.settle(input) as any;
  assert.equal(replay.status, "SETTLED");
  assert.equal(replay.settlement_id, "settle:old-local-call");
  assert.equal((await account(identity)).spent_minor, 1);
  assert.equal((await account(identity)).reserved_minor, 0);
  await assert.rejects(budget.settle({ ...input, actualMinor: 2 }), { code: "IDEMPOTENCY_CONFLICT" });
  assert.equal((await account(identity)).spent_minor, 1);
});
