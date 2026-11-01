import { strict as assert } from "assert";
import * as http from "node:http";
import { test } from "node:test";
import { AddressInfo } from "node:net";

import { MockDurableObjectState } from "../src/cloudflare/env";
import { ProviderQuotaCell } from "../src/persistence/provider-quota";
import { RoutingConfigCell } from "../src/persistence/routing-config";
import { ProviderQuotaCell as ProviderQuotaDurableObject } from "../src/cloudflare/provider_quota_cell";
import { RoutingConfigCell as RoutingConfigDurableObject } from "../src/cloudflare/routing_config_cell";
import { MeteredProviderCallExecutor } from "../src/router/provider_call";
import { ProviderQuotaCellResolver } from "../src/router/cell_authorities";
import { LiveOpenAiCompatibleSseAdapter } from "../src/router/live_llm_adapter";

const route = {
  poolName: "DeepSeek",
  provider: "deepseek",
  modelId: "deepseek-chat",
  quotaDomain: "account:primary",
  quotaDemandTokensPerSecond: 10,
  maxInputTokens: 100,
  maxOutputTokens: 100,
  pricing: {
    priceVersion: "deepseek-2026-10",
    currency: "USD",
    inputMinorPerMillionTokens: 10_000,
    outputMinorPerMillionTokens: 20_000,
  },
};

function config(override: Record<string, unknown> = {}) {
  return {
    tenantId: "tenant-1",
    policyVersion: "tenant-routing-v1",
    routes: [route],
    ...override,
  };
}

function routingAuthority(snapshot: { tenantId: string; version: number; publishedAt: number; routes: any[] }): any {
  return {
    getCurrent: async () => snapshot,
    getVersion: async (_tenantId: string, version: number) => version === snapshot.version ? snapshot : null,
    pinCall: async (_tenantId: string, callId: string, poolName: string, routingVersion?: number) => {
      const route = snapshot.routes.find((candidate) => candidate.poolName === poolName);
      if (!route || (routingVersion !== undefined && routingVersion !== snapshot.version)) throw new Error("route pin unavailable");
      return { callId, poolName, routingVersion: snapshot.version, priceVersion: route.pricing?.priceVersion ?? null, route };
    },
  };
}

const serverInputEstimator = {
  estimateUpperBound: (_payload: unknown, selectedRoute: { maxInputTokens?: number }) => {
    const estimatedTokens = selectedRoute.maxInputTokens ?? 0;
    return { estimatedTokens, upperBoundTokens: estimatedTokens };
  },
};

test("routing configuration versions preserve the old route and price snapshot", async () => {
  const state = new MockDurableObjectState("tenant-1");
  const cell = new RoutingConfigCell(state, {} as never);
  const first = await cell.publish({
    operationId: "route-publish-1",
    expectedVersion: 0,
    configuration: config(),
  });
  const second = await cell.publish({
    operationId: "route-publish-2",
    expectedVersion: 1,
    configuration: config({
      routes: [{ ...route, modelId: "deepseek-chat-v2", pricing: { ...route.pricing, priceVersion: "deepseek-2026-11", outputMinorPerMillionTokens: 50_000 } }],
    }),
  });

  const restarted = new RoutingConfigCell(state, {} as never);
  assert.equal(first.version, 1);
  assert.equal(second.version, 2);
  assert.equal((await restarted.getVersion(1))?.routes[0].pricing?.priceVersion, "deepseek-2026-10");
  assert.equal((await restarted.getVersion(1))?.routes[0].modelId, "deepseek-chat");
  assert.equal((await restarted.getCurrent())?.routes[0].pricing?.priceVersion, "deepseek-2026-11");
});

test("Cloudflare owner wrappers expose durable routing, pin and provider quota HTTP protocols", async () => {
  const routingState = new MockDurableObjectState("tenant-http");
  const routing = new RoutingConfigDurableObject(routingState, {} as never);
  const configResponse = await routing.fetch(new Request("https://cell/routing-config/versions", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ operationId: "http-config-1", expectedVersion: 0, configuration: config({ tenantId: "tenant-http" }) }),
  }));
  assert.equal(configResponse.status, 201);
  const pinResponse = await routing.fetch(new Request("https://cell/routing-config/call-pins", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ callId: "http-call-1", poolName: "DeepSeek" }),
  }));
  assert.equal(pinResponse.status, 201);
  assert.equal((await pinResponse.json() as { routingVersion: number }).routingVersion, 1);

  const quotaState = new MockDurableObjectState("deepseek:shared");
  const quota = new ProviderQuotaDurableObject(quotaState, {} as never);
  const policyResponse = await quota.fetch(new Request("https://cell/provider-quota/policy", {
    method: "PUT", headers: { "content-type": "application/json" },
    body: JSON.stringify({ operationId: "http-policy-1", expectedVersion: 0, policy: { requestsPerMinute: 2, tokensPerSecond: 100, grantTtlMs: 500 } }),
  }));
  assert.equal(policyResponse.status, 201);
  const grantRequest = {
    requestId: "tenant-http:task-http:call-http",
    tenantId: "tenant-http",
    provider: "deepseek",
    routeVersion: 1,
    requests: 1,
    tokensPerSecond: 10,
  };
  const grantResponse = await quota.fetch(new Request("https://cell/provider-quota/grants", {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(grantRequest),
  }));
  assert.equal(grantResponse.status, 201);
  const grant = await grantResponse.json() as { grantId: string };
  const replayResponse = await quota.fetch(new Request(`https://cell/provider-quota/grants/${encodeURIComponent(grant.grantId)}/start`, {
    method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ requestId: grantRequest.requestId }),
  }));
  assert.equal((await replayResponse.json() as { state: string }).state, "STARTED");
});

test("a call pin keeps its original route and price after publish, and quota domains resolve to separate owners", async () => {
  const state = new MockDurableObjectState("tenant-1");
  const cell = new RoutingConfigCell(state, {} as never);
  await cell.publish({ operationId: "pin-config-1", expectedVersion: 0, configuration: config() });
  const firstPin = await cell.pinCall({ callId: "pinned-call", poolName: "DeepSeek" });
  await cell.publish({
    operationId: "pin-config-2",
    expectedVersion: 1,
    configuration: config({ routes: [{ ...route, pricing: { ...route.pricing, priceVersion: "deepseek-2026-11", outputMinorPerMillionTokens: 90_000 } }] }),
  });
  const replayedPin = await new RoutingConfigCell(state, {} as never).pinCall({ callId: "pinned-call", poolName: "DeepSeek" });
  assert.equal(replayedPin.routingVersion, firstPin.routingVersion);
  assert.equal(replayedPin.priceVersion, "deepseek-2026-10");

  const ownerNames: string[] = [];
  const namespace = {
    idFromName: (name: string) => { ownerNames.push(name); return { name, toString: () => name }; },
    get: () => ({ fetch: async () => new Response("{}") }),
  };
  const resolver = new ProviderQuotaCellResolver(namespace as never);
  const deepseekOwner = resolver.forRoute({ provider: "openai-compatible", quotaDomain: "credential-a" });
  const otherOwner = resolver.forRoute({ provider: "openai-compatible", quotaDomain: "credential-b" });
  assert.notEqual(deepseekOwner, otherOwner);
  assert.equal(resolver.forRoute({ provider: "openai-compatible", quotaDomain: "credential-a" }), deepseekOwner);
  assert.deepEqual(ownerNames, ["openai-compatible:credential-a", "openai-compatible:credential-b"]);
});

test("provider quota grants use one durable authority and a stable request replays after restart", async () => {
  const state = new MockDurableObjectState("deepseek-account-primary");
  let now = 1_800_000_000_000;
  const policy = { requestsPerMinute: 1, tokensPerSecond: 10, grantTtlMs: 500 };
  const cell = new ProviderQuotaCell(state, {} as never, () => now);
  await cell.configurePolicy({ operationId: "quota-policy-1", expectedVersion: 0, policy });
  const request = {
    requestId: "tenant-1:task-1:turn-1",
    tenantId: "tenant-1",
    provider: "deepseek",
    routeVersion: 1,
    requests: 1,
    tokensPerSecond: 8,
  };
  const first = await cell.acquireGrant(request);
  const restarted = new ProviderQuotaCell(state, {} as never, () => now);
  const replay = await restarted.acquireGrant(request);

  assert.equal(first.grantId, replay.grantId);
  assert.equal(first.policyVersion, replay.policyVersion);
  await assert.rejects(
    restarted.acquireGrant({ ...request, requestId: "tenant-2:task-2:turn-1", tenantId: "tenant-2", tokensPerSecond: 3 }),
    { code: "PROVIDER_QUOTA_EXCEEDED" },
  );
  assert.equal(state.durableSyncCalls, 4);
  now += 600;
  const begin = await restarted.beginCall(first.grantId, request.requestId);
  assert.equal(begin.state, "EXPIRED");
});

test("an uncertain started grant blocks a second provider call after a simulated restart", async () => {
  const state = new MockDurableObjectState("glm-account-primary");
  let now = 1_800_000_000_100;
  const cell = new ProviderQuotaCell(state, {} as never, () => now);
  await cell.configurePolicy({
    operationId: "glm-policy-1",
    expectedVersion: 0,
    policy: { requestsPerMinute: 10, tokensPerSecond: 10, grantTtlMs: 500 },
  });
  const grant = await cell.acquireGrant({
    requestId: "tenant-1:task-1:turn-2",
    tenantId: "tenant-1",
    provider: "glm",
    routeVersion: 4,
    requests: 1,
    tokensPerSecond: 5,
  });
  assert.equal((await cell.beginCall(grant.grantId, grant.requestId)).state, "STARTED");

  now = 1_800_000_000_200;
  const restarted = new ProviderQuotaCell(state, {} as never, () => now);
  assert.equal((await restarted.beginCall(grant.grantId, grant.requestId)).state, "OUTCOME_UNKNOWN");
  await restarted.markUnknown(grant.grantId, grant.requestId, "router restarted during provider call");
  assert.equal((await restarted.getGrant(grant.grantId))?.status, "UNKNOWN");
  now += 5_000;
  await assert.rejects(
    restarted.acquireGrant({
      requestId: "tenant-2:task-2:turn-2",
      tenantId: "tenant-2",
      provider: "glm",
      routeVersion: 4,
      requests: 1,
      tokensPerSecond: 6,
    }),
    { code: "PROVIDER_QUOTA_EXCEEDED" },
  );
});

test("metered provider calls reserve the versioned maximum cost before calling and settle verified usage", async () => {
  const events: string[] = [];
  const executor = new MeteredProviderCallExecutor({
    routingConfig: routingAuthority({ ...config(), version: 7, publishedAt: 10 }),
    providerQuota: {
      acquireGrant: async (input) => {
        events.push("quota");
        return { grantId: `grant:${input.requestId}`, requestId: input.requestId, tenantId: input.tenantId, provider: input.provider,
          routeVersion: input.routeVersion, requests: input.requests, tokensPerSecond: input.tokensPerSecond,
          status: "GRANTED", expiresAt: 99_999, policyVersion: 3 };
      },
      getGrantByRequestId: async () => null,
      beginCall: async () => {
        events.push("begin");
        return { state: "STARTED" };
      },
      completeCall: async () => { events.push("complete"); return {} as never; },
      markUnknown: async () => { events.push("quota-unknown"); return {} as never; },
    },
    budget: {
      reserve: async (input) => {
        events.push("reserve");
        assert.equal(input.priceVersion, "deepseek-2026-10");
        return { holdId: input.holdId, amountMinor: input.amountMinor, currency: input.currency, priceVersion: input.priceVersion, status: "HELD" };
      },
      settle: async (input) => {
        events.push("settle");
        assert.equal(input.currency, "USD");
        const evidence = JSON.parse(input.usageReference) as Record<string, unknown>;
        assert.equal(evidence.providerUsageReference, "resp-123");
        assert.equal(evidence.routingVersion, 7);
        assert.equal(evidence.priceVersion, "deepseek-2026-10");
        return { settlementId: input.settlementId, actualMinor: input.actualMinor };
      },
      markUnknown: async () => { events.push("budget-unknown"); },
    },
    provider: {
      call: async (input) => {
        events.push("provider");
        assert.equal(input.maxOutputTokens, 100);
        return { response: { text: "ok" }, usage: { verified: true, reference: "resp-123", inputTokens: 5, outputTokens: 7 } };
      },
    },
    inputTokenEstimator: serverInputEstimator,
  });

  const result = await executor.execute({
    callId: "call-1",
    tenantId: "tenant-1",
    taskId: "task-1",
    poolName: "DeepSeek",
    periodKey: "2026-11",
    requestedMaxOutputTokens: 100,
    payload: { prompt: "hello", callerClaimedInputTokens: 1 },
  });

  assert.equal(result.status, "SETTLED");
  assert.equal(result.routingVersion, 7);
  assert.equal(result.priceVersion, "deepseek-2026-10");
  assert.equal(result.reservedMinor, 3);
  assert.equal(result.actualMinor, 1);
  assert.ok(events.indexOf("reserve") < events.indexOf("provider"));
  assert.equal(events.filter((event) => event === "provider").length, 1);
});

test("a staged model response recovers after provider success but before quota completion", async () => {
  const events: string[] = [];
  let providerCalls = 0;
  let quotaCompletions = 0;
  let stored: { response: unknown; usage: { verified: true; reference: string; inputTokens: number; outputTokens: number }; routeRefs?: Record<string, unknown> } | null = null;
  const resultJournal = {
    getModelCallResult: async () => stored,
    recordModelCallResult: async (input: { response: unknown; usage: { verified: true; reference: string; inputTokens: number; outputTokens: number }; routeRefs: Record<string, unknown> }) => {
      events.push("journal");
      stored = { response: input.response, usage: input.usage, routeRefs: input.routeRefs };
    },
  };
  const executor = new MeteredProviderCallExecutor({
    routingConfig: routingAuthority({ ...config(), version: 7, publishedAt: 10 }),
    providerQuota: {
      acquireGrant: async (input) => ({ grantId: `grant:${input.requestId}`, requestId: input.requestId, tenantId: input.tenantId,
        provider: input.provider, routeVersion: input.routeVersion, requests: input.requests, tokensPerSecond: input.tokensPerSecond,
        status: "GRANTED", expiresAt: 99_999, policyVersion: 3 }),
      getGrantByRequestId: async () => null,
      beginCall: async () => ({ state: "STARTED" }),
      completeCall: async () => {
        events.push("quota-complete");
        quotaCompletions += 1;
        if (quotaCompletions === 1) throw new Error("simulated lost response from quota owner");
        return {} as never;
      },
      markUnknown: async () => ({} as never),
    },
    budget: {
      reserve: async (input) => ({ holdId: input.holdId, amountMinor: input.amountMinor, currency: input.currency,
        priceVersion: input.priceVersion, status: "HELD" }),
      settle: async () => ({ status: "SETTLED" }),
      markUnknown: async () => {},
    },
    provider: {
      call: async () => {
        providerCalls += 1;
        events.push("provider");
        return { response: { content_text: "durable answer" }, usage: { verified: true, reference: "provider-response-9", inputTokens: 5, outputTokens: 7 } };
      },
    },
    inputTokenEstimator: serverInputEstimator,
    resultJournal,
  });
  const request = {
    callId: "call-journal-1", tenantId: "tenant-1", taskId: "task-1", poolName: "DeepSeek",
    periodKey: "2026-11", payload: { messages: [{ role: "user", content: "hello" }] },
  };

  await assert.rejects(executor.execute(request), /simulated lost response/);
  const recovered = await executor.execute(request);
  assert.equal(recovered.status, "SETTLED");
  if (recovered.status !== "SETTLED") throw new Error("expected settled recovered provider result");
  assert.deepEqual(recovered.response, { content_text: "durable answer" });
  assert.equal(recovered.recovered, true);
  assert.equal(providerCalls, 1, "a result already journaled in TaskCell must not call the provider again");
  assert.ok(events.indexOf("journal") < events.indexOf("quota-complete"));
});

test("strict hard-budget calls reject without an explicit credible input, output, and price bound", async () => {
  const events: string[] = [];
  const executor = new MeteredProviderCallExecutor({
    routingConfig: routingAuthority({ ...config({ routes: [{ ...route, maxOutputTokens: undefined }] }), version: 1, publishedAt: 1 }),
    providerQuota: {
      acquireGrant: async () => { events.push("quota"); throw new Error("should not request quota"); },
      getGrantByRequestId: async () => null,
      beginCall: async () => ({ state: "STARTED" }), completeCall: async () => ({} as never), markUnknown: async () => ({} as never),
    },
    budget: {
      reserve: async () => { events.push("reserve"); throw new Error("should not reserve"); },
      settle: async () => ({}), markUnknown: async () => {},
    },
    provider: { call: async () => { events.push("provider"); return {}; } },
    inputTokenEstimator: serverInputEstimator,
  });

  await assert.rejects(
    executor.execute({ callId: "unbounded-call", tenantId: "tenant-1", taskId: "task-1", poolName: "DeepSeek", periodKey: "2026-11", payload: {} }),
    { code: "COST_BOUND_UNAVAILABLE" },
  );
  assert.deepEqual(events, []);
});

test("an input estimate below the trusted token count rejects before reserve or provider execution", async () => {
  const events: string[] = [];
  const executor = new MeteredProviderCallExecutor({
    routingConfig: routingAuthority({ ...config(), version: 1, publishedAt: 1 }),
    providerQuota: {
      acquireGrant: async () => { events.push("quota"); throw new Error("should not request quota"); },
      getGrantByRequestId: async () => null,
      beginCall: async () => ({ state: "STARTED" }),
      completeCall: async () => ({} as never),
      markUnknown: async () => ({} as never),
    },
    budget: {
      reserve: async () => { events.push("reserve"); throw new Error("should not reserve"); },
      settle: async () => ({}),
      markUnknown: async () => ({}),
    },
    provider: { call: async () => { events.push("provider"); return {}; } },
    inputTokenEstimator: {
      estimateUpperBound: async () => ({ estimatedTokens: 101, upperBoundTokens: 100 }),
    },
  });

  await assert.rejects(
    executor.execute({
      callId: "underestimated-input",
      tenantId: "tenant-1",
      taskId: "task-1",
      poolName: "DeepSeek",
      periodKey: "2026-11",
      payload: { prompt: "too many tokens", callerClaimedInputTokens: 1 },
    }),
    { code: "COST_BOUND_UNAVAILABLE" },
  );
  assert.deepEqual(events, []);
});

test("provider calls without verified usage preserve the hold as unknown", async () => {
  const events: string[] = [];
  const executor = new MeteredProviderCallExecutor({
    routingConfig: routingAuthority({ ...config(), version: 2, publishedAt: 2 }),
    providerQuota: {
      acquireGrant: async (input) => ({ grantId: `grant:${input.requestId}`, requestId: input.requestId, tenantId: input.tenantId, provider: input.provider,
        routeVersion: input.routeVersion, requests: input.requests, tokensPerSecond: input.tokensPerSecond,
        status: "GRANTED", expiresAt: 99_999, policyVersion: 1 }),
      getGrantByRequestId: async () => null,
      beginCall: async () => ({ state: "STARTED" }), completeCall: async () => { events.push("complete"); return {} as never; },
      markUnknown: async () => { events.push("quota-unknown"); return {} as never; },
    },
    budget: {
      reserve: async (input) => ({ holdId: input.holdId, amountMinor: input.amountMinor, currency: input.currency, priceVersion: input.priceVersion, status: "HELD" }),
      settle: async () => { events.push("settle"); return {}; },
      markUnknown: async (_input) => { events.push("budget-unknown"); },
    },
    provider: { call: async () => ({ response: { text: "partial" } }) },
    inputTokenEstimator: serverInputEstimator,
  });

  const result = await executor.execute({
    callId: "unknown-call", tenantId: "tenant-1", taskId: "task-1", poolName: "DeepSeek", periodKey: "2026-11",
    requestedMaxOutputTokens: 50, payload: {},
  });

  assert.equal(result.status, "OUTCOME_UNKNOWN");
  assert.deepEqual(events, ["budget-unknown", "quota-unknown"]);
  assert.ok(!events.includes("settle"));
});

test("a retry after a routing publish reuses the durable route pin and the same cost quote", async () => {
  const state = new MockDurableObjectState("tenant-1");
  const cell = new RoutingConfigCell(state, {} as never);
  await cell.publish({ operationId: "retry-config-1", expectedVersion: 0, configuration: config() });
  const quotes: Array<{ amount: number; price: string; version: number }> = [];
  const routingConfig = {
    getCurrent: async (_tenantId: string) => cell.getCurrent(),
    getVersion: async (_tenantId: string, version: number) => cell.getVersion(version),
    pinCall: async (_tenantId: string, callId: string, poolName: string, version?: number) => cell.pinCall({ callId, poolName, routingVersion: version }),
  };
  const providerQuota = {
    acquireGrant: async () => { throw new Error("quota authority response was lost"); },
    getGrantByRequestId: async () => null,
    beginCall: async () => ({ state: "STARTED" as const }),
    completeCall: async () => ({}) as never,
    markUnknown: async () => ({}) as never,
  };
  const executor = new MeteredProviderCallExecutor({
    routingConfig,
    providerQuota,
    budget: {
      reserve: async (input) => {
        quotes.push({ amount: input.amountMinor, price: input.priceVersion, version: input.routingVersion });
        return { holdId: input.holdId, amountMinor: input.amountMinor, currency: input.currency, priceVersion: input.priceVersion, status: "HELD" };
      },
      settle: async () => ({}),
      markUnknown: async () => ({}),
    },
    provider: { call: async () => { throw new Error("must not call without a grant response"); } },
    inputTokenEstimator: serverInputEstimator,
  });
  const call = { callId: "pin-before-reserve", tenantId: "tenant-1", taskId: "task-1", poolName: "DeepSeek", periodKey: "2026-11", requestedMaxOutputTokens: 100, payload: {} };
  const first = await executor.execute(call);
  await cell.publish({
    operationId: "retry-config-2",
    expectedVersion: 1,
    configuration: config({ routes: [{ ...route, pricing: { ...route.pricing, priceVersion: "deepseek-2026-11", outputMinorPerMillionTokens: 90_000 } }] }),
  });
  const retry = await executor.execute(call);
  assert.equal(first.status, "OUTCOME_UNKNOWN");
  assert.equal(retry.status, "OUTCOME_UNKNOWN");
  assert.deepEqual(quotes, [
    { amount: 3, price: "deepseek-2026-10", version: 1 },
    { amount: 3, price: "deepseek-2026-10", version: 1 },
  ]);
});

test("fallback gets its own hold, and usage above its bound leaves both attempts frozen", async () => {
  const events: string[] = [];
  const fallbackRoute = {
    ...route,
    poolName: "Qwen-3.8",
    provider: "qwen",
    modelId: "qwen-plus",
    quotaDomain: "account:qwen",
    pricing: { ...route.pricing, priceVersion: "qwen-2026-10" },
  };
  const executor = new MeteredProviderCallExecutor({
    routingConfig: routingAuthority({ ...config({ routes: [route, fallbackRoute] }), version: 8, publishedAt: 80 }),
    providerQuota: {
      acquireGrant: async (input) => {
        events.push(`quota:${input.requestId}`);
        return { grantId: `grant:${input.requestId}`, requestId: input.requestId, tenantId: input.tenantId, provider: input.provider,
          routeVersion: input.routeVersion, requests: input.requests, tokensPerSecond: input.tokensPerSecond,
          status: "GRANTED", expiresAt: 99_999, policyVersion: 1 };
      },
      getGrantByRequestId: async () => null,
      beginCall: async (_grantId, requestId) => { events.push(`begin:${requestId}`); return { state: "STARTED" }; },
      completeCall: async () => { events.push("complete"); return {} as never; },
      markUnknown: async (_grantId, requestId) => { events.push(`quota-unknown:${requestId}`); return {} as never; },
    },
    budget: {
      reserve: async (input) => {
        events.push(`reserve:${input.holdId}`);
        return { holdId: input.holdId, amountMinor: input.amountMinor, currency: input.currency, priceVersion: input.priceVersion, status: "HELD" };
      },
      settle: async (input) => { events.push(`settle:${input.holdId}`); return {}; },
      markUnknown: async (input) => { events.push(`budget-unknown:${input.holdId}`); return {}; },
    },
    provider: {
      call: async (input) => {
        events.push(`provider:${input.callId}`);
        if (input.callId === "primary-call") return { response: { partial: true } };
        return { response: { text: "too much usage" }, usage: { verified: true, reference: "qwen-resp-2", inputTokens: 10, outputTokens: 21 } };
      },
    },
    inputTokenEstimator: serverInputEstimator,
  });

  const primary = await executor.execute({
    callId: "primary-call", tenantId: "tenant-1", taskId: "task-1", poolName: "DeepSeek", periodKey: "2026-11",
    requestedMaxOutputTokens: 30, payload: {},
  });
  const fallback = await executor.execute({
    callId: "fallback-call", fallbackOfCallId: "primary-call", tenantId: "tenant-1", taskId: "task-1", poolName: "Qwen-3.8",
    periodKey: "2026-11", requestedMaxOutputTokens: 20, payload: {},
  });

  assert.equal(primary.status, "OUTCOME_UNKNOWN");
  assert.equal(fallback.status, "OUTCOME_UNKNOWN");
  assert.notEqual(primary.holdId, fallback.holdId);
  assert.notEqual(primary.grantId, fallback.grantId);
  assert.ok(events.indexOf(`reserve:${fallback.holdId}`) < events.indexOf("provider:fallback-call"));
  assert.ok(events.includes(`budget-unknown:${primary.holdId}`));
  assert.ok(events.includes(`budget-unknown:${fallback.holdId}`));
  assert.equal(events.filter((event) => event.startsWith("settle:")).length, 0);
});

test("live single-pool adapter enforces output cap and returns provider usage evidence", async () => {
  let receivedIdempotencyKey = "";
  let receivedPayload: Record<string, unknown> = {};
  const server = http.createServer((request, response) => {
    receivedIdempotencyKey = String(request.headers["idempotency-key"] ?? "");
    let body = "";
    request.setEncoding("utf8");
    request.on("data", (chunk: string) => { body += chunk; });
    request.on("end", () => {
      receivedPayload = JSON.parse(body) as Record<string, unknown>;
      response.writeHead(200, { "content-type": "text/event-stream", "x-request-id": "provider-request-17" });
      response.write(`data: ${JSON.stringify({ choices: [{ delta: { content: "ok" } }] })}\n\n`);
      response.write(`data: ${JSON.stringify({ choices: [], usage: { prompt_tokens: 8, completion_tokens: 4, total_tokens: 12 } })}\n\n`);
      response.write("data: [DONE]\n\n");
      response.end();
    });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as AddressInfo).port;
  try {
    const adapter = new LiveOpenAiCompatibleSseAdapter([{
      poolName: "DeepSeek", baseUrl: `http://127.0.0.1:${port}/v1/chat/completions`, apiKey: "test-key", modelId: "deepseek-chat",
    }]);
    const result = await adapter.streamSinglePoolCompletionTurn({
      pool: "DeepSeek", messages: [{ role: "user", content: "hello" }], maxOutputTokens: 40, idempotencyKey: "call-live-1",
    });
    assert.equal(result.failoverAttempted, false);
    assert.equal(receivedIdempotencyKey, "call-live-1");
    assert.equal(receivedPayload.max_tokens, 40);
    assert.deepEqual(result.usageEvidence, { verified: true, reference: "provider-request-17", inputTokens: 8, outputTokens: 4 });
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});
