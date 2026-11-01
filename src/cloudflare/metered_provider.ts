import { ModelPoolName } from "../models";
import { ModelRouteConfig } from "../persistence/routing-config";
import {
  ProviderCallAdapter,
  TrustedInputTokenEstimate,
  TrustedInputTokenEstimator,
} from "../router/provider_call";
import { CloudflareCelldEnv } from "./env";

interface ProviderEndpoint {
  url: string;
  apiKey: string;
}

/** Uses Workers/Celld fetch, buffers one committed result, and never performs model failover. */
export class FetchOpenAiCompatibleProviderCallAdapter implements ProviderCallAdapter {
  private readonly endpoints: Partial<Record<ModelPoolName, ProviderEndpoint>>;

  constructor(private readonly env: CloudflareCelldEnv) {
    this.endpoints = {
      DeepSeek: configuredEndpoint(env.DEEPSEEK_CHAT_COMPLETIONS_URL, env.DEEPSEEK_API_KEY),
      "GLM-5.3": configuredEndpoint(env.GLM53_CHAT_COMPLETIONS_URL, env.GLM53_API_KEY),
      "Qwen-3.8": configuredEndpoint(env.QWEN_CHAT_COMPLETIONS_URL, env.QWEN_API_KEY),
    };
  }

  public hasEndpoint(pool: string): boolean {
    return isModelPoolName(pool) && this.endpoints[pool] !== undefined;
  }

  public preflight(input: { tenantId: string; taskId: string; route: ModelRouteConfig; payload: unknown }): void {
    if (!isModelPoolName(input.route.poolName) || !this.endpoints[input.route.poolName]) {
      throw new Error(`Provider endpoint is not configured for ${input.route.poolName}.`);
    }
    normalizeMessages(input.payload);
  }

  public async call(input: Parameters<ProviderCallAdapter["call"]>[0]): Promise<{
    response?: unknown;
    usage?: import("../persistence/provider-quota").ProviderUsageEvidence;
  }> {
    if (!isModelPoolName(input.route.poolName)) throw new TypeError(`Unsupported model pool: ${input.route.poolName}.`);
    const endpoint = this.endpoints[input.route.poolName];
    if (!endpoint) throw new Error(`Provider endpoint is not configured for ${input.route.poolName}.`);
    const messages = normalizeMessages(input.payload);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30_000);
    try {
      const request = new Request(endpoint.url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          accept: "text/event-stream, application/json",
          authorization: `Bearer ${endpoint.apiKey}`,
          "idempotency-key": input.idempotencyKey,
        },
        body: JSON.stringify({
          model: input.route.modelId,
          stream: true,
          stream_options: { include_usage: true },
          messages,
          reasoning_effort: input.reasoningEffort ?? "high",
          max_tokens: input.maxOutputTokens,
        }),
        signal: controller.signal,
      });
      const response = this.env.MODEL_PROVIDER_PROXY
        ? await this.env.MODEL_PROVIDER_PROXY.fetch(request)
        : await fetch(request);
      if (!response.ok) {
        await response.body?.cancel().catch(() => undefined);
        throw new Error(`Provider returned HTTP ${response.status}.`);
      }
      const requestId = response.headers.get("x-request-id") ?? response.headers.get("request-id") ?? undefined;
      const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
      if (contentType.includes("application/json")) {
        return parseJsonCompletion(await response.json(), requestId);
      }
      return parseSseCompletion(await response.text(), requestId, input.route.modelId);
    } finally {
      clearTimeout(timeout);
    }
  }
}

/** A server-side byte bound: UTF-8 payload bytes bound content tokens, with framing overhead per message. */
export class SerializedPayloadTokenUpperBoundEstimator implements TrustedInputTokenEstimator {
  public estimateUpperBound(payload: unknown, _route: ModelRouteConfig): TrustedInputTokenEstimate {
    const messages = normalizeMessages(payload);
    const bytes = new TextEncoder().encode(JSON.stringify(messages)).byteLength;
    const estimatedTokens = bytes;
    const upperBoundTokens = bytes + messages.length * 32 + 64;
    return { estimatedTokens, upperBoundTokens };
  }
}

export function normalizeChatMessages(value: unknown): Array<{ role: "system" | "user" | "assistant"; content: string }> {
  return normalizeMessages(value);
}

function normalizeMessages(value: unknown): Array<{ role: "system" | "user" | "assistant"; content: string }> {
  if (!Array.isArray(value) || value.length === 0) throw new TypeError("Provider payload must contain a non-empty messages array.");
  return value.map((message, index) => {
    if (!message || typeof message !== "object") throw new TypeError(`messages[${index}] must be an object.`);
    const record = message as Record<string, unknown>;
    if (record.role !== "system" && record.role !== "user" && record.role !== "assistant") {
      throw new TypeError(`messages[${index}].role is invalid.`);
    }
    if (typeof record.content !== "string") throw new TypeError(`messages[${index}].content must be a string.`);
    return { role: record.role, content: record.content };
  });
}

function configuredEndpoint(url: string | undefined, apiKey: string | undefined): ProviderEndpoint | undefined {
  if (!url || !apiKey) return undefined;
  const parsed = new URL(url);
  if (parsed.protocol !== "https:" && parsed.hostname !== "localhost" && parsed.hostname !== "127.0.0.1") {
    throw new TypeError("Provider endpoints must use HTTPS except for explicit local development endpoints.");
  }
  return { url: parsed.toString(), apiKey };
}

function isModelPoolName(value: string): value is ModelPoolName {
  return value === "DeepSeek" || value === "GLM-5.3" || value === "Qwen-3.8";
}

function parseJsonCompletion(value: unknown, headerRequestId?: string): {
  response: unknown;
  usage?: import("../persistence/provider-quota").ProviderUsageEvidence;
} {
  if (!value || typeof value !== "object") throw new Error("Provider returned an invalid completion response.");
  const body = value as Record<string, any>;
  const usage = extractUsage(body.usage, headerRequestId ?? body.id);
  return {
    response: {
      provider_request_id: headerRequestId ?? body.id,
      model: body.model,
      choices: body.choices ?? [],
    },
    usage,
  };
}

function parseSseCompletion(text: string, headerRequestId: string | undefined, requestedModel: string): {
  response: unknown;
  usage?: import("../persistence/provider-quota").ProviderUsageEvidence;
} {
  let requestId = headerRequestId;
  let model = requestedModel;
  let inputTokens: number | undefined;
  let outputTokens: number | undefined;
  let reasoningText = "";
  let contentText = "";
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line.startsWith("data:")) continue;
    const data = line.slice(5).trim();
    if (!data || data === "[DONE]") continue;
    let chunk: Record<string, any>;
    try {
      chunk = JSON.parse(data) as Record<string, any>;
    } catch {
      throw new Error("Provider returned malformed SSE data.");
    }
    if (typeof chunk.id === "string" && chunk.id.length > 0) requestId = chunk.id;
    if (typeof chunk.model === "string") model = chunk.model;
    const choice = Array.isArray(chunk.choices) ? chunk.choices[0] : undefined;
    const delta = choice?.delta;
    if (typeof delta?.reasoning_content === "string") reasoningText += delta.reasoning_content;
    if (typeof delta?.content === "string") contentText += delta.content;
    const usage = chunk.usage;
    inputTokens = usage?.prompt_tokens ?? usage?.input_tokens ?? inputTokens;
    outputTokens = usage?.completion_tokens ?? usage?.output_tokens ?? outputTokens;
  }
  const usage = extractUsage({ prompt_tokens: inputTokens, completion_tokens: outputTokens }, requestId);
  return {
    response: { provider_request_id: requestId, model, reasoning_text: reasoningText, content_text: contentText },
    usage,
  };
}

function extractUsage(raw: unknown, reference: unknown): import("../persistence/provider-quota").ProviderUsageEvidence | undefined {
  if (!raw || typeof raw !== "object" || typeof reference !== "string" || reference.length === 0) return undefined;
  const usage = raw as Record<string, unknown>;
  const inputTokens = usage.prompt_tokens ?? usage.input_tokens;
  const outputTokens = usage.completion_tokens ?? usage.output_tokens;
  if (!Number.isSafeInteger(inputTokens) || Number(inputTokens) < 0 ||
      !Number.isSafeInteger(outputTokens) || Number(outputTokens) < 0) return undefined;
  return { verified: true, reference, inputTokens: Number(inputTokens), outputTokens: Number(outputTokens) };
}
