import { ModelPoolName } from "../models";
import { ProviderCallAdapter } from "./provider_call";
import { LiveOpenAiCompatibleSseAdapter } from "./live_llm_adapter";

/** Adapts a single SSE endpoint attempt to the metered-call contract; it never fails over itself. */
export class LiveOpenAiProviderCallAdapter implements ProviderCallAdapter {
  constructor(private readonly adapter: LiveOpenAiCompatibleSseAdapter) {}

  public async call(input: Parameters<ProviderCallAdapter["call"]>[0]): Promise<{ response?: unknown; usage?: import("../persistence/provider-quota").ProviderUsageEvidence }> {
    const pool = asModelPoolName(input.route.poolName);
    const messages = normalizeMessages(input.payload);
    const result = await this.adapter.streamSinglePoolCompletionTurn({
      pool,
      modelId: input.route.modelId,
      messages,
      reasoningEffort: input.reasoningEffort,
      maxOutputTokens: input.maxOutputTokens,
      idempotencyKey: input.idempotencyKey,
    });
    return { response: result, usage: result.usageEvidence };
  }
}

export function asModelPoolName(value: string): ModelPoolName {
  if (value === "DeepSeek" || value === "GLM-5.3" || value === "Qwen-3.8") return value;
  throw new TypeError(`Unsupported model pool: ${value}.`);
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
