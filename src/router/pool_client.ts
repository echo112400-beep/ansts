/**
 * @fileoverview M3 TASK-06: Multi-Provider LLM Pool Client Adapter with Token Accounting,
 * SSE Stream Assembly (`LiveOpenAiCompatibleSseAdapter`), Sliding-Window Token Bucket, and 3-State Circuit Breaker.
 */
import { LlmEndpointMetrics } from "../models.js";
import { LiveOpenAiCompatibleSseAdapter } from "./live_llm_adapter.js";
import { SlidingWindowTokenBucket, ThreeStateCircuitBreaker, CircuitState } from "./breaker_limiter.js";

export interface LlmInferenceRequest {
  taskId: string;
  turnIndex: number;
  prompt: string;
  estimatedPromptTokens: number;
  maxOutputTokens: number;
  requireJsonSchema?: boolean;
}

export interface LlmInferenceResponse {
  endpointId: string;
  provider: string;
  modelName: string;
  ttftMs: number;
  tpotMs?: number;
  totalLatencyMs: number;
  promptTokens: number;
  completionTokens: number;
  cachedTokens: number;
  costUsd: number;
  outputText: string;
  circuitState?: CircuitState;
  sseChunksReceived?: number;
}

export class ModelPoolClientAdapter {
  private readonly pricePer1kInputUsd: Map<string, number> = new Map();
  private readonly pricePer1kOutputUsd: Map<string, number> = new Map();
  private readonly sseAdapter = new LiveOpenAiCompatibleSseAdapter();
  private readonly buckets: Map<string, SlidingWindowTokenBucket> = new Map();
  private readonly breakers: Map<string, ThreeStateCircuitBreaker> = new Map();

  public setPricing(endpointId: string, inputPer1kUsd: number, outputPer1kUsd: number): void {
    this.pricePer1kInputUsd.set(endpointId, inputPer1kUsd);
    this.pricePer1kOutputUsd.set(endpointId, outputPer1kUsd);
  }

  public getOrCreateBreaker(endpointId: string): ThreeStateCircuitBreaker {
    let br = this.breakers.get(endpointId);
    if (!br) {
      br = new ThreeStateCircuitBreaker(3, 15_000);
      this.breakers.set(endpointId, br);
    }
    return br;
  }

  public getOrCreateBucket(endpointId: string, maxRpm = 120, maxTpm = 240_000): SlidingWindowTokenBucket {
    let b = this.buckets.get(endpointId);
    if (!b) {
      b = new SlidingWindowTokenBucket(maxRpm, maxTpm);
      this.buckets.set(endpointId, b);
    }
    return b;
  }

  public executeInference(
    endpoint: LlmEndpointMetrics,
    req: LlmInferenceRequest,
    prefixCacheMatchedRatio = 0.5
  ): LlmInferenceResponse {
    const breaker = this.getOrCreateBreaker(endpoint.endpoint_id);
    if (!endpoint.healthy || !breaker.canPass()) {
      breaker.recordFailure();
      throw new Error(`Endpoint ${endpoint.endpoint_id} is currently unhealthy or circuit OPEN (${breaker.getState()})`);
    }

    const bucket = this.getOrCreateBucket(endpoint.endpoint_id);
    const totalRequestedTokens = req.estimatedPromptTokens + req.maxOutputTokens;
    const consumeRes = bucket.tryConsume(totalRequestedTokens);
    if (!consumeRes.allowed) {
      throw new Error(`RateLimitExceeded on ${endpoint.endpoint_id}: wait ${consumeRes.waitMs}ms`);
    }

    const cachedTokens = Math.floor(req.estimatedPromptTokens * prefixCacheMatchedRatio);
    const uncachedTokens = req.estimatedPromptTokens - cachedTokens;
    const baseTtftMs = Math.max(15, endpoint.ttft_ewma_ms * (1 - 0.35 * prefixCacheMatchedRatio));

    const outputText = req.requireJsonSchema
      ? JSON.stringify({ action: "act", tool: "shell_exec", turn: req.turnIndex })
      : `Completed reasoning step ${req.turnIndex} on ${endpoint.endpoint_id}`;
    const tokenChunks = outputText.match(/.{1,12}/g) ?? [outputText];

    const sseSummary = this.sseAdapter.streamAndParseCompletion({
      modelName: endpoint.model_name,
      tokens: tokenChunks,
      promptTokens: req.estimatedPromptTokens,
      cachedPromptTokens: cachedTokens,
      baseTtftMs,
      perTokenStepMs: 4.5,
    });

    const totalLatencyMs = baseTtftMs + req.maxOutputTokens * 4.5;

    const inRate = this.pricePer1kInputUsd.get(endpoint.endpoint_id) ?? 0.003;
    const outRate = this.pricePer1kOutputUsd.get(endpoint.endpoint_id) ?? 0.015;
    const costUsd =
      ((uncachedTokens + cachedTokens * 0.1) / 1000) * inRate + (req.maxOutputTokens / 1000) * outRate;

    breaker.recordSuccess();
    endpoint.ttft_ewma_ms = Math.round(endpoint.ttft_ewma_ms * 0.7 + sseSummary.ttftMs * 0.3);

    return {
      endpointId: endpoint.endpoint_id,
      provider: endpoint.provider,
      modelName: endpoint.model_name,
      ttftMs: baseTtftMs,
      tpotMs: sseSummary.tpotMs,
      totalLatencyMs,
      promptTokens: req.estimatedPromptTokens,
      completionTokens: req.maxOutputTokens,
      cachedTokens: sseSummary.cachedPromptTokens,
      costUsd: Number(costUsd.toFixed(6)),
      outputText: sseSummary.assembledContent,
      circuitState: breaker.getState(),
      sseChunksReceived: sseSummary.chunksReceived,
    };
  }
}
