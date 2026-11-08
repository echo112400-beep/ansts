/**
 * @fileoverview M3 TASK-06: Multi-Provider LLM Pool Client Adapter with Token Accounting.
 */
import { LlmEndpointMetrics } from "../models.js";

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
  totalLatencyMs: number;
  promptTokens: number;
  completionTokens: number;
  cachedTokens: number;
  costUsd: number;
  outputText: string;
}

export class ModelPoolClientAdapter {
  private readonly pricePer1kInputUsd: Map<string, number> = new Map();
  private readonly pricePer1kOutputUsd: Map<string, number> = new Map();

  public setPricing(endpointId: string, inputPer1kUsd: number, outputPer1kUsd: number): void {
    this.pricePer1kInputUsd.set(endpointId, inputPer1kUsd);
    this.pricePer1kOutputUsd.set(endpointId, outputPer1kUsd);
  }

  public executeInference(
    endpoint: LlmEndpointMetrics,
    req: LlmInferenceRequest,
    prefixCacheMatchedRatio = 0.5
  ): LlmInferenceResponse {
    if (!endpoint.healthy) {
      throw new Error(`Endpoint ${endpoint.endpoint_id} is currently unhealthy`);
    }
    const cachedTokens = Math.floor(req.estimatedPromptTokens * prefixCacheMatchedRatio);
    const uncachedTokens = req.estimatedPromptTokens - cachedTokens;
    const ttftMs = Math.max(15, endpoint.ttft_ewma_ms * (1 - 0.35 * prefixCacheMatchedRatio));
    const totalLatencyMs = ttftMs + req.maxOutputTokens * 4.5;

    const inRate = this.pricePer1kInputUsd.get(endpoint.endpoint_id) ?? 0.003;
    const outRate = this.pricePer1kOutputUsd.get(endpoint.endpoint_id) ?? 0.015;
    const costUsd =
      ((uncachedTokens + cachedTokens * 0.1) / 1000) * inRate + (req.maxOutputTokens / 1000) * outRate;

    return {
      endpointId: endpoint.endpoint_id,
      provider: endpoint.provider,
      modelName: endpoint.model_name,
      ttftMs,
      totalLatencyMs,
      promptTokens: req.estimatedPromptTokens,
      completionTokens: req.maxOutputTokens,
      cachedTokens,
      costUsd: Number(costUsd.toFixed(6)),
      outputText: req.requireJsonSchema
        ? JSON.stringify({ action: "act", tool: "shell_exec", turn: req.turnIndex })
        : `Completed reasoning step ${req.turnIndex} on ${endpoint.endpoint_id}`,
    };
  }
}
