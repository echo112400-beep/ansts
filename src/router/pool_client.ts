/**
 * TASK-12: ModelPoolClientAdapter for DeepSeek, GLM-5.3, and Qwen-3.8 Streaming & Token Metering.
 */

import { EFFORT_MULTIPLIERS, ModelPoolName, ReasoningEffort } from "../models";

export interface SseTokenChunk {
  modelPool: ModelPoolName;
  chunkIndex: number;
  deltaType: "thinking" | "content";
  text: string;
  tokens: number;
}

export interface TurnMeteringSummary {
  modelPool: ModelPoolName;
  effectiveEffort: ReasoningEffort;
  inputTokens: number;
  thinkingTokens: number;
  outputTokens: number;
  ttftSec: number;
  totalTokens: number;
}

export class ModelPoolClientAdapter {
  public static readonly POOL_BASE_TTFT: Record<ModelPoolName, number> = {
    DeepSeek: 0.40,
    "GLM-5.3": 0.18,
    "Qwen-3.8": 0.35,
  };

  public streamCompletion(
    modelPool: ModelPoolName,
    effectiveEffort: ReasoningEffort,
    promptTokens: number = 400,
  ): { chunks: SseTokenChunk[]; summary: TurnMeteringSummary } {
    const mult = EFFORT_MULTIPLIERS[effectiveEffort] ?? 1.0;
    const cotTokens = mult > 1.0 ? Math.round(120 * (mult - 0.8)) : 0;
    const outTokens = 180;

    const chunks: SseTokenChunk[] = [];
    if (cotTokens > 0) {
      chunks.push({
        modelPool,
        chunkIndex: 0,
        deltaType: "thinking",
        text: `[${modelPool} CoT (${effectiveEffort}) reasoning...]`,
        tokens: cotTokens,
      });
    }
    chunks.push({
      modelPool,
      chunkIndex: chunks.length,
      deltaType: "content",
      text: `[${modelPool} action response]`,
      tokens: outTokens,
    });

    const summary: TurnMeteringSummary = {
      modelPool,
      effectiveEffort,
      inputTokens: promptTokens,
      thinkingTokens: cotTokens,
      outputTokens: outTokens,
      ttftSec: ModelPoolClientAdapter.POOL_BASE_TTFT[modelPool] ?? 0.40,
      totalTokens: promptTokens + cotTokens + outTokens,
    };
    return { chunks, summary };
  }
}
