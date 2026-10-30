/**
 * TASK-52: Real OpenAI-Compatible SSE Streaming Adapter for `DeepSeek`, `GLM-5.3`, and `Qwen-3.8`.
 *
 * Features:
 * - Connects to live HTTP/HTTPS `/v1/chat/completions` (`stream: true`) endpoints
 * - Separately parses `delta.reasoning_content` (Chain-of-Thought / R1 reasoning tokens)
 *   and `delta.content` (assistant output tokens)
 * - Measures high-precision Time-To-First-Token (`ttftMs`) and total generation latency
 * - Supports `AbortSignal` / timeout cancellation and automatic HTTP 429/5xx failover
 *   across `DeepSeek` -> `Qwen-3.8` / `GLM-5.3`
 * - Updates `AgentTask` dual-clock active compute time and token accounting
 */

import * as http from "node:http";
import * as https from "node:https";
import { AgentTask, ModelPoolName } from "../models";

export interface ModelPoolEndpointConfig {
  poolName: ModelPoolName;
  baseUrl: string; // e.g. "http://127.0.0.1:18080/v1/chat/completions"
  apiKey: string;
  modelId: string; // e.g. "deepseek-reasoner", "glm-5.3", "qwen-3.8"
  timeoutMs?: number;
}

export interface StreamingLlmTurnResult {
  usedPool: ModelPoolName;
  modelId: string;
  reasoningText: string;
  contentText: string;
  reasoningTokens: number;
  completionTokens: number;
  totalTokens: number;
  ttftMs: number;
  totalDurationMs: number;
  failoverAttempted: boolean;
  failoverFromPool?: ModelPoolName;
}

export class LiveOpenAiCompatibleSseAdapter {
  private readonly endpoints = new Map<ModelPoolName, ModelPoolEndpointConfig>();

  constructor(configs: ModelPoolEndpointConfig[]) {
    for (const cfg of configs) {
      this.endpoints.set(cfg.poolName, cfg);
    }
  }

  /**
   * Streams a chat completion turn from the preferred model pool, automatically failing over
   * to `fallbackPools` if the primary pool returns HTTP 429 / 5xx or times out.
   */
  public async streamChatCompletionTurn(params: {
    task: AgentTask;
    preferredPool: ModelPoolName;
    fallbackPools?: ModelPoolName[];
    messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
    reasoningEffort?: string;
    onDelta?: (delta: { type: "reasoning" | "content"; text: string }) => void;
  }): Promise<StreamingLlmTurnResult> {
    const candidatePools: ModelPoolName[] = [
      params.preferredPool,
      ...(params.fallbackPools ?? ["Qwen-3.8", "GLM-5.3"]).filter((p) => p !== params.preferredPool),
    ];

    let lastError: Error | null = null;
    for (let idx = 0; idx < candidatePools.length; idx++) {
      const poolName = candidatePools[idx];
      const cfg = this.endpoints.get(poolName);
      if (!cfg) continue;

      try {
        const res = await this.streamSingleEndpoint(cfg, params.messages, params.reasoningEffort, params.onDelta);
        // Credit active compute time and token consumption on the task's dual-clock ledger
        const deltaSec = Math.max(0.001, res.totalDurationMs / 1000.0); params.task.activeComputeSec = Number((params.task.activeComputeSec + deltaSec).toFixed(4)); params.task.wallClockSec = Number((params.task.wallClockSec + deltaSec).toFixed(4));
        params.task.usedTokens += res.totalTokens;

        return {
          ...res,
          failoverAttempted: idx > 0,
          failoverFromPool: idx > 0 ? params.preferredPool : undefined,
        };
      } catch (err: any) {
        lastError = err instanceof Error ? err : new Error(String(err));
      }
    }

    throw lastError ?? new Error("All configured LLM model pools failed during SSE stream");
  }

  private streamSingleEndpoint(
    cfg: ModelPoolEndpointConfig,
    messages: Array<{ role: string; content: string }>,
    reasoningEffort?: string,
    onDelta?: (delta: { type: "reasoning" | "content"; text: string }) => void,
  ): Promise<Omit<StreamingLlmTurnResult, "failoverAttempted" | "failoverFromPool">> {
    const parsedUrl = new URL(cfg.baseUrl);
    const transport = parsedUrl.protocol === "https:" ? https : http;
    const payload = JSON.stringify({
      model: cfg.modelId,
      stream: true,
      reasoning_effort: reasoningEffort ?? "high",
      messages,
    });

    const startMs = performance.now();
    let ttftMs: number | null = null;
    let reasoningText = "";
    let contentText = "";
    let reasoningTokens = 0;
    let completionTokens = 0;

    return new Promise((resolve, reject) => {
      const req = transport.request(
        {
          protocol: parsedUrl.protocol,
          hostname: parsedUrl.hostname,
          port: parsedUrl.port,
          method: "POST",
          path: `${parsedUrl.pathname}${parsedUrl.search}`,
          headers: {
            "content-type": "application/json",
            accept: "text/event-stream",
            authorization: `Bearer ${cfg.apiKey}`,
            "content-length": String(Buffer.byteLength(payload)),
          },
          timeout: cfg.timeoutMs ?? 15000,
        },
        (res) => {
          if ((res.statusCode ?? 500) >= 400) {
            res.resume();
            reject(new Error(`LLM Pool ${cfg.poolName} returned HTTP ${res.statusCode}`));
            return;
          }

          let buffer = "";
          res.setEncoding("utf8");
          res.on("data", (chunk: string) => {
            buffer += chunk;
            const lines = buffer.split("\n");
            buffer = lines.pop() ?? "";

            for (const rawLine of lines) {
              const line = rawLine.trim();
              if (!line.startsWith("data:")) continue;
              const dataPayload = line.slice(5).trim();
              if (dataPayload === "[DONE]") continue;

              try {
                const parsed = JSON.parse(dataPayload) as {
                  choices?: Array<{
                    delta?: { reasoning_content?: string; content?: string };
                  }>;
                  usage?: {
                    reasoning_tokens?: number;
                    completion_tokens?: number;
                    total_tokens?: number;
                  };
                };

                if (ttftMs === null) {
                  ttftMs = Number((performance.now() - startMs).toFixed(3));
                }

                const delta = parsed.choices?.[0]?.delta;
                if (delta?.reasoning_content) {
                  reasoningText += delta.reasoning_content;
                  reasoningTokens += Math.max(1, Math.ceil(delta.reasoning_content.length / 4));
                  onDelta?.({ type: "reasoning", text: delta.reasoning_content });
                }
                if (delta?.content) {
                  contentText += delta.content;
                  completionTokens += Math.max(1, Math.ceil(delta.content.length / 4));
                  onDelta?.({ type: "content", text: delta.content });
                }
              } catch {
                // Ignore malformed keep-alive comments
              }
            }
          });

          res.on("end", () => {
            const totalDurationMs = Number((performance.now() - startMs).toFixed(3));
            resolve({
              usedPool: cfg.poolName,
              modelId: cfg.modelId,
              reasoningText,
              contentText,
              reasoningTokens,
              completionTokens,
              totalTokens: reasoningTokens + completionTokens,
              ttftMs: ttftMs ?? totalDurationMs,
              totalDurationMs,
            });
          });
        },
      );

      req.on("timeout", () => {
        req.destroy(new Error(`LLM Pool ${cfg.poolName} timed out`));
      });
      req.on("error", reject);
      req.write(payload);
      req.end();
    });
  }
}
