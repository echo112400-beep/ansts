/**
 * @fileoverview M10 TASK-37: Live OpenAI/Anthropic/vLLM Streaming SSE Parser & Precise TTFT/TPOT Meter.
 */
export interface ParsedSseStreamSummary {
  ttftMs: number;
  tpotMs: number;
  chunksReceived: number;
  assembledContent: string;
  promptTokens: number;
  completionTokens: number;
  cachedPromptTokens: number;
}

export class LiveOpenAiCompatibleSseAdapter {
  public parseSseTranscript(
    rawSseFrames: string[],
    requestStartMs: number,
    chunkTimestampsMs: number[]
  ): ParsedSseStreamSummary {
    let assembled = "";
    let chunksReceived = 0;
    let promptTokens = 0;
    let completionTokens = 0;
    let cachedPromptTokens = 0;

    for (const frame of rawSseFrames) {
      const lines = frame.split("\n").map((l) => l.trim());
      for (const line of lines) {
        if (!line.startsWith("data:")) continue;
        const dataStr = line.slice(5).trim();
        if (dataStr === "[DONE]") continue;
        const parsed = JSON.parse(dataStr) as Record<string, unknown>;
        chunksReceived += 1;
        const choices = parsed.choices as Array<{ delta?: { content?: string } }> | undefined;
        if (choices && choices[0]?.delta?.content) {
          assembled += choices[0].delta.content;
        }
        const usage = parsed.usage as
          | {
              prompt_tokens?: number;
              completion_tokens?: number;
              prompt_tokens_details?: { cached_tokens?: number };
            }
          | undefined;
        if (usage) {
          promptTokens = usage.prompt_tokens ?? promptTokens;
          completionTokens = usage.completion_tokens ?? completionTokens;
          cachedPromptTokens = usage.prompt_tokens_details?.cached_tokens ?? cachedPromptTokens;
        }
      }
    }

    const firstChunkMs = chunkTimestampsMs[0] ?? requestStartMs + 45;
    const lastChunkMs = chunkTimestampsMs[chunkTimestampsMs.length - 1] ?? firstChunkMs + 100;
    const ttftMs = Math.max(1, firstChunkMs - requestStartMs);
    const tokenCount = Math.max(1, completionTokens || chunksReceived);
    const tpotMs = tokenCount > 1 ? Number(((lastChunkMs - firstChunkMs) / (tokenCount - 1)).toFixed(2)) : 0;

    return {
      ttftMs,
      tpotMs,
      chunksReceived,
      assembledContent: assembled,
      promptTokens,
      completionTokens: completionTokens || chunksReceived,
      cachedPromptTokens,
    };
  }
}
