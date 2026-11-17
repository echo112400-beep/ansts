/** @fileoverview Attention-sink-aware KV cache block eviction for the router (LRU + sink retention). */
export interface KvCacheMemoryBlock {
  blockId: string;
  taskId: string;
  tokenCount: number;
  attentionScore: number;
  isAttentionSink: boolean;
  lastAccessedTurnSeq: number;
}
export class KvCacheBlockEvictionManager {
  public readonly blocks = new Map<string, KvCacheMemoryBlock>();
  constructor(public readonly maxKvBlocksPerSession = 32) {}
  public registerOrTouchBlock(params: { blockId: string; taskId: string; tokenCount: number; attentionScore: number; isAttentionSink: boolean; turnSeq: number }): { totalBlocks: number; evictedBlockIds: string[] } {
    this.blocks.set(params.blockId, {
      blockId: params.blockId,
      taskId: params.taskId,
      tokenCount: params.tokenCount,
      attentionScore: params.attentionScore,
      isAttentionSink: params.isAttentionSink,
      lastAccessedTurnSeq: params.turnSeq,
    });
    const sessionBlocks = Array.from(this.blocks.values()).filter((b) => b.taskId === params.taskId);
    const evictedBlockIds: string[] = [];
    if (sessionBlocks.length > this.maxKvBlocksPerSession) {
      const sorted = sessionBlocks.filter((b) => !b.isAttentionSink).sort((a, b) => a.attentionScore - b.attentionScore);
      const overflowCount = sessionBlocks.length - this.maxKvBlocksPerSession;
      for (let i = 0; i < overflowCount && i < sorted.length; i++) {
        const victim = sorted[i];
        this.blocks.delete(victim.blockId);
        evictedBlockIds.push(victim.blockId);
      }
    }
    return { totalBlocks: Array.from(this.blocks.values()).filter((b) => b.taskId === params.taskId).length, evictedBlockIds };
  }
}
