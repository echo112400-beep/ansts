"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KvCacheBlockEvictionManager = void 0;
class KvCacheBlockEvictionManager {
    maxKvBlocksPerSession;
    blocks = new Map();
    constructor(maxKvBlocksPerSession = 32) {
        this.maxKvBlocksPerSession = maxKvBlocksPerSession;
    }
    registerOrTouchBlock(params) {
        this.blocks.set(params.blockId, {
            blockId: params.blockId,
            taskId: params.taskId,
            tokenCount: params.tokenCount,
            attentionScore: params.attentionScore,
            isAttentionSink: params.isAttentionSink,
            lastAccessedTurnSeq: params.turnSeq,
        });
        const sessionBlocks = Array.from(this.blocks.values()).filter((b) => b.taskId === params.taskId);
        const evictedBlockIds = [];
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
exports.KvCacheBlockEvictionManager = KvCacheBlockEvictionManager;
