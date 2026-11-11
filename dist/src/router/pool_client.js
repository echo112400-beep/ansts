"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ModelPoolClientAdapter = void 0;
const live_llm_adapter_js_1 = require("./live_llm_adapter.js");
const breaker_limiter_js_1 = require("./breaker_limiter.js");
class ModelPoolClientAdapter {
    pricePer1kInputUsd = new Map();
    pricePer1kOutputUsd = new Map();
    sseAdapter = new live_llm_adapter_js_1.LiveOpenAiCompatibleSseAdapter();
    buckets = new Map();
    breakers = new Map();
    setPricing(endpointId, inputPer1kUsd, outputPer1kUsd) {
        const safeIn = Number.isFinite(inputPer1kUsd) && inputPer1kUsd >= 0 ? inputPer1kUsd : 0.003;
        const safeOut = Number.isFinite(outputPer1kUsd) && outputPer1kUsd >= 0 ? outputPer1kUsd : 0.015;
        this.pricePer1kInputUsd.set(endpointId, safeIn);
        this.pricePer1kOutputUsd.set(endpointId, safeOut);
    }
    getOrCreateBreaker(endpointId) {
        let br = this.breakers.get(endpointId);
        if (!br) {
            br = new breaker_limiter_js_1.ThreeStateCircuitBreaker(3, 15_000);
            this.breakers.set(endpointId, br);
        }
        return br;
    }
    getOrCreateBucket(endpointId, maxRpm = 120, maxTpm = 240_000) {
        let b = this.buckets.get(endpointId);
        if (!b) {
            b = new breaker_limiter_js_1.SlidingWindowTokenBucket(maxRpm, maxTpm);
            this.buckets.set(endpointId, b);
        }
        return b;
    }
    executeInference(endpoint, req, prefixCacheMatchedRatio = 0.5, nowMs = Date.now()) {
        const breaker = this.getOrCreateBreaker(endpoint.endpoint_id);
        if (!endpoint.healthy) {
            breaker.recordFailure(nowMs);
            throw new Error(`Endpoint ${endpoint.endpoint_id} is currently unhealthy (${breaker.getState(nowMs)})`);
        }
        const slot = breaker.tryAcquireRequestSlot(nowMs);
        if (!slot.admitted) {
            throw new Error(`Endpoint ${endpoint.endpoint_id} circuit breaker rejected request (state=${slot.state})`);
        }
        const safePromptTokens = Number.isFinite(req.estimatedPromptTokens) && req.estimatedPromptTokens >= 0
            ? Math.floor(req.estimatedPromptTokens)
            : 0;
        const safeMaxOutTokens = Number.isFinite(req.maxOutputTokens) && req.maxOutputTokens >= 0
            ? Math.floor(req.maxOutputTokens)
            : 0;
        const clampedCacheRatio = Number.isFinite(prefixCacheMatchedRatio)
            ? Math.max(0, Math.min(1, prefixCacheMatchedRatio))
            : 0;
        const bucket = this.getOrCreateBucket(endpoint.endpoint_id);
        const totalRequestedTokens = safePromptTokens + safeMaxOutTokens;
        const consumeRes = bucket.tryConsume(totalRequestedTokens, nowMs);
        if (!consumeRes.allowed) {
            if (slot.isHalfOpenProbe) {
                breaker.releaseHalfOpenProbe();
            }
            if (consumeRes.unserviceable) {
                throw new Error(`QuotaCapacityExceeded on ${endpoint.endpoint_id}: requested ${totalRequestedTokens} tokens exceeds bucket capacity`);
            }
            throw new Error(`RateLimitExceeded on ${endpoint.endpoint_id}: wait ${consumeRes.waitMs}ms`);
        }
        const cachedTokens = Math.min(safePromptTokens, Math.floor(safePromptTokens * clampedCacheRatio));
        const uncachedTokens = Math.max(0, safePromptTokens - cachedTokens);
        const baseTtftMs = Math.max(15, endpoint.ttft_ewma_ms * (1 - 0.35 * clampedCacheRatio));
        const outputText = req.requireJsonSchema
            ? JSON.stringify({ action: "act", tool: "shell_exec", turn: req.turnIndex })
            : `Completed reasoning step ${req.turnIndex} on ${endpoint.endpoint_id}`;
        const tokenChunks = outputText.match(/.{1,12}/g) ?? [outputText];
        const sseSummary = this.sseAdapter.streamAndParseCompletion({
            modelName: endpoint.model_name,
            tokens: tokenChunks,
            promptTokens: safePromptTokens,
            cachedPromptTokens: cachedTokens,
            baseTtftMs,
            perTokenStepMs: 4.5,
        });
        const totalLatencyMs = baseTtftMs + safeMaxOutTokens * 4.5;
        const inRate = this.pricePer1kInputUsd.get(endpoint.endpoint_id) ?? 0.003;
        const outRate = this.pricePer1kOutputUsd.get(endpoint.endpoint_id) ?? 0.015;
        const costUsd = Math.max(0, ((uncachedTokens + cachedTokens * 0.1) / 1000) * inRate + (safeMaxOutTokens / 1000) * outRate);
        breaker.recordSuccess();
        endpoint.ttft_ewma_ms = Math.round(endpoint.ttft_ewma_ms * 0.7 + sseSummary.ttftMs * 0.3);
        return {
            endpointId: endpoint.endpoint_id,
            provider: endpoint.provider,
            modelName: endpoint.model_name,
            ttftMs: baseTtftMs,
            tpotMs: sseSummary.tpotMs,
            totalLatencyMs,
            promptTokens: safePromptTokens,
            completionTokens: safeMaxOutTokens,
            cachedTokens: sseSummary.cachedPromptTokens,
            costUsd: Number(costUsd.toFixed(6)),
            outputText: sseSummary.assembledContent,
            circuitState: breaker.getState(nowMs),
            sseChunksReceived: sseSummary.chunksReceived,
        };
    }
}
exports.ModelPoolClientAdapter = ModelPoolClientAdapter;
