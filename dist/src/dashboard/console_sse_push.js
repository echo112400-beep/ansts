"use strict";
/**
 * @fileoverview M36 P0 (TASK-150): Full-Duplex Console SSE Real-Time Delta Push Bus
 * (`ConsoleSsePushGateway`).
 *
 * Streams real-time control-plane state mutations (`SLOT_MIGRATED`, `SPOT_HOST_EVACUATED`,
 * `HITL_STAGE_UPDATED`, `TASK_DISPATCHED`, `RBAC_AUDIT_LOGGED`, `GC_SWEEP_COMPLETED`) over
 * `GET /v1beta1/console/events/stream` (`text/event-stream`) in `< 10ms`.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConsoleSsePushGateway = void 0;
class ConsoleSsePushGateway {
    subscribers = new Set();
    recentEvents = [];
    nextEventId = 1;
    attachSseSubscriber(res) {
        res.writeHead(200, {
            "content-type": "text/event-stream; charset=utf-8",
            "cache-control": "no-cache, no-transform",
            connection: "keep-alive",
            "access-control-allow-origin": "*",
        });
        res.write(`retry: 2000\nevent: connected\ndata: ${JSON.stringify({ status: "LIVE_SSE_CONNECTED" })}\n\n`);
        this.subscribers.add(res);
        res.on("close", () => {
            this.subscribers.delete(res);
        });
    }
    broadcastDeltaEvent(eventType, s3CasGeneration, payload) {
        const startHr = process.hrtime.bigint();
        const envelope = {
            eventId: this.nextEventId++,
            eventType,
            s3CasGeneration,
            timestampMs: Date.now(),
            payload,
        };
        this.recentEvents.unshift(envelope);
        if (this.recentEvents.length > 50) {
            this.recentEvents.pop();
        }
        const wireChunk = `id: ${envelope.eventId}\nevent: console_delta\ndata: ${JSON.stringify(envelope)}\n\n`;
        let delivered = 0;
        for (const sub of this.subscribers) {
            if (!sub.writableEnded) {
                sub.write(wireChunk);
                delivered += 1;
            }
            else {
                this.subscribers.delete(sub);
            }
        }
        const broadcastLatencyMs = Number(Math.max(0.005, Number(process.hrtime.bigint() - startHr) / 1_000_000).toFixed(3));
        return {
            envelope,
            deliveredSubscribersCount: delivered,
            broadcastLatencyMs,
        };
    }
    closeAllSubscribers() {
        for (const sub of this.subscribers) {
            try {
                sub.end();
            }
            catch {
                // Ignore
            }
        }
        this.subscribers.clear();
    }
}
exports.ConsoleSsePushGateway = ConsoleSsePushGateway;
