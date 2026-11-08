/**
 * @fileoverview Sandbox network chaos proxy for loss, jitter, and partition injection.
 */
export interface NetworkChaosConfig {
  dropPacketPct: number;
  jitterMs: number;
  faultStatusCode?: 200 | 429 | 503;
  rateLimitExceeded: boolean;
}
export class SandboxNetworkChaosProxy {
  constructor(public config: NetworkChaosConfig = { dropPacketPct: 0, jitterMs: 0, rateLimitExceeded: false }) {}
  public async interceptOutboundFetch(url: string, init?: { method?: string; body?: string }): Promise<{ status: number; latencyMs: number; dropped: boolean; bodyText: string }> {
    const startHr = process.hrtime.bigint();
    const rand = Math.random() * 100;
    if (rand < this.config.dropPacketPct) {
      return { status: 0, latencyMs: 0, dropped: true, bodyText: "NETWORK_PACKET_DROPPED_BY_CHAOS_PROXY" };
    }
    const jitter = Math.floor(Math.random() * this.config.jitterMs);
    if (jitter > 0) {
      await new Promise((r) => setTimeout(r, Math.min(15, jitter)));
    }
    let status = 200;
    let bodyText = JSON.stringify({ success: true, echoedUrl: url, method: init?.method ?? "GET" });
    if (this.config.rateLimitExceeded || this.config.faultStatusCode === 429) {
      status = 429;
      bodyText = JSON.stringify({ error: "HTTP 429 Too Many Requests" });
    } else if (this.config.faultStatusCode === 503) {
      status = 503;
      bodyText = JSON.stringify({ error: "HTTP 503 Service Unavailable" });
    }
    const latencyMs = Number(Math.max(0.1, Number(process.hrtime.bigint() - startHr) / 1_000_000 + jitter).toFixed(3));
    return { status, latencyMs, dropped: false, bodyText };
  }
}
