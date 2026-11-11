/**
 * @fileoverview M75-1 / N4: Cross-Node eBPF Dynamic Network Tracing & Microsecond Circuit Breaker
 * Provides:
 * - Real socket-level RTT tracking with RFC 6298 EWMA smoothed RTT (SRTT) and variance (RTTVAR).
 * - Sliding-window packet loss ratio and socket buffer queue pressure calculation.
 * - Microsecond-grade circuit breaking (TRIPPED_MICROS_OPEN) that intercepts outbound socket transfers
 *   at the simulated eBPF tc/sock_ops egress hook before kernel sk_buff backlog queue overflow.
 * - Cooldown half-open probe transitioning and SQLite persistence (persisted_ebpf_socket_traces).
 */
import { SqlStorageLike } from "../cloudflare/env.js";

export type SocketProtocol = "TCP" | "UDP" | "VSOCK";
export type SocketBreakerState = "CLOSED" | "HALF_OPEN" | "TRIPPED_MICROS_OPEN";

export interface EbpfSocketSample {
  peerEndpoint: string;
  protocol?: SocketProtocol;
  rttUs: number;
  retransmitted: boolean;
  bufferBytes: number;
  maxBufferBytes: number;
  timestampUs?: number;
}

export interface EbpfSocketNetworkTelemetry {
  peerEndpoint: string;
  protocol: SocketProtocol;
  srttUs: number;
  rttVarUs: number;
  latestRttUs: number;
  packetsSent: number;
  packetsRetransmitted: number;
  packetLossRatio: number; // 0..1
  socketBufferPressurePct: number; // 0..100
  circuitBreakerState: SocketBreakerState;
  lastTripTimestampUs: number;
  totalTripsCount: number;
  droppedPacketsCount: number;
}

export interface SocketInterceptResult {
  allowed: boolean;
  reason: string;
  circuitBreakerState: SocketBreakerState;
  srttUs: number;
  lossRatioPct: number;
  bufferPressurePct: number;
}

export class EbpfSocketNetworkTrackerAndBreaker {
  private readonly socketTelemetry: Map<string, EbpfSocketNetworkTelemetry> = new Map();
  private readonly recentWindowSamples: Map<
    string,
    Array<{ rttUs: number; retransmitted: boolean; timestampUs: number }>
  > = new Map();

  // Thresholds for microsecond circuit trip
  public readonly p99LatencyThresholdUs: number;
  public readonly packetLossThresholdRatio: number;
  public readonly bufferPressureThresholdPct: number;
  public readonly cooldownDurationUs: number;

  constructor(
    private readonly sql?: SqlStorageLike,
    options?: {
      p99LatencyThresholdUs?: number;
      packetLossThresholdRatio?: number;
      bufferPressureThresholdPct?: number;
      cooldownDurationUs?: number;
    }
  ) {
    this.p99LatencyThresholdUs = options?.p99LatencyThresholdUs ?? 50_000; // 50ms
    this.packetLossThresholdRatio = options?.packetLossThresholdRatio ?? 0.15; // 15%
    this.bufferPressureThresholdPct = options?.bufferPressureThresholdPct ?? 80.0; // 80%
    this.cooldownDurationUs = options?.cooldownDurationUs ?? 200_000; // 200ms

    if (this.sql) {
      this.initTable();
      this.rehydrateFromSql();
    }
  }

  private initTable(): void {
    if (!this.sql) return;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS persisted_ebpf_socket_traces (
        peer_endpoint TEXT PRIMARY KEY,
        protocol TEXT NOT NULL,
        srtt_us REAL NOT NULL,
        rtt_var_us REAL NOT NULL,
        loss_ratio REAL NOT NULL,
        buffer_pressure_pct REAL NOT NULL,
        breaker_state TEXT NOT NULL,
        total_trips INTEGER NOT NULL,
        dropped_packets INTEGER NOT NULL,
        updated_at_us INTEGER NOT NULL
      )
    `);
  }

  private rehydrateFromSql(): void {
    if (!this.sql) return;
    try {
      const rows = this.sql.exec<Record<string, unknown>>(
        "SELECT peer_endpoint, protocol, srtt_us, rtt_var_us, loss_ratio, buffer_pressure_pct, breaker_state, total_trips, dropped_packets, updated_at_us FROM persisted_ebpf_socket_traces"
      ).toArray();
      for (const row of rows) {
        const ep = String(row.peer_endpoint);
        this.socketTelemetry.set(ep, {
          peerEndpoint: ep,
          protocol: (row.protocol as SocketProtocol) ?? "TCP",
          srttUs: Number(row.srtt_us ?? 0),
          rttVarUs: Number(row.rtt_var_us ?? 0),
          latestRttUs: Number(row.srtt_us ?? 0),
          packetsSent: 100,
          packetsRetransmitted: Math.round(Number(row.loss_ratio ?? 0) * 100),
          packetLossRatio: Number(row.loss_ratio ?? 0),
          socketBufferPressurePct: Number(row.buffer_pressure_pct ?? 0),
          circuitBreakerState: (row.breaker_state as SocketBreakerState) ?? "CLOSED",
          lastTripTimestampUs: Number(row.updated_at_us ?? 0),
          totalTripsCount: Number(row.total_trips ?? 0),
          droppedPacketsCount: Number(row.dropped_packets ?? 0),
        });
      }
    } catch {
      // Table may be empty on first run
    }
  }

  /**
   * Records a raw socket sample captured via eBPF sock_ops or TCP probe.
   * Updates RFC 6298 EWMA SRTT, RTTVAR, and evaluates microsecond trip conditions.
   */
  public recordSocketSample(sample: EbpfSocketSample): EbpfSocketNetworkTelemetry {
    const peer = sample.peerEndpoint.trim();
    const nowUs = sample.timestampUs ?? Date.now() * 1000;
    const protocol: SocketProtocol = sample.protocol ?? "TCP";

    let state = this.socketTelemetry.get(peer);
    if (!state) {
      state = {
        peerEndpoint: peer,
        protocol,
        srttUs: sample.rttUs,
        rttVarUs: sample.rttUs / 2,
        latestRttUs: sample.rttUs,
        packetsSent: 0,
        packetsRetransmitted: 0,
        packetLossRatio: 0,
        socketBufferPressurePct: 0,
        circuitBreakerState: "CLOSED",
        lastTripTimestampUs: 0,
        totalTripsCount: 0,
        droppedPacketsCount: 0,
      };
      this.socketTelemetry.set(peer, state);
    }

    state.packetsSent += 1;
    if (sample.retransmitted) {
      state.packetsRetransmitted += 1;
    }
    state.latestRttUs = sample.rttUs;

    // RFC 6298 EWMA calculation
    // SRTT = (1 - alpha) * SRTT + alpha * RTT (alpha = 0.125)
    // RTTVAR = (1 - beta) * RTTVAR + beta * |SRTT - RTT| (beta = 0.25)
    const alpha = 0.125;
    const beta = 0.25;
    const rttDiff = Math.abs(state.srttUs - sample.rttUs);
    state.rttVarUs = Number(((1 - beta) * state.rttVarUs + beta * rttDiff).toFixed(2));
    state.srttUs = Number(((1 - alpha) * state.srttUs + alpha * sample.rttUs).toFixed(2));

    // Sliding window for recent packet loss calculation (last 20 samples)
    let window = this.recentWindowSamples.get(peer);
    if (!window) {
      window = [];
      this.recentWindowSamples.set(peer, window);
    }
    window.push({
      rttUs: sample.rttUs,
      retransmitted: sample.retransmitted,
      timestampUs: nowUs,
    });
    if (window.length > 20) {
      window.shift();
    }

    const retransmitsInWindow = window.filter((w) => w.retransmitted).length;
    state.packetLossRatio = Number((retransmitsInWindow / window.length).toFixed(4));

    // Buffer pressure
    const maxBuf = Math.max(1, sample.maxBufferBytes);
    state.socketBufferPressurePct = Number(
      Math.min(100, Math.max(0, (sample.bufferBytes / maxBuf) * 100)).toFixed(2)
    );

    // Evaluate trip conditions:
    // 1. Loss ratio exceeds threshold (e.g. > 15%)
    // 2. Buffer pressure exceeds threshold (e.g. > 80%)
    // 3. P99 / smoothed RTT exceeds threshold (e.g. > 50ms)
    const shouldTrip =
      state.packetLossRatio >= this.packetLossThresholdRatio ||
      state.socketBufferPressurePct >= this.bufferPressureThresholdPct ||
      state.srttUs >= this.p99LatencyThresholdUs;

    if (shouldTrip && state.circuitBreakerState !== "TRIPPED_MICROS_OPEN") {
      state.circuitBreakerState = "TRIPPED_MICROS_OPEN";
      state.lastTripTimestampUs = nowUs;
      state.totalTripsCount += 1;
    } else if (state.circuitBreakerState === "HALF_OPEN" && !shouldTrip) {
      state.circuitBreakerState = "CLOSED";
    }

    this.persistTelemetry(state, nowUs);
    return state;
  }

  /**
   * Evaluates whether an outbound packet to peerEndpoint should be allowed or dropped
   * at the simulated eBPF tc egress hook.
   */
  public interceptOutboundSocketTransfer(
    peerEndpoint: string,
    nowUs = Date.now() * 1000
  ): SocketInterceptResult {
    const peer = peerEndpoint.trim();
    const state = this.socketTelemetry.get(peer);

    if (!state) {
      return {
        allowed: true,
        reason: "NO_TELEMETRY_RECORDED_DEFAULT_ALLOW",
        circuitBreakerState: "CLOSED",
        srttUs: 0,
        lossRatioPct: 0,
        bufferPressurePct: 0,
      };
    }

    // Check cooldown if tripped
    if (state.circuitBreakerState === "TRIPPED_MICROS_OPEN") {
      const elapsedUs = nowUs - state.lastTripTimestampUs;
      if (elapsedUs >= this.cooldownDurationUs) {
        state.circuitBreakerState = "HALF_OPEN";
      } else {
        state.droppedPacketsCount += 1;
        this.persistTelemetry(state, nowUs);
        return {
          allowed: false,
          reason: `eBPF_TC_EGRESS_CIRCUIT_TRIPPED: RTT=${state.srttUs}us, Loss=${(state.packetLossRatio * 100).toFixed(1)}%, Buffer=${state.socketBufferPressurePct}% (remaining cooldown: ${Math.round((this.cooldownDurationUs - elapsedUs) / 1000)}ms)`,
          circuitBreakerState: "TRIPPED_MICROS_OPEN",
          srttUs: state.srttUs,
          lossRatioPct: state.packetLossRatio * 100,
          bufferPressurePct: state.socketBufferPressurePct,
        };
      }
    }

    // HALF_OPEN allows probe packet
    if (state.circuitBreakerState === "HALF_OPEN") {
      return {
        allowed: true,
        reason: "HALF_OPEN_SINGLE_PROBE_ALLOWED",
        circuitBreakerState: "HALF_OPEN",
        srttUs: state.srttUs,
        lossRatioPct: state.packetLossRatio * 100,
        bufferPressurePct: state.socketBufferPressurePct,
      };
    }

    return {
      allowed: true,
      reason: "SOCKET_CIRCUIT_CLOSED_NORMAL_EGRESS",
      circuitBreakerState: "CLOSED",
      srttUs: state.srttUs,
      lossRatioPct: state.packetLossRatio * 100,
      bufferPressurePct: state.socketBufferPressurePct,
    };
  }

  public getSocketTelemetry(peerEndpoint: string): EbpfSocketNetworkTelemetry | undefined {
    return this.socketTelemetry.get(peerEndpoint.trim());
  }

  public listAllTelemetry(): EbpfSocketNetworkTelemetry[] {
    return Array.from(this.socketTelemetry.values());
  }

  public resetSocketBreaker(peerEndpoint: string): boolean {
    const peer = peerEndpoint.trim();
    const state = this.socketTelemetry.get(peer);
    if (!state) return false;
    state.circuitBreakerState = "CLOSED";
    state.packetLossRatio = 0;
    state.socketBufferPressurePct = 0;
    this.recentWindowSamples.delete(peer);
    this.persistTelemetry(state, Date.now() * 1000);
    return true;
  }

  private persistTelemetry(state: EbpfSocketNetworkTelemetry, nowUs: number): void {
    if (!this.sql) return;
    this.sql.exec(
      `INSERT OR REPLACE INTO persisted_ebpf_socket_traces 
       (peer_endpoint, protocol, srtt_us, rtt_var_us, loss_ratio, buffer_pressure_pct, breaker_state, total_trips, dropped_packets, updated_at_us)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      state.peerEndpoint,
      state.protocol,
      state.srttUs,
      state.rttVarUs,
      state.packetLossRatio,
      state.socketBufferPressurePct,
      state.circuitBreakerState,
      state.totalTripsCount,
      state.droppedPacketsCount,
      nowUs
    );
  }
}
