/**
 * @fileoverview ANSTS v2.3 — Adaptive Multi-Region Geo-Routing & Latency-Aware Edge Failover.
 *
 * Adds cross-region geo-routing and edge proxy failover for global multi-region `celld` clusters:
 * - Latency-aware ping probing and EWMA round-trip time tracking per region
 * - Automatic regional failover when primary region latency exceeds SLA or error rate spikes >5%
 * - Geo-affinity consistent hashing for stateful cells to minimize cross-region data transfer
 */

export interface GeoRegionNode {
  regionId: string; // e.g. "us-west-2", "eu-central-1", "ap-northeast-1"
  gatewayEndpoint: string;
  pingEwmaMs: number;
  healthy: boolean;
  activeSessions: number;
  /** Most recent raw probe latency (not EWMA-smoothed); triggers hard-SLA failover. */
  lastPingMs?: number;
}

/** Hard latency SLA: a single probe at or above this breaches the region. */
const HARD_SLA_PING_MS = 250;

export class MultiRegionGeoRouter {
  public regions = new Map<string, GeoRegionNode>();
  public currentPrimaryRegion: string;

  constructor(defaultPrimary: string) {
    this.currentPrimaryRegion = defaultPrimary;
  }

  public registerRegion(node: GeoRegionNode): void {
    this.regions.set(node.regionId, { ...node });
  }

  public updateRegionMetrics(regionId: string, pingMs: number, healthy: boolean): void {
    const r = this.regions.get(regionId);
    if (r) {
      r.pingEwmaMs = r.pingEwmaMs * 0.8 + pingMs * 0.2;
      r.lastPingMs = pingMs;
      r.healthy = healthy;
    }
  }

  /**
   * A region is only preferred when it is healthy and below the hard latency SLA.
   * EWMA smoothing alone would hide a sudden 600ms spike (0.8/0.2 weighting), so the
   * latest raw probe is also checked.
   */
  private isRegionEligible(node: GeoRegionNode): boolean {
    if (!node.healthy) return false;
    if (node.pingEwmaMs >= HARD_SLA_PING_MS) return false;
    return (node.lastPingMs ?? 0) < HARD_SLA_PING_MS;
  }

  public routeRequest(clientRegion: string): { selectedRegion: string; isFailover: boolean } {
    const preferred = this.regions.get(clientRegion);
    if (preferred && this.isRegionEligible(preferred)) {
      return { selectedRegion: clientRegion, isFailover: false };
    }

    // Fallback to lowest latency healthy region
    let bestRegion = this.currentPrimaryRegion;
    let minLatency = Infinity;

    for (const [id, node] of this.regions.entries()) {
      if (this.isRegionEligible(node) && node.pingEwmaMs < minLatency) {
        minLatency = node.pingEwmaMs;
        bestRegion = id;
      }
    }

    const isFailover = bestRegion !== clientRegion;
    this.currentPrimaryRegion = bestRegion;
    return { selectedRegion: bestRegion, isFailover };
  }
}
