/**
 * @fileoverview TASK-84: Production Sizing & SLA Capacity Admission Enforcer.
 *
 * Implements the ANSTS v2.3 sizing & reliability constraints:
 * 1. Brain (`celld` Isolate): 128 MB heap cap (>=75% triggers GC, >=90% trips admission circuit breaker),
 *    ~0.47 MB per active Cell budget, and <3000ms cold/crash rehydration RTO SLA verification.
 * 2. Body Host (`NodeDaemon`): Max 32 concurrent sandboxes per 16vCPU/64GB bare-metal host,
 *    <1.5ms Cgroups v2 phase boost SLA, and <0.15ms UDS/AF_VSOCK fastpath SLA.
 * 3. Tunnel Controller (`TunnelControllerServer`): Max 20,000 concurrent reverse mTLS HTTP/2 streams
 *    per 4vCPU/8GB gateway instance, <0.95ms 1-Hop relay SLA, and <15ms secondary failover SLA.
 * 4. RustFS S3 EC:4 Storage: 12 drives (3 nodes x 4 drives) with 4 parity blocks (tolerates up to 4
 *    simultaneous drive failures without data loss) and <12ms ETag CAS latency SLA.
 */

export interface BrainIsolateSizingSnapshot {
  isolateId: string;
  heapUsedMb: number;
  heapLimitMb: number;
  activeCells: number;
  lastRehydrateRtoMs: number;
}

export interface BodyHostSizingSnapshot {
  hostId: string;
  vcpuCount: number;
  ramGb: number;
  activeSandboxes: number;
  maxSandboxesLimit: number;
  lastCgroupBoostLatencyMs: number;
  lastVsockFastpathLatencyMs: number;
}

export interface TunnelGatewaySizingSnapshot {
  gatewayId: string;
  activeReverseStreams: number;
  maxStreamsLimit: number;
  lastOneHopRelayLatencyMs: number;
  lastFailoverLatencyMs: number;
}

export interface RustFsEc4ClusterStatus {
  totalDrives: number;
  dataBlocks: number;
  parityBlocks: number;
  healthyDrives: number;
  failedDrives: number;
  lastEtagCasLatencyMs: number;
}

export interface SizingAdmissionVerdict {
  allowed: boolean;
  gcTriggered: boolean;
  circuitBreakerTripped: boolean;
  slaViolations: string[];
  estimatedCellRamMb: number;
}

export class CapacitySlaEnforcer {
  public static readonly BRAIN_HEAP_LIMIT_MB = 128;
  public static readonly BRAIN_GC_WATERMARK_RATIO = 0.75;
  public static readonly BRAIN_SHED_WATERMARK_RATIO = 0.90;
  public static readonly CELL_FOOTPRINT_MB = 0.47;
  public static readonly BRAIN_RTO_SLA_MS = 3000;

  public static readonly BODY_MAX_SANDBOXES_PER_HOST = 32;
  public static readonly BODY_CGROUP_BOOST_SLA_MS = 1.5;
  public static readonly BODY_VSOCK_FASTPATH_SLA_MS = 0.15;

  public static readonly TUNNEL_MAX_STREAMS_PER_GW = 20000;
  public static readonly TUNNEL_ONE_HOP_RELAY_SLA_MS = 0.95;
  public static readonly TUNNEL_FAILOVER_SLA_MS = 15.0;

  public static readonly RUSTFS_EC4_PARITY_BLOCKS = 4;
  public static readonly RUSTFS_CAS_SLA_MS = 12.0;

  /**
   * Evaluates Brain Isolate heap watermarks, active Cell memory footprint, and RTO SLA.
   */
  public evaluateBrainIsolateAdmission(snapshot: BrainIsolateSizingSnapshot): SizingAdmissionVerdict {
    const limitMb = snapshot.heapLimitMb || CapacitySlaEnforcer.BRAIN_HEAP_LIMIT_MB;
    const estimatedCellRamMb = Number((snapshot.activeCells * CapacitySlaEnforcer.CELL_FOOTPRINT_MB).toFixed(2));
    const effectiveHeapMb = Math.max(snapshot.heapUsedMb, estimatedCellRamMb);
    const ratio = effectiveHeapMb / limitMb;

    const gcTriggered = ratio >= CapacitySlaEnforcer.BRAIN_GC_WATERMARK_RATIO;
    const circuitBreakerTripped = ratio >= CapacitySlaEnforcer.BRAIN_SHED_WATERMARK_RATIO;
    const slaViolations: string[] = [];

    if (snapshot.lastRehydrateRtoMs > CapacitySlaEnforcer.BRAIN_RTO_SLA_MS) {
      slaViolations.push(
        `Brain Rehydration RTO ${snapshot.lastRehydrateRtoMs}ms exceeds SLA (${CapacitySlaEnforcer.BRAIN_RTO_SLA_MS}ms)`,
      );
    }
    if (circuitBreakerTripped) {
      slaViolations.push(
        `Brain Isolate ${snapshot.isolateId} heap ${effectiveHeapMb.toFixed(1)}MB/${limitMb}MB (${(ratio * 100).toFixed(1)}%) >= 90% shedding watermark`,
      );
    }

    return {
      allowed: !circuitBreakerTripped,
      gcTriggered,
      circuitBreakerTripped,
      slaViolations,
      estimatedCellRamMb,
    };
  }

  /**
   * Evaluates Body Host 32-sandbox hard cap and Cgroups/VSOCK latency SLAs.
   */
  public evaluateBodyHostAdmission(snapshot: BodyHostSizingSnapshot): {
    allowed: boolean;
    availableSlots: number;
    slaViolations: string[];
  } {
    const maxLimit = snapshot.maxSandboxesLimit || CapacitySlaEnforcer.BODY_MAX_SANDBOXES_PER_HOST;
    const availableSlots = Math.max(0, maxLimit - snapshot.activeSandboxes);
    const slaViolations: string[] = [];

    if (snapshot.activeSandboxes >= maxLimit) {
      slaViolations.push(
        `BodyHost ${snapshot.hostId} reached max sandbox capacity (${snapshot.activeSandboxes}/${maxLimit})`,
      );
    }
    if (snapshot.lastCgroupBoostLatencyMs > CapacitySlaEnforcer.BODY_CGROUP_BOOST_SLA_MS) {
      slaViolations.push(
        `Cgroups v2 boost latency ${snapshot.lastCgroupBoostLatencyMs}ms exceeds ${CapacitySlaEnforcer.BODY_CGROUP_BOOST_SLA_MS}ms SLA`,
      );
    }
    if (snapshot.lastVsockFastpathLatencyMs > CapacitySlaEnforcer.BODY_VSOCK_FASTPATH_SLA_MS) {
      slaViolations.push(
        `UDS/AF_VSOCK fastpath latency ${snapshot.lastVsockFastpathLatencyMs}ms exceeds ${CapacitySlaEnforcer.BODY_VSOCK_FASTPATH_SLA_MS}ms SLA`,
      );
    }

    return {
      allowed: snapshot.activeSandboxes < maxLimit,
      availableSlots,
      slaViolations,
    };
  }

  /**
   * Evaluates TunnelControllerServer 20,000 stream cap and 1-Hop Relay / Failover SLAs.
   */
  public evaluateTunnelGatewayCapacity(snapshot: TunnelGatewaySizingSnapshot): {
    allowed: boolean;
    utilizationRatio: number;
    slaViolations: string[];
  } {
    const maxStreams = snapshot.maxStreamsLimit || CapacitySlaEnforcer.TUNNEL_MAX_STREAMS_PER_GW;
    const utilizationRatio = snapshot.activeReverseStreams / maxStreams;
    const slaViolations: string[] = [];

    if (snapshot.activeReverseStreams >= maxStreams) {
      slaViolations.push(
        `TunnelGateway ${snapshot.gatewayId} stream cap reached (${snapshot.activeReverseStreams}/${maxStreams})`,
      );
    }
    if (snapshot.lastOneHopRelayLatencyMs > CapacitySlaEnforcer.TUNNEL_ONE_HOP_RELAY_SLA_MS) {
      slaViolations.push(
        `1-Hop relay latency ${snapshot.lastOneHopRelayLatencyMs}ms exceeds ${CapacitySlaEnforcer.TUNNEL_ONE_HOP_RELAY_SLA_MS}ms SLA`,
      );
    }
    if (snapshot.lastFailoverLatencyMs > CapacitySlaEnforcer.TUNNEL_FAILOVER_SLA_MS) {
      slaViolations.push(
        `Secondary tunnel failover ${snapshot.lastFailoverLatencyMs}ms exceeds ${CapacitySlaEnforcer.TUNNEL_FAILOVER_SLA_MS}ms SLA`,
      );
    }

    return {
      allowed: snapshot.activeReverseStreams < maxStreams,
      utilizationRatio,
      slaViolations,
    };
  }

  /**
   * Evaluates RustFS S3 EC:4 (4 parity blocks) storage quorum and ETag CAS latency SLA.
   */
  public evaluateRustFsEc4Reliability(status: RustFsEc4ClusterStatus): {
    rpoZeroGuaranteed: boolean;
    quorumIntact: boolean;
    remainingFaultToleranceDrives: number;
    slaViolations: string[];
  } {
    const quorumIntact = status.failedDrives <= status.parityBlocks;
    const remainingFaultToleranceDrives = Math.max(0, status.parityBlocks - status.failedDrives);
    const slaViolations: string[] = [];

    if (!quorumIntact) {
      slaViolations.push(
        `RustFS EC:${status.parityBlocks} quorum lost: ${status.failedDrives} drives failed > ${status.parityBlocks} parity blocks`,
      );
    }
    if (status.lastEtagCasLatencyMs > CapacitySlaEnforcer.RUSTFS_CAS_SLA_MS) {
      slaViolations.push(
        `RustFS ETag CAS latency ${status.lastEtagCasLatencyMs}ms exceeds ${CapacitySlaEnforcer.RUSTFS_CAS_SLA_MS}ms SLA`,
      );
    }

    return {
      rpoZeroGuaranteed: quorumIntact,
      quorumIntact,
      remainingFaultToleranceDrives,
      slaViolations,
    };
  }
}
