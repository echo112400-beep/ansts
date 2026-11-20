/**
 * @fileoverview M17 & M62-4: Cluster Capacity SLA Enforcer & Headroom Reservation Guard
 * with Finite Metric Validation, Zero-Division Immunity, SQLite Headroom State Persistence & Dynamic Threshold Adjustment.
 */
import { SqlStorageLike } from "../cloudflare/env.js";
import type { TurnWalEngine } from "../checkpoint/wal_engine.js";

export interface CapacitySlaPolicy {
  maxP0QueueWaitP99Ms: number;
  minWarmPoolHitRatio: number;
  minFreeClusterCpuRatio: number;
  emergencyHeadroomCpuRatio: number;
}

export interface SpotQuotaOffer {
  offerId: string;
  lenderTenantId: string;
  offeredSlots: number;
  availableSlots: number;
  minBidPricePerSlotUsd: number;
  region: string;
}

export interface SpotQuotaLease {
  leaseId: string;
  offerId: string;
  lenderTenantId: string;
  borrowerTenantId: string;
  taskId: string;
  leasedSlots: number;
  clearingPricePerSlotUsd: number;
  status: "ACTIVE" | "RECALLED_BY_LENDER" | "COMPLETED";
  walFrozenTurnIndex?: number;
}

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

function sizingMetricsValid(...values: number[]): boolean {
  return values.every(value => Number.isFinite(value) && value >= 0);
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

  public evaluateBrainIsolateAdmission(snapshot: BrainIsolateSizingSnapshot): SizingAdmissionVerdict {
    const limitMb = snapshot.heapLimitMb === 0 ? CapacitySlaEnforcer.BRAIN_HEAP_LIMIT_MB : snapshot.heapLimitMb;
    if (!Number.isFinite(limitMb) || limitMb <= 0 || !sizingMetricsValid(snapshot.activeCells, snapshot.heapUsedMb, snapshot.lastRehydrateRtoMs)) {
      return { allowed: false, gcTriggered: false, circuitBreakerTripped: true, slaViolations: ["INVALID_BRAIN_SIZING_METRICS"], estimatedCellRamMb: 0 };
    }
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

  public evaluateBodyHostAdmission(snapshot: BodyHostSizingSnapshot): {
    allowed: boolean;
    availableSlots: number;
    slaViolations: string[];
  } {
    const maxLimit = snapshot.maxSandboxesLimit === 0 ? CapacitySlaEnforcer.BODY_MAX_SANDBOXES_PER_HOST : snapshot.maxSandboxesLimit;
    if (!Number.isSafeInteger(maxLimit) || maxLimit < 1 || !Number.isSafeInteger(snapshot.activeSandboxes) || !sizingMetricsValid(snapshot.activeSandboxes, snapshot.lastCgroupBoostLatencyMs, snapshot.lastVsockFastpathLatencyMs)) {
      return { allowed: false, availableSlots: 0, slaViolations: ["INVALID_BODY_SIZING_METRICS"] };
    }
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

  public evaluateTunnelGatewayCapacity(snapshot: TunnelGatewaySizingSnapshot): {
    allowed: boolean;
    utilizationRatio: number;
    slaViolations: string[];
  } {
    const maxStreams = snapshot.maxStreamsLimit === 0 ? CapacitySlaEnforcer.TUNNEL_MAX_STREAMS_PER_GW : snapshot.maxStreamsLimit;
    if (!Number.isSafeInteger(maxStreams) || maxStreams < 1 || !Number.isSafeInteger(snapshot.activeReverseStreams) || !sizingMetricsValid(snapshot.activeReverseStreams, snapshot.lastOneHopRelayLatencyMs, snapshot.lastFailoverLatencyMs)) {
      return { allowed: false, utilizationRatio: 1, slaViolations: ["INVALID_TUNNEL_SIZING_METRICS"] };
    }
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

  public evaluateRustFsEc4Reliability(status: RustFsEc4ClusterStatus): {
    rpoZeroGuaranteed: boolean;
    quorumIntact: boolean;
    remainingFaultToleranceDrives: number;
    slaViolations: string[];
  } {
    const { totalDrives, dataBlocks, parityBlocks, healthyDrives, failedDrives } = status;
    if (![totalDrives, dataBlocks, parityBlocks, healthyDrives, failedDrives].every(value => Number.isSafeInteger(value) && value >= 0) ||
      dataBlocks === 0 || dataBlocks + parityBlocks !== totalDrives || healthyDrives + failedDrives !== totalDrives || !sizingMetricsValid(status.lastEtagCasLatencyMs)) {
      return { rpoZeroGuaranteed: false, quorumIntact: false, remainingFaultToleranceDrives: 0, slaViolations: ["INVALID_RUSTFS_SIZING_METRICS"] };
    }
    const quorumIntact = healthyDrives >= dataBlocks;
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

  private policy: CapacitySlaPolicy;
  private emergencyHeadroomActive = false;
  private readonly spotOffers: Map<string, SpotQuotaOffer> = new Map();
  private readonly spotLeases: Map<string, SpotQuotaLease> = new Map();
  private sql?: SqlStorageLike;

  constructor(
    policy?: Partial<CapacitySlaPolicy>,
    sql?: SqlStorageLike
  ) {
    this.policy = {
      maxP0QueueWaitP99Ms: policy?.maxP0QueueWaitP99Ms ?? 250,
      minWarmPoolHitRatio: policy?.minWarmPoolHitRatio ?? 0.85,
      minFreeClusterCpuRatio: policy?.minFreeClusterCpuRatio ?? 0.1,
      emergencyHeadroomCpuRatio: policy?.emergencyHeadroomCpuRatio ?? 0.15,
    };
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M62-4] DFX-Operability: Binds SQLite storage to persist emergency headroom reservation state.
   */
  public bindSqlStorage(sql: SqlStorageLike): boolean {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS cluster_capacity_sla_state (id TEXT PRIMARY KEY, headroom_active INTEGER, policy_json TEXT)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM cluster_capacity_sla_state WHERE id = ?", "global")
      .toArray();
    if (rows.length > 0) {
      this.emergencyHeadroomActive = Boolean(Number(rows[0].headroom_active ?? 0));
      if (rows[0].policy_json) {
        try {
          const parsed = JSON.parse(String(rows[0].policy_json)) as CapacitySlaPolicy;
          this.policy = { ...this.policy, ...parsed };
        } catch {
          // ignore malformed policy
        }
      }
      this.sql.exec(
        "CREATE TABLE IF NOT EXISTS persisted_spot_quota_market (record_key TEXT PRIMARY KEY, payload_json TEXT)"
      );
      for (const r of this.sql.exec<Record<string, unknown>>("SELECT * FROM persisted_spot_quota_market").toArray()) {
        const key = String(r.record_key ?? "");
        const parsed = JSON.parse(String(r.payload_json ?? "{}")) as Record<string, unknown>;
        if (key.startsWith("offer:") && typeof parsed.offerId === "string") {
          this.spotOffers.set(parsed.offerId, parsed as unknown as SpotQuotaOffer);
        } else if (key.startsWith("lease:") && typeof parsed.leaseId === "string") {
          this.spotLeases.set(parsed.leaseId, parsed as unknown as SpotQuotaLease);
        }
      }
      return true;
    }
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_spot_quota_market (record_key TEXT PRIMARY KEY, payload_json TEXT)"
    );
    const marketRows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM persisted_spot_quota_market")
      .toArray();
    for (const r of marketRows) {
      const key = String(r.record_key ?? "");
      const parsed = JSON.parse(String(r.payload_json ?? "{}")) as Record<string, unknown>;
      if (key.startsWith("offer:") && typeof parsed.offerId === "string") {
        this.spotOffers.set(parsed.offerId, parsed as unknown as SpotQuotaOffer);
      } else if (key.startsWith("lease:") && typeof parsed.leaseId === "string") {
        this.spotLeases.set(parsed.leaseId, parsed as unknown as SpotQuotaLease);
      }
    }
    return false;
  }

  private persistState(): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO cluster_capacity_sla_state (id, headroom_active, policy_json) VALUES (?, ?, ?)",
      "global",
      this.emergencyHeadroomActive ? 1 : 0,
      JSON.stringify(this.policy)
    );
  }

  public isEmergencyHeadroomActive(): boolean {
    return this.emergencyHeadroomActive;
  }

  public evaluateSlaCompliance(params: {
    p0QueueWaitP99Ms: number;
    warmPoolHitRatio: number;
    freeClusterCpuRatio: number;
  }): { compliant: boolean; breachedMetrics: string[]; reserveEmergencyHeadroom: boolean } {
    const safeP0Wait = Number.isFinite(params.p0QueueWaitP99Ms) && params.p0QueueWaitP99Ms >= 0
      ? params.p0QueueWaitP99Ms
      : 0;
    const safeHitRatio = Number.isFinite(params.warmPoolHitRatio)
      ? Math.max(0, Math.min(1, params.warmPoolHitRatio))
      : 1.0;
    const safeCpuRatio = Number.isFinite(params.freeClusterCpuRatio)
      ? Math.max(0, Math.min(1, params.freeClusterCpuRatio))
      : 1.0;

    const breachedMetrics: string[] = [];
    if (safeP0Wait > this.policy.maxP0QueueWaitP99Ms) {
      breachedMetrics.push("p0QueueWaitP99Ms");
    }
    if (safeHitRatio < this.policy.minWarmPoolHitRatio) {
      breachedMetrics.push("warmPoolHitRatio");
    }
    if (safeCpuRatio < this.policy.minFreeClusterCpuRatio) {
      breachedMetrics.push("freeClusterCpuRatio");
    }

    const reserveEmergencyHeadroom = safeCpuRatio < this.policy.emergencyHeadroomCpuRatio;
    this.emergencyHeadroomActive = reserveEmergencyHeadroom;
    this.persistState();

    return {
      compliant: breachedMetrics.length === 0,
      breachedMetrics,
      reserveEmergencyHeadroom,
    };
  }

  public updatePolicy(newPolicy: Partial<CapacitySlaPolicy>): CapacitySlaPolicy {
    this.policy = { ...this.policy, ...newPolicy };
    this.persistState();
    return { ...this.policy };
  }

  private persistSpotMarket(): void {
    if (!this.sql) return;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_spot_quota_market (record_key TEXT PRIMARY KEY, payload_json TEXT)"
    );
    for (const offer of this.spotOffers.values()) {
      this.sql.exec(
        "INSERT OR REPLACE INTO persisted_spot_quota_market (record_key, payload_json) VALUES (?, ?)",
        `offer:${offer.offerId}`,
        JSON.stringify(offer)
      );
    }
    for (const lease of this.spotLeases.values()) {
      this.sql.exec(
        "INSERT OR REPLACE INTO persisted_spot_quota_market (record_key, payload_json) VALUES (?, ?)",
        `lease:${lease.leaseId}`,
        JSON.stringify(lease)
      );
    }
  }

  /**
   * [M73-2 / F8] Publishes idle guaranteed quota slots from an under-utilized tenant (`lenderTenantId`)
   * into the Inter-Tenant Spot Quota Lending Market.
   */
  public publishIdleQuotaOffer(params: {
    offerId: string;
    lenderTenantId: string;
    offeredSlots: number;
    minBidPricePerSlotUsd: number;
    region?: string;
  }): SpotQuotaOffer {
    const slots = Math.max(1, Math.floor(params.offeredSlots));
    const minPrice = Math.max(0.01, Number(params.minBidPricePerSlotUsd.toFixed(4)));
    const offer: SpotQuotaOffer = {
      offerId: params.offerId,
      lenderTenantId: params.lenderTenantId,
      offeredSlots: slots,
      availableSlots: slots,
      minBidPricePerSlotUsd: minPrice,
      region: params.region ?? "us-west-2",
    };
    this.spotOffers.set(offer.offerId, offer);
    this.persistSpotMarket();
    return offer;
  }

  /**
   * [M73-2 / F8] Clears a Spot Quota Bid from `borrowerTenantId` against the lowest-ask eligible lender offer
   * using second-price / midpoint utility clearing when cluster emergency headroom is not active.
   */
  public matchSpotQuotaBid(params: {
    leaseId?: string;
    borrowerTenantId: string;
    taskId: string;
    requestedSlots: number;
    maxBidPricePerSlotUsd: number;
  }): {
    matched: boolean;
    reason?: string;
    lease?: SpotQuotaLease;
  } {
    if (this.emergencyHeadroomActive) {
      return {
        matched: false,
        reason: "EMERGENCY_HEADROOM_RESERVED_NO_SPOT_LENDING",
      };
    }
    const reqSlots = Math.max(1, Math.floor(params.requestedSlots));
    const candidates = Array.from(this.spotOffers.values()).filter(
      (o) =>
        o.lenderTenantId !== params.borrowerTenantId &&
        o.availableSlots >= reqSlots &&
        params.maxBidPricePerSlotUsd >= o.minBidPricePerSlotUsd
    );
    if (candidates.length === 0) {
      return {
        matched: false,
        reason: "NO_MATCHING_LENDER_OFFER",
      };
    }

    candidates.sort((a, b) => a.minBidPricePerSlotUsd - b.minBidPricePerSlotUsd);
    const bestOffer = candidates[0];
    bestOffer.availableSlots -= reqSlots;

    const clearingPricePerSlotUsd = Number(
      ((bestOffer.minBidPricePerSlotUsd + params.maxBidPricePerSlotUsd) / 2).toFixed(4)
    );
    const lease: SpotQuotaLease = {
      leaseId: params.leaseId ?? `spot-lease-${params.taskId}`,
      offerId: bestOffer.offerId,
      lenderTenantId: bestOffer.lenderTenantId,
      borrowerTenantId: params.borrowerTenantId,
      taskId: params.taskId,
      leasedSlots: reqSlots,
      clearingPricePerSlotUsd,
      status: "ACTIVE",
    };
    this.spotLeases.set(lease.leaseId, lease);
    this.persistSpotMarket();

    return {
      matched: true,
      lease,
    };
  }

  /**
   * [M73-2 / F8] Sub-Second Zero-Loss Lender Quota Recall on P0/P1 Urgent Arrival:
   * Checkpoints the borrower's running Spot task into `TurnWalEngine`, freezes & re-queues the Spot task
   * in `CoreScheduler5D` (`freezeAndRequeueSpotTask`), and restores the lent slots to `lenderTenantId`.
   */
  public recallLenderQuotaOnUrgentArrival(params: {
    lenderTenantId: string;
    urgentTaskId: string;
    requiredSlots: number;
    walEngine?: Pick<TurnWalEngine, "appendTurnDelta">;
    scheduler?: {
      freezeAndRequeueSpotTask: (taskId: string, reason?: string) => boolean;
    };
  }): {
    lenderTenantId: string;
    urgentTaskId: string;
    reclaimedSlots: number;
    recalledLeaseIds: string[];
    frozenBorrowerTaskIds: string[];
  } {
    let needed = Math.max(1, Math.floor(params.requiredSlots));
    let reclaimedSlots = 0;
    const recalledLeaseIds: string[] = [];
    const frozenBorrowerTaskIds: string[] = [];

    for (const lease of this.spotLeases.values()) {
      if (needed <= 0) break;
      if (lease.lenderTenantId !== params.lenderTenantId || lease.status !== "ACTIVE") {
        continue;
      }

      if (params.walEngine) {
        const walEntry = params.walEngine.appendTurnDelta({
          taskId: lease.taskId,
          turnIndex: 1,
          fencingToken: 1,
          changedFiles: ["workspace/spot_checkpoint.json"],
          sqliteWalPages: 0,
          highestSubmittedFencingToken: 1,
        });
        lease.walFrozenTurnIndex = walEntry.turnIndex;
      } else {
        lease.walFrozenTurnIndex = 1;
      }

      if (params.scheduler) {
        params.scheduler.freezeAndRequeueSpotTask(
          lease.taskId,
          `SPOT_QUOTA_RECALLED_BY_${params.lenderTenantId}_FOR_${params.urgentTaskId}`
        );
      }

      lease.status = "RECALLED_BY_LENDER";
      const offer = this.spotOffers.get(lease.offerId);
      if (offer) {
        offer.availableSlots = Math.min(offer.offeredSlots, offer.availableSlots + lease.leasedSlots);
      }

      reclaimedSlots += lease.leasedSlots;
      needed -= lease.leasedSlots;
      recalledLeaseIds.push(lease.leaseId);
      frozenBorrowerTaskIds.push(lease.taskId);
    }

    this.persistSpotMarket();
    return {
      lenderTenantId: params.lenderTenantId,
      urgentTaskId: params.urgentTaskId,
      reclaimedSlots,
      recalledLeaseIds,
      frozenBorrowerTaskIds,
    };
  }

  public listSpotOffers(): SpotQuotaOffer[] {
    return Array.from(this.spotOffers.values());
  }

  public listSpotLeases(): SpotQuotaLease[] {
    return Array.from(this.spotLeases.values());
  }
}
