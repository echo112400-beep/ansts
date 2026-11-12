/**
 * @fileoverview M17 & M62-4: Cluster Capacity SLA Enforcer & Headroom Reservation Guard
 * with Finite Metric Validation, Zero-Division Immunity, SQLite Headroom State Persistence & Dynamic Threshold Adjustment.
 */
import { SqlStorageLike } from "../cloudflare/env.js";

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

export interface CapacitySlaViolation {
  rule: string;
  detail: string;
}

export interface BrainIsolateAdmission {
  isolateId: string;
  allowed: boolean;
  heapUsedRatioPct: number;
  gcTriggered: boolean;
  circuitBreakerTripped: boolean;
  activeCells: number;
  slaViolations: CapacitySlaViolation[];
}

export interface BodyHostAdmission {
  hostId: string;
  allowed: boolean;
  availableSlots: number;
  slaViolations: CapacitySlaViolation[];
}

export interface RustFsEc4ReliabilityVerdict {
  rpoZeroGuaranteed: boolean;
  remainingFaultToleranceDrives: number;
  totalDrives: number;
  dataBlocks: number;
  parityBlocks: number;
  slaViolations: CapacitySlaViolation[];
}

export class CapacitySlaEnforcer {
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
    walEngine?: {
      appendTurnDelta: (args: {
        taskId: string;
        turnIndex: number;
        fencingToken: number;
        thoughtSummary: string;
        toolName: string;
        changedFiles: string[];
      }) => { turnIndex: number };
    };
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
          thoughtSummary: `Sub-second WAL checkpoint freeze before Spot quota recall by lender ${params.lenderTenantId}`,
          toolName: "spot_wal_freeze",
          changedFiles: ["workspace/spot_checkpoint.json"],
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

  /**
   * [TASK-84] Brain isolate admission: 128MB heap watermark with a GC band and a circuit breaker.
   * The isolate is admitted until the heap crosses the breaker threshold, because a rehydrate under
   * a nearly full heap is exactly how an OOM cascade starts.
   */
  public evaluateBrainIsolateAdmission(params: {
    isolateId: string;
    heapUsedMb: number;
    heapLimitMb: number;
    activeCells: number;
    lastRehydrateRtoMs: number;
  }): BrainIsolateAdmission {
    const violations: CapacitySlaViolation[] = [];
    const limit = Math.max(1, params.heapLimitMb);
    const heapRatio = params.heapUsedMb / limit;
    const gcTriggered = heapRatio >= 0.75;
    const circuitBreakerTripped = heapRatio >= 0.9;

    if (circuitBreakerTripped) {
      violations.push({
        rule: "BRAIN_HEAP_CIRCUIT_BREAKER",
        detail: `heap ${params.heapUsedMb}MB / ${limit}MB (${(heapRatio * 100).toFixed(1)}%) breached 90% breaker`,
      });
    }
    if (params.lastRehydrateRtoMs > 3000) {
      violations.push({
        rule: "BRAIN_REHYDRATE_RTO",
        detail: `last rehydrate RTO ${params.lastRehydrateRtoMs}ms exceeded 3000ms`,
      });
    }

    return {
      isolateId: params.isolateId,
      allowed: !circuitBreakerTripped,
      heapUsedRatioPct: Number((heapRatio * 100).toFixed(1)),
      gcTriggered,
      circuitBreakerTripped,
      activeCells: params.activeCells,
      slaViolations: violations,
    };
  }

  /**
   * [TASK-84] Body host admission: hard 32-sandbox cap plus the cgroup-boost / vsock fast-path latencies.
   */
  public evaluateBodyHostAdmission(params: {
    hostId: string;
    vcpuCount: number;
    ramGb: number;
    activeSandboxes: number;
    maxSandboxesLimit: number;
    lastCgroupBoostLatencyMs: number;
    lastVsockFastpathLatencyMs: number;
  }): BodyHostAdmission {
    const violations: CapacitySlaViolation[] = [];
    const limit = Math.max(0, params.maxSandboxesLimit);
    const availableSlots = Math.max(0, limit - params.activeSandboxes);

    if (params.activeSandboxes >= limit) {
      violations.push({
        rule: "BODY_SANDBOX_CAP",
        detail: `${params.activeSandboxes}/${limit} sandbox slots in use`,
      });
    }
    if (params.lastCgroupBoostLatencyMs > 1.5) {
      violations.push({
        rule: "BODY_CGROUP_BOOST_LATENCY",
        detail: `cgroup boost ${params.lastCgroupBoostLatencyMs}ms exceeded 1.5ms`,
      });
    }
    if (params.lastVsockFastpathLatencyMs > 0.2) {
      violations.push({
        rule: "BODY_VSOCK_FASTPATH_LATENCY",
        detail: `vsock fastpath ${params.lastVsockFastpathLatencyMs}ms exceeded 0.2ms`,
      });
    }

    return {
      hostId: params.hostId,
      allowed: availableSlots > 0 && violations.length === 0,
      availableSlots,
      slaViolations: violations,
    };
  }

  /**
   * [TASK-84] RustFS EC:4 reliability: whether the array can still reconstruct every block (RPO=0).
   * Fault tolerance is spent one drive at a time, so parity leaving zero spare drives is still RPO=0 —
   * losing one more is not.
   */
  public evaluateRustFsEc4Reliability(params: {
    totalDrives: number;
    dataBlocks: number;
    parityBlocks: number;
    healthyDrives: number;
    failedDrives: number;
    lastEtagCasLatencyMs: number;
  }): RustFsEc4ReliabilityVerdict {
    const violations: CapacitySlaViolation[] = [];
    const remainingFaultToleranceDrives = params.parityBlocks - params.failedDrives;
    const canReconstruct = params.healthyDrives >= params.dataBlocks;
    const rpoZeroGuaranteed = remainingFaultToleranceDrives >= 0 && canReconstruct;

    if (remainingFaultToleranceDrives < 0) {
      violations.push({
        rule: "RUSTFS_EC4_QUORUM_LOST",
        detail: `${params.failedDrives} failed drives exceeded ${params.parityBlocks} parity blocks`,
      });
    }
    if (params.lastEtagCasLatencyMs > 12) {
      violations.push({
        rule: "RUSTFS_ETAG_CAS_LATENCY",
        detail: `etag CAS ${params.lastEtagCasLatencyMs}ms exceeded 12ms`,
      });
    }

    return {
      rpoZeroGuaranteed,
      remainingFaultToleranceDrives,
      totalDrives: params.totalDrives,
      dataBlocks: params.dataBlocks,
      parityBlocks: params.parityBlocks,
      slaViolations: violations,
    };
  }
}
