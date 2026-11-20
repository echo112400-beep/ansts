import { CapacitySlaEnforcer } from "./capacity_sla_enforcer.js";
import type { BrainIsolateSizingSnapshot, TunnelGatewaySizingSnapshot, RustFsEc4ClusterStatus } from "./capacity_sla_enforcer.js";

export interface DrainPreMigrateNotice {
  type: "TUNNEL_DRAIN_PRE_MIGRATE";
  drainingGatewayId: string;
  recommendedCandidateGateways: Array<{ gatewayId: string; publicEdgeIngressUrl: string }>;
  gracePeriodMs: number;
}

interface DrainPlanner {
  initiateMakeBeforeBreakDrain(gatewayId: string, gracePeriodMs: number): Map<string, DrainPreMigrateNotice>;
}

export type RolloutPhase =
  | "IDLE_STABLE"
  | "PHASE_1_SHADOW_1_SLOT"
  | "PHASE_2_CANARY_8_SLOTS"
  | "PHASE_3_PARTIAL_64_SLOTS"
  | "PHASE_4_FULL_256_SLOTS"
  | "ROLLED_BACK_TO_STABLE";

export type SlotMigrationStage = "STABLE" | "DRAINING" | "SNAPSHOT_TRANSFERRED" | "CUTOVER_CANARY";

export interface VirtualSlotRolloutState {
  slotId: number;
  ownerVersion: string;
  targetNodeId: string;
  migrationStage: SlotMigrationStage;
  epoch: number;
}

export interface CanaryHealthTelemetryInput {
  brainSnapshot: BrainIsolateSizingSnapshot;
  tunnelSnapshot: TunnelGatewaySizingSnapshot;
  rustFsStatus: RustFsEc4ClusterStatus;
  staleFenceErrorCount: number;
  slowSqlAbortCount: number;
  droppedOtelSpansCount: number;
}

export interface RollbackVerdict {
  healthy: boolean;
  rollbackTriggered: boolean;
  previousPhase: RolloutPhase;
  currentPhase: RolloutPhase;
  reasons: string[];
  topologyEpoch: number;
}

export class SlotCanaryRolloutPlanner {
  public static readonly TOTAL_VIRTUAL_SLOTS = 256;
  public readonly slots = new Map<number, VirtualSlotRolloutState>();

  public readonly activeTaskVersionPins = new Map<string, { version: string; slotId: number; epoch: number }>();
  public readonly allowlistedCanaryTenants = new Set<string>();
  public readonly cordonedBodyHosts = new Set<string>();

  public currentPhase: RolloutPhase = "IDLE_STABLE";
  public topologyEpoch = 1;
  public readonly slaEnforcer = new CapacitySlaEnforcer();

  constructor(
    public readonly stableVersion: string = "v2.3.0",
    public readonly canaryVersion: string = "v2.3.1-canary",
    public readonly stableNodeId: string = "celld-brain-stable-01",
    public readonly canaryNodeId: string = "celld-brain-canary-01",
  ) {
    for (let slotId = 0; slotId < SlotCanaryRolloutPlanner.TOTAL_VIRTUAL_SLOTS; slotId++) {
      this.slots.set(slotId, {
        slotId,
        ownerVersion: stableVersion,
        targetNodeId: stableNodeId,
        migrationStage: "STABLE",
        epoch: 1,
      });
    }
  }

  public addCanaryTenantAllowlist(tenantId: string): void {
    this.allowlistedCanaryTenants.add(tenantId);
  }

  public resolveTaskVersionAndNode(params: {
    taskId: string;
    tenantId: string;
    slotId: number;
    isExistingActiveTask: boolean;
    forceCanaryHeader?: boolean;
  }): { routedVersion: string; targetNodeId: string; slotId: number; epoch: number; pinned: boolean } {
    if (!Number.isSafeInteger(params.slotId)) throw new RangeError("INVALID_ROLLOUT_SLOT");
    const normalizedSlot = ((params.slotId % 256) + 256) % 256;

    const existingPin = this.activeTaskVersionPins.get(params.taskId);
    if (params.isExistingActiveTask && existingPin) {
      const targetNodeId =
        existingPin.version === this.canaryVersion ? this.canaryNodeId : this.stableNodeId;
      return {
        routedVersion: existingPin.version,
        targetNodeId,
        slotId: existingPin.slotId,
        epoch: existingPin.epoch,
        pinned: true,
      };
    }

    const slotState = this.slots.get(normalizedSlot)!;
    if (
      this.currentPhase !== "ROLLED_BACK_TO_STABLE" &&
      (params.forceCanaryHeader || this.allowlistedCanaryTenants.has(params.tenantId))
    ) {
      this.activeTaskVersionPins.set(params.taskId, {
        version: this.canaryVersion,
        slotId: normalizedSlot,
        epoch: slotState.epoch,
      });
      return {
        routedVersion: this.canaryVersion,
        targetNodeId: this.canaryNodeId,
        slotId: normalizedSlot,
        epoch: slotState.epoch,
        pinned: true,
      };
    }

    this.activeTaskVersionPins.set(params.taskId, {
      version: slotState.ownerVersion,
      slotId: normalizedSlot,
      epoch: slotState.epoch,
    });

    return {
      routedVersion: slotState.ownerVersion,
      targetNodeId: slotState.targetNodeId,
      slotId: normalizedSlot,
      epoch: slotState.epoch,
      pinned: true,
    };
  }

  public advanceRolloutPhase(
    targetPhase:
      | "PHASE_1_SHADOW_1_SLOT"
      | "PHASE_2_CANARY_8_SLOTS"
      | "PHASE_3_PARTIAL_64_SLOTS"
      | "PHASE_4_FULL_256_SLOTS",
  ): { phase: RolloutPhase; canarySlotCount: number; topologyEpoch: number } {
    const canarySlotsCountMap: Record<typeof targetPhase, number> = {
      PHASE_1_SHADOW_1_SLOT: 1,
      PHASE_2_CANARY_8_SLOTS: 8,
      PHASE_3_PARTIAL_64_SLOTS: 64,
      PHASE_4_FULL_256_SLOTS: 256,
    };

    const canaryCount = canarySlotsCountMap[targetPhase];
    const startCanarySlot = SlotCanaryRolloutPlanner.TOTAL_VIRTUAL_SLOTS - canaryCount;
    this.topologyEpoch += 1;

    for (let slotId = 0; slotId < SlotCanaryRolloutPlanner.TOTAL_VIRTUAL_SLOTS; slotId++) {
      const slot = this.slots.get(slotId)!;
      if (slotId >= startCanarySlot) {
        if (slot.ownerVersion !== this.canaryVersion) {

          slot.migrationStage = "DRAINING";
          slot.migrationStage = "SNAPSHOT_TRANSFERRED";
          slot.migrationStage = "CUTOVER_CANARY";
          slot.ownerVersion = this.canaryVersion;
          slot.targetNodeId = this.canaryNodeId;
          slot.epoch = this.topologyEpoch;
        }
      } else {
        slot.ownerVersion = this.stableVersion;
        slot.targetNodeId = this.stableNodeId;
        slot.migrationStage = "STABLE";
      }
    }

    this.currentPhase = targetPhase;
    return {
      phase: this.currentPhase,
      canarySlotCount: canaryCount,
      topologyEpoch: this.topologyEpoch,
    };
  }

  public evaluateHealthAndAutoRollback(
    telemetry: CanaryHealthTelemetryInput,
    tunnelRouter?: DrainPlanner,
    canaryGatewayId?: string,
  ): RollbackVerdict {
    const previousPhase = this.currentPhase;
    const reasons: string[] = [];

    const brainVerdict = this.slaEnforcer.evaluateBrainIsolateAdmission(telemetry.brainSnapshot);
    if (brainVerdict.gcTriggered || !brainVerdict.allowed) {
      reasons.push(
        `Canary Brain heap watermark exceeded (${telemetry.brainSnapshot.heapUsedMb}MB/${telemetry.brainSnapshot.heapLimitMb}MB)`,
      );
    }
    reasons.push(...brainVerdict.slaViolations);

    const tunnelVerdict = this.slaEnforcer.evaluateTunnelGatewayCapacity(telemetry.tunnelSnapshot);
    reasons.push(...tunnelVerdict.slaViolations);

    const rustFsVerdict = this.slaEnforcer.evaluateRustFsEc4Reliability(telemetry.rustFsStatus);
    reasons.push(...rustFsVerdict.slaViolations);

    if (telemetry.staleFenceErrorCount > 0) {
      reasons.push(`Detected ${telemetry.staleFenceErrorCount} 409 StaleFenceError conflicts during Canary`);
    }
    if (telemetry.slowSqlAbortCount > 0) {
      reasons.push(`Detected ${telemetry.slowSqlAbortCount} slow SQL (>250ms) aborts during Canary`);
    }
    if (telemetry.droppedOtelSpansCount > 0) {
      reasons.push(`Detected ${telemetry.droppedOtelSpansCount} dropped OTLP spans in ring spooler`);
    }

    if (reasons.length > 0) {
      this.triggerInstantRollback(tunnelRouter, canaryGatewayId);
      return {
        healthy: false,
        rollbackTriggered: true,
        previousPhase,
        currentPhase: this.currentPhase,
        reasons,
        topologyEpoch: this.topologyEpoch,
      };
    }

    return {
      healthy: true,
      rollbackTriggered: false,
      previousPhase,
      currentPhase: this.currentPhase,
      reasons: [],
      topologyEpoch: this.topologyEpoch,
    };
  }

  public triggerInstantRollback(
    tunnelRouter?: DrainPlanner,
    canaryGatewayId?: string,
  ): Map<string, DrainPreMigrateNotice> {
    this.topologyEpoch += 1;
    for (const slot of this.slots.values()) {
      slot.ownerVersion = this.stableVersion;
      slot.targetNodeId = this.stableNodeId;
      slot.migrationStage = "STABLE";
      slot.epoch = this.topologyEpoch;
    }

    for (const [taskId, pin] of this.activeTaskVersionPins.entries()) {
      if (pin.version === this.canaryVersion) {
        this.activeTaskVersionPins.set(taskId, {
          version: this.stableVersion,
          slotId: pin.slotId,
          epoch: this.topologyEpoch,
        });
      }
    }
    this.currentPhase = "ROLLED_BACK_TO_STABLE";

    if (tunnelRouter && canaryGatewayId) {
      return tunnelRouter.initiateMakeBeforeBreakDrain(canaryGatewayId, 15000);
    }
    return new Map();
  }

  public cordonBodyHostForUpgrade(hostId: string): void {
    this.cordonedBodyHosts.add(hostId);
  }

  public uncordonBodyHostAfterUpgrade(hostId: string): void {
    this.cordonedBodyHosts.delete(hostId);
  }

  public verifyNMinusOneWalSchemaCompatibility(canaryWalJson: string): {
    compatible: boolean;
    preservedUnknownFields: string[];
  } {
    let value: unknown;
    try { value = JSON.parse(canaryWalJson); } catch { return { compatible: false, preservedUnknownFields: [] }; }
    if (!value || typeof value !== "object" || Array.isArray(value)) return { compatible: false, preservedUnknownFields: [] };
    const parsed = value as Record<string, unknown>;
    const requiredCoreKeys = ["taskId", "fence", "state", "diffSnapshot"];
    for (const reqKey of requiredCoreKeys) {
      if (!(reqKey in parsed)) {
        return { compatible: false, preservedUnknownFields: [] };
      }
    }
    const knownCoreSet = new Set([...requiredCoreKeys, "timestampMs"]);
    const preservedUnknownFields = Object.keys(parsed).filter((k) => !knownCoreSet.has(k));
    return {
      compatible: true,
      preservedUnknownFields,
    };
  }
}
