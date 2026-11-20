import { TurnWalEngine } from "../checkpoint/wal_engine.js";
import { UnifiedComponentPersistenceStore } from "../checkpoint/unified_state_store.js";
import { InMemoryCellSqlStorage } from "../cloudflare/env.js";
import { LeaseCoordinator } from "../lease_coordinator.js";
import { NetworkChaosFaultInjectionProxy } from "../runtime/network_chaos_proxy.js";
import { CloudEventsBus } from "../telemetry/cloudevents.js";
import { FaultTolerantObservabilityCoordinator } from "../telemetry/observability_recovery.js";
import { OtelAndPrometheusExporter } from "../telemetry/otel_prometheus.js";
import { CapacitySlaEnforcer } from "./capacity_sla_enforcer.js";
import { SlotCanaryRolloutPlanner } from "./slot_canary_rollout_planner.js";

interface LocalDrillReport {
  drillId: string;
  passed: boolean;
  recoveryLatencyMs: number;
  checks: Record<string, boolean>;
}

export class LocalReliabilityDrillRunner {
  public readonly reports: LocalDrillReport[] = [];

  public run() {
    this.reports.length = 0;
    this.record("PARTITION_AND_FENCING", () => {
      const proxy = new NetworkChaosFaultInjectionProxy();
      const leases = new LeaseCoordinator();
      const taskId = "local-drill-task", host = "local-primary";
      const oldLease = leases.acquireLease(taskId, host);
      const sql = new InMemoryCellSqlStorage();
      const wal = new TurnWalEngine(sql);
      const saved = wal.appendTurnDelta({
        taskId, turnIndex: 1, fencingToken: oldLease.fencing_token, highestSubmittedFencingToken: oldLease.fencing_token,
        changedFiles: ["check.txt"], sqliteWalPages: 1,
      });
      let partitionBlocked = false;
      proxy.injectPartition(host, 30_000);
      try { proxy.assertReachable(host); } catch { partitionBlocked = true; } finally { proxy.healPartition(host); }
      const restored = new TurnWalEngine(sql).getTaskWalEntries(taskId)[0];
      leases.revokeLease(taskId);
      const replacement = leases.acquireLease(taskId, "local-standby");
      proxy.assertReachable(host);
      return {
        partitionBlocked,
        partitionHealed: proxy.inspectActiveFaults().partitionedHosts.length === 0,
        staleLeaseRejected: !leases.validateFencingToken(taskId, oldLease.fencing_token),
        replacementLeaseValid: leases.validateFencingToken(taskId, replacement.fencing_token),
        checkpointRecovered: restored?.checksumSha256 === saved.checksumSha256,
      };
    });
    this.record("STORAGE_PARITY_POLICY", () => {
      const capacity = new CapacitySlaEnforcer();
      const metrics = { totalDrives: 12, dataBlocks: 8, parityBlocks: 4, lastEtagCasLatencyMs: 4 };
      const withinParity = capacity.evaluateRustFsEc4Reliability({ ...metrics, healthyDrives: 8, failedDrives: 4 });
      const beyondParity = capacity.evaluateRustFsEc4Reliability({ ...metrics, healthyDrives: 7, failedDrives: 5 });
      return { withinParity: withinParity.quorumIntact, beyondParityRejected: !beyondParity.quorumIntact };
    });
    this.record("CANARY_HEAP_GUARD", () => {
      const planner = new SlotCanaryRolloutPlanner();
      planner.advanceRolloutPhase("PHASE_2_CANARY_8_SLOTS");
      const result = planner.evaluateHealthAndAutoRollback({
        brainSnapshot: { isolateId: "canary", heapUsedMb: 112, heapLimitMb: 128, activeCells: 20, lastRehydrateRtoMs: 200 },
        tunnelSnapshot: { gatewayId: "local", activeReverseStreams: 10, maxStreamsLimit: 20_000, lastOneHopRelayLatencyMs: 0.4, lastFailoverLatencyMs: 4 },
        rustFsStatus: { totalDrives: 12, dataBlocks: 8, parityBlocks: 4, healthyDrives: 12, failedDrives: 0, lastEtagCasLatencyMs: 4 },
        staleFenceErrorCount: 0, slowSqlAbortCount: 0, droppedOtelSpansCount: 0,
      });
      return {
        rollbackTriggered: result.rollbackTriggered,
        stableSlotsRestored: [...planner.slots.values()].every(slot => slot.ownerVersion === planner.stableVersion),
      };
    });
    this.record("COLLECTOR_OUTAGE", () => {
      const original = new FaultTolerantObservabilityCoordinator(new OtelAndPrometheusExporter(), new CloudEventsBus());
      const emit = (coordinator: typeof original) => coordinator.recordAndSpoolCorrelatedEvent({
        taskId: "local-trace-task", tenantId: "fixture", spanName: "turn", cloudEventType: "local.turn",
        durationMs: 1, data: {},
      });
      const initial = emit(original);
      const outage = original.spooler.flushSpooledTelemetry(() => 503);
      const store = new UnifiedComponentPersistenceStore(new InMemoryCellSqlStorage());
      store.persistObservabilityState(original, "local-tracing");
      const resumed = new FaultTolerantObservabilityCoordinator(new OtelAndPrometheusExporter(), new CloudEventsBus());
      const rehydrated = store.rehydrateObservabilityState(resumed, "local-tracing");
      const after = emit(resumed);
      const flushed = resumed.spooler.flushSpooledTelemetry(() => 200);
      return {
        outageRetainedSpan: outage.remainingSpooledSpans === 1,
        traceRestored: rehydrated && after.span.traceId === initial.span.traceId && after.span.parentSpanId === initial.span.spanId,
        allSpansFlushed: flushed.flushedSpansCount === 2,
      };
    });
    return { scope: "LOCAL_SIMULATION" as const, totalDrills: this.reports.length, allPassed: this.reports.every(report => report.passed), reports: [...this.reports] };
  }

  private record(drillId: string, scenario: () => Record<string, boolean>): void {
    const started = performance.now();
    const checks = scenario();
    this.reports.push({ drillId, checks, passed: Object.values(checks).every(Boolean), recoveryLatencyMs: performance.now() - started });
  }
}
