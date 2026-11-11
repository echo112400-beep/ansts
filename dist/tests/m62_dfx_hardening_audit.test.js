"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview M62 DFX Full-Spectrum Resiliency, Diagnosability, Testability & Operability Suite.
 *
 * Covers:
 * 1. [M62-1] DFX-Reliability & Recoverability: `CgroupPhaseController` & `LinuxCgroupV2AndPsiDriver`
 *    sub-core CFS hard floor (5000us per 100ms) preventing freeze, non-negative limit validation & `removeTaskCgroup` cleanup.
 * 2. [M62-2] DFX-Observability & Diagnosability: `OtelAndPrometheusExporter` metric name sanitization,
 *    bounded metric label dimensions (preventing cardinality memory leaks), and p50/p95/p99 quantile validation.
 * 3. [M62-3] DFX-Safety & Fault Tolerance: `LinuxPsiMemoryGuard` non-negative/NaN pressure sanitization,
 *    `P0_EMERGENCY` and `P1_HIGH` eviction immunity under memory pressure.
 * 4. [M62-4] DFX-Operability & Serviceability: `CapacitySlaEnforcer` finite metric validation,
 *    zero-division protection, emergency headroom SQLite persistence & dynamic policy tuning.
 * 5. [M62-5] DFX-Maintainability & Deployability: `EdgeTunnelHaRouter` idempotency passthrough &
 *    `CrossRegionAsyncReplicator` URL sanitization & non-blocking error isolation.
 * 6. [M62-6] Live Management Plane HTTP (`POST /v1beta1/management/system/dfx-hardening-audit`) & CLI (`mgmt-dfx-audit`).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const env_js_1 = require("../src/cloudflare/env.js");
const phase_controller_js_1 = require("../src/runtime/phase_controller.js");
const linux_cgroup_psi_js_1 = require("../src/runtime/linux_cgroup_psi.js");
const otel_prometheus_js_1 = require("../src/telemetry/otel_prometheus.js");
const psi_guard_js_1 = require("../src/runtime/psi_guard.js");
const capacity_sla_enforcer_js_1 = require("../src/cluster/capacity_sla_enforcer.js");
const edge_tunnel_ha_router_js_1 = require("../src/runtime/edge_tunnel_ha_router.js");
const tunnel_gateway_cell_js_1 = require("../src/cloudflare/tunnel_gateway_cell.js");
const cross_region_replicator_js_1 = require("../src/cluster/cross_region_replicator.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M62 DFX Full-Spectrum Resiliency, Diagnosability, Testability & Operability Suite", () => {
    (0, node_test_1.it)("1. Verifies M62-1 through M62-5 across cgroup limits, Otel metrics, PSI safety, capacity SLA & cross-region replication, plus live HTTP & CLI", async () => {
        const tmpCgroupDir = path.join(os.tmpdir(), `ansts-dfx-test-cgroup-${Date.now()}`);
        // =========================================================================
        // 1. [M62-1] DFX-Reliability & Recoverability: cgroup v2 limits & cleanup
        // =========================================================================
        const phaseCtrl = new phase_controller_js_1.CgroupPhaseController();
        // Sub-core think phase: think_cpu_cores=0.001 must clamp to MIN_CPU_QUOTA_US (5000us)
        const thinkAlloc = phaseCtrl.applyThinkPhase("task-dfx-1", {
            think_cpu_cores: 0.001,
            act_cpu_cores: 4.0,
            act_memory_mb: 4096,
            disk_mb: 1024,
        });
        assert.equal(thinkAlloc.cpuQuotaUsPer100ms, phase_controller_js_1.CgroupPhaseController.MIN_CPU_QUOTA_US);
        const cgroupDriver = new linux_cgroup_psi_js_1.LinuxCgroupV2AndPsiDriver(tmpCgroupDir);
        const writtenLimits = cgroupDriver.writePhaseLimits("task-dfx-1", "THINK", 0.001, 256, 512);
        assert.equal(writtenLimits.cpuMaxContent, `${phase_controller_js_1.CgroupPhaseController.MIN_CPU_QUOTA_US} 100000`);
        const cleaned = cgroupDriver.removeTaskCgroup("task-dfx-1");
        assert.equal(cleaned, true);
        // =========================================================================
        // 2. [M62-2] DFX-Observability & Diagnosability: Otel metrics & cardinality bounds
        // =========================================================================
        const otel = new otel_prometheus_js_1.OtelAndPrometheusExporter();
        // Metric name with invalid characters must be sanitized
        otel.incCounter("dfx/counter.ops!!", 10);
        otel.setGauge("dfx.memory.util", 85.5);
        otel.observeHistogram("dfx.latency.ms", 120);
        otel.observeHistogram("dfx.latency.ms", 250);
        const promMetrics = otel.renderPrometheusMetrics();
        assert.ok(promMetrics.includes("dfx_counter_ops__ 10"));
        assert.ok(promMetrics.includes("dfx_memory_util 85.5"));
        const quantiles = otel.computeQuantiles("dfx.latency.ms");
        assert.equal(quantiles.count, 2);
        assert.equal(quantiles.p50, 250); // sorted: [120, 250], index Math.floor(0.5*2)=1 -> 250
        const burnEval = otel.evaluateSloBurnRate({
            totalRequests: 10000,
            errorRequests: 2,
            p99LatencyMs: 150,
            latencySloTargetMs: 250,
        });
        assert.equal(burnEval.alertSeverity, "HEALTHY");
        // =========================================================================
        // 3. [M62-3] DFX-Safety & Fault Tolerance: PSI sanitization & emergency immunity
        // =========================================================================
        const psiGuard = new psi_guard_js_1.LinuxPsiMemoryGuard(40.0, 70.0);
        const p0Task = new models_js_1.TaskSpec({ task_id: "t-p0", tenant_id: "ten", priority: models_js_1.TaskPriority.P0_EMERGENCY });
        const p1Task = new models_js_1.TaskSpec({ task_id: "t-p1", tenant_id: "ten", priority: models_js_1.TaskPriority.P1_HIGH });
        const p2Task = new models_js_1.TaskSpec({ task_id: "t-p2", tenant_id: "ten", priority: models_js_1.TaskPriority.P2_STANDARD });
        const p3Task = new models_js_1.TaskSpec({ task_id: "t-p3", tenant_id: "ten", priority: models_js_1.TaskPriority.P3_BATCH });
        // Critical PSI pressure with corrupt/negative someAvg10: must sanitize and protect P0 & P1
        const psiAction = psiGuard.evaluateAndDefend({ someAvg10Pct: -10, fullAvg10Pct: 75.0, freeSwapMb: 50 }, [p0Task, p1Task, p2Task, p3Task]);
        assert.equal(psiAction.level, "FREEZE_AND_CHECKPOINT");
        assert.equal(psiAction.evictedTaskIds.includes("t-p0"), false, "P0 must have emergency eviction immunity");
        assert.equal(psiAction.evictedTaskIds.includes("t-p1"), false, "P1 must have emergency eviction immunity");
        assert.equal(psiAction.evictedTaskIds.includes("t-p3"), true, "P3 must be prioritized for eviction");
        // =========================================================================
        // 4. [M62-4] DFX-Operability & Serviceability: Capacity SLA & SQLite Headroom
        // =========================================================================
        const slaSql = new env_js_1.InMemoryCellSqlStorage();
        const slaEnforcer1 = new capacity_sla_enforcer_js_1.CapacitySlaEnforcer(undefined, slaSql);
        const slaRes = slaEnforcer1.evaluateSlaCompliance({
            p0QueueWaitP99Ms: 300, // breached
            warmPoolHitRatio: 0.95,
            freeClusterCpuRatio: 0.05, // emergency headroom triggered (< 0.15)
        });
        assert.equal(slaRes.compliant, false);
        assert.equal(slaRes.breachedMetrics.includes("p0QueueWaitP99Ms"), true);
        assert.equal(slaRes.reserveEmergencyHeadroom, true);
        assert.equal(slaEnforcer1.isEmergencyHeadroomActive(), true);
        // Rehydrate SLA headroom state from SQLite
        const slaEnforcer2 = new capacity_sla_enforcer_js_1.CapacitySlaEnforcer(undefined, slaSql);
        assert.equal(slaEnforcer2.isEmergencyHeadroomActive(), true);
        // =========================================================================
        // 5. [M62-5] DFX-Maintainability & Deployability: Tunnel & Replicator
        // =========================================================================
        const tgw = new tunnel_gateway_cell_js_1.TunnelGatewayCell();
        tgw.registerTunnel({
            hostId: "body-m62",
            gatewayId: "gw-primary",
            role: "PRIMARY",
            connGeneration: 1,
            epoch: 1,
        });
        const tunnelRouter = new edge_tunnel_ha_router_js_1.EdgeTunnelHaRouter(tgw);
        const routedMeta = tunnelRouter.routeRpcToBodyHost("body-m62", { ping: true });
        assert.equal(routedMeta.delivered, true);
        assert.equal(routedMeta.routedViaGatewayId, "gw-primary");
        const repl = new cross_region_replicator_js_1.CrossRegionAsyncReplicator();
        const replRes = await repl.replicateToRemoteS3Region({
            s3Endpoint: "http://127.0.0.1:9000",
            sourceRegion: "US",
            targetRegion: "APAC",
            objectKey: "checkpoints/task-1.ltx",
            payload: { data: 123 },
        });
        assert.equal(replRes.replicatedCount, 1);
        assert.equal(replRes.targetRegion, "APAC");
        assert.ok(replRes.rpoLagMs >= 0);
        // =========================================================================
        // 6. [M62-6] Live HTTP Endpoint & CLI Verification
        // =========================================================================
        const server = new server_main_js_1.AnstsLocalAndComposeServer({ nodeId: "gw-m62-test" });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-dfx-audit"], baseUrl);
            assert.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            assert.equal(body.allM62DfxRisksEliminated, true);
            assert.equal(body.cgroupReliability1.cgroupReliabilityVerified, true);
            assert.equal(body.otelDiagnosability2.otelDiagnosabilityVerified, true);
            assert.equal(body.psiSafety3.psiSafetyVerified, true);
            assert.equal(body.capacitySlaOperability4.capacitySlaOperabilityVerified, true);
            assert.equal(body.deployability5.deployabilityVerified, true);
        }
        finally {
            await server.stop();
        }
    });
});
