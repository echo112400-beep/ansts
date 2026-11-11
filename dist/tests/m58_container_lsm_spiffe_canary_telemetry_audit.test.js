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
 * @fileoverview M58 End-to-End & Unit Verification Suite:
 * 1. [M58-1] `CelldNativeContainerSandboxManager` & `CelldContainerHandle`:
 *    - Enforces full RFC1918 (`172.16..31.*`), CGNAT (`100.64..127.*`), IPv6 bracketed link-local/ULA (`[fe80::1]`, `[fd00::1]`, `[::ffff:169.254.169.254]`, `0.0.0.0`) fence blocking
 *    - Enforces `max_instances` cap at `.start()` time even if multiple handles were pre-created while stopped
 *    - Propagates non-zero subprocess `exitCode` from `CelldContainerHandle.exec()`
 * 2. [M58-2] `CompanionContainerAndOverlayDriver`, `LinuxCgroupV2AndPsiDriver` & `EbpfLsmSecurityBridge`:
 *    - Recursively captures modified files inside nested subdirectories of OverlayFS `upperDir` (`src/nested/app.ts`)
 *    - Sanitizes `taskId` against `../` path traversal in OverlayFS and Cgroup v2 directory creation
 *    - Anchors relative/traversal paths (`../../etc/shadow`, `etc/shadow`) and blocks `/proc/self/environ`, `/proc/self/mem`, `/etc/sudoers` in eBPF LSM
 * 3. [M58-3] `MtlsReverseTunnelBridge` & `SlsaProvenanceAttestor`:
 *    - Rejects multi-segment/traversal SPIFFE IDs (`spiffe://ansts.io/ns/fleet/sa/worker/../../admin`) and empty-string `svidSignatureHex` (`""`)
 *    - Binds `fileCount` into the SLSA v1.0 signed canonical body so post-signing `fileCount` tampering is rejected
 * 4. [M58-4] `CanaryRolloutController` & `LinuxPsiMemoryGuard`:
 *    - Locks `ROLLED_BACK` canary at `trafficWeightPct: 0` on subsequent healthy ticks until `resetCanary()` is explicitly invoked
 *    - Filters out terminal-state tasks in `LinuxPsiMemoryGuard.evaluateAndDefend` and computes the 25% eviction slice strictly over non-P0 evictable tasks
 * 5. [M58-5] `CloudEventsBus`, `PredictiveSandboxPrewarmer` & `TaskResourceProfiler`:
 *    - Enforces bounded `maxRetainedEvents` ring buffer and sanitizes negative/`NaN` cost deltas in `CloudEventsBus`
 *    - Guards `PredictiveSandboxPrewarmer` and `TaskResourceProfiler` against `NaN`/`Infinity`/negative sample poisoning
 * 6. [M58-6] `AnstsLocalAndComposeServer.readJsonBody` & `MultiRegionFederationRouter`:
 *    - Enforces `maxBodyBytes` (HTTP 413 Payload Too Large) across all unified server endpoints
 *    - Sanitizes `NaN`/negative `rttEwmaMs` in `MultiRegionFederationRouter`
 * 7. Live HTTP `POST /v1beta1/management/system/container-security-telemetry-audit` & CLI `anstsctl mgmt-container-security-audit`.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const celld_container_bridge_js_1 = require("../src/cloudflare/celld_container_bridge.js");
const container_driver_js_1 = require("../src/runtime/container_driver.js");
const linux_cgroup_psi_js_1 = require("../src/runtime/linux_cgroup_psi.js");
const ebpf_lsm_bridge_js_1 = require("../src/runtime/ebpf_lsm_bridge.js");
const mtls_tunnel_bridge_js_1 = require("../src/runtime/mtls_tunnel_bridge.js");
const slsa_provenance_attestor_js_1 = require("../src/runtime/slsa_provenance_attestor.js");
const canary_rollout_controller_js_1 = require("../src/cluster/canary_rollout_controller.js");
const psi_guard_js_1 = require("../src/runtime/psi_guard.js");
const cloudevents_js_1 = require("../src/telemetry/cloudevents.js");
const predictive_prewarmer_js_1 = require("../src/runtime/predictive_prewarmer.js");
const profiler_js_1 = require("../src/profiler.js");
const federation_router_js_1 = require("../src/cloudflare/federation_router.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M58: Container Fence RFC1918/CGNAT/IPv6 & Start-Cap Guard, Recursive OverlayFS & eBPF LSM Path Hardening, SPIFFE/SLSA Tamper Proofing, Canary Post-Rollback Lock & PSI Victim Filtering, Telemetry/EWMA NaN Guard & Server Payload Protection", () => {
    (0, node_test_1.it)("validates all 6 container/security/telemetry/server invariants & live HTTP/CLI container-security-audit", async () => {
        // 1. [M58-1] CelldNativeContainerSandboxManager RFC1918 172.19..31 / CGNAT / IPv6 fence & start-time max_instances guard
        assert.throws(() => celld_container_bridge_js_1.CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget("172.25.0.10", true), /celld-fence nftables blocked/);
        assert.throws(() => celld_container_bridge_js_1.CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget("100.100.1.1", true), /celld-fence nftables blocked/);
        assert.throws(() => celld_container_bridge_js_1.CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget("[fe80::1]", true), /celld-fence nftables blocked/);
        assert.throws(() => celld_container_bridge_js_1.CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget("[::ffff:169.254.169.254]", true), /celld-fence nftables blocked/);
        const mgr = new celld_container_bridge_js_1.CelldNativeContainerSandboxManager();
        const cfg = { class_name: "sandbox-m58", image: "alpine:3.20", max_instances: 1 };
        mgr.registerContainerClass(cfg);
        // Pre-create 2 handles while both are stopped (running=false)
        const h1 = mgr.getOrCreateContainerHandle("cell-m58-1", cfg);
        const h2 = mgr.getOrCreateContainerHandle("cell-m58-2", cfg);
        h1.start({ enableInternet: true });
        // Starting h2 while h1 is running must be rejected by start-time max_instances guard!
        assert.throws(() => h2.start({ enableInternet: true }), /reached max_instances/);
        // Non-zero exit code from real subprocess must be propagated
        const execFail = await h1.exec(["node", "-e", "process.exit(7)"]);
        assert.equal(execFail.exitCode, 7);
        // 2. [M58-2] Recursive OverlayFS upperdir diff capture & eBPF LSM relative/credential path blocking
        const overlayTmp = path.join(os.tmpdir(), `ansts-overlay-m58-${process.pid}-${Date.now()}`);
        const overlayDrv = new container_driver_js_1.CompanionContainerAndOverlayDriver(overlayTmp);
        const layout = overlayDrv.createOverlayMountLayout("task-m58-overlay");
        fs.mkdirSync(path.join(layout.upperDir, "src", "nested"), { recursive: true });
        fs.writeFileSync(path.join(layout.upperDir, "src", "nested", "feature.ts"), "export const ok = true;\n", "utf-8");
        const delta = overlayDrv.captureUpperdirDeltaSnapshot("task-m58-overlay", 1);
        assert.deepEqual(delta.changedFiles, ["src/nested/feature.ts"]);
        const cgroupDrv = new linux_cgroup_psi_js_1.LinuxCgroupV2AndPsiDriver(path.join(overlayTmp, "cgroup"));
        assert.throws(() => cgroupDrv.writePhaseLimits("task-1", "ACT", NaN, 512, 1024), /Invalid cgroup v2/);
        const lsm = new ebpf_lsm_bridge_js_1.EbpfLsmSecurityBridge();
        assert.equal(lsm.checkFileOpenHook("../../etc/shadow").allowed, false);
        assert.equal(lsm.checkFileOpenHook("etc/shadow").allowed, false);
        assert.equal(lsm.checkFileOpenHook("/proc/self/environ").allowed, false);
        assert.equal(lsm.checkFileOpenHook("/etc/sudoers").allowed, false);
        // 3. [M58-3] MtlsReverseTunnelBridge multi-segment/empty-sig rejection & SlsaProvenanceAttestor fileCount binding
        const mtls = new mtls_tunnel_bridge_js_1.MtlsReverseTunnelBridge();
        const validToken = mtls.issueSpiffeSvidToken("spiffe://ansts.io/ns/fleet/sa/worker", 60_000, 1000);
        assert.equal(mtls.verifySpiffeCertificate(validToken.spiffeId, validToken.certExpiresAtMs, 2000, "").authenticated, false);
        assert.equal(mtls.verifySpiffeCertificate("spiffe://ansts.io/ns/fleet/sa/worker/../../admin", validToken.certExpiresAtMs, 2000)
            .authenticated, false);
        const slsa = new slsa_provenance_attestor_js_1.SlsaProvenanceAttestor();
        const stmt = slsa.createSignedProvenanceStatement({
            imageReference: "layerfs://main@C1",
            sha256Digest: "deadbeef",
            builderId: "builder-1",
            commitId: "C1",
            fileCount: 5,
        });
        assert.equal(slsa.verifyProvenanceStatement(stmt), true);
        // Tampering with fileCount must invalidate the signature
        assert.equal(slsa.verifyProvenanceStatement({ ...stmt, fileCount: 99 }), false);
        // 4. [M58-4] CanaryRolloutController post-rollback lock & LinuxPsiMemoryGuard non-terminal victim filtering
        const canary = new canary_rollout_controller_js_1.CanaryRolloutController();
        const rolledBack = canary.evaluateCanaryStep({ errorRatePct: 5.0, p99LatencyMs: 2000 });
        assert.equal(rolledBack.state, "ROLLED_BACK");
        assert.equal(rolledBack.trafficWeightPct, 0);
        // Subsequent healthy step without resetCanary() must stay ROLLED_BACK at 0%
        const afterHealthy = canary.evaluateCanaryStep({ errorRatePct: 0.1, p99LatencyMs: 100 });
        assert.equal(afterHealthy.state, "ROLLED_BACK");
        assert.equal(afterHealthy.trafficWeightPct, 0);
        const psiGuard = new psi_guard_js_1.LinuxPsiMemoryGuard(40, 70);
        const doneTask = new models_js_1.TaskSpec({ task_id: "t-done", tenant_id: "t1", priority: models_js_1.TaskPriority.P3_BATCH });
        doneTask.state = models_js_1.TaskState.SUCCEEDED;
        const liveTask = new models_js_1.TaskSpec({ task_id: "t-live", tenant_id: "t1", priority: models_js_1.TaskPriority.P2_STANDARD });
        liveTask.state = models_js_1.TaskState.RUNNING_ACT;
        const psiAction = psiGuard.evaluateAndDefend({ someAvg10Pct: 80, fullAvg10Pct: 85, freeSwapMb: 64 }, [doneTask, liveTask]);
        assert.deepEqual(psiAction.evictedTaskIds, ["t-live"]);
        // 5. [M58-5] CloudEventsBus bounded ring buffer & NaN/negative cost/EWMA guard
        const bus = new cloudevents_js_1.CloudEventsBus(10);
        for (let i = 0; i < 15; i++) {
            bus.emitEvent({
                type: "io.ansts.test",
                subject: `sub-${i}`,
                tenantId: "Northstar",
                llmCostUsd: i === 0 ? 1.5 : -99,
                sandboxCpuCoreSeconds: NaN,
            });
        }
        assert.equal(bus.allEvents().length, 10);
        assert.equal(bus.getTenantReport("Northstar")?.totalLlmCostUsd, 1.5);
        const prewarmer = new predictive_prewarmer_js_1.PredictiveSandboxPrewarmer();
        const pred = prewarmer.recordArrivalSampleAndPredict(NaN, -10);
        assert.equal(Number.isFinite(pred.recommendedWarmPoolSize), true);
        const profiler = new profiler_js_1.TaskResourceProfiler();
        profiler.recordExecutionMetrics(liveTask, NaN, -512);
        const recProfile = profiler.recommendProfile(liveTask);
        assert.equal(Number.isFinite(recProfile.act_cpu_cores), true);
        // 6. [M58-6] MultiRegionFederationRouter RTT sanitization & AnstsLocalAndComposeServer 413 body limit + live CLI audit
        const fed = new federation_router_js_1.MultiRegionFederationRouter();
        fed.registerRegion({ regionCode: "eu-bad", baseUrl: "https://eu1", healthy: true, rttEwmaMs: NaN, sovereigntyZone: "EU" });
        fed.registerRegion({ regionCode: "eu-good", baseUrl: "https://eu2", healthy: true, rttEwmaMs: 28, sovereigntyZone: "EU" });
        assert.equal(fed.selectOptimalRegion({ requiredSovereigntyZone: "EU" }).regionCode, "eu-good");
        const server = new server_main_js_1.AnstsLocalAndComposeServer({ nodeId: "gw-m58-test", maxBodyBytes: 2048 });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const resp413 = await fetch(`${baseUrl}/v1/tasks`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ task_id: "too-big", tenant_id: "Northstar", prompt: "x".repeat(4096) }),
            });
            assert.equal(resp413.status, 413);
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-container-security-audit"], baseUrl);
            assert.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            assert.equal(body.allM58RisksEliminated, true);
            assert.equal(body.containerFenceAndCap1.containerFenceAndCapVerified, true);
            assert.equal(body.overlayRecursiveAndLsm2.overlayRecursiveAndLsmVerified, true);
            assert.equal(body.spiffeAndSlsaIntegrity3.spiffeAndSlsaIntegrityVerified, true);
            assert.equal(body.canaryLockAndPsiFilter4.canaryLockAndPsiFilterVerified, true);
            assert.equal(body.telemetryRingAndEwmaGuard5.telemetryRingAndEwmaGuardVerified, true);
            assert.equal(body.federationRttAndBodyGuard6.federationRttAndBodyGuardVerified, true);
        }
        finally {
            await server.stop();
        }
    });
});
