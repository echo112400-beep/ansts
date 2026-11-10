"use strict";







var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M49 End-to-End Verification: Deep Backend Dependency Hardening & Full-Spectrum Management Plane Governance Suite
 * Verifies:
 * 1. [M49-1] Cognitive Router Full-Chain Backend Pipeline (`ModelPoolClientAdapter` + `LiveOpenAiCompatibleSseAdapter` + `SlidingWindowTokenBucket` + `ThreeStateCircuitBreaker`) (`POST /v1beta1/management/router/inference-pipeline`).
 * 2. [M49-2] Live Unix Domain Socket `ZeroTrustVsockHostDaemon`, `EbpfLsmSecurityBridge`, Ed25519 `SlsaProvenanceAttestor` & `UffdLazyPageFaultRestorer` (`POST /v1beta1/management/security/kernel-attestation`).
 * 3. [M49-3] Automated Split-Brain / Network Partition Chaos Drill, Host Preflight Readiness Gate & SQLite-Backed Topology CAS Coordinator (`POST /v1beta1/management/chaos/drill`).
 * 4. [M49-4] Dual-Homed Reverse Tunnel Failover (`TunnelGatewayCell`), Multi-Region Federation (`MultiRegionFederationRouter`), `AgentTaskHarnessCell` Auto-Heal & Velocity/Staging Gate (`POST /v1beta1/management/federation/tunnel-harness`).
 */(0, _nodeTest.describe)("M49 Deep Backend Dependency Hardening & Full-Spectrum Management Plane Governance Suite", () => {(0, _nodeTest.it)("executes SSE router pipeline + breaker/token-bucket, live Unix socket Vsock + Ed25519 SLSA + UFFD, split-brain chaos + Topology CAS, and reverse tunnel failover + federation + harness auto-heal", async () => {const tmpDir = `/tmp/ansts-m49-${process.pid}-${Date.now()}`;fs.mkdirSync(tmpDir, { recursive: true });const server = new _server_main.AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: tmpDir
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. [M49-1] Cognitive Router SSE Inference Pipeline, 3-State Circuit Breaker & Token Bucket
      const infCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-router-pipeline", "execute_sse_inference", "Northstar"], baseUrl);
      assert.equal(infCli.exitCode, 0);
      const infData = JSON.parse(infCli.stdout);











      assert.equal(infData.tenantId, "Northstar");
      assert.ok(infData.inference.ttftMs > 0);
      assert.ok(infData.inference.tpotMs > 0);
      assert.ok(infData.inference.cachedTokens > 0);
      assert.ok(infData.inference.sseChunksReceived >= 2);
      assert.equal(infData.inference.circuitState, "CLOSED");

      const breakerCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-router-pipeline", "trip_and_recover_circuit_breaker"], baseUrl);
      assert.equal(breakerCli.exitCode, 0);
      const breakerData = JSON.parse(breakerCli.stdout);



      assert.deepEqual(breakerData.stateSequence, ["CLOSED", "OPEN", "HALF_OPEN", "CLOSED"]);
      assert.equal(breakerData.recovered, true);

      const bucketCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-router-pipeline", "exhaust_tpm_bucket"], baseUrl);
      assert.equal(bucketCli.exitCode, 0);
      const bucketData = JSON.parse(bucketCli.stdout);




      assert.equal(bucketData.firstConsume.allowed, true);
      assert.equal(bucketData.throttledConsume.allowed, false);
      assert.ok(bucketData.throttledConsume.waitMs > 0);
      assert.equal(bucketData.refilledConsume.allowed, true);

      // 2. [M49-2] Live Unix Socket Vsock Daemon, eBPF LSM, Ed25519 SLSA Provenance & UFFD Page Restorer
      const vsockCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-security", "vsock_unix_daemon_probe"], baseUrl);
      assert.equal(vsockCli.exitCode, 0);
      const vsockData = JSON.parse(vsockCli.stdout);



      assert.equal(vsockData.allowedProbe.allowed, true);
      assert.equal(vsockData.allowedProbe.status, 200);
      assert.equal(vsockData.blockedProbe.allowed, false);
      assert.equal(vsockData.blockedProbe.status, 403);

      const slsaCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-security", "ebpf_and_slsa_attest"], baseUrl);
      assert.equal(slsaCli.exitCode, 0);
      const slsaData = JSON.parse(slsaCli.stdout);





      assert.equal(slsaData.shadowHook.allowed, false);
      assert.equal(slsaData.shadowHook.errno, 13);
      assert.equal(slsaData.workspaceHook.allowed, true);
      assert.equal(slsaData.slsaVerified, true);
      assert.ok(slsaData.slsaProvenance.ed25519SignatureHex.length >= 64);

      const uffdCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-security", "uffd_page_fault_restore"], baseUrl);
      assert.equal(uffdCli.exitCode, 0);
      const uffdData = JSON.parse(uffdCli.stdout);





      assert.ok(uffdData.stagedPagesCount >= 3);
      assert.equal(uffdData.fault0Cold.fetchedFromRemote, true);
      assert.equal(uffdData.fault0Cached.fetchedFromRemote, false);
      assert.equal(uffdData.stats.faultedPagesCount, 2);
      assert.equal(uffdData.stats.restoredTotalBytes, 8192);

      // 3. [M49-3] Split-Brain / Network Partition Chaos Drill, Host Preflight & Topology CAS
      const chaosCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-chaos", "run_partition_and_split_brain_drill"], baseUrl);
      assert.equal(chaosCli.exitCode, 0);
      const chaosData = JSON.parse(chaosCli.stdout);



      assert.equal(chaosData.drill.partitionBlockedTraffic, true);
      assert.equal(chaosData.drill.partitionHealed, true);
      assert.equal(chaosData.drill.zombieBlocked, true);
      assert.equal(chaosData.drill.rpoZeroVerified, true);
      assert.equal(chaosData.staleFenceRejected409, true);

      const casCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-chaos", "probe_host_preflight_and_topology_cas"], baseUrl);
      assert.equal(casCli.exitCode, 0);
      const casData = JSON.parse(casCli.stdout);



      assert.equal(casData.preflight.ready, true);
      assert.ok(casData.topologyCas.generation >= 2);
      assert.equal(casData.topologyCas.ringDigest.length, 64);

      // Verify Topology CAS conflict returns 409
      const conflictResp = await fetch(`${baseUrl}/v1beta1/management/chaos/drill`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "probe_host_preflight_and_topology_cas",
          expectedGeneration: 0,
          actor: "chen",
          role: "SRE_ADMIN"
        })
      });
      assert.equal(conflictResp.status, 409);

      // 4. [M49-4] Dual-Homed Reverse Tunnel Failover, Multi-Region Federation, Harness Auto-Heal & Velocity/Staging Gate
      const tunCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-federation", "tunnel_command_and_failover"], baseUrl);
      assert.equal(tunCli.exitCode, 0);
      const tunData = JSON.parse(tunCli.stdout);




      assert.equal(tunData.polledCount, 1);
      assert.equal(tunData.ackResult.status, "EXECUTED_OVER_REVERSE_TUNNEL");
      assert.equal(tunData.failoverTunnel.gatewayId, "celld-gw-02");
      assert.equal(tunData.failoverTunnel.role, "SECONDARY");

      const fedCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-federation", "federation_and_harness_autoheal", "EU"], baseUrl);
      assert.equal(fedCli.exitCode, 0);
      const fedData = JSON.parse(fedCli.stdout);




      assert.equal(fedData.selectedRegion.regionCode, "eu-central-1");
      assert.equal(fedData.selectedRegion.sovereigntyZone, "EU");
      assert.equal(fedData.rehydratedTurnIndex, 1);
      assert.equal(fedData.autoHealResult.recoveredFromMigrationInterrupt, true);

      const velCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-federation", "velocity_breaker_and_staging_commit"], baseUrl);
      assert.equal(velCli.exitCode, 0);
      const velData = JSON.parse(velCli.stdout);




      assert.equal(velData.astValidation.valid, true);
      assert.equal(velData.velocityBreakerCheck.tripped, true);
      assert.equal(velData.velocityBreakerCheck.reason, "SEMANTIC_DEAD_LOOP");
      assert.equal(velData.committedEffects.length, 1);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});