/**
 * @fileoverview M49 End-to-End Verification: Deep Backend Dependency Hardening & Full-Spectrum Management Plane Governance Suite
 * Verifies:
 * 1. [M49-1] Cognitive Router Full-Chain Backend Pipeline (`ModelPoolClientAdapter` + `LiveOpenAiCompatibleSseAdapter` + `SlidingWindowTokenBucket` + `ThreeStateCircuitBreaker`) (`POST /v1beta1/management/router/inference-pipeline`).
 * 2. [M49-2] Live Unix Domain Socket `ZeroTrustVsockHostDaemon`, `EbpfLsmSecurityBridge`, Ed25519 `SlsaProvenanceAttestor` & `UffdLazyPageFaultRestorer` (`POST /v1beta1/management/security/kernel-attestation`).
 * 3. [M49-3] Automated Split-Brain / Network Partition Chaos Drill, Host Preflight Readiness Gate & SQLite-Backed Topology CAS Coordinator (`POST /v1beta1/management/chaos/drill`).
 * 4. [M49-4] Dual-Homed Reverse Tunnel Failover (`TunnelGatewayCell`), Multi-Region Federation (`MultiRegionFederationRouter`), `AgentTaskHarnessCell` Auto-Heal & Velocity/Staging Gate (`POST /v1beta1/management/federation/tunnel-harness`).
 */
import "./console_write_auth.js";
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M49 Deep Backend Dependency Hardening & Full-Spectrum Management Plane Governance Suite", () => {
  it("executes SSE router pipeline + breaker/token-bucket, live Unix socket Vsock + Ed25519 SLSA + UFFD, split-brain chaos + Topology CAS, and reverse tunnel failover + federation + harness auto-heal", async () => {
    const tmpDir = `/tmp/ansts-m49-${process.pid}-${Date.now()}`;
    fs.mkdirSync(tmpDir, { recursive: true });

    const server = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: tmpDir,
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. [M49-1] Cognitive Router SSE Inference Pipeline, 3-State Circuit Breaker & Token Bucket
      const infCli = await runAnstsCtlRemote(["mgmt-router-pipeline", "execute_sse_inference", "Northstar"], baseUrl);
      assert.equal(infCli.exitCode, 0);
      const infData = JSON.parse(infCli.stdout) as {
        tenantId: string;
        inference: {
          modelName: string;
          ttftMs: number;
          tpotMs: number;
          cachedTokens: number;
          costUsd: number;
          circuitState: string;
          sseChunksReceived: number;
        };
      };
      assert.equal(infData.tenantId, "Northstar");
      assert.ok(infData.inference.ttftMs > 0);
      assert.ok(infData.inference.tpotMs > 0);
      assert.ok(infData.inference.cachedTokens > 0);
      assert.ok(infData.inference.sseChunksReceived >= 2);
      assert.equal(infData.inference.circuitState, "CLOSED");

      const breakerCli = await runAnstsCtlRemote(["mgmt-router-pipeline", "trip_and_recover_circuit_breaker"], baseUrl);
      assert.equal(breakerCli.exitCode, 0);
      const breakerData = JSON.parse(breakerCli.stdout) as {
        stateSequence: string[];
        recovered: boolean;
      };
      assert.deepEqual(breakerData.stateSequence, ["CLOSED", "OPEN", "HALF_OPEN", "CLOSED"]);
      assert.equal(breakerData.recovered, true);

      const bucketCli = await runAnstsCtlRemote(["mgmt-router-pipeline", "exhaust_tpm_bucket"], baseUrl);
      assert.equal(bucketCli.exitCode, 0);
      const bucketData = JSON.parse(bucketCli.stdout) as {
        firstConsume: { allowed: boolean };
        throttledConsume: { allowed: boolean; waitMs: number };
        refilledConsume: { allowed: boolean };
      };
      assert.equal(bucketData.firstConsume.allowed, true);
      assert.equal(bucketData.throttledConsume.allowed, false);
      assert.ok(bucketData.throttledConsume.waitMs > 0);
      assert.equal(bucketData.refilledConsume.allowed, true);

      // 2. [M49-2] Live Unix Socket Vsock Daemon, eBPF LSM, Ed25519 SLSA Provenance & UFFD Page Restorer
      const vsockCli = await runAnstsCtlRemote(["mgmt-security", "vsock_unix_daemon_probe"], baseUrl);
      assert.equal(vsockCli.exitCode, 0);
      const vsockData = JSON.parse(vsockCli.stdout) as {
        allowedProbe: { allowed: boolean; status: number };
        blockedProbe: { allowed: boolean; status: number };
      };
      assert.equal(vsockData.allowedProbe.allowed, true);
      assert.equal(vsockData.allowedProbe.status, 200);
      assert.equal(vsockData.blockedProbe.allowed, false);
      assert.equal(vsockData.blockedProbe.status, 403);

      const slsaCli = await runAnstsCtlRemote(["mgmt-security", "ebpf_and_slsa_attest"], baseUrl);
      assert.equal(slsaCli.exitCode, 0);
      const slsaData = JSON.parse(slsaCli.stdout) as {
        shadowHook: { allowed: boolean; errno: number };
        workspaceHook: { allowed: boolean; errno: number };
        slsaProvenance: { ed25519SignatureHex: string; publicKeyDerHex: string };
        slsaVerified: boolean;
      };
      assert.equal(slsaData.shadowHook.allowed, false);
      assert.equal(slsaData.shadowHook.errno, 13);
      assert.equal(slsaData.workspaceHook.allowed, true);
      assert.equal(slsaData.slsaVerified, true);
      assert.ok(slsaData.slsaProvenance.ed25519SignatureHex.length >= 64);

      const uffdCli = await runAnstsCtlRemote(["mgmt-security", "uffd_page_fault_restore"], baseUrl);
      assert.equal(uffdCli.exitCode, 0);
      const uffdData = JSON.parse(uffdCli.stdout) as {
        stagedPagesCount: number;
        fault0Cold: { fetchedFromRemote: boolean };
        fault0Cached: { fetchedFromRemote: boolean };
        stats: { faultedPagesCount: number; restoredTotalBytes: number };
      };
      assert.ok(uffdData.stagedPagesCount >= 3);
      assert.equal(uffdData.fault0Cold.fetchedFromRemote, true);
      assert.equal(uffdData.fault0Cached.fetchedFromRemote, false);
      assert.equal(uffdData.stats.faultedPagesCount, 2);
      assert.equal(uffdData.stats.restoredTotalBytes, 8192);

      // 3. [M49-3] Split-Brain / Network Partition Chaos Drill, Host Preflight & Topology CAS
      const chaosCli = await runAnstsCtlRemote(["mgmt-chaos", "run_partition_and_split_brain_drill"], baseUrl);
      assert.equal(chaosCli.exitCode, 0);
      const chaosData = JSON.parse(chaosCli.stdout) as {
        drill: { partitionBlockedTraffic: boolean; partitionHealed: boolean; zombieBlocked: boolean; rpoZeroVerified: boolean };
        staleFenceRejected409: boolean;
      };
      assert.equal(chaosData.drill.partitionBlockedTraffic, true);
      assert.equal(chaosData.drill.partitionHealed, true);
      assert.equal(chaosData.drill.zombieBlocked, true);
      assert.equal(chaosData.drill.rpoZeroVerified, true);
      assert.equal(chaosData.staleFenceRejected409, true);

      const casCli = await runAnstsCtlRemote(["mgmt-chaos", "probe_host_preflight_and_topology_cas"], baseUrl);
      assert.equal(casCli.exitCode, 0);
      const casData = JSON.parse(casCli.stdout) as {
        preflight: { ready: boolean; freeDiskGb: number; freeMemoryMb: number };
        topologyCas: { generation: number; members: string[]; ringDigest: string };
      };
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
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(conflictResp.status, 409);

      // 4. [M49-4] Dual-Homed Reverse Tunnel Failover, Multi-Region Federation, Harness Auto-Heal & Velocity/Staging Gate
      const tunCli = await runAnstsCtlRemote(["mgmt-federation", "tunnel_command_and_failover"], baseUrl);
      assert.equal(tunCli.exitCode, 0);
      const tunData = JSON.parse(tunCli.stdout) as {
        polledCount: number;
        ackResult: { status: string };
        failoverTunnel: { gatewayId: string; role: string };
      };
      assert.equal(tunData.polledCount, 1);
      assert.equal(tunData.ackResult.status, "EXECUTED_OVER_REVERSE_TUNNEL");
      assert.equal(tunData.failoverTunnel.gatewayId, "celld-gw-02");
      assert.equal(tunData.failoverTunnel.role, "SECONDARY");

      const fedCli = await runAnstsCtlRemote(["mgmt-federation", "federation_and_harness_autoheal", "EU"], baseUrl);
      assert.equal(fedCli.exitCode, 0);
      const fedData = JSON.parse(fedCli.stdout) as {
        selectedRegion: { regionCode: string; sovereigntyZone: string };
        rehydratedTurnIndex: number;
        autoHealResult: { recoveredFromMigrationInterrupt: boolean };
      };
      assert.equal(fedData.selectedRegion.regionCode, "eu-central-1");
      assert.equal(fedData.selectedRegion.sovereigntyZone, "EU");
      assert.equal(fedData.rehydratedTurnIndex, 1);
      assert.equal(fedData.autoHealResult.recoveredFromMigrationInterrupt, true);

      const velCli = await runAnstsCtlRemote(["mgmt-federation", "velocity_breaker_and_staging_commit"], baseUrl);
      assert.equal(velCli.exitCode, 0);
      const velData = JSON.parse(velCli.stdout) as {
        astValidation: { valid: boolean };
        velocityBreakerCheck: { tripped: boolean; reason: string };
        committedEffects: Array<{ effectId: string }>;
      };
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
