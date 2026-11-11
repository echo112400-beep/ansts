/**
 * @fileoverview M47 End-to-End Verification: Real Disk/Subprocess Vsock RPC (`ZeroTrustVsockBridge`),
 * Real Ed25519 Asymmetric Cryptographic Attestation (`ConfidentialTeeAttestationBridge` & `MtlsReverseTunnelBridge`),
 * Real 4KiB SHA-256 Page-Fault Hydration in `WalCompactorAndPagedRestorer` (`UffdLazyPageFaultRestorer`),
 * Real SQLite/R2-Backed `CelldAdminClient`, and Physical Linux Cgroup v2 / OverlayFS Upperdir / MicroVM Warm Pool Lifecycle.
 */
import "./console_write_auth.js";
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M47 Full-Stack Real-Execution Grounding of All Remaining Runtime, Cryptographic, Storage & Sandbox Modules", () => {
  it("executes physical Linux cgroup v2 & OverlayFS upperdir mounts, real disk/subprocess Vsock RPC, Ed25519 SPIFFE/TEE cryptographic verification, 4KiB SHA-256 UFFD page hydration, and SQLite/R2 CelldAdmin migration", async () => {
    const tmpDir = `/tmp/ansts-m47-${process.pid}-${Date.now()}`;
    fs.mkdirSync(tmpDir, { recursive: true });

    const server = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: `${tmpDir}/gw-01`,
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. [M47-3] Allocate MicroVM from WarmPool, mount 16-slot Multiplexer, write physical Linux cgroup v2 & OverlayFS upperdir on disk
      const allocCli = await runAnstsCtlRemote(["mgmt-sandbox", "allocate_and_mount", "task-ns-8841"], baseUrl);
      assert.equal(allocCli.exitCode, 0);
      const allocData = JSON.parse(allocCli.stdout) as {
        microVm: { vmId: string; state: string };
        multiplexer: { sandboxNumber: number; activeWorkspacesOnSandbox: number };
        cgroupLimits: { cpuMaxContent: string };
        overlayUpperSnapshot: { changedFiles: string[]; sha256: string };
        rightSizedProfile: { act_cpu_cores: number; act_memory_mb: number };
      };
      assert.equal(allocData.microVm.state, "ALLOCATED");
      assert.equal(allocData.multiplexer.sandboxNumber, 1);
      assert.equal(allocData.cgroupLimits.cpuMaxContent, "400000 100000");
      assert.ok(allocData.overlayUpperSnapshot.changedFiles.includes("sandbox_mount.meta"));
      assert.equal(allocData.overlayUpperSnapshot.sha256.length, 64);
      assert.ok(allocData.rightSizedProfile.act_cpu_cores >= 3.0);

      // 2. [M47-1] Real Sandboxed File I/O (`WRITE_FILE` / `READ_FILE`) & Subprocess Command (`EXEC_CMD`) over `ZeroTrustVsockBridge`
      const vsockResp = await fetch(`${baseUrl}/v1beta1/management/runtime/sandbox`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "vsock_exec_and_io",
          taskId: "task-ns-8841",
          filePath: "sub_agent/result.json",
          fileContent: "{\"status\":\"REAL_VSOCK_DISK_IO\"}\n",
          command: "cat sub_agent/result.json",
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(vsockResp.status, 200);
      const vsockData = (await vsockResp.json()) as {
        writeResult: { bytesWritten: number; sha256: string };
        execResult: { exitCode: number; rawStdout: string };
        readResult: { content: string; sha256: string };
      };
      assert.ok(vsockData.writeResult.bytesWritten > 10);
      assert.equal(vsockData.writeResult.sha256, vsockData.readResult.sha256);
      assert.equal(vsockData.execResult.exitCode, 0);
      assert.ok(vsockData.execResult.rawStdout.includes("REAL_VSOCK_DISK_IO"));

      // 3. [M47-1] Real Ed25519 Asymmetric Cryptographic Attestation (`MtlsReverseTunnelBridge` SPIFFE SVID & `ConfidentialTeeAttestationBridge` SEV-SNP Quote)
      const cryptoResp = await fetch(`${baseUrl}/v1beta1/management/runtime/sandbox`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "issue_and_verify_mtls_tee",
          taskId: "task-ns-8841",
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(cryptoResp.status, 200);
      const cryptoData = (await cryptoResp.json()) as {
        spiffeSvid: { svidSignatureHex: string; caPublicKeyHex: string };
        spiffeVerification: { authenticated: boolean; cryptographicallyVerified: boolean };
        teeAttestation: { verified: boolean; asymmetricVerified: boolean; ed25519SignatureHex: string };
      };
      assert.equal(cryptoData.spiffeVerification.authenticated, true);
      assert.equal(cryptoData.spiffeVerification.cryptographicallyVerified, true);
      assert.equal(cryptoData.spiffeSvid.svidSignatureHex.length, 128);
      assert.equal(cryptoData.teeAttestation.verified, true);
      assert.equal(cryptoData.teeAttestation.asymmetricVerified, true);
      assert.equal(cryptoData.teeAttestation.ed25519SignatureHex.length, 128);

      // 4. [M47-2] Real 4KiB SHA-256 Page-Fault Hydration in `WalCompactorAndPagedRestorer` & SQLite/R2 `CelldAdminClient` Cell Migration
      const ltxCompactResp = await fetch(`${baseUrl}/v1beta1/celld/ltx-compaction`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ taskId: "task-ns-8841", l0ObjectCount: 320, txidDistance: 512, totalL0Megabytes: 48 }),
      });
      assert.equal(ltxCompactResp.status, 200);
      const ltxCompactData = (await ltxCompactResp.json()) as {
        uffdRestore: { eagerPagesLoaded: number; faultedPageDigests: string[]; actualFaultDurationMs: number };
      };
      assert.ok(ltxCompactData.uffdRestore.eagerPagesLoaded >= 1);
      assert.equal(
        ltxCompactData.uffdRestore.faultedPageDigests.length,
        ltxCompactData.uffdRestore.eagerPagesLoaded
      );
      assert.equal(ltxCompactData.uffdRestore.faultedPageDigests[0].length, 64);
      assert.ok(ltxCompactData.uffdRestore.actualFaultDurationMs > 0);

      const cellMigCli = await runAnstsCtlRemote(["mgmt-sandbox", "migrate_celld_actor", "task-ns-8841"], baseUrl);
      assert.equal(cellMigCli.exitCode, 0);
      const cellMigData = JSON.parse(cellMigCli.stdout) as {
        previousNodeId: string;
        newNodeId: string;
        newEpochFencingToken: number;
        migrationSha256: string;
      };
      assert.equal(cellMigData.previousNodeId, "celld-brain-01");
      assert.equal(cellMigData.newNodeId, "celld-brain-03");
      assert.equal(cellMigData.newEpochFencingToken, 2);
      assert.equal(cellMigData.migrationSha256.length, 64);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
