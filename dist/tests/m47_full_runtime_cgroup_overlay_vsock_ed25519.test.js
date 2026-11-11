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
 * @fileoverview M47 End-to-End Verification: Real Disk/Subprocess Vsock RPC (`ZeroTrustVsockBridge`),
 * Real Ed25519 Asymmetric Cryptographic Attestation (`ConfidentialTeeAttestationBridge` & `MtlsReverseTunnelBridge`),
 * Real 4KiB SHA-256 Page-Fault Hydration in `WalCompactorAndPagedRestorer` (`UffdLazyPageFaultRestorer`),
 * Real SQLite/R2-Backed `CelldAdminClient`, and Physical Linux Cgroup v2 / OverlayFS Upperdir / MicroVM Warm Pool Lifecycle.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M47 Full-Stack Real-Execution Grounding of All Remaining Runtime, Cryptographic, Storage & Sandbox Modules", () => {
    (0, node_test_1.it)("executes physical Linux cgroup v2 & OverlayFS upperdir mounts, real disk/subprocess Vsock RPC, Ed25519 SPIFFE/TEE cryptographic verification, 4KiB SHA-256 UFFD page hydration, and SQLite/R2 CelldAdmin migration", async () => {
        const tmpDir = `/tmp/ansts-m47-${process.pid}-${Date.now()}`;
        fs.mkdirSync(tmpDir, { recursive: true });
        const server = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-01",
            nodeRole: "gateway",
            storageDir: `${tmpDir}/gw-01`,
        });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            // 1. [M47-3] Allocate MicroVM from WarmPool, mount 16-slot Multiplexer, write physical Linux cgroup v2 & OverlayFS upperdir on disk
            const allocCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-sandbox", "allocate_and_mount", "task-ns-8841"], baseUrl);
            assert.equal(allocCli.exitCode, 0);
            const allocData = JSON.parse(allocCli.stdout);
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
            const vsockData = (await vsockResp.json());
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
            const cryptoData = (await cryptoResp.json());
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
            const ltxCompactData = (await ltxCompactResp.json());
            assert.ok(ltxCompactData.uffdRestore.eagerPagesLoaded >= 1);
            assert.equal(ltxCompactData.uffdRestore.faultedPageDigests.length, ltxCompactData.uffdRestore.eagerPagesLoaded);
            assert.equal(ltxCompactData.uffdRestore.faultedPageDigests[0].length, 64);
            assert.ok(ltxCompactData.uffdRestore.actualFaultDurationMs > 0);
            const cellMigCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-sandbox", "migrate_celld_actor", "task-ns-8841"], baseUrl);
            assert.equal(cellMigCli.exitCode, 0);
            const cellMigData = JSON.parse(cellMigCli.stdout);
            assert.equal(cellMigData.previousNodeId, "celld-brain-01");
            assert.equal(cellMigData.newNodeId, "celld-brain-03");
            assert.equal(cellMigData.newEpochFencingToken, 2);
            assert.equal(cellMigData.migrationSha256.length, 64);
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
