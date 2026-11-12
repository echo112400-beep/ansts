import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M85 TanStack Console Security, RBAC & Release Governance Center, Live Intrusion Probe & Theme/Polling Controls", () => {
  it("[M85-1] Exposes RBAC principals, SHA-256 Audit Chain, 8/8 Release Gates & Zero-Trust Intrusion Probes via /v1beta1/console/security", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m85-sec-"));
    const server = new AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new ConsoleRbacAuthGuard();
      const chenToken = guard.issuePrincipalToken("chen");
      const xuToken = guard.issuePrincipalToken("xu");

      // 1. Inspect initial securityVisualization via GET /v1beta1/console/security
      const secResp = await fetch(`${baseUrl}/v1beta1/console/security`);
      assert.equal(secResp.status, 200);
      const secData = (await secResp.json()) as {
        rbacPrincipals: Array<{ sub: string; role: string; canApprove: boolean }>;
        auditChainIntegrity: { valid: boolean; totalEntries: number; headHashHex: string };
        zeroTrustPosture: { spiffeTrustDomain: string; teePlatform: string };
        releaseReadiness: { readyForProduction: boolean; passedCount: number; totalGates: number };
      };
      assert.equal(secData.rbacPrincipals.length, 4);
      assert.equal(secData.auditChainIntegrity.valid, true);
      assert.equal(secData.zeroTrustPosture.spiffeTrustDomain, "spiffe://ansts.io");
      assert.equal(secData.zeroTrustPosture.teePlatform, "AMD_SEV_SNP");
      assert.equal(secData.releaseReadiness.readyForProduction, true);
      assert.equal(secData.releaseReadiness.passedCount, 8);
      assert.equal(secData.releaseReadiness.totalGates, 8);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerProbe = await fetch(`${baseUrl}/v1beta1/management/security/probe`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`,
        },
        body: JSON.stringify({ probeType: "ssrf_loopback" }),
      });
      assert.equal(viewerProbe.status, 403);

      // 3. Execute all 4 Zero-Trust Intrusion Probes and verify all are BLOCKED
      for (const probeType of [
        "ssrf_loopback",
        "ssrf_imds_hex",
        "lsm_proc_escape",
        "vsock_cred_injection",
      ]) {
        const probeResp = await fetch(`${baseUrl}/v1beta1/management/security/probe`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${chenToken}`,
          },
          body: JSON.stringify({ probeType }),
        });
        assert.equal(probeResp.status, 200);
        const probeBody = (await probeResp.json()) as {
          probe: { blocked: boolean; target: string };
        };
        assert.equal(probeBody.probe.blocked, true);
      }

      // 4. Verify CLI anstsctl mgmt-security-console
      const cliSec = await runAnstsCtlRemote(["mgmt-security-console", "inspect"], baseUrl);
      assert.equal(cliSec.exitCode, 0);
      const cliParsed = JSON.parse(cliSec.stdout) as {
        releaseReadiness: { passedCount: number };
      };
      assert.equal(cliParsed.releaseReadiness.passedCount, 8);

      // 5. Verify UI bindings in prototype.js & docs/index.html
      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        assert.ok(content.includes("renderSecuritySection"));
        assert.ok(content.includes("sec-run-probe"));
        assert.ok(content.includes("sec-run-custom-probe"));
        assert.ok(content.includes("sec-issue-verify-jwt"));
        assert.ok(content.includes("toggle-theme"));
        assert.ok(content.includes("toggle-auto-refresh"));
      }
      assert.ok(indexHtml.includes("html.dark-theme"));
    } finally {
      await server.stop();
    }
  });
});
