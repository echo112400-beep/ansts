import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

test("M89: Config Revision Diff, Dry-Run Blast-Radius Simulator, HTTP BFF, CLI & Console UI", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m89-cfg-"));
  const srv = new AnstsLocalAndComposeServer({
    apiPort: 0,
    dashboardPort: 0,
    storageDir: tmpDir,
  });
  const { apiPort } = await srv.start();
  const baseUrl = `http://127.0.0.1:${apiPort}`;

  try {
    const guard = new ConsoleRbacAuthGuard();
    const adminToken = guard.issuePrincipalToken("chen");

    // 1. Apply a new config revision (Rev #1 -> Rev #2) so we have two revisions to diff
    const applyRes = srv.managementPlane.executeConfigurationCenterGovernance({
      action: "validate_and_apply_config",
      expectedRevision: 1,
      runtimeOverrides: {
        maxCellConcurrentRequests: 128,
        actMemoryMb: 12288,
        durabilityMode: "bucket",
      },
      note: "M89 test config bump",
      actor: "chen",
      role: "SRE_ADMIN",
    });
    assert.equal(applyRes.status, 200);

    // 2. HTTP GET /v1beta1/console/configs/diff?fromRev=1&toRev=2
    const diffHttp = await fetch(`${baseUrl}/v1beta1/console/configs/diff?fromRev=1&toRev=2`, {
      headers: { authorization: `Bearer ${adminToken}` },
    });
    assert.equal(diffHttp.status, 200);
    const diffBody = (await diffHttp.json()) as {
      fromRev: number;
      toRev: number;
      changedCount: number;
      diffs: Array<{ field: string; riskLevel: string }>;
      blastRadius: {
        highestRisk: string;
        estimatedActMemoryDeltaMb: number;
        estimatedConcurrentCapacityDelta: number;
        recommendedRolloutStrategy: string;
      };
    };
    assert.equal(diffBody.fromRev, 1);
    assert.equal(diffBody.toRev, 2);
    assert.equal(diffBody.changedCount, 3);
    assert.equal(diffBody.blastRadius.highestRisk, "HIGH_DURABILITY_OR_SLO");
    assert.equal(diffBody.blastRadius.estimatedActMemoryDeltaMb, 4096);
    assert.equal(diffBody.blastRadius.estimatedConcurrentCapacityDelta, 64);
    assert.equal(diffBody.blastRadius.recommendedRolloutStrategy, "DUAL_APPROVAL_AND_CANARY");

    // 3. Dry-run valid patch (non-mutating)
    const dryRunHttp = await fetch(`${baseUrl}/v1beta1/management/configs/dry-run`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      },
      body: JSON.stringify({
        patch: { maxCellConcurrentRequests: 256, actMemoryMb: 16384 },
        actor: "chen",
        role: "SRE_ADMIN",
      }),
    });
    assert.equal(dryRunHttp.status, 200);
    const dryRunBody = (await dryRunHttp.json()) as {
      valid: boolean;
      validationErrors: string[];
      activeRevision: number;
      simulatedSha256: string;
      blastRadius: { highestRisk: string; recommendedRolloutStrategy: string };
    };
    assert.equal(dryRunBody.valid, true);
    assert.equal(dryRunBody.validationErrors.length, 0);
    assert.equal(dryRunBody.activeRevision, 2);
    assert.ok(dryRunBody.simulatedSha256.length === 64);
    assert.equal(dryRunBody.blastRadius.highestRisk, "MEDIUM_CAPACITY_SHIFT");
    assert.equal(dryRunBody.blastRadius.recommendedRolloutStrategy, "CANARY_10PCT_5M");

    // Verify activeRevision remained 2 (non-mutating)
    const inspectAfter = srv.managementPlane.executeConfigurationCenterGovernance({
      action: "inspect_active_config",
      actor: "chen",
      role: "SRE_ADMIN",
    });
    assert.equal(inspectAfter.body.activeRevision, 2);

    // 4. Dry-run invalid bounds & RBAC VIEWER 403
    const invalidRes = srv.managementPlane.dryRunConfigPatch({
      patch: { maxCellConcurrentRequests: 99999, psiFullFreezePct: 5 },
      actor: "chen",
      role: "SRE_ADMIN",
    });
    assert.equal(invalidRes.status, 200);
    assert.equal(invalidRes.body.valid, false);
    assert.ok((invalidRes.body.validationErrors as string[]).length >= 2);

    const rbacRes = srv.managementPlane.dryRunConfigPatch({
      patch: { maxCellConcurrentRequests: 96 },
      actor: "xu",
      role: "VIEWER",
    });
    assert.equal(rbacRes.status, 403);
    assert.equal(rbacRes.body.error, "RBAC_FORBIDDEN");

    // 5. CLI mgmt-config-diff & mgmt-config-dry-run
    const cliDiff = await runAnstsCtlRemote(["mgmt-config-diff", "1", "2"], baseUrl);
    assert.equal(cliDiff.exitCode, 0);
    const cliDiffJson = JSON.parse(cliDiff.stdout) as { changedCount: number };
    assert.equal(cliDiffJson.changedCount, 3);

    const cliDryRun = await runAnstsCtlRemote([
      "mgmt-config-dry-run",
      JSON.stringify({ maxCellConcurrentRequests: 192 }),
    ], baseUrl);
    assert.equal(cliDryRun.exitCode, 0);
    const cliDryRunJson = JSON.parse(cliDryRun.stdout) as { valid: boolean };
    assert.equal(cliDryRunJson.valid, true);

    // 6. Frontend UI markers in docs/index.html and docs/console-management/prototype.js
    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = fs.readFileSync(path.resolve(process.cwd(), rel), "utf8");
      assert.ok(content.includes("config-diff-revisions"), `${rel} missing config-diff-revisions`);
      assert.ok(content.includes("open-config-dry-run-dialog"), `${rel} missing open-config-dry-run-dialog`);
      assert.ok(content.includes("/v1beta1/console/configs/diff"), `${rel} missing /v1beta1/console/configs/diff`);
    }
  } finally {
    await srv.stop();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});
