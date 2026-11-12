"use strict";var _strict = _interopRequireDefault(require("node:assert/strict"));
var _nodeFs = _interopRequireDefault(require("node:fs"));
var _nodeOs = _interopRequireDefault(require("node:os"));
var _nodePath = _interopRequireDefault(require("node:path"));
var _nodeTest = _interopRequireDefault(require("node:test"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.default)("M89: Config Revision Diff, Dry-Run Blast-Radius Simulator, HTTP BFF, CLI & Console UI", async () => {
  const tmpDir = _nodeFs.default.mkdtempSync(_nodePath.default.join(_nodeOs.default.tmpdir(), "ansts-m89-cfg-"));
  const srv = new _server_main.AnstsLocalAndComposeServer({
    apiPort: 0,
    dashboardPort: 0,
    storageDir: tmpDir
  });
  const { apiPort } = await srv.start();
  const baseUrl = `http://127.0.0.1:${apiPort}`;

  try {
    const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
    const adminToken = guard.issuePrincipalToken("chen");

    // 1. Apply a new config revision (Rev #1 -> Rev #2) so we have two revisions to diff
    const applyRes = srv.managementPlane.executeConfigurationCenterGovernance({
      action: "validate_and_apply_config",
      expectedRevision: 1,
      runtimeOverrides: {
        maxCellConcurrentRequests: 128,
        actMemoryMb: 12288,
        durabilityMode: "bucket"
      },
      note: "M89 test config bump",
      actor: "chen",
      role: "SRE_ADMIN"
    });
    _strict.default.equal(applyRes.status, 200);

    // 2. HTTP GET /v1beta1/console/configs/diff?fromRev=1&toRev=2
    const diffHttp = await fetch(`${baseUrl}/v1beta1/console/configs/diff?fromRev=1&toRev=2`, {
      headers: { authorization: `Bearer ${adminToken}` }
    });
    _strict.default.equal(diffHttp.status, 200);
    const diffBody = await diffHttp.json();











    _strict.default.equal(diffBody.fromRev, 1);
    _strict.default.equal(diffBody.toRev, 2);
    _strict.default.equal(diffBody.changedCount, 3);
    _strict.default.equal(diffBody.blastRadius.highestRisk, "HIGH_DURABILITY_OR_SLO");
    _strict.default.equal(diffBody.blastRadius.estimatedActMemoryDeltaMb, 4096);
    _strict.default.equal(diffBody.blastRadius.estimatedConcurrentCapacityDelta, 64);
    _strict.default.equal(diffBody.blastRadius.recommendedRolloutStrategy, "DUAL_APPROVAL_AND_CANARY");

    // 3. Dry-run valid patch (non-mutating)
    const dryRunHttp = await fetch(`${baseUrl}/v1beta1/management/configs/dry-run`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      },
      body: JSON.stringify({
        patch: { maxCellConcurrentRequests: 256, actMemoryMb: 16384 },
        actor: "chen",
        role: "SRE_ADMIN"
      })
    });
    _strict.default.equal(dryRunHttp.status, 200);
    const dryRunBody = await dryRunHttp.json();






    _strict.default.equal(dryRunBody.valid, true);
    _strict.default.equal(dryRunBody.validationErrors.length, 0);
    _strict.default.equal(dryRunBody.activeRevision, 2);
    _strict.default.ok(dryRunBody.simulatedSha256.length === 64);
    _strict.default.equal(dryRunBody.blastRadius.highestRisk, "MEDIUM_CAPACITY_SHIFT");
    _strict.default.equal(dryRunBody.blastRadius.recommendedRolloutStrategy, "CANARY_10PCT_5M");

    // Verify activeRevision remained 2 (non-mutating)
    const inspectAfter = srv.managementPlane.executeConfigurationCenterGovernance({
      action: "inspect_active_config",
      actor: "chen",
      role: "SRE_ADMIN"
    });
    _strict.default.equal(inspectAfter.body.activeRevision, 2);

    // 4. Dry-run invalid bounds & RBAC VIEWER 403
    const invalidRes = srv.managementPlane.dryRunConfigPatch({
      patch: { maxCellConcurrentRequests: 99999, psiFullFreezePct: 5 },
      actor: "chen",
      role: "SRE_ADMIN"
    });
    _strict.default.equal(invalidRes.status, 200);
    _strict.default.equal(invalidRes.body.valid, false);
    _strict.default.ok(invalidRes.body.validationErrors.length >= 2);

    const rbacRes = srv.managementPlane.dryRunConfigPatch({
      patch: { maxCellConcurrentRequests: 96 },
      actor: "xu",
      role: "VIEWER"
    });
    _strict.default.equal(rbacRes.status, 403);
    _strict.default.equal(rbacRes.body.error, "RBAC_FORBIDDEN");

    // 5. CLI mgmt-config-diff & mgmt-config-dry-run
    const cliDiff = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-config-diff", "1", "2"], baseUrl);
    _strict.default.equal(cliDiff.exitCode, 0);
    const cliDiffJson = JSON.parse(cliDiff.stdout);
    _strict.default.equal(cliDiffJson.changedCount, 3);

    const cliDryRun = await (0, _anstsctl.runAnstsCtlRemote)([
    "mgmt-config-dry-run",
    JSON.stringify({ maxCellConcurrentRequests: 192 })],
    baseUrl);
    _strict.default.equal(cliDryRun.exitCode, 0);
    const cliDryRunJson = JSON.parse(cliDryRun.stdout);
    _strict.default.equal(cliDryRunJson.valid, true);

    // 6. Frontend UI markers in docs/index.html and docs/console-management/prototype.js
    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = _nodeFs.default.readFileSync(_nodePath.default.resolve(process.cwd(), rel), "utf8");
      _strict.default.ok(content.includes("config-diff-revisions"), `${rel} missing config-diff-revisions`);
      _strict.default.ok(content.includes("open-config-dry-run-dialog"), `${rel} missing open-config-dry-run-dialog`);
      _strict.default.ok(content.includes("/v1beta1/console/configs/diff"), `${rel} missing /v1beta1/console/configs/diff`);
    }
  } finally {
    await srv.stop();
    _nodeFs.default.rmSync(tmpDir, { recursive: true, force: true });
  }
});