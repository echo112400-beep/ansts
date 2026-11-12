"use strict";var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.describe)("M80 TanStack Console Configuration Management Center & Deep Runtime Visualization Suite", () => {
  (0, _nodeTest.it)("[M80-1] Configuration Management Center (/v1beta1/console/configs & /v1beta1/management/config/center) supports CAS updates, schema guard, RBAC, diff preview & rollback", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m80-cfg-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
      const chenToken = guard.issuePrincipalToken("chen");
      const xuToken = guard.issuePrincipalToken("xu");

      // 1. Inspect initial config visualization via GET /v1beta1/console/configs
      const initResp = await fetch(`${baseUrl}/v1beta1/console/configs`);
      _strict.default.equal(initResp.status, 200);
      const initCfg = await initResp.json();





      _strict.default.equal(initCfg.activeRevision, 1);
      _strict.default.equal(initCfg.activeConfig.maxCellConcurrentRequests, 64);
      _strict.default.equal(initCfg.schemaRules.length, 11);
      _strict.default.equal(initCfg.revisions.length, 1);
      _strict.default.equal(initCfg.revisions[0].config.maxCellConcurrentRequests, 64);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`
        },
        body: JSON.stringify({
          action: "validate_and_apply_config",
          expectedRevision: 1,
          runtimeOverrides: { maxCellConcurrentRequests: 128 }
        })
      });
      _strict.default.equal(viewerResp.status, 403);

      // 3. Invalid schema parameter (maxCellConcurrentRequests: -10) is rejected with 400 CONFIG_SCHEMA_INVALID
      const invalidResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "validate_and_apply_config",
          expectedRevision: 1,
          runtimeOverrides: { maxCellConcurrentRequests: -10 }
        })
      });
      _strict.default.equal(invalidResp.status, 400);
      const invalidBody = await invalidResp.json();
      _strict.default.equal(invalidBody.error, "CONFIG_SCHEMA_INVALID");

      // 4. Valid CAS hot-reload update advances revision 1 -> 2
      const applyResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "validate_and_apply_config",
          expectedRevision: 1,
          runtimeOverrides: {
            maxCellConcurrentRequests: 128,
            actCpuCores: 6.0,
            psiFullFreezePct: 75.0
          },
          note: "Scale up cell concurrency and ACT cores for peak traffic"
        })
      });
      _strict.default.equal(applyResp.status, 200);
      const applyBody = await applyResp.json();





      _strict.default.equal(applyBody.previousRevision, 1);
      _strict.default.equal(applyBody.activeRevision, 2);
      _strict.default.ok(applyBody.changedKeys.includes("maxCellConcurrentRequests"));
      _strict.default.equal(applyBody.activeConfig.maxCellConcurrentRequests, 128);
      _strict.default.equal(applyBody.activeConfig.actCpuCores, 6.0);

      // 5. Stale expectedRevision=1 now triggers 409 CONFIG_CAS_CONFLICT
      const casConflictResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "validate_and_apply_config",
          expectedRevision: 1,
          runtimeOverrides: { maxCellConcurrentRequests: 256 }
        })
      });
      _strict.default.equal(casConflictResp.status, 409);
      const casConflictBody = await casConflictResp.json();
      _strict.default.equal(casConflictBody.error, "CONFIG_CAS_CONFLICT");
      _strict.default.equal(casConflictBody.actualRevision, 2);

      // 6. Rollback to revision #1 generates revision #3 with restored config
      const rollbackResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "rollback_revision",
          targetRevision: 1
        })
      });
      _strict.default.equal(rollbackResp.status, 200);
      const rollbackBody = await rollbackResp.json();




      _strict.default.equal(rollbackBody.rolledBackToRevision, 1);
      _strict.default.equal(rollbackBody.activeRevision, 3);
      _strict.default.equal(rollbackBody.activeConfig.maxCellConcurrentRequests, 64);
      _strict.default.equal(rollbackBody.activeConfig.actCpuCores, 4.0);
    } finally {
      await server.stop();
    }
  });

  (0, _nodeTest.it)("[M80-2] Deep Runtime Visualization (/v1beta1/console/runtime), LayerFS CoW Branch Management, CLI mgmt-runtime & UI Wiring", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m80-rt-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
      const chenToken = guard.issuePrincipalToken("chen");
      const xuToken = guard.issuePrincipalToken("xu");

      // 1. Inspect runtimeVisualization via GET /v1beta1/console/runtime
      const rtResp = await fetch(`${baseUrl}/v1beta1/console/runtime`);
      _strict.default.equal(rtResp.status, 200);
      const rtData = await rtResp.json();










      _strict.default.ok(rtData.layerfsBranches.some((b) => b.branchName === "main"));
      _strict.default.ok(rtData.layerfsBranches.some((b) => b.branchName === "feat-ns-auth"));
      _strict.default.equal(rtData.sandboxAndCgroup.warmPool.maxWarmCount, 8);
      _strict.default.equal(rtData.sandboxAndCgroup.phaseSpecs.length, 2);
      _strict.default.ok(rtData.cognitiveRouterEndpoints.length >= 2);
      _strict.default.equal(rtData.zeroTrustSecurity.spiffeTrustDomain, "spiffe://ansts.io");
      _strict.default.equal(rtData.zeroTrustSecurity.teePlatform, "AMD_SEV_SNP");

      // 2. Fork a new LayerFS CoW branch via POST /v1beta1/management/runtime/layerfs-branch
      const forkResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "fork_branch",
          sourceBranch: "main",
          branchName: "feat-m80-runtime-viz"
        })
      });
      _strict.default.equal(forkResp.status, 201);
      const forkBody = await forkResp.json();




      _strict.default.equal(forkBody.branchName, "feat-m80-runtime-viz");
      _strict.default.equal(forkBody.copiedFilesCount, 0);

      // 3. Commit a file snapshot onto feat-m80-runtime-viz
      const commitResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "commit_file",
          branchName: "feat-m80-runtime-viz",
          filePath: "src/m80_probe.ts",
          content: "export const m80RuntimeViz = true;\n"
        })
      });
      _strict.default.equal(commitResp.status, 200);
      const commitBody = await commitResp.json();



      _strict.default.equal(commitBody.committedNewSnapshot, true);
      _strict.default.notEqual(commitBody.commitId, "C0");

      // 4. Rollback branch to C0
      const rbBranchResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "rollback_branch",
          branchName: "feat-m80-runtime-viz",
          targetCommitId: "C0"
        })
      });
      _strict.default.equal(rbBranchResp.status, 200);
      const rbBranchBody = await rbBranchResp.json();
      _strict.default.equal(rbBranchBody.restoredCommitId, "C0");

      // 5. Protecting main branch from deletion (400 CANNOT_DELETE_MAIN_BRANCH) & VIEWER RBAC (403)
      const delMainResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "delete_branch",
          branchName: "main"
        })
      });
      _strict.default.equal(delMainResp.status, 400);

      const viewerBranchResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`
        },
        body: JSON.stringify({
          action: "delete_branch",
          branchName: "feat-m80-runtime-viz"
        })
      });
      _strict.default.equal(viewerBranchResp.status, 403);

      // 6. Delete non-main branch feat-m80-runtime-viz
      const delResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "delete_branch",
          branchName: "feat-m80-runtime-viz"
        })
      });
      _strict.default.equal(delResp.status, 200);
      const delBody = await delResp.json();
      _strict.default.equal(delBody.deleted, true);

      // 7. Verify anstsctl mgmt-runtime CLI
      const cliInspect = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-runtime", "inspect"], baseUrl);
      _strict.default.equal(cliInspect.exitCode, 0);
      const cliParsed = JSON.parse(cliInspect.stdout);
      _strict.default.equal(cliParsed.zeroTrustSecurity.teePlatform, "AMD_SEV_SNP");

      const cliFork = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-runtime", "fork_branch", "feat-cli-m80", "main"], baseUrl);
      _strict.default.equal(cliFork.exitCode, 0);
      const cliForkParsed = JSON.parse(cliFork.stdout);
      _strict.default.equal(cliForkParsed.branchName, "feat-cli-m80");

      // 8. Verify UI source files contain Configs & Runtime visualization bindings
      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        _strict.default.ok(content.includes("renderConfigsSection"));
        _strict.default.ok(content.includes("open-config-editor-dialog"));
        _strict.default.ok(content.includes("rollback-config-revision"));
        _strict.default.ok(content.includes("open-layerfs-fork-dialog"));
        _strict.default.ok(content.includes("open-layerfs-commit-dialog"));
        _strict.default.ok(content.includes("/v1beta1/management/runtime/layerfs-branch"));
      }
    } finally {
      await server.stop();
    }
  });
});