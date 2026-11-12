"use strict";var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.describe)("M85 TanStack Console Security, RBAC & Release Governance Center, Live Intrusion Probe & Theme/Polling Controls", () => {
  (0, _nodeTest.it)("[M85-1] Exposes RBAC principals, SHA-256 Audit Chain, 8/8 Release Gates & Zero-Trust Intrusion Probes via /v1beta1/console/security", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m85-sec-"));
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

      // 1. Inspect initial securityVisualization via GET /v1beta1/console/security
      const secResp = await fetch(`${baseUrl}/v1beta1/console/security`);
      _strict.default.equal(secResp.status, 200);
      const secData = await secResp.json();





      _strict.default.equal(secData.rbacPrincipals.length, 4);
      _strict.default.equal(secData.auditChainIntegrity.valid, true);
      _strict.default.equal(secData.zeroTrustPosture.spiffeTrustDomain, "spiffe://ansts.io");
      _strict.default.equal(secData.zeroTrustPosture.teePlatform, "AMD_SEV_SNP");
      _strict.default.equal(secData.releaseReadiness.readyForProduction, true);
      _strict.default.equal(secData.releaseReadiness.passedCount, 8);
      _strict.default.equal(secData.releaseReadiness.totalGates, 8);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerProbe = await fetch(`${baseUrl}/v1beta1/management/security/probe`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`
        },
        body: JSON.stringify({ probeType: "ssrf_loopback" })
      });
      _strict.default.equal(viewerProbe.status, 403);

      // 3. Execute all 4 Zero-Trust Intrusion Probes and verify all are BLOCKED
      for (const probeType of [
      "ssrf_loopback",
      "ssrf_imds_hex",
      "lsm_proc_escape",
      "vsock_cred_injection"])
      {
        const probeResp = await fetch(`${baseUrl}/v1beta1/management/security/probe`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${chenToken}`
          },
          body: JSON.stringify({ probeType })
        });
        _strict.default.equal(probeResp.status, 200);
        const probeBody = await probeResp.json();


        _strict.default.equal(probeBody.probe.blocked, true);
      }

      // 4. Verify CLI anstsctl mgmt-security-console
      const cliSec = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-security-console", "inspect"], baseUrl);
      _strict.default.equal(cliSec.exitCode, 0);
      const cliParsed = JSON.parse(cliSec.stdout);


      _strict.default.equal(cliParsed.releaseReadiness.passedCount, 8);

      // 5. Verify UI bindings in prototype.js & docs/index.html
      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        _strict.default.ok(content.includes("renderSecuritySection"));
        _strict.default.ok(content.includes("sec-run-probe"));
        _strict.default.ok(content.includes("sec-run-custom-probe"));
        _strict.default.ok(content.includes("sec-issue-verify-jwt"));
        _strict.default.ok(content.includes("toggle-theme"));
        _strict.default.ok(content.includes("toggle-auto-refresh"));
      }
      _strict.default.ok(indexHtml.includes("html.dark-theme"));
    } finally {
      await server.stop();
    }
  });
});