"use strict";var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.describe)("M87 Live FinOps Multi-Dimensional Cost Attribution, Prefix-Cache Savings & Budget Gate Simulator Suite", () => {
  (0, _nodeTest.it)("[M87-1] Exposes FinOps multi-dimensional attribution via GET /v1beta1/console/finops and executes spend simulation / 402 hard gate / reset", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m87-finops-"));
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

      // 1. Inspect initial finopsVisualization via GET /v1beta1/console/finops
      const initResp = await fetch(`${baseUrl}/v1beta1/console/finops`);
      _strict.default.equal(initResp.status, 200);
      const initFin = await initResp.json();





      _strict.default.equal(initFin.attributions.length, 4);
      _strict.default.ok(initFin.totalPrefixSavedUsd > 0);
      _strict.default.ok(initFin.totalBudgetUsd >= 2900);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/finops/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`
        },
        body: JSON.stringify({
          action: "simulate_spend_step",
          tenantId: "Northstar",
          amountUsd: 200
        })
      });
      _strict.default.equal(viewerResp.status, 403);

      // 3. Simulate spend step on Northstar ($840 -> $1040 / $1200 = 86.7% -> soft-downgrade to Qwen-3.8)
      const step1Resp = await fetch(`${baseUrl}/v1beta1/management/finops/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "simulate_spend_step",
          tenantId: "Northstar",
          amountUsd: 200
        })
      });
      _strict.default.equal(step1Resp.status, 200);
      const step1Body = await step1Resp.json();




      _strict.default.equal(step1Body.totalSpentUsd, 1040);
      _strict.default.equal(step1Body.downgraded, true);
      _strict.default.equal(step1Body.selectedModel, "Qwen-3.8");

      // 4. Exhaust remaining budget ($1040 -> $1200) and verify next call triggers 402 PAYMENT_REQUIRED_BUDGET_EXHAUSTED
      const step2Resp = await fetch(`${baseUrl}/v1beta1/management/finops/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "simulate_spend_step",
          tenantId: "Northstar",
          amountUsd: 200
        })
      });
      _strict.default.equal(step2Resp.status, 200);
      const step2Body = await step2Resp.json();
      _strict.default.equal(step2Body.hardBlocked, true);

      const gate402Resp = await fetch(`${baseUrl}/v1beta1/management/finops/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "simulate_spend_step",
          tenantId: "Northstar",
          amountUsd: 50
        })
      });
      _strict.default.equal(gate402Resp.status, 402);

      // 5. Reset Northstar spend via CLI anstsctl mgmt-finops
      const cliReset = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-finops", "reset_tenant_spend", "Northstar"], baseUrl);
      _strict.default.equal(cliReset.exitCode, 0);
      const cliParsed = JSON.parse(cliReset.stdout);
      _strict.default.equal(cliParsed.totalSpentUsd, 0);
      _strict.default.equal(cliParsed.activeModel, "DeepSeek-R1");

      // 6. Verify UI bindings in prototype.js & docs/index.html
      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        _strict.default.ok(content.includes("finops-simulate-spend"));
        _strict.default.ok(content.includes("finops-reset-spend"));
        _strict.default.ok(content.includes("/v1beta1/management/finops/action"));
      }
    } finally {
      await server.stop();
    }
  });
});