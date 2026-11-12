"use strict";var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.describe)("M82 TanStack Console Multi-Agent Swarm, Policy-as-Code Formal Guardrail, Episodic Golden Hatchery & Spot Market Suite", () => {
  (0, _nodeTest.it)("[M82-1] Exposes Policy-as-Code rules, Spot Quota Market & Episodic Golden Store via GET /v1beta1/console/swarm and executes interactive mutations", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m82-swarm-"));
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

      // 1. Inspect initial swarmVisualization via GET /v1beta1/console/swarm
      const initResp = await fetch(`${baseUrl}/v1beta1/console/swarm`);
      _strict.default.equal(initResp.status, 200);
      const initSwarm = await initResp.json();















      _strict.default.equal(initSwarm.formalGuardrail.builtinRules.length, 3);
      _strict.default.equal(initSwarm.formalGuardrail.customRules.length, 2);
      _strict.default.equal(initSwarm.formalGuardrail.totalRulesCount, 5);
      _strict.default.equal(initSwarm.spotQuotaMarket.totalAvailableSpotSlots, 20);
      _strict.default.equal(initSwarm.episodicGoldenStore.totalEpisodesCount, 1);
      _strict.default.equal(initSwarm.episodicGoldenStore.episodes[0].goldenBaseBranch, "golden/ep-ns-zero-trust-auth");

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`
        },
        body: JSON.stringify({
          action: "register_policy_rule",
          ruleId: "RULE-UNAUTHORIZED"
        })
      });
      _strict.default.equal(viewerResp.status, 403);

      // 3. Register and delete a custom Policy-as-Code rule, and evaluate formal safety probes
      const regRuleResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "register_policy_rule",
          ruleId: "RULE-M82-TERRAFORM-DENY",
          description: "Block destructive terraform destroy in production",
          toolNamePattern: "terraform_.*",
          forbiddenArgRegex: "destroy",
          effect: "DENY"
        })
      });
      _strict.default.equal(regRuleResp.status, 201);
      const regRuleBody = await regRuleResp.json();


      _strict.default.equal(regRuleBody.swarmVisualization.formalGuardrail.totalRulesCount, 6);

      const probeDenyResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "evaluate_policy_probe",
          toolName: "sql_exec",
          toolArgs: { sql: "DELETE FROM orders WHERE 1=1; DROP TABLE audit_logs;", filePath: "/etc/shadow" }
        })
      });
      _strict.default.equal(probeDenyResp.status, 200);
      const probeDenyBody = await probeDenyResp.json();


      _strict.default.equal(probeDenyBody.evaluation.decision, "DENY_FORMAL_VIOLATION");
      _strict.default.ok(probeDenyBody.evaluation.violatedRuleIds.includes("BUILTIN_SQL_DESTRUCTIVE_DDL"));

      const delRuleResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "delete_policy_rule",
          ruleId: "RULE-M82-TERRAFORM-DENY"
        })
      });
      _strict.default.equal(delRuleResp.status, 200);

      // 4. Match a Spot Quota Bid and trigger urgent Lender Recall
      const matchResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "match_spot_bid",
          borrowerTenantId: "Northstar",
          requestedSlots: 4,
          maxBidPricePerSlotUsd: 0.08
        })
      });
      _strict.default.equal(matchResp.status, 200);
      const matchBody = await matchResp.json();




      _strict.default.equal(matchBody.matched, true);
      _strict.default.equal(matchBody.lease.lenderTenantId, "Helios");
      _strict.default.equal(matchBody.lease.leasedSlots, 4);
      _strict.default.equal(matchBody.lease.clearingPricePerSlotUsd, 0.06);
      _strict.default.equal(matchBody.swarmVisualization.spotQuotaMarket.activeLeasedSpotSlots, 4);

      const recallResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "recall_spot_quota",
          lenderTenantId: "Helios",
          requestedSlots: 4
        })
      });
      _strict.default.equal(recallResp.status, 200);
      const recallBody = await recallResp.json();



      _strict.default.equal(recallBody.reclaimedSlots, 4);
      _strict.default.equal(recallBody.swarmVisualization.spotQuotaMarket.activeLeasedSpotSlots, 0);

      // 5. Hatch a new Golden Base Episode via CLI anstsctl mgmt-swarm
      const cliHatch = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-swarm", "hatch_golden_episode", "ep-cli-m82"], baseUrl);
      _strict.default.equal(cliHatch.exitCode, 0);
      const cliHatchParsed = JSON.parse(cliHatch.stdout);



      _strict.default.equal(cliHatchParsed.goldenBaseBranch, "golden/ep-cli-m82");
      _strict.default.equal(cliHatchParsed.swarmVisualization.episodicGoldenStore.totalEpisodesCount, 2);

      // 6. Verify UI bindings in prototype.js & docs/index.html
      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        _strict.default.ok(content.includes("renderSwarmSection"));
        _strict.default.ok(content.includes("open-add-policy-rule-dialog"));
        _strict.default.ok(content.includes("swarm-delete-policy-rule"));
        _strict.default.ok(content.includes("swarm-probe-policy"));
        _strict.default.ok(content.includes("open-publish-spot-offer-dialog"));
        _strict.default.ok(content.includes("swarm-match-spot-bid"));
        _strict.default.ok(content.includes("swarm-recall-spot-quota"));
        _strict.default.ok(content.includes("swarm-hatch-golden-episode"));
      }
    } finally {
      await server.stop();
    }
  });
});