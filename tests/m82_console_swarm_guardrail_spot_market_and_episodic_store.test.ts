import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M82 TanStack Console Multi-Agent Swarm, Policy-as-Code Formal Guardrail, Episodic Golden Hatchery & Spot Market Suite", () => {
  it("[M82-1] Exposes Policy-as-Code rules, Spot Quota Market & Episodic Golden Store via GET /v1beta1/console/swarm and executes interactive mutations", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m82-swarm-"));
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

      // 1. Inspect initial swarmVisualization via GET /v1beta1/console/swarm
      const initResp = await fetch(`${baseUrl}/v1beta1/console/swarm`);
      assert.equal(initResp.status, 200);
      const initSwarm = (await initResp.json()) as {
        formalGuardrail: {
          builtinRules: unknown[];
          customRules: Array<{ ruleId: string }>;
          totalRulesCount: number;
        };
        spotQuotaMarket: {
          offers: Array<{ offerId: string; availableSlots: number }>;
          leases: unknown[];
          totalAvailableSpotSlots: number;
        };
        episodicGoldenStore: {
          episodes: Array<{ episodeId: string; goldenBaseBranch: string }>;
          totalEpisodesCount: number;
        };
      };
      assert.equal(initSwarm.formalGuardrail.builtinRules.length, 3);
      assert.equal(initSwarm.formalGuardrail.customRules.length, 2);
      assert.equal(initSwarm.formalGuardrail.totalRulesCount, 5);
      assert.equal(initSwarm.spotQuotaMarket.totalAvailableSpotSlots, 20);
      assert.equal(initSwarm.episodicGoldenStore.totalEpisodesCount, 1);
      assert.equal(initSwarm.episodicGoldenStore.episodes[0].goldenBaseBranch, "golden/ep-ns-zero-trust-auth");

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`,
        },
        body: JSON.stringify({
          action: "register_policy_rule",
          ruleId: "RULE-UNAUTHORIZED",
        }),
      });
      assert.equal(viewerResp.status, 403);

      // 3. Register and delete a custom Policy-as-Code rule, and evaluate formal safety probes
      const regRuleResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "register_policy_rule",
          ruleId: "RULE-M82-TERRAFORM-DENY",
          description: "Block destructive terraform destroy in production",
          toolNamePattern: "terraform_.*",
          forbiddenArgRegex: "destroy",
          effect: "DENY",
        }),
      });
      assert.equal(regRuleResp.status, 201);
      const regRuleBody = (await regRuleResp.json()) as {
        swarmVisualization: { formalGuardrail: { totalRulesCount: number } };
      };
      assert.equal(regRuleBody.swarmVisualization.formalGuardrail.totalRulesCount, 6);

      const probeDenyResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "evaluate_policy_probe",
          toolName: "sql_exec",
          toolArgs: { sql: "DELETE FROM orders WHERE 1=1; DROP TABLE audit_logs;", filePath: "/etc/shadow" },
        }),
      });
      assert.equal(probeDenyResp.status, 200);
      const probeDenyBody = (await probeDenyResp.json()) as {
        evaluation: { decision: string; violatedRuleIds: string[] };
      };
      assert.equal(probeDenyBody.evaluation.decision, "DENY_FORMAL_VIOLATION");
      assert.ok(probeDenyBody.evaluation.violatedRuleIds.includes("BUILTIN_SQL_DESTRUCTIVE_DDL"));

      const delRuleResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "delete_policy_rule",
          ruleId: "RULE-M82-TERRAFORM-DENY",
        }),
      });
      assert.equal(delRuleResp.status, 200);

      // 4. Match a Spot Quota Bid and trigger urgent Lender Recall
      const matchResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "match_spot_bid",
          borrowerTenantId: "Northstar",
          requestedSlots: 4,
          maxBidPricePerSlotUsd: 0.08,
        }),
      });
      assert.equal(matchResp.status, 200);
      const matchBody = (await matchResp.json()) as {
        matched: boolean;
        lease: { lenderTenantId: string; leasedSlots: number; clearingPricePerSlotUsd: number };
        swarmVisualization: { spotQuotaMarket: { activeLeasedSpotSlots: number } };
      };
      assert.equal(matchBody.matched, true);
      assert.equal(matchBody.lease.lenderTenantId, "Helios");
      assert.equal(matchBody.lease.leasedSlots, 4);
      assert.equal(matchBody.lease.clearingPricePerSlotUsd, 0.06);
      assert.equal(matchBody.swarmVisualization.spotQuotaMarket.activeLeasedSpotSlots, 4);

      const recallResp = await fetch(`${baseUrl}/v1beta1/management/swarm/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "recall_spot_quota",
          lenderTenantId: "Helios",
          requestedSlots: 4,
        }),
      });
      assert.equal(recallResp.status, 200);
      const recallBody = (await recallResp.json()) as {
        reclaimedSlots: number;
        swarmVisualization: { spotQuotaMarket: { activeLeasedSpotSlots: number } };
      };
      assert.equal(recallBody.reclaimedSlots, 4);
      assert.equal(recallBody.swarmVisualization.spotQuotaMarket.activeLeasedSpotSlots, 0);

      // 5. Hatch a new Golden Base Episode via CLI anstsctl mgmt-swarm
      const cliHatch = await runAnstsCtlRemote(["mgmt-swarm", "hatch_golden_episode", "ep-cli-m82"], baseUrl);
      assert.equal(cliHatch.exitCode, 0);
      const cliHatchParsed = JSON.parse(cliHatch.stdout) as {
        goldenBaseBranch: string;
        swarmVisualization: { episodicGoldenStore: { totalEpisodesCount: number } };
      };
      assert.equal(cliHatchParsed.goldenBaseBranch, "golden/ep-cli-m82");
      assert.equal(cliHatchParsed.swarmVisualization.episodicGoldenStore.totalEpisodesCount, 2);

      // 6. Verify UI bindings in prototype.js & docs/index.html
      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        assert.ok(content.includes("renderSwarmSection"));
        assert.ok(content.includes("open-add-policy-rule-dialog"));
        assert.ok(content.includes("swarm-delete-policy-rule"));
        assert.ok(content.includes("swarm-probe-policy"));
        assert.ok(content.includes("open-publish-spot-offer-dialog"));
        assert.ok(content.includes("swarm-match-spot-bid"));
        assert.ok(content.includes("swarm-recall-spot-quota"));
        assert.ok(content.includes("swarm-hatch-golden-episode"));
      }
    } finally {
      await server.stop();
    }
  });
});
