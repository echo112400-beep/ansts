import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M83 TanStack Console Multi-Region Federation, Dual-Homed Reverse Tunnel, RS(4,2) Erasure Coding & Chaos Suite", () => {
  it("[M83-1] Exposes Federation, Dual-Homed Reverse Tunnels, RS(4,2) Erasure Coding & Chaos via GET /v1beta1/console/federation and executes failover/rebuild", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m83-fed-"));
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

      // 1. Inspect initial federationVisualization via GET /v1beta1/console/federation
      const initResp = await fetch(`${baseUrl}/v1beta1/console/federation`);
      assert.equal(initResp.status, 200);
      const initFed = (await initResp.json()) as {
        federatedRegions: Array<{ regionCode: string; sovereigntyZone: string }>;
        reverseTunnels: {
          totalBindingsCount: number;
          healthyBindingsCount: number;
          hosts: Array<{ hostId: string; activeGatewayId: string; activeRole: string }>;
        };
        erasureCoding: {
          manifestsCount: number;
          manifests: Array<{ walId: string; shards: unknown[] }>;
        };
        chaosEngineering: { activePartitionCount: number };
      };
      assert.equal(initFed.federatedRegions.length, 3);
      assert.equal(initFed.reverseTunnels.totalBindingsCount, 6);
      assert.equal(initFed.reverseTunnels.healthyBindingsCount, 6);
      assert.equal(initFed.reverseTunnels.hosts[0].activeGatewayId, "celld-gw-01");
      assert.equal(initFed.erasureCoding.manifestsCount, 1);
      assert.equal(initFed.erasureCoding.manifests[0].shards.length, 6);
      assert.equal(initFed.chaosEngineering.activePartitionCount, 0);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`,
        },
        body: JSON.stringify({
          action: "inject_chaos_partition",
          targetHost: "body-host-02",
        }),
      });
      assert.equal(viewerResp.status, 403);

      // 3. Toggle PRIMARY tunnel gateway down on body-host-01 -> auto-failover to SECONDARY (celld-gw-02)
      const toggleResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "toggle_tunnel_gateway_health",
          hostId: "body-host-01",
          gatewayId: "celld-gw-01",
          healthy: false,
        }),
      });
      assert.equal(toggleResp.status, 200);
      const toggleBody = (await toggleResp.json()) as {
        activeTunnel: { gatewayId: string; role: string };
      };
      assert.equal(toggleBody.activeTunnel.gatewayId, "celld-gw-02");
      assert.equal(toggleBody.activeTunnel.role, "SECONDARY");

      // Dispatch reverse tunnel probe over surviving SECONDARY gateway
      const probeResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "dispatch_tunnel_probe",
          hostId: "body-host-01",
        }),
      });
      assert.equal(probeResp.status, 200);
      const probeBody = (await probeResp.json()) as { ack: { viaGateway: string; status: string } };
      assert.equal(probeBody.ack.viaGateway, "celld-gw-02");
      assert.equal(probeBody.ack.status, "TUNNEL_RTT_OK");

      // 4. Encode a new RS(4,2) WAL manifest and simulate 2-region outage lossless rebuild
      const encResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "encode_erasure_wal",
          walId: "wal-m83-test",
          payloadText: '{"taskId":"task-m83","turn":9,"rpo":0}',
        }),
      });
      assert.equal(encResp.status, 201);

      const rebuildResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "simulate_dual_region_outage_rebuild",
          walId: "wal-m83-test",
          droppedShardIndices: [1, 3],
        }),
      });
      assert.equal(rebuildResp.status, 200);
      const rebuildBody = (await rebuildResp.json()) as {
        rebuildReport: {
          rebuiltSuccessfully: boolean;
          crc32Match: boolean;
          missingShardsCount: number;
          reconstructedDataText: string;
        };
      };
      assert.equal(rebuildBody.rebuildReport.rebuiltSuccessfully, true);
      assert.equal(rebuildBody.rebuildReport.crc32Match, true);
      assert.equal(rebuildBody.rebuildReport.missingShardsCount, 2);
      assert.equal(rebuildBody.rebuildReport.reconstructedDataText, '{"taskId":"task-m83","turn":9,"rpo":0}');

      // 5. Inject and heal chaos partition
      const injResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "inject_chaos_partition",
          targetHost: "body-host-02",
          ttlMs: 60000,
        }),
      });
      assert.equal(injResp.status, 200);
      const injBody = (await injResp.json()) as { activeFaults: { partitionedHosts: string[] } };
      assert.ok(injBody.activeFaults.partitionedHosts.includes("body-host-02"));

      const healResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({ action: "heal_all_chaos_faults" }),
      });
      assert.equal(healResp.status, 200);
      const healBody = (await healResp.json()) as { healedCount: number };
      assert.equal(healBody.healedCount, 1);

      // 6. Verify CLI anstsctl mgmt-federation-console & UI bindings
      const cliFed = await runAnstsCtlRemote(["mgmt-federation-console", "inspect"], baseUrl);
      assert.equal(cliFed.exitCode, 0);
      const cliFedParsed = JSON.parse(cliFed.stdout) as { erasureCoding: { manifestsCount: number } };
      assert.equal(cliFedParsed.erasureCoding.manifestsCount, 2);

      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        assert.ok(content.includes("renderFederationSection"));
        assert.ok(content.includes("fed-toggle-tunnel-gw"));
        assert.ok(content.includes("fed-dispatch-tunnel-cmd"));
        assert.ok(content.includes("fed-encode-erasure-wal"));
        assert.ok(content.includes("fed-rebuild-erasure-wal"));
        assert.ok(content.includes("fed-inject-partition"));
        assert.ok(content.includes("fed-heal-partitions"));
      }
    } finally {
      await server.stop();
    }
  });
});
