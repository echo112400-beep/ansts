import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M80 TanStack Console Configuration Management Center & Deep Runtime Visualization Suite", () => {
  it("[M80-1] Configuration Management Center (/v1beta1/console/configs & /v1beta1/management/config/center) supports CAS updates, schema guard, RBAC, diff preview & rollback", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m80-cfg-"));
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

      // 1. Inspect initial config visualization via GET /v1beta1/console/configs
      const initResp = await fetch(`${baseUrl}/v1beta1/console/configs`);
      assert.equal(initResp.status, 200);
      const initCfg = (await initResp.json()) as {
        activeRevision: number;
        activeConfig: Record<string, unknown>;
        revisions: Array<{ revision: number; config: Record<string, unknown> }>;
        schemaRules: Array<{ field: string }>;
      };
      assert.equal(initCfg.activeRevision, 1);
      assert.equal(initCfg.activeConfig.maxCellConcurrentRequests, 64);
      assert.equal(initCfg.schemaRules.length, 11);
      assert.equal(initCfg.revisions.length, 1);
      assert.equal(initCfg.revisions[0].config.maxCellConcurrentRequests, 64);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`,
        },
        body: JSON.stringify({
          action: "validate_and_apply_config",
          expectedRevision: 1,
          runtimeOverrides: { maxCellConcurrentRequests: 128 },
        }),
      });
      assert.equal(viewerResp.status, 403);

      // 3. Invalid schema parameter (maxCellConcurrentRequests: -10) is rejected with 400 CONFIG_SCHEMA_INVALID
      const invalidResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "validate_and_apply_config",
          expectedRevision: 1,
          runtimeOverrides: { maxCellConcurrentRequests: -10 },
        }),
      });
      assert.equal(invalidResp.status, 400);
      const invalidBody = (await invalidResp.json()) as { error: string };
      assert.equal(invalidBody.error, "CONFIG_SCHEMA_INVALID");

      // 4. Valid CAS hot-reload update advances revision 1 -> 2
      const applyResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "validate_and_apply_config",
          expectedRevision: 1,
          runtimeOverrides: {
            maxCellConcurrentRequests: 128,
            actCpuCores: 6.0,
            psiFullFreezePct: 75.0,
          },
          note: "Scale up cell concurrency and ACT cores for peak traffic",
        }),
      });
      assert.equal(applyResp.status, 200);
      const applyBody = (await applyResp.json()) as {
        previousRevision: number;
        activeRevision: number;
        changedKeys: string[];
        activeConfig: Record<string, unknown>;
      };
      assert.equal(applyBody.previousRevision, 1);
      assert.equal(applyBody.activeRevision, 2);
      assert.ok(applyBody.changedKeys.includes("maxCellConcurrentRequests"));
      assert.equal(applyBody.activeConfig.maxCellConcurrentRequests, 128);
      assert.equal(applyBody.activeConfig.actCpuCores, 6.0);

      // 5. Stale expectedRevision=1 now triggers 409 CONFIG_CAS_CONFLICT
      const casConflictResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "validate_and_apply_config",
          expectedRevision: 1,
          runtimeOverrides: { maxCellConcurrentRequests: 256 },
        }),
      });
      assert.equal(casConflictResp.status, 409);
      const casConflictBody = (await casConflictResp.json()) as { error: string; actualRevision: number };
      assert.equal(casConflictBody.error, "CONFIG_CAS_CONFLICT");
      assert.equal(casConflictBody.actualRevision, 2);

      // 6. Rollback to revision #1 generates revision #3 with restored config
      const rollbackResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "rollback_revision",
          targetRevision: 1,
        }),
      });
      assert.equal(rollbackResp.status, 200);
      const rollbackBody = (await rollbackResp.json()) as {
        rolledBackToRevision: number;
        activeRevision: number;
        activeConfig: Record<string, unknown>;
      };
      assert.equal(rollbackBody.rolledBackToRevision, 1);
      assert.equal(rollbackBody.activeRevision, 3);
      assert.equal(rollbackBody.activeConfig.maxCellConcurrentRequests, 64);
      assert.equal(rollbackBody.activeConfig.actCpuCores, 4.0);
    } finally {
      await server.stop();
    }
  });

  it("[M80-2] Deep Runtime Visualization (/v1beta1/console/runtime), LayerFS CoW Branch Management, CLI mgmt-runtime & UI Wiring", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m80-rt-"));
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

      // 1. Inspect runtimeVisualization via GET /v1beta1/console/runtime
      const rtResp = await fetch(`${baseUrl}/v1beta1/console/runtime`);
      assert.equal(rtResp.status, 200);
      const rtData = (await rtResp.json()) as {
        layerfsBranches: Array<{ branchName: string; headCommitId: string; fileCount: number }>;
        layerfsStorage: { totalObjects: number; compressionRatioPct: number };
        sandboxAndCgroup: {
          warmPool: { warmCount: number; maxWarmCount: number };
          phaseSpecs: Array<{ phase: string; cpuCores: number }>;
        };
        celldRuntime: { maxCellConcurrentRequests: number; topologyCas: { generation: number; members: string[] } };
        cognitiveRouterEndpoints: Array<{ endpointId: string; circuitState: string }>;
        zeroTrustSecurity: { spiffeTrustDomain: string; teePlatform: string };
      };
      assert.ok(rtData.layerfsBranches.some((b) => b.branchName === "main"));
      assert.ok(rtData.layerfsBranches.some((b) => b.branchName === "feat-ns-auth"));
      assert.equal(rtData.sandboxAndCgroup.warmPool.maxWarmCount, 8);
      assert.equal(rtData.sandboxAndCgroup.phaseSpecs.length, 2);
      assert.ok(rtData.cognitiveRouterEndpoints.length >= 2);
      assert.equal(rtData.zeroTrustSecurity.spiffeTrustDomain, "spiffe://ansts.io");
      assert.equal(rtData.zeroTrustSecurity.teePlatform, "AMD_SEV_SNP");

      // 2. Fork a new LayerFS CoW branch via POST /v1beta1/management/runtime/layerfs-branch
      const forkResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "fork_branch",
          sourceBranch: "main",
          branchName: "feat-m80-runtime-viz",
        }),
      });
      assert.equal(forkResp.status, 201);
      const forkBody = (await forkResp.json()) as {
        branchName: string;
        headCommitId: string;
        copiedFilesCount: number;
      };
      assert.equal(forkBody.branchName, "feat-m80-runtime-viz");
      assert.equal(forkBody.copiedFilesCount, 0);

      // 3. Commit a file snapshot onto feat-m80-runtime-viz
      const commitResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "commit_file",
          branchName: "feat-m80-runtime-viz",
          filePath: "src/m80_probe.ts",
          content: "export const m80RuntimeViz = true;\n",
        }),
      });
      assert.equal(commitResp.status, 200);
      const commitBody = (await commitResp.json()) as {
        committedNewSnapshot: boolean;
        commitId: string;
      };
      assert.equal(commitBody.committedNewSnapshot, true);
      assert.notEqual(commitBody.commitId, "C0");

      // 4. Rollback branch to C0
      const rbBranchResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "rollback_branch",
          branchName: "feat-m80-runtime-viz",
          targetCommitId: "C0",
        }),
      });
      assert.equal(rbBranchResp.status, 200);
      const rbBranchBody = (await rbBranchResp.json()) as { restoredCommitId: string };
      assert.equal(rbBranchBody.restoredCommitId, "C0");

      // 5. Protecting main branch from deletion (400 CANNOT_DELETE_MAIN_BRANCH) & VIEWER RBAC (403)
      const delMainResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "delete_branch",
          branchName: "main",
        }),
      });
      assert.equal(delMainResp.status, 400);

      const viewerBranchResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`,
        },
        body: JSON.stringify({
          action: "delete_branch",
          branchName: "feat-m80-runtime-viz",
        }),
      });
      assert.equal(viewerBranchResp.status, 403);

      // 6. Delete non-main branch feat-m80-runtime-viz
      const delResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "delete_branch",
          branchName: "feat-m80-runtime-viz",
        }),
      });
      assert.equal(delResp.status, 200);
      const delBody = (await delResp.json()) as { deleted: boolean };
      assert.equal(delBody.deleted, true);

      // 7. Verify anstsctl mgmt-runtime CLI
      const cliInspect = await runAnstsCtlRemote(["mgmt-runtime", "inspect"], baseUrl);
      assert.equal(cliInspect.exitCode, 0);
      const cliParsed = JSON.parse(cliInspect.stdout) as { zeroTrustSecurity: { teePlatform: string } };
      assert.equal(cliParsed.zeroTrustSecurity.teePlatform, "AMD_SEV_SNP");

      const cliFork = await runAnstsCtlRemote(["mgmt-runtime", "fork_branch", "feat-cli-m80", "main"], baseUrl);
      assert.equal(cliFork.exitCode, 0);
      const cliForkParsed = JSON.parse(cliFork.stdout) as { branchName: string };
      assert.equal(cliForkParsed.branchName, "feat-cli-m80");

      // 8. Verify UI source files contain Configs & Runtime visualization bindings
      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        assert.ok(content.includes("renderConfigsSection"));
        assert.ok(content.includes("open-config-editor-dialog"));
        assert.ok(content.includes("rollback-config-revision"));
        assert.ok(content.includes("open-layerfs-fork-dialog"));
        assert.ok(content.includes("open-layerfs-commit-dialog"));
        assert.ok(content.includes("/v1beta1/management/runtime/layerfs-branch"));
      }
    } finally {
      await server.stop();
    }
  });
});
