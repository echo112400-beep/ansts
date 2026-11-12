import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M88 LayerFS Workspace File Explorer, Line-Level Blame Workbench & Console URL Deep-Linking Suite", () => {
  it("[M88-1] Reads and updates LayerFS workspace files via GET /v1beta1/console/layerfs/file & POST /v1beta1/management/runtime/layerfs-branch", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m88-lfs-"));
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

      // 1. Read package.json on main branch via GET /v1beta1/console/layerfs/file
      const readResp = await fetch(`${baseUrl}/v1beta1/console/layerfs/file?branch=main&filePath=package.json`);
      assert.equal(readResp.status, 200);
      const readBody = (await readResp.json()) as {
        branchName: string;
        filePath: string;
        commitId: string;
        content: string;
        blameLines: Array<{ lineNumber: number; stepId: string }>;
      };
      assert.equal(readBody.branchName, "main");
      assert.equal(readBody.filePath, "package.json");
      assert.ok(readBody.content.includes("workspace-app"));
      assert.ok(readBody.blameLines.length >= 1);

      // 2. Reject path traversal (../etc/passwd) with 400 LAYERFS_FILE_READ_FAILED
      const badResp = await fetch(`${baseUrl}/v1beta1/console/layerfs/file?branch=main&filePath=../etc/passwd`);
      assert.equal(badResp.status, 400);

      // 3. Edit & commit a file on feat-ns-auth, then read back the updated content
      const commitResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "commit_file",
          branchName: "feat-ns-auth",
          filePath: "src/m88_editor.ts",
          content: "export const editedInConsole = 'M88_OK';\n",
        }),
      });
      assert.equal(commitResp.status, 200);

      const readEditedResp = await fetch(
        `${baseUrl}/v1beta1/console/layerfs/file?branch=feat-ns-auth&filePath=src/m88_editor.ts`
      );
      assert.equal(readEditedResp.status, 200);
      const editedBody = (await readEditedResp.json()) as { content: string };
      assert.equal(editedBody.content, "export const editedInConsole = 'M88_OK';\n");

      // 4. Verify CLI anstsctl mgmt-layerfs-file & UI deep-linking bindings
      const cliFile = await runAnstsCtlRemote(["mgmt-layerfs-file", "feat-ns-auth", "src/m88_editor.ts"], baseUrl);
      assert.equal(cliFile.exitCode, 0);
      const cliParsed = JSON.parse(cliFile.stdout) as { content: string };
      assert.equal(cliParsed.content, "export const editedInConsole = 'M88_OK';\n");

      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        assert.ok(content.includes("layerfs-open-file-explorer"));
        assert.ok(content.includes("/v1beta1/console/layerfs/file"));
        assert.ok(content.includes("hydrateStateFromUrl"));
      }
    } finally {
      await server.stop();
    }
  });
});
