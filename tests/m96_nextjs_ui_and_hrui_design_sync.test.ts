/**
 * @fileoverview M96: Next.js `ui/` App Router + `hrui` Design System Synchronization Verification Suite
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M96 Next.js ui/ App Router & hrui Design System Synchronization Suite", () => {
  it("verifies ui/ Next.js App Router files and hrui design tokens across ui/ and docs/ console files", () => {
    const requiredUiFiles = [
      "ui/package.json",
      "ui/tsconfig.json",
      "ui/next.config.ts",
      "ui/src/components/hrui/theme.ts",
      "ui/src/components/hrui/hrui.css",
      "ui/src/components/hrui/icons.tsx",
      "ui/src/components/hrui/Shell.tsx",
      "ui/src/components/hrui/AnstsHruiConsole.tsx",
      "ui/src/app/layout.tsx",
      "ui/src/app/page.tsx",
    ];
    for (const rel of requiredUiFiles) {
      assert.equal(
        fs.existsSync(path.resolve(process.cwd(), rel)),
        true,
        `Missing required Next.js + hrui file: ${rel}`
      );
    }

    const hruiCss = fs.readFileSync(path.resolve(process.cwd(), "ui/src/components/hrui/hrui.css"), "utf-8");
    const protoCss = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.css"), "utf-8");
    const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
    const mgmtHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/ansts_console_management.html"), "utf-8");
    const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");

    // Verify canonical hrui tokens in both ui/src/components/hrui/hrui.css and docs/console-management/prototype.css
    for (const css of [hruiCss, protoCss, indexHtml]) {
      assert.ok(css.includes("--accent: #285aff"));
      assert.ok(css.includes("--ok: #1a7a43"));
      assert.ok(css.includes("--warn: #9a6500"));
      assert.ok(css.includes("--bad: #c0262f"));
      assert.ok(css.includes("Bricolage Grotesque"));
      assert.ok(css.includes("#hrui[data-theme=\"dark\"]"));
      assert.ok(css.includes("border-radius: 980px"));
    }

    assert.ok(indexHtml.includes('id="hrui"'));
    assert.ok(mgmtHtml.includes('id="hrui"'));
    assert.ok(indexHtml.includes("HRUI · NEXT.JS (ui/)"));
    assert.ok(protoJs.includes("HRUI_ICONS"));
    assert.ok(protoJs.includes("open-hrui-nextjs-sync-dialog"));
  });

  it("serves GET /v1beta1/console/ui-architecture and CLI anstsctl mgmt-ui-arch", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m96-"));
    const server = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const res = await fetch(`${baseUrl}/v1beta1/console/ui-architecture`);
      assert.equal(res.status, 200);
      const body = (await res.json()) as {
        framework: string;
        designSystem: string;
        synchronizedWithPrototype: boolean;
        themeTokens: { light: { accent: string } };
        files: string[];
      };
      assert.equal(body.framework, "Next.js App Router (ui/)");
      assert.equal(body.designSystem, "hrui");
      assert.equal(body.synchronizedWithPrototype, true);
      assert.equal(body.themeTokens.light.accent, "#285aff");
      assert.ok(body.files.includes("ui/src/components/hrui/AnstsHruiConsole.tsx"));

      const cliRes = await runAnstsCtlRemote(["mgmt-ui-arch"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const parsedCli = JSON.parse(cliRes.stdout) as { designSystem: string };
      assert.equal(parsedCli.designSystem, "hrui");
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
