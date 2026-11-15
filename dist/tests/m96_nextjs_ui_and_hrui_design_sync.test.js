"use strict";


var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M96: Next.js `ui/` App Router + `hrui` Design System Synchronization Verification Suite
 */(0, _nodeTest.describe)("M96 Next.js ui/ App Router & hrui Design System Synchronization Suite", () => {
  (0, _nodeTest.it)("verifies ui/ Next.js App Router files and hrui design tokens across ui/ and docs/ console files", () => {
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
    "ui/src/app/page.tsx"];

    for (const rel of requiredUiFiles) {
      _strict.default.equal(
      fs.existsSync(path.resolve(process.cwd(), rel)),
      true,
      `Missing required Next.js + hrui file: ${rel}`);

    }

    const hruiCss = fs.readFileSync(path.resolve(process.cwd(), "ui/src/components/hrui/hrui.css"), "utf-8");
    const protoCss = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.css"), "utf-8");
    const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
    const mgmtHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/ansts_console_management.html"), "utf-8");
    const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");

    // Verify canonical hrui tokens in both ui/src/components/hrui/hrui.css and docs/console-management/prototype.css
    for (const css of [hruiCss, protoCss, indexHtml]) {
      _strict.default.ok(css.includes("--accent: #285aff"));
      _strict.default.ok(css.includes("--ok: #1a7a43"));
      _strict.default.ok(css.includes("--warn: #9a6500"));
      _strict.default.ok(css.includes("--bad: #c0262f"));
      _strict.default.ok(css.includes("Bricolage Grotesque"));
      _strict.default.ok(css.includes("#hrui[data-theme=\"dark\"]"));
      _strict.default.ok(css.includes("border-radius: 980px"));
    }

    _strict.default.ok(indexHtml.includes('id="hrui"'));
    _strict.default.ok(mgmtHtml.includes('id="hrui"'));
    _strict.default.ok(indexHtml.includes("HRUI · NEXT.JS (ui/)"));
    _strict.default.ok(protoJs.includes("HRUI_ICONS"));
    _strict.default.ok(protoJs.includes("open-hrui-nextjs-sync-dialog"));
  });

  (0, _nodeTest.it)("serves GET /v1beta1/console/ui-architecture and CLI anstsctl mgmt-ui-arch", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m96-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const res = await fetch(`${baseUrl}/v1beta1/console/ui-architecture`);
      _strict.default.equal(res.status, 200);
      const body = await res.json();






      _strict.default.equal(body.framework, "Next.js App Router (ui/)");
      _strict.default.equal(body.designSystem, "hrui");
      _strict.default.equal(body.synchronizedWithPrototype, true);
      _strict.default.equal(body.themeTokens.light.accent, "#285aff");
      _strict.default.ok(body.files.includes("ui/src/components/hrui/AnstsHruiConsole.tsx"));

      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-ui-arch"], baseUrl);
      _strict.default.equal(cliRes.exitCode, 0);
      const parsedCli = JSON.parse(cliRes.stdout);
      _strict.default.equal(parsedCli.designSystem, "hrui");
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});