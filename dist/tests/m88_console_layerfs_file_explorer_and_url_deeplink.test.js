"use strict";var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.describe)("M88 LayerFS Workspace File Explorer, Line-Level Blame Workbench & Console URL Deep-Linking Suite", () => {
  (0, _nodeTest.it)("[M88-1] Reads and updates LayerFS workspace files via GET /v1beta1/console/layerfs/file & POST /v1beta1/management/runtime/layerfs-branch", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m88-lfs-"));
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

      // 1. Read package.json on main branch via GET /v1beta1/console/layerfs/file
      const readResp = await fetch(`${baseUrl}/v1beta1/console/layerfs/file?branch=main&filePath=package.json`);
      _strict.default.equal(readResp.status, 200);
      const readBody = await readResp.json();






      _strict.default.equal(readBody.branchName, "main");
      _strict.default.equal(readBody.filePath, "package.json");
      _strict.default.ok(readBody.content.includes("workspace-app"));
      _strict.default.ok(readBody.blameLines.length >= 1);

      // 2. Reject path traversal (../etc/passwd) with 400 LAYERFS_FILE_READ_FAILED
      const badResp = await fetch(`${baseUrl}/v1beta1/console/layerfs/file?branch=main&filePath=../etc/passwd`);
      _strict.default.equal(badResp.status, 400);

      // 3. Edit & commit a file on feat-ns-auth, then read back the updated content
      const commitResp = await fetch(`${baseUrl}/v1beta1/management/runtime/layerfs-branch`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "commit_file",
          branchName: "feat-ns-auth",
          filePath: "src/m88_editor.ts",
          content: "export const editedInConsole = 'M88_OK';\n"
        })
      });
      _strict.default.equal(commitResp.status, 200);

      const readEditedResp = await fetch(
      `${baseUrl}/v1beta1/console/layerfs/file?branch=feat-ns-auth&filePath=src/m88_editor.ts`);

      _strict.default.equal(readEditedResp.status, 200);
      const editedBody = await readEditedResp.json();
      _strict.default.equal(editedBody.content, "export const editedInConsole = 'M88_OK';\n");

      // 4. Verify CLI anstsctl mgmt-layerfs-file & UI deep-linking bindings
      const cliFile = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-layerfs-file", "feat-ns-auth", "src/m88_editor.ts"], baseUrl);
      _strict.default.equal(cliFile.exitCode, 0);
      const cliParsed = JSON.parse(cliFile.stdout);
      _strict.default.equal(cliParsed.content, "export const editedInConsole = 'M88_OK';\n");

      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        _strict.default.ok(content.includes("layerfs-open-file-explorer"));
        _strict.default.ok(content.includes("/v1beta1/console/layerfs/file"));
        _strict.default.ok(content.includes("hydrateStateFromUrl"));
      }
    } finally {
      await server.stop();
    }
  });
});