"use strict";var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.describe)("M86 Live DAG Dependency & Escrow Topology Visualizer, Sub-Agent Lifecycle & Batch Task Governance Suite", () => {
  (0, _nodeTest.it)("[M86-1] Exposes live DAG escrows & edges via GET /v1beta1/console/dag and executes spawn/complete/fail/batch-cancel", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m86-dag-"));
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

      // 1. Inspect initial dagVisualization via GET /v1beta1/console/dag
      const initResp = await fetch(`${baseUrl}/v1beta1/console/dag`);
      _strict.default.equal(initResp.status, 200);
      const initDag = await initResp.json();









      _strict.default.equal(initDag.totalEscrowsCount, 1);
      _strict.default.equal(initDag.totalEdgesCount, 2);
      _strict.default.equal(initDag.escrows[0].parentTaskId, "task-ns-8842");
      _strict.default.equal(initDag.escrows[0].usedSlots, 2);
      _strict.default.equal(initDag.escrows[0].escrowedSlots, 3);
      _strict.default.equal(initDag.escrows[0].children.length, 2);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/tasks/dag-action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`
        },
        body: JSON.stringify({
          action: "spawn_sub_agent",
          parentTaskId: "task-ns-8842"
        })
      });
      _strict.default.equal(viewerResp.status, 403);

      // 3. Spawn a 3rd sub-agent and complete it
      const spawnResp = await fetch(`${baseUrl}/v1beta1/management/tasks/dag-action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "spawn_sub_agent",
          parentTaskId: "task-ns-8842",
          childTaskId: "task-ns-8842-sub3",
          prompt: "Verify M86 dynamic sub-agent"
        })
      });
      _strict.default.equal(spawnResp.status, 201);
      const spawnBody = await spawnResp.json();



      _strict.default.equal(spawnBody.childTaskId, "task-ns-8842-sub3");
      _strict.default.equal(spawnBody.dagVisualization.totalEdgesCount, 3);
      _strict.default.equal(spawnBody.dagVisualization.escrows[0].usedSlots, 3);

      const compResp = await fetch(`${baseUrl}/v1beta1/management/tasks/dag-action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "complete_sub_agent",
          parentTaskId: "task-ns-8842",
          childTaskId: "task-ns-8842-sub3"
        })
      });
      _strict.default.equal(compResp.status, 200);

      // 4. Batch cancel tasks
      const batchResp = await fetch(`${baseUrl}/v1beta1/management/tasks/dag-action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "batch_cancel_tasks",
          taskIds: ["task-ns-8842-sub1", "task-ns-8842-sub2"]
        })
      });
      _strict.default.equal(batchResp.status, 200);
      const batchBody = await batchResp.json();
      _strict.default.equal(batchBody.batchCount, 2);

      // 5. Verify CLI anstsctl mgmt-dag & UI bindings
      const cliDag = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-dag", "inspect"], baseUrl);
      _strict.default.equal(cliDag.exitCode, 0);
      const cliParsed = JSON.parse(cliDag.stdout);
      _strict.default.ok(cliParsed.totalEdgesCount >= 2);

      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        _strict.default.ok(content.includes("task-batch-check"));
        _strict.default.ok(content.includes("dag-batch-cancel-tasks"));
        _strict.default.ok(content.includes("dag-spawn-subagent"));
        _strict.default.ok(content.includes("dag-complete-subagent"));
        _strict.default.ok(content.includes("dag-fail-subagent"));
      }
    } finally {
      await server.stop();
    }
  });
});