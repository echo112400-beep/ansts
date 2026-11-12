"use strict";










var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M79: TanStack Console Cluster Management, Health Matrix, State Visualization & DRF Queue Visualizer Suite
 * Verifies:
 * 1. [M79-1] `HierarchicalQueueManager.inspectQueueSnapshot()` & Live Queue BFF (`GET /v1beta1/console/queues`, `POST /v1beta1/management/queues/action`, `anstsctl mgmt-queues`):
 *    - Real-time per-tenant Binary Max-Heap inspection (`baseScore`, `agingBonus`, `preemptionBonus`, `drfPenalty`, `effectiveScore`, `nextTournamentWinner`).
 *    - Interactive `enqueue_task`, in-place `reprioritize_queued_task` (`P2 -> P0`), tournament `pop_and_schedule_next` (transitioning to `RUNNING_ACT` in SQLite `v25_scheduler_tasks` + `TurnWalEngine`), `remove_queued_task`, and `enqueue_multi_tenant_burst`.
 * 2. [M79-2] Cluster Health Matrix & State Machine Visualization (`GET /v1beta1/console/health` & `GET /v1beta1/management/state`):
 *    - 6 live subsystem health probes (`PROBE-GATEWAY`, `PROBE-CELLD`, `PROBE-SHARD-RING`, `PROBE-SQLITE-RPO0`, `PROBE-TUNNEL-HA`, `PROBE-AUDIT-CHAIN`) + real V8 process memory (`rssMb`, `heapUsedMb`).
 *    - `stateVisualization.taskStateDistribution` (`QUEUED`, `LEASED`, `RUNNING_ACT`, `COMPLETED`, etc.) and `celldNodeLoad` (`/state` counters).
 * 3. [M79-3] TanStack Console UI (`docs/console-management/prototype.js` & `docs/index.html`) Queue, Health & State Visualization Workbench.
 */(0, _nodeTest.describe)("M79 TanStack Console Cluster Management, Health Matrix, State Visualization & DRF Queue Visualizer Suite", () => {(0, _nodeTest.it)("[M79-3] Exposes renderQueuesAndHealthSection, queue workbench actions, and health/state visual panels in prototype.js & docs/index.html", () => {const indexPath = path.resolve(process.cwd(), "docs", "index.html");const protoPath = path.resolve(process.cwd(), "docs", "console-management", "prototype.js");for (const p of [indexPath, protoPath]) {const content = fs.readFileSync(p, "utf-8");_strict.default.equal(content.includes("renderQueuesAndHealthSection"), true);_strict.default.equal(content.includes("open-enqueue-task-dialog"), true);
      _strict.default.equal(content.includes("queue-enqueue-burst"), true);
      _strict.default.equal(content.includes("queue-pop-schedule"), true);
      _strict.default.equal(content.includes("queue-promote-p0"), true);
      _strict.default.equal(content.includes("queue-remove-task"), true);
      _strict.default.equal(content.includes("/v1beta1/management/queues/action"), true);
    }
  });

  (0, _nodeTest.it)("[M79-1 & M79-2] Executes live DRF Binary Max-Heap Queue visualization, in-place P0 promotion, tournament pop-and-schedule, and 6-probe Health & State Matrix", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m79-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
      const adminToken = guard.issuePrincipalToken("chen");
      const viewerToken = guard.issuePrincipalToken("xu");
      const adminHeaders = {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      };

      // 1. Verify GET /v1beta1/console/health returns 6 healthy probes, processMemory, and stateVisualization
      const healthResp = await fetch(`${baseUrl}/v1beta1/console/health`);
      _strict.default.equal(healthResp.status, 200);
      const healthData = await healthResp.json();











      _strict.default.equal(healthData.healthMatrix.overallHealthy, true);
      _strict.default.equal(healthData.healthMatrix.overallStatus, "HEALTHY");
      _strict.default.equal(healthData.healthMatrix.probes.length, 6);
      _strict.default.equal(
      healthData.healthMatrix.probes.every((p) => p.status === "HEALTHY"),
      true);

      _strict.default.ok(healthData.healthMatrix.processMemory.rssMb > 0);
      _strict.default.ok(healthData.stateVisualization.taskStateDistribution.QUEUED >= 3);
      _strict.default.ok(healthData.stateVisualization.celldNodeLoad.owned_cells >= 7);

      // 2. Verify VIEWER token is blocked with 403 on POST /v1beta1/management/queues/action
      const viewerQueueResp = await fetch(`${baseUrl}/v1beta1/management/queues/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${viewerToken}`
        },
        body: JSON.stringify({ action: "pop_and_schedule_next" })
      });
      _strict.default.equal(viewerQueueResp.status, 403);

      // 3. Enqueue a P2 task "task-vg-m79" for Vanguard -> reprioritize it in-place to P0 -> verify it becomes nextTournamentWinner -> pop & schedule!
      const enqResp = await fetch(`${baseUrl}/v1beta1/management/queues/action`, {
        method: "POST",
        headers: adminHeaders,
        body: JSON.stringify({
          action: "enqueue_task",
          taskId: "task-vg-m79",
          tenantId: "Vanguard",
          priority: 2,
          prompt: "Vanguard M79 queue test task"
        })
      });
      _strict.default.equal(enqResp.status, 201);

      const promoteResp = await fetch(`${baseUrl}/v1beta1/management/queues/action`, {
        method: "POST",
        headers: adminHeaders,
        body: JSON.stringify({
          action: "reprioritize_queued_task",
          taskId: "task-vg-m79",
          priority: 0
        })
      });
      _strict.default.equal(promoteResp.status, 200);
      const promoteBody = await promoteResp.json();






      _strict.default.equal(promoteBody.reprioritized, true);
      _strict.default.equal(promoteBody.newPriority, 0);
      _strict.default.equal(promoteBody.queueVisualization.nextTournamentWinner.taskId, "task-vg-m79");
      _strict.default.equal(promoteBody.queueVisualization.nextTournamentWinner.tenantId, "Vanguard");
      _strict.default.ok(promoteBody.queueVisualization.nextTournamentWinner.effectiveScore >= 1000);

      // Pop & schedule the tournament winner ("task-vg-m79") and verify it transitions to RUNNING_ACT in SQLite & TurnWalEngine
      const popResp = await fetch(`${baseUrl}/v1beta1/management/queues/action`, {
        method: "POST",
        headers: adminHeaders,
        body: JSON.stringify({
          action: "pop_and_schedule_next"
        })
      });
      _strict.default.equal(popResp.status, 200);
      const popBody = await popResp.json();




      _strict.default.equal(popBody.popped, true);
      _strict.default.equal(popBody.scheduledTaskId, "task-vg-m79");
      _strict.default.ok(popBody.assignedBodyHostId.startsWith("body-"));

      const taskCheckResp = await fetch(`${baseUrl}/v1/tasks/task-vg-m79`);
      _strict.default.equal(taskCheckResp.status, 200);
      const taskCheckBody = await taskCheckResp.json();
      _strict.default.equal(taskCheckBody.state, "RUNNING_ACT");

      // 4. Remove a queued task ("task-vg-1049") and verify CLI `mgmt-queues`
      const remResp = await fetch(`${baseUrl}/v1beta1/management/queues/action`, {
        method: "POST",
        headers: adminHeaders,
        body: JSON.stringify({
          action: "remove_queued_task",
          taskId: "task-vg-1049"
        })
      });
      _strict.default.equal(remResp.status, 200);

      const cliInspect = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-queues", "inspect"], baseUrl);
      _strict.default.equal(cliInspect.exitCode, 0);
      const cliInspectJson = JSON.parse(cliInspect.stdout);
      _strict.default.equal(cliInspectJson.totalQueuedCount, 2);

      const cliBurst = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-queues", "enqueue_multi_tenant_burst"], baseUrl);
      _strict.default.equal(cliBurst.exitCode, 0);
      const cliBurstJson = JSON.parse(cliBurst.stdout);



      _strict.default.equal(cliBurstJson.enqueuedBurstCount, 4);
      _strict.default.equal(cliBurstJson.queueVisualization.totalQueuedCount, 6);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});