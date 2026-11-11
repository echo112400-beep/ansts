"use strict";









var assert = _interopRequireWildcard(require("node:assert/strict"));
var _nodeTest = require("node:test");
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _env = require("../src/cloudflare/env.js");
var _outbox_webhook = require("../src/telemetry/outbox_webhook.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M70 E2E Verification Suite: Console Day-2 Operations & Governance Workbench Suite
 * Covers:
 * - [M70-1] `TransactionalOutboxDispatcher` Zero-Downtime Dual-Key HMAC Rotation & DLQ Task/Webhook Replay Workbench (`POST /v1beta1/management/ops/dlq-replay`)
 * - [M70-2] Task Breakpoint Crash-Recovery (`POST /v1beta1/management/tasks/breakpoint-recover`) & Duplicate Coalescing Inspector (`POST /v1beta1/management/tasks/coalesce-inspect`)
 * - [M70-3] M60–M69 One-Click Full-Link Health Scorecard (`POST /v1beta1/management/ops/health-scorecard`)
 * - [M70-4] Day-2 Governance Suite: Zero-Trust Security Fence Alert Stream, FinOps Multi-Dimensional Cost Attribution, Config Center Diff Preview & Dual-Key Rotation (`POST /v1beta1/management/ops/day2-governance`)
 * - [M70-5] Console Frontend (`docs/console-management/prototype.js`) Interactive Workbench Controls
 * - [M70-6] CLI (`anstsctl mgmt-day2-ops`)
 */(0, _nodeTest.describe)("M70 Console Day-2 Operations & Governance Workbench Suite", () => {(0, _nodeTest.it)("[M70-1] TransactionalOutboxDispatcher supports zero-downtime dual-key HMAC rotation with grace-window verification", () => {const sql = new _env.InMemoryCellSqlStorage();const outbox = new _outbox_webhook.TransactionalOutboxDispatcher("primary-secret-v1", 3, sql);const payloadJson = JSON.stringify({ event: "io.ansts.task.completed", taskId: "t-m70" });const sigV1 = outbox.signPayload(payloadJson, 1700000000);assert.equal(outbox.verifySignature(payloadJson, sigV1, 1700000050, 300), true);
    // Rotate to primary-secret-v2 with a 600s grace window for primary-secret-v1
    const rot = outbox.rotateSigningSecret("primary-secret-v2", 600, 1700000050);
    assert.equal(rot.keyRotationGeneration, 2);
    assert.equal(rot.secondaryActive, true);

    const sigV2 = outbox.signPayload(payloadJson, 1700000060);
    // Both in-flight sigV1 (within grace window) and new sigV2 must pass!
    assert.equal(outbox.verifySignature(payloadJson, sigV1, 1700000100, 300), true);
    assert.equal(outbox.verifySignature(payloadJson, sigV2, 1700000100, 300), true);

    // Once grace window expires (t > 1700000650), old sigV1 is rejected while sigV2 remains valid
    const sigV1Late = sigV1.replace("t=1700000000", "t=1700000700");
    assert.equal(outbox.verifySignature(payloadJson, sigV1Late, 1700000700, 300), false);
  });

  (0, _nodeTest.it)("[M70-1 ~ M70-6] Live Console BFF & CLI execute DLQ Replay, Breakpoint Recovery, Duplicate Coalescing, Health Scorecard, Security Alerts, FinOps Attribution, Config Diff & Dual-Key Rotation", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m70-live-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
      tickIntervalMs: 60_000
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. Seed & inspect DLQ, then replay dead-lettered task & outbox webhook
      const seedResp = await fetch(`${baseUrl}/v1beta1/management/ops/dlq-replay`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action: "seed_dlq_samples", actor: "chen", role: "SRE_ADMIN" })
      });
      assert.equal(seedResp.status, 200);
      const seedData = await seedResp.json();



      assert.equal(seedData.deadLetterTaskCount >= 1, true);
      assert.equal(seedData.deadLetterWebhookCount >= 1, true);

      const replayTaskResp = await fetch(`${baseUrl}/v1beta1/management/ops/dlq-replay`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "replay_dead_letter_task",
          taskId: "task-dlq-9901",
          actor: "chen",
          role: "SRE_ADMIN"
        })
      });
      const replayTaskData = await replayTaskResp.json();



      assert.equal(replayTaskData.replayed, true);
      assert.equal(replayTaskData.newState, "QUEUED");

      const replayHookResp = await fetch(`${baseUrl}/v1beta1/management/ops/dlq-replay`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "replay_outbox_webhook",
          messageId: "outbox-dlq-9901",
          newTargetUrl: "https://recovered-sink.internal/webhook",
          actor: "chen",
          role: "SRE_ADMIN"
        })
      });
      const replayHookData = await replayHookResp.json();



      assert.equal(replayHookData.replayed, true);
      assert.equal(replayHookData.delivered, true);

      // 2. Breakpoint Crash-Recovery & Duplicate Coalescing Inspector
      const bpResp = await fetch(`${baseUrl}/v1beta1/management/tasks/breakpoint-recover`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "recover_breakpoint",
          taskId: "task-ns-8841",
          actor: "chen",
          role: "SRE_ADMIN"
        })
      });
      const bpData = await bpResp.json();



      assert.equal(bpData.restoredTurnIndex >= 1, true);
      assert.equal(bpData.nextTurnToExecute, bpData.restoredTurnIndex + 1);

      const coalResp = await fetch(`${baseUrl}/v1beta1/management/tasks/coalesce-inspect`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "coalesce_duplicate",
          taskId: "task-ns-8841",
          actor: "chen",
          role: "SRE_ADMIN"
        })
      });
      const coalData = await coalResp.json();




      assert.equal(coalData.deduplicated, true);
      assert.equal(coalData.canonicalTaskId, "task-ns-8841");
      assert.equal(coalData.mergedDuplicateCount >= 1, true);

      // Verify state endpoint exposes mergedDuplicateCount on task-ns-8841
      const stateResp = await fetch(`${baseUrl}/v1beta1/management/state`);
      const stateData = await stateResp.json();


      const nsTask = stateData.tasks.find((t) => t.id === "task-ns-8841");
      assert.equal((nsTask?.mergedDuplicateCount ?? 0) >= 1, true);

      // 3. Day-2 Governance Operations (Security Fence Alerts, FinOps Cost Attribution, Config Diff, Dual-Key Rotation)
      const secCli = await (0, _anstsctl.runAnstsCtlRemote)(
      ["mgmt-day2-ops", "security_fence_alerts"],
      baseUrl);

      assert.equal(secCli.exitCode, 0);
      const secBody = JSON.parse(secCli.stdout);
      assert.equal(secBody.allBlocked, true);
      assert.equal(secBody.totalAlerts, 4);

      const finCli = await (0, _anstsctl.runAnstsCtlRemote)(
      ["mgmt-day2-ops", "finops_cost_attribution"],
      baseUrl);

      assert.equal(finCli.exitCode, 0);
      const finBody = JSON.parse(finCli.stdout);


      assert.equal(finBody.attributions.length >= 4, true);

      const diffCli = await (0, _anstsctl.runAnstsCtlRemote)(
      ["mgmt-day2-ops", "config_diff_preview"],
      baseUrl);

      assert.equal(diffCli.exitCode, 0);
      const diffBody = JSON.parse(diffCli.stdout);
      assert.equal(diffBody.modifiedCount >= 1, true);

      const rotCli = await (0, _anstsctl.runAnstsCtlRemote)(
      ["mgmt-day2-ops", "rotate_webhook_secret"],
      baseUrl);

      assert.equal(rotCli.exitCode, 0);
      const rotBody = JSON.parse(rotCli.stdout);



      assert.equal(rotBody.oldSignatureStillValidInGraceWindow, true);
      assert.equal(rotBody.newSignatureValid, true);

      // 4. One-Click Full-Link Health Scorecard via CLI
      const scoreCli = await (0, _anstsctl.runAnstsCtlRemote)(
      ["mgmt-day2-ops", "health-scorecard"],
      baseUrl);

      assert.equal(scoreCli.exitCode, 0);
      const scoreBody = JSON.parse(scoreCli.stdout);




      assert.equal(scoreBody.overallHealthy, true);
      assert.equal(scoreBody.overallScorePct, 100);
      assert.equal(scoreBody.totalSuites, 10);

      // 5. Verify Console prototype.js serves all new Day-2 action bindings
      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      assert.equal(jsResp.status, 200);
      const jsText = await jsResp.text();
      assert.equal(jsText.includes("open-dlq-workbench"), true);
      assert.equal(jsText.includes("recover-task-breakpoint"), true);
      assert.equal(jsText.includes("coalesce-duplicate-task"), true);
      assert.equal(jsText.includes("run-health-scorecard"), true);
      assert.equal(jsText.includes("open-security-fence-alerts"), true);
      assert.equal(jsText.includes("open-finops-attribution"), true);
      assert.equal(jsText.includes("open-config-diff-preview"), true);
      assert.equal(jsText.includes("open-dual-key-rotation"), true);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});