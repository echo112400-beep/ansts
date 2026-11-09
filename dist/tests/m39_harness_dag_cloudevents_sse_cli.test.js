"use strict";





var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var http = _interopRequireWildcard(require("node:http"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M39 Next-Batch Deep Optimization E2E Suite:
 * 1. [M39-1] Multi-Turn Agent Harness Loop (`THINK` <-> `ACT` cgroup phase), Velocity Dead-Loop Breaker (`429`) & DAG Quota Escrow (`400` cycle guard)
 * 2. [M39-2] W3C CloudEvents v1.0 Telemetry Bus, HMAC-SHA256 Signed Outbox Webhook Delivery & SSE `Last-Event-ID` Stream Replay
 * 3. [M39-3] Live `anstsctl` Remote Dev-Cluster CLI (`runAnstsCtlRemote`: `cluster-overview`, `dispatch`, `attest`, `gc`)
 */(0, _nodeTest.describe)("M39 Next-Batch Deep Optimization E2E Suite (Agent Harness + Velocity Breaker + DAG Escrow, CloudEvents Outbox + SSE Replay & Live Remote CLI)", () => {(0, _nodeTest.it)("1. [M39-1] Executes Agent Harness turns with THINK/ACT cgroup phases, trips Velocity Breaker on 3 identical tool calls (429), and guards DAG Escrow against cycles (400)", async () => {const srv = new _index.MultiNodeDevServer({ host: "127.0.0.1",
      nodeId: "body-harness-01",
      nodeRole: "body"
    });
    const { apiPort } = await srv.start(0, 0);

    try {
      // 1. Execute Turn 1 & Turn 2 with different args -> succeeds and records SSE events
      const t1 = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/harness/run-turn`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-loop-01",
          tenantId: "tenant-m39",
          toolName: "shell_exec",
          toolArgs: { cmd: "echo step-1" },
          psiSample: { someAvg10Pct: 12.0, fullAvg10Pct: 2.0, freeSwapMb: 2048 }
        })
      });
      assert.equal(t1.status, 200);
      const t1Json = await t1.json();






      assert.equal(t1Json.turnSummary.turnIndex, 1);
      assert.equal(t1Json.thinkCgroup.phase, "THINK");
      assert.equal(t1Json.actCgroup.phase, "ACT");
      assert.equal(t1Json.psiDefense.level, "NORMAL");

      // 2. Now repeat the exact same tool call & argsHash? Wait: SagaToolGateway.generateIdempotencyKey includes turnIndex!
      // Let's also test token velocity breaker (`estimatedTokens: 150000` > 120,000 tokens/min limit!)
      const tVelocity = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/harness/run-turn`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-loop-01",
          tenantId: "tenant-m39",
          toolName: "shell_exec",
          toolArgs: { cmd: "echo burst" },
          estimatedTokens: 130_000
        })
      });
      assert.equal(tVelocity.status, 429);
      const tVelJson = await tVelocity.json();
      assert.equal(tVelJson.error, "VELOCITY_BREAKER_TRIPPED");
      assert.equal(tVelJson.taskState, "SUSPENDED_APPROVAL");

      // 3. Test Parent-Child DAG Quota Escrow & Cycle Detection
      const escRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/dag/escrow`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "reserveParent",
          parentTaskId: "task-p1",
          expectedSubAgentCount: 2,
          maxTenantQuota: 10,
          currentTenantUsed: 1
        })
      });
      assert.equal(escRes.status, 200);

      const childRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/dag/escrow`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "registerChild",
          parentTaskId: "task-p1",
          childTaskId: "task-c1",
          dependencies: []
        })
      });
      assert.equal(childRes.status, 200);
      const childJson = await childRes.json();
      assert.equal(childJson.usedEscrowSlot, true);
      assert.equal(childJson.parentState, "WAITING_SUB_AGENT");

      // Self-dependency cycle (`task-c2` depends on `task-c2`) -> HTTP 400 DAG_CYCLE_DETECTED
      const cycRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/dag/escrow`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "registerChild",
          parentTaskId: "task-p1",
          childTaskId: "task-c2",
          dependencies: ["task-c2"]
        })
      });
      assert.equal(cycRes.status, 400);
      const cycJson = await cycRes.json();
      assert.equal(cycJson.error, "DAG_CYCLE_DETECTED");

      // Complete child `task-c1` -> parent resumes `RUNNING_THINK`
      const doneRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/dag/escrow`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "completeChild",
          parentTaskId: "task-p1",
          childTaskId: "task-c1"
        })
      });
      const doneJson = await doneRes.json();
      assert.equal(doneJson.allChildrenDone, true);
      assert.equal(doneJson.parentState, "RUNNING_THINK");
    } finally {
      await srv.stop();
    }
  });

  (0, _nodeTest.it)("2. [M39-2 & M39-3] Delivers HMAC-SHA256 signed CloudEvents webhooks, replays SSE streams from Last-Event-ID, and executes live anstsctl remote CLI commands", async () => {
    let receivedSignature = "";
    let receivedCloudEventType = "";
    const webhookServer = http.createServer((req, res) => {
      receivedSignature = String(req.headers["x-ansts-signature"] ?? "");
      const chunks = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
        const parsed = JSON.parse(Buffer.concat(chunks).toString("utf-8"));


        receivedCloudEventType = parsed.cloudEvent?.type ?? "";
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ received: true }));
      });
    });
    const whPort = await new Promise((resolve) => {
      webhookServer.listen(0, "127.0.0.1", () => {
        const addr = webhookServer.address();
        resolve(typeof addr === "object" && addr ? addr.port : 0);
      });
    });

    const srv = new _index.MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway"
    });
    const { apiPort } = await srv.start(0, 0);
    const gwUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. Emit CloudEvent + deliver signed webhook
      const telRes = await fetch(`${gwUrl}/v1beta1/telemetry/events`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          tenantId: "tenant-wh",
          taskId: "task-wh-01",
          type: "io.ansts.task.succeeded",
          llmCostUsd: 0.45,
          sandboxCpuCoreSeconds: 25,
          webhookUrl: `http://127.0.0.1:${whPort}/hook`
        })
      });
      assert.equal(telRes.status, 200);
      const telJson = await telRes.json();



      assert.equal(telJson.webhookDelivery.delivered, true);
      assert.ok(receivedSignature.startsWith("t="));
      assert.equal(receivedCloudEventType, "io.ansts.task.succeeded");
      assert.ok(telJson.tenantFinOpsReport.grandTotalUsd >= 0.45);

      // 2. Publish 2 harness turns on `task-sse-01` and replay from Turn 1's event ID via Last-Event-ID
      const r1 = await fetch(`${gwUrl}/v1beta1/harness/run-turn`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-sse-01",
          toolName: "shell_exec",
          toolArgs: { cmd: "echo t1" }
        })
      });
      const r1Json = await r1.json();

      await fetch(`${gwUrl}/v1beta1/harness/run-turn`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-sse-01",
          toolName: "shell_exec",
          toolArgs: { cmd: "echo t2" }
        })
      });

      const sseRes = await fetch(`${gwUrl}/v1/events/stream?taskId=task-sse-01`, {
        headers: { "Last-Event-ID": r1Json.sseEvtId }
      });
      assert.equal(sseRes.status, 200);
      const sseText = await sseRes.text();
      assert.ok(sseText.includes("evt-task-sse-01-t2"));
      assert.ok(!sseText.includes("evt-task-sse-01-t1\n")); // t1 skipped because Last-Event-ID was t1!

      // 3. Test `runAnstsCtlRemote` CLI against the live running server
      const cliOv = await (0, _index.runAnstsCtlRemote)(["cluster-overview"], gwUrl);
      assert.equal(cliOv.exitCode, 0);
      assert.ok(JSON.parse(cliOv.stdout).overview.status === "HEALTHY");

      const cliDisp = await (0, _index.runAnstsCtlRemote)(
      ["dispatch", "tenant-cli", "task-cli-live-01", "hello from remote cli"],
      gwUrl);

      assert.equal(cliDisp.exitCode, 0);
      assert.equal(JSON.parse(cliDisp.stdout).dispatched, true);

      const cliAttest = await (0, _index.runAnstsCtlRemote)(["attest", "main"], gwUrl);
      assert.equal(cliAttest.exitCode, 0);
      assert.equal(JSON.parse(cliAttest.stdout).slsaVerified, true);

      const cliGc = await (0, _index.runAnstsCtlRemote)(["gc"], gwUrl);
      assert.equal(cliGc.exitCode, 0);
      assert.ok(Array.isArray(JSON.parse(cliGc.stdout).retainedBlobIds));
    } finally {
      await srv.stop();
      await new Promise((resolve) => webhookServer.close(() => resolve()));
    }
  });
});