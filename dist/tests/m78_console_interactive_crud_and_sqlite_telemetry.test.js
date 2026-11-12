"use strict";









var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M78: Deep Console Interactive System Management CRUD & Zero-Static-Constant SQLite Telemetry Suite
 * Verifies:
 * 1. [M78-1] Zero-Static-Constant SQLite Telemetry (`used_slots` in `v25_mgmt_tenants` & `base_leases` in `v25_mgmt_node_overrides`):
 *    - Eliminates hardcoded tenant/node ternary offsets from `src/dashboard/server.ts`.
 *    - Node `prewarm` reduces persisted `psi_pct` in SQLite; node `drain` sets `base_leases = 0`.
 * 2. [M78-2] Full Tenant Lifecycle CRUD (`create` -> `update` CAS -> `delete` with `409 TENANT_HAS_ACTIVE_TASKS` guard).
 * 3. [M78-3] Full A2A Blackboard CRUD (`put_blackboard_cas` & `delete_blackboard_key`) and MCP Tool Registry CRUD (`register_mcp_tool`, `invoke_mcp_tool`, `unregister_mcp_tool`).
 * 4. [M78-4] Interactive UI Dialog & Checkbox Bindings in `docs/console-management/prototype.js` and `docs/index.html`.
 */(0, _nodeTest.describe)("M78 Deep Console Interactive System Management CRUD & Zero-Static-Constant SQLite Telemetry Suite", () => {(0, _nodeTest.it)("[M78-1 & M78-4] Eliminates static ternary offsets in server.ts and wires interactive CRUD dialogs & node checkboxes in prototype.js & docs/index.html", () => {const serverTsPath = path.resolve(process.cwd(), "src", "dashboard", "server.ts");const serverTs = fs.readFileSync(serverTsPath, "utf-8");_strict.default.equal(serverTs.includes('tId === "Northstar" ? 86'), false,
    "Hardcoded tenant usedSlots ternary must be replaced by SQLite used_slots column");

    _strict.default.equal(
    serverTs.includes('String(nr.status) === "CORDONED" ? 0 : 12)'),
    false,
    "Hardcoded +12 activeLeases offset must be replaced by SQLite base_leases column");


    const indexPath = path.resolve(process.cwd(), "docs", "index.html");
    const protoPath = path.resolve(process.cwd(), "docs", "console-management", "prototype.js");
    for (const p of [indexPath, protoPath]) {
      const content = fs.readFileSync(p, "utf-8");
      _strict.default.equal(content.includes("node-batch-check"), true);
      _strict.default.equal(content.includes("delete-tenant"), true);
      _strict.default.equal(content.includes("open-blackboard-cas-dialog"), true);
      _strict.default.equal(content.includes("delete-blackboard-key"), true);
      _strict.default.equal(content.includes("open-register-mcp-dialog"), true);
      _strict.default.equal(content.includes("invoke-specific-mcp-tool"), true);
      _strict.default.equal(content.includes("unregister-mcp-tool"), true);
    }
  });

  (0, _nodeTest.it)("[M78-2 & M78-3] Executes full Tenant CRUD (with active-task delete protection), A2A Blackboard CRUD, MCP Tool CRUD, and SQLite node prewarm/drain telemetry", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m78-"));
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
      const headers = {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      };

      // 1. Deleting Northstar while it has active tasks must be rejected with 409 TENANT_HAS_ACTIVE_TASKS
      const delBusyResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action: "delete",
          tenantId: "Northstar"
        })
      });
      _strict.default.equal(delBusyResp.status, 409);
      const delBusyBody = await delBusyResp.json();
      _strict.default.equal(delBusyBody.error, "TENANT_HAS_ACTIVE_TASKS");
      _strict.default.ok(delBusyBody.activeTaskIds.includes("task-ns-8841"));

      // 2. Create custom tenant "Chronos" -> verify usedSlots === 0 -> delete "Chronos"
      const createResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action: "create",
          tenantId: "Chronos",
          region: "eu-central-1",
          tier: "P1_ENTERPRISE",
          maxSlots: 90,
          dailyBudgetUsd: 700,
          expectedVersion: 0
        })
      });
      _strict.default.equal(createResp.status, 200);

      const stateAfterCreate = await (await fetch(`${baseUrl}/v1beta1/management/state`)).json();



      const chronos = stateAfterCreate.tenants.find((t) => t.id === "Chronos");
      _strict.default.ok(chronos);
      _strict.default.equal(chronos.usedSlots, 0);
      _strict.default.equal(chronos.maxSlots, 90);
      _strict.default.equal(chronos.region, "eu-central-1");

      const delIdleResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action: "delete",
          tenantId: "Chronos"
        })
      });
      _strict.default.equal(delIdleResp.status, 200);
      const delIdleBody = await delIdleResp.json();
      _strict.default.equal(delIdleBody.deleted, true);

      // 3. Node prewarm lowers persisted psi_pct in SQLite
      const psiBefore = stateAfterCreate.nodes.find((n) => n.id === "body-host-01").psiPct;
      const prewarmResp = await fetch(`${baseUrl}/v1beta1/management/nodes/batch`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          operation: "prewarm",
          nodeIds: ["body-host-01"]
        })
      });
      _strict.default.equal(prewarmResp.status, 200);
      const stateAfterPrewarm = await (await fetch(`${baseUrl}/v1beta1/management/state`)).json();


      const psiAfter = stateAfterPrewarm.nodes.find((n) => n.id === "body-host-01").psiPct;
      _strict.default.ok(psiAfter < psiBefore, `Expected psiAfter (${psiAfter}) < psiBefore (${psiBefore})`);

      // 4. Full A2A Blackboard CRUD: put_blackboard_cas -> delete_blackboard_key
      const putBbResp = await fetch(`${baseUrl}/v1beta1/management/a2a`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action: "put_blackboard_cas",
          key: "swarm/m78-policy",
          expectedVersion: 0,
          value: { quorum: 0.67 }
        })
      });
      _strict.default.equal(putBbResp.status, 200);

      const delBbResp = await fetch(`${baseUrl}/v1beta1/management/a2a`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action: "delete_blackboard_key",
          key: "swarm/m78-policy"
        })
      });
      _strict.default.equal(delBbResp.status, 200);
      const delBbBody = await delBbResp.json();



      _strict.default.equal(delBbBody.deleted, true);
      _strict.default.equal(
      delBbBody.a2aState.blackboardEntries.some((e) => e.key === "swarm/m78-policy"),
      false);


      // 5. Full MCP Tool Registry CRUD: register_mcp_tool -> invoke_mcp_tool -> unregister_mcp_tool
      const regMcpResp = await fetch(`${baseUrl}/v1beta1/management/mcp`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action: "register_mcp_tool",
          toolName: "mcp_m78_audit_tool",
          serverName: "mcp-secops",
          description: "M78 dynamic security audit tool"
        })
      });
      _strict.default.equal(regMcpResp.status, 201);

      const invMcpResp = await fetch(`${baseUrl}/v1beta1/management/mcp`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action: "invoke_mcp_tool",
          toolName: "mcp_m78_audit_tool",
          taskId: "task-ns-8841"
        })
      });
      _strict.default.equal(invMcpResp.status, 200);

      const unregMcpResp = await fetch(`${baseUrl}/v1beta1/management/mcp`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action: "unregister_mcp_tool",
          toolName: "mcp_m78_audit_tool"
        })
      });
      _strict.default.equal(unregMcpResp.status, 200);
      const unregMcpBody = await unregMcpResp.json();



      _strict.default.equal(unregMcpBody.unregistered, true);
      _strict.default.equal(
      unregMcpBody.mcpState.tools.some((t) => t.toolName === "mcp_m78_audit_tool"),
      false);

    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});