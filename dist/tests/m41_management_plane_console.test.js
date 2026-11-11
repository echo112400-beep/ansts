"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview M41 End-to-End Verification: ANSTS Platform Management Console Prototype & Real Backend Integration
 * Verifies that all Management Console endpoints (`/v1beta1/management/*`) are backed by real
 * stateful subsystems (`PlatformManagementPlaneEngine`, `CoreScheduler5D`, `SchedulerV25SingleWritePathStore`,
 * `TurnWalEngine`, `DeterministicTimeTravelReplayer`, `LayerFsWorkspaceDriver`, `FinOpsBudgetController`,
 * `CapacitySlaEnforcer`) rather than stateless stubs.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const http = __importStar(require("node:http"));
const server_main_js_1 = require("../src/server_main.js");
(0, node_test_1.describe)("M41 Platform Management Console Prototype & Real Backend BFF Suite", () => {
    (0, node_test_1.it)("executes real SQLite-persisted tenant CAS, P0 lease migration drain, Turn WAL time-travel fork, and 4-eyes approval", async () => {
        const tmpDir = `/tmp/ansts-m41-${process.pid}-${Date.now()}`;
        fs.mkdirSync(tmpDir, { recursive: true });
        const server = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-01",
            nodeRole: "gateway",
            storageDir: tmpDir,
        });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            // 1. Verify prototype static assets & visual style tokens
            const htmlResp = await fetch(`${baseUrl}/ansts_console_management.html`);
            assert.equal(htmlResp.status, 200);
            const html = await htmlResp.text();
            assert.ok(html.includes("ANSTS · 平台管理控制台"));
            assert.ok(html.includes('id="scenario"'));
            assert.ok(html.includes('id="identity"'));
            assert.ok(html.includes('id="dialog"'));
            const cssResp = await fetch(`${baseUrl}/console-management/prototype.css`);
            assert.equal(cssResp.status, 200);
            const css = await cssResp.text();
            assert.ok(css.includes("--bg-rail: #12161f"));
            assert.ok(css.includes("aside.rail"));
            const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
            assert.equal(jsResp.status, 200);
            const js = await jsResp.text();
            assert.ok(js.includes("/v1beta1/management/state"));
            assert.ok(js.includes("/v1beta1/management/tasks/time-travel"));
            // 2. Verify GET /v1beta1/management/state returns real seeded tasks, tenants, SLA, and audit logs
            const stateResp = await fetch(`${baseUrl}/v1beta1/management/state`);
            assert.equal(stateResp.status, 200);
            const stateData = (await stateResp.json());
            const ns = stateData.tenants.find((t) => t.id === "Northstar");
            assert.ok(ns);
            assert.equal(ns.maxSlots, 120);
            assert.equal(ns.configVersion, 14);
            assert.equal(stateData.sla.compliant, true);
            const task8841 = stateData.tasks.find((t) => t.id === "task-ns-8841");
            assert.ok(task8841);
            assert.equal(task8841.body, "body-host-02");
            assert.equal(task8841.fence, 12);
            assert.equal(task8841.turn, 5);
            // 3. Verify POST /v1beta1/management/tenants/config mutates SQLite & FinOpsBudgetController and enforces CAS
            const viewerResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ role: "VIEWER", tenantId: "Northstar", expectedVersion: 14, maxSlots: 160 }),
            });
            assert.equal(viewerResp.status, 403);
            const casOkResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    role: "SRE_ADMIN",
                    actor: "chen",
                    tenantId: "Northstar",
                    expectedVersion: 14,
                    maxSlots: 160,
                    dailyBudgetUsd: 1800,
                }),
            });
            assert.equal(casOkResp.status, 200);
            // Stale version 14 must now fail with 409
            const casConflictResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    role: "SRE_ADMIN",
                    actor: "zhou",
                    tenantId: "Northstar",
                    expectedVersion: 14,
                    maxSlots: 200,
                }),
            });
            assert.equal(casConflictResp.status, 409);
            // Verify state reflects persisted v15 and maxSlots=160
            const afterTenantState = (await (await fetch(`${baseUrl}/v1beta1/management/state`)).json());
            const updatedNs = afterTenantState.tenants.find((t) => t.id === "Northstar");
            assert.equal(updatedNs.maxSlots, 160);
            assert.equal(updatedNs.configVersion, 15);
            // 4. Verify POST /v1beta1/management/nodes/batch checks real P0 task lease on body-host-02 and migrates it on retry
            const batchPartialResp = await fetch(`${baseUrl}/v1beta1/management/nodes/batch`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    operation: "drain",
                    nodeIds: ["celld-brain-01", "body-host-01", "body-host-02"],
                    forceMigrateLeases: false,
                    actor: "chen",
                    role: "SRE_ADMIN",
                }),
            });
            assert.equal(batchPartialResp.status, 207);
            const batchMigrateResp = await fetch(`${baseUrl}/v1beta1/management/nodes/batch`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    operation: "drain",
                    nodeIds: ["body-host-02"],
                    forceMigrateLeases: true,
                    actor: "chen",
                    role: "SRE_ADMIN",
                }),
            });
            assert.equal(batchMigrateResp.status, 200);
            // Verify task-ns-8841 was genuinely migrated to body-host-01 and its fencing token incremented to 13
            const migratedTaskResp = await fetch(`${baseUrl}/v1/tasks/task-ns-8841`);
            assert.equal(migratedTaskResp.status, 200);
            const migratedTask = (await migratedTaskResp.json());
            assert.equal(migratedTask.assigned_node_id, "body-host-01");
            assert.equal(migratedTask.lease_fencing_token, 13);
            // 5. Verify POST /v1beta1/management/tasks/time-travel genuinely forks task & LayerFS branch
            const ttForkResp = await fetch(`${baseUrl}/v1beta1/management/tasks/time-travel`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "task-ns-8841",
                    targetTurnIndex: 3,
                    actor: "chen",
                    role: "SRE_ADMIN",
                }),
            });
            assert.equal(ttForkResp.status, 200);
            const ttForkData = (await ttForkResp.json());
            assert.equal(ttForkData.forkedTaskId, "task-ns-8841-fork-t3");
            // Verify forked task exists in real scheduler/v25Store and LayerFS branch exists
            const getForkedResp = await fetch(`${baseUrl}/v1/tasks/task-ns-8841-fork-t3`);
            assert.equal(getForkedResp.status, 200);
            const headCommit = server.layerfs.getBranchHeadCommit(ttForkData.layerfsBranch);
            assert.equal(headCommit.commitId, "C0");
            // 6. Verify POST /v1beta1/management/tasks/cancel (202 CANCEL_UNCONFIRMED -> 200 FENCED_TERMINATED)
            const cancelUnconfResp = await fetch(`${baseUrl}/v1beta1/management/tasks/cancel`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ taskId: "task-ns-8842", mode: "graceful", actor: "chen", role: "SRE_ADMIN" }),
            });
            assert.equal(cancelUnconfResp.status, 202);
            const forceFenceResp = await fetch(`${baseUrl}/v1beta1/management/tasks/cancel`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ taskId: "task-ns-8842", mode: "force_fence", actor: "chen", role: "SRE_ADMIN" }),
            });
            assert.equal(forceFenceResp.status, 200);
            const forceFenceData = (await forceFenceResp.json());
            assert.equal(forceFenceData.state, "FENCED_TERMINATED");
            assert.equal(forceFenceData.newFencingToken, 9);
            // 7. Verify POST /v1beta1/management/approvals/decide enforces 4-eyes and mutates SQLite approval + tenant quota
            const selfApproveResp = await fetch(`${baseUrl}/v1beta1/management/approvals/decide`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    approvalId: "APR-2026-091",
                    approver: "chen",
                    role: "SRE_ADMIN",
                    decision: "approve",
                }),
            });
            assert.equal(selfApproveResp.status, 409);
            const peerApproveResp = await fetch(`${baseUrl}/v1beta1/management/approvals/decide`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    approvalId: "APR-2026-091",
                    approver: "zhou",
                    role: "SRE_ADMIN",
                    decision: "approve",
                }),
            });
            assert.equal(peerApproveResp.status, 200);
            // 8. Verify offline scenario returns real HTTP 503 and ?recover=1 restores 200
            await fetch(`${baseUrl}/v1beta1/management/scenario`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ scenario: "offline", actor: "chen" }),
            });
            const offlineResp = await fetch(`${baseUrl}/v1beta1/management/state`);
            assert.equal(offlineResp.status, 503);
            const recoverResp = await fetch(`${baseUrl}/v1beta1/management/state?recover=1`);
            assert.equal(recoverResp.status, 200);
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
    (0, node_test_1.it)("propagates the management scenario across gateways through SigV4-signed S3 requests", async () => {
        const objects = new Map();
        const signedRequests = [];
        const fakeS3 = http.createServer((req, res) => {
            const authorization = req.headers.authorization;
            if (typeof authorization !== "string" || !authorization.startsWith("AWS4-HMAC-SHA256")) {
                res.writeHead(403);
                res.end();
                return;
            }
            signedRequests.push(`${req.method} ${req.url ?? ""}`);
            if (req.method === "HEAD") {
                res.writeHead(200);
                res.end();
                return;
            }
            if (req.method === "GET") {
                const body = objects.get(req.url ?? "");
                if (body === undefined) {
                    res.writeHead(404);
                    res.end();
                    return;
                }
                res.writeHead(200, { "content-type": "application/json" });
                res.end(body);
                return;
            }
            if (req.method === "PUT") {
                const chunks = [];
                req.on("data", (chunk) => chunks.push(chunk));
                req.on("end", () => {
                    objects.set(req.url ?? "", Buffer.concat(chunks).toString("utf-8"));
                    res.writeHead(200);
                    res.end();
                });
                return;
            }
            res.writeHead(405);
            res.end();
        });
        const listening = Promise.withResolvers();
        fakeS3.listen(0, "127.0.0.1", listening.resolve);
        await listening.promise;
        const s3Endpoint = `http://127.0.0.1:${fakeS3.address().port}`;
        const tmpA = `/tmp/ansts-m41a-${process.pid}-${Date.now()}`;
        const tmpB = `/tmp/ansts-m41b-${process.pid}-${Date.now()}`;
        fs.mkdirSync(tmpA, { recursive: true });
        fs.mkdirSync(tmpB, { recursive: true });
        const s3Options = {
            host: "127.0.0.1",
            nodeRole: "gateway",
            s3Endpoint,
            s3AccessKeyId: "test-access-key",
            s3SecretAccessKey: "test-secret-key",
            s3Region: "us-east-1",
        };
        const gatewayA = new server_main_js_1.AnstsLocalAndComposeServer({ ...s3Options, nodeId: "celld-gw-01", storageDir: tmpA });
        const gatewayB = new server_main_js_1.AnstsLocalAndComposeServer({ ...s3Options, nodeId: "celld-gw-02", storageDir: tmpB });
        try {
            const { apiPort: portA } = await gatewayA.start(0, 0);
            const { apiPort: portB } = await gatewayB.start(0, 0);
            const urlA = `http://127.0.0.1:${portA}/v1beta1/management/state`;
            const urlB = `http://127.0.0.1:${portB}/v1beta1/management/state`;
            assert.equal((await fetch(urlA)).status, 200);
            assert.equal((await fetch(urlB)).status, 200);
            // A write accepted by gateway B must become visible on gateway A (shared S3 snapshot).
            const switchResp = await fetch(`http://127.0.0.1:${portB}/v1beta1/management/scenario`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ scenario: "offline", actor: "chen" }),
            });
            assert.equal(switchResp.status, 200);
            assert.equal((await fetch(urlB)).status, 503);
            assert.equal((await fetch(urlA)).status, 503);
            // A recovery performed on gateway A must restore gateway B as well.
            assert.equal((await fetch(`${urlA}?recover=1`)).status, 200);
            assert.equal((await fetch(urlB)).status, 200);
            assert.equal((await fetch(urlA)).status, 200);
            const persisted = JSON.parse(objects.get("/ansts-fleet/management/cluster_state.json") ?? "{}");
            assert.equal(persisted.scenario, "normal");
            assert.ok(signedRequests.some((r) => r.startsWith("PUT ")));
        }
        finally {
            await gatewayA.stop();
            await gatewayB.stop();
            const closed = Promise.withResolvers();
            fakeS3.close(closed.resolve);
            await closed.promise;
            fs.rmSync(tmpA, { recursive: true, force: true });
            fs.rmSync(tmpB, { recursive: true, force: true });
        }
    });
});
