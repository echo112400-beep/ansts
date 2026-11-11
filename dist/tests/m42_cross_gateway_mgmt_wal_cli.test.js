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
 * @fileoverview M42 End-to-End Verification: Cross-Gateway S3 Management State Sync,
 * Real Turn WAL CRC32 Inspector, Dynamic Approval Creation & `anstsctl` Management CLI.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const http = __importStar(require("node:http"));
const fs = __importStar(require("node:fs"));
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M42 Cross-Gateway Management S3 Sync, Turn WAL CRC32 Inspector & Management CLI Suite", () => {
    (0, node_test_1.it)("synchronizes management state across Gateway-01 and Gateway-02 via S3, inspects CRC32 WAL turns, and runs mgmt-* CLI commands", async () => {
        const tmpDir = `/tmp/ansts-m42-${process.pid}-${Date.now()}`;
        fs.mkdirSync(tmpDir, { recursive: true });
        // Start in-memory S3 CAS/LTX endpoint
        const s3Objects = new Map();
        const s3Server = http.createServer((req, res) => {
            const u = new URL(req.url ?? "/", "http://127.0.0.1");
            if (req.method === "PUT") {
                const chunks = [];
                req.on("data", (c) => chunks.push(c));
                req.on("end", () => {
                    s3Objects.set(u.pathname, Buffer.concat(chunks));
                    res.writeHead(200, { "content-type": "application/json" });
                    res.end(JSON.stringify({ stored: true }));
                });
                return;
            }
            if (req.method === "GET" && s3Objects.has(u.pathname)) {
                res.writeHead(200, { "content-type": "application/json" });
                res.end(s3Objects.get(u.pathname));
                return;
            }
            res.writeHead(404, { "content-type": "application/json" });
            res.end(JSON.stringify({ error: "NOT_FOUND" }));
        });
        const s3Port = await new Promise((resolve) => {
            s3Server.listen(0, "127.0.0.1", () => {
                const addr = s3Server.address();
                resolve(typeof addr === "object" && addr ? addr.port : 0);
            });
        });
        const s3Endpoint = `http://127.0.0.1:${s3Port}`;
        const gw1 = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-01",
            nodeRole: "gateway",
            storageDir: `${tmpDir}/gw-01`,
            s3Endpoint,
        });
        const gw2 = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-02",
            nodeRole: "gateway",
            storageDir: `${tmpDir}/gw-02`,
            s3Endpoint,
        });
        const { apiPort: gw1Port } = await gw1.start(0, 0);
        const { apiPort: gw2Port } = await gw2.start(0, 0);
        const gw1Url = `http://127.0.0.1:${gw1Port}`;
        const gw2Url = `http://127.0.0.1:${gw2Port}`;
        try {
            // 1. [M42-1] Update Northstar quota on Gateway-01 via CLI `mgmt-quota`, then verify Gateway-02 sees v15!
            const cliQuotaOut = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-quota", "Northstar", "175", "1600", "14", "chen"], gw1Url);
            assert.equal(cliQuotaOut.exitCode, 0);
            const quotaRes = JSON.parse(cliQuotaOut.stdout);
            assert.equal(quotaRes.configVersion, 15);
            assert.equal(quotaRes.maxSlots, 175);
            // Query Gateway-02 (`gw2Url`) via CLI `mgmt-state` — must see v15 and maxSlots=175 via S3 sync!
            const cliStateOut = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-state", "us-west-2"], gw2Url);
            assert.equal(cliStateOut.exitCode, 0);
            const gw2State = JSON.parse(cliStateOut.stdout);
            const nsOnGw2 = gw2State.tenants.find((t) => t.id === "Northstar");
            assert.equal(nsOnGw2.configVersion, 15);
            assert.equal(nsOnGw2.maxSlots, 175);
            // Attempting stale v14 update on Gateway-02 must fail with 409 CAS_VERSION_CONFLICT
            const staleOnGw2 = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-quota", "Northstar", "190", "1600", "14", "zhou"], gw2Url);
            assert.equal(staleOnGw2.exitCode, 1);
            assert.ok(staleOnGw2.stdout.includes("CAS_VERSION_CONFLICT"));
            // 2. [M42-2] Create dynamic approval ticket on Gateway-01 (`POST /v1beta1/management/approvals/create`) and approve on Gateway-02 via CLI `mgmt-approve`
            const createAprResp = await fetch(`${gw1Url}/v1beta1/management/approvals/create`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    type: "QUOTA_EMERGENCY_BOOST",
                    target: "Northstar (175 → 180 并发槽)",
                    region: "us-west-2",
                    tenant: "Northstar",
                    initiator: "lin",
                    role: "OPERATOR",
                }),
            });
            assert.equal(createAprResp.status, 201);
            const createdTicket = (await createAprResp.json());
            assert.ok(createdTicket.approvalId.startsWith("APR-2026-"));
            const cliApproveOut = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-approve", createdTicket.approvalId, "zhou", "approve"], gw2Url);
            assert.equal(cliApproveOut.exitCode, 0);
            const approvedData = JSON.parse(cliApproveOut.stdout);
            assert.equal(approvedData.status, "APPROVED (zhou)");
            // 3. [M42-3] Inspect real Turn WAL CRC32 checksums via `mgmt-turns` CLI (`GET /v1beta1/management/tasks/:id/turns`)
            const cliTurnsOut = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-turns", "task-ns-8841"], gw1Url);
            assert.equal(cliTurnsOut.exitCode, 0);
            const turnsData = JSON.parse(cliTurnsOut.stdout);
            assert.equal(turnsData.taskId, "task-ns-8841");
            assert.equal(turnsData.turns.length, 5);
            assert.equal(turnsData.turns[0].integrityVerified, true);
            assert.ok(turnsData.turns[0].crc32Hex.length === 8);
            // 4. [M42-4] Drain body-host-02 with `--force-migrate` via CLI `mgmt-drain`
            const cliDrainFail = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-drain", "body-host-02"], gw1Url);
            assert.equal(cliDrainFail.exitCode, 1); // 207 partial failure without --force-migrate
            const cliDrainOk = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-drain", "body-host-02", "--force-migrate"], gw2Url);
            assert.equal(cliDrainOk.exitCode, 0);
        }
        finally {
            await gw1.stop();
            await gw2.stop();
            await new Promise((resolve) => s3Server.close(() => resolve()));
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
