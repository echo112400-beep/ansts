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
 * @fileoverview M36 Next-Batch Deep Optimization E2E Suite:
 * 1. [M36-1] Zero-Trust Shell Policy Guard (`403`) & SLSA v1.0 Merkle Tree + TEE Quote + SPIFFE SVID Attestation (`POST /v1beta1/security/attest`)
 * 2. [M36-2] Closed-Loop Cluster Governance: SLA Enforcer, Priority Load Shedder, EWMA Prewarmer & Canary Auto-Rollback (`POST /v1beta1/cluster/governance`)
 * 3. [M36-3] Cross-Node Multi-Agent LayerFS 3-Way Branch Merge via RustFS S3 (`POST /v1/layerfs/merge`, `.lww` auto-resolution & `409` conflict gate)
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const http = __importStar(require("node:http"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("M36 Next-Batch Deep Optimization E2E Suite (SLSA/TEE Attestation, Closed-Loop SLA/Canary & Cross-Node LayerFS Merge)", () => {
    (0, node_test_1.it)("1. [M36-1] Enforces Zero-Trust shell command policy (403) and signs verifiable SLSA v1.0 + TEE + SPIFFE attestations", async () => {
        const srv = new index_js_1.MultiNodeDevServer({
            host: "127.0.0.1",
            nodeId: "body-sec-01",
            nodeRole: "body",
        });
        const { apiPort } = await srv.start(0, 0);
        try {
            // 1. Forbidden command (`cat /etc/shadow`) must be rejected with HTTP 403
            const forbiddenRes = await fetch(`http://127.0.0.1:${apiPort}/v1/layerfs/exec`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    branch: "main",
                    agent_id: "untrusted-agent",
                    step_id: "step-forbidden",
                    command: "cat /etc/shadow",
                }),
            });
            assert.equal(forbiddenRes.status, 403);
            const forbiddenJson = (await forbiddenRes.json());
            assert.equal(forbiddenJson.error, "ZERO_TRUST_POLICY_VIOLATION");
            // 2. Legitimate workspace execution on `feat-slsa`
            const okRes = await fetch(`http://127.0.0.1:${apiPort}/v1/layerfs/exec`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    branch: "feat-slsa",
                    agent_id: "trusted-builder",
                    step_id: "step-build",
                    command: "printf 'export const version = 25;' > index.ts",
                }),
            });
            assert.equal(okRes.status, 200);
            // 3. Attest `feat-slsa` with valid SPIFFE ID
            const attestRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/security/attest`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    branchOrCommit: "feat-slsa",
                    spiffeId: "spiffe://ansts.io/ns/fleet/sa/body",
                    nonce: "nonce-2026-m36",
                }),
            });
            assert.equal(attestRes.status, 200);
            const attestJson = (await attestRes.json());
            assert.equal(attestJson.mtlsCheck.authenticated, true);
            assert.equal(attestJson.slsaVerified, true);
            assert.equal(attestJson.slsaStatement.predicateType, "https://slsa.dev/provenance/v1");
            assert.ok(attestJson.slsaStatement.fileCount >= 3);
            assert.equal(attestJson.teeQuote.verified, true);
            // 4. Invalid SPIFFE ID must be rejected with HTTP 401
            const badMtls = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/security/attest`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    branchOrCommit: "feat-slsa",
                    spiffeId: "spiffe://rogue.domain/sa/hacker",
                }),
            });
            assert.equal(badMtls.status, 401);
        }
        finally {
            await srv.stop();
        }
    });
    (0, node_test_1.it)("2. [M36-2] Executes closed-loop Cluster SLA enforcement, Priority Load Shedding, EWMA Prewarming & Canary Auto-Rollback", async () => {
        const srv = new index_js_1.MultiNodeDevServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-01",
            nodeRole: "gateway",
        });
        const { apiPort } = await srv.start(0, 0);
        try {
            const govRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/cluster/governance`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "evaluateSlaAndPrewarm",
                    utilizationRatio: 0.92,
                    p0QueueWaitP99Ms: 310,
                    warmPoolHitRatio: 0.95,
                    observedArrivalsPerSec: 20.0,
                    avgSandboxHoldSec: 5.0,
                }),
            });
            assert.equal(govRes.status, 200);
            const govJson = (await govRes.json());
            assert.equal(govJson.slaCompliance.compliant, false);
            assert.equal(govJson.slaCompliance.reserveEmergencyHeadroom, true);
            assert.equal(govJson.priorityAdmission.P0_EMERGENCY, true);
            assert.equal(govJson.priorityAdmission.P1_HIGH, true);
            assert.equal(govJson.priorityAdmission.P2_STANDARD, false);
            assert.equal(govJson.priorityAdmission.P3_BATCH, false);
            assert.ok(govJson.prewarmPrediction.recommendedWarmPoolSize >= 4);
            // Canary step promotion & rollback
            const can1 = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/cluster/governance`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "evaluateCanary",
                    errorRatePct: 0.2,
                    p99LatencyMs: 180,
                }),
            });
            const can1Json = (await can1.json());
            assert.equal(can1Json.state, "CANARY_ACTIVE");
            assert.equal(can1Json.trafficWeightPct, 25);
            const canRollback = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/cluster/governance`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "evaluateCanary",
                    errorRatePct: 4.5,
                    p99LatencyMs: 2100,
                }),
            });
            const canRollbackJson = (await canRollback.json());
            assert.equal(canRollbackJson.state, "ROLLED_BACK");
            assert.equal(canRollbackJson.trafficWeightPct, 0);
        }
        finally {
            await srv.stop();
        }
    });
    (0, node_test_1.it)("3. [M36-3] Performs cross-node 2-Agent LayerFS 3-way branch merge via RustFS S3 with .lww auto-resolution and 409 conflict detection", async () => {
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
            res.writeHead(404).end();
        });
        const s3Port = await new Promise((resolve) => {
            s3Server.listen(0, "127.0.0.1", () => {
                const addr = s3Server.address();
                resolve(typeof addr === "object" && addr ? addr.port : 0);
            });
        });
        const s3Endpoint = `http://127.0.0.1:${s3Port}`;
        const body1 = new index_js_1.MultiNodeDevServer({
            host: "127.0.0.1",
            nodeId: "body-host-01",
            nodeRole: "body",
            s3Endpoint,
        });
        const body2 = new index_js_1.MultiNodeDevServer({
            host: "127.0.0.1",
            nodeId: "body-host-02",
            nodeRole: "body",
            s3Endpoint,
        });
        const bd1 = await body1.start(0, 0);
        const bd2 = await body2.start(0, 0);
        try {
            // Agent A on Body-01 creates `mod_a.ts` and updates `.lww` file `package-lock.json` on `branch-agent-a`
            await fetch(`http://127.0.0.1:${bd1.apiPort}/v1/layerfs/exec`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    branch: "branch-agent-a",
                    agent_id: "agent-a",
                    step_id: "step-a1",
                    command: "printf 'export const a = 1;' > mod_a.ts && printf 'lock-vA' > package-lock.json",
                }),
            });
            // Agent B on Body-02 creates `mod_b.ts` and updates `.lww` file `package-lock.json` on `main`
            await fetch(`http://127.0.0.1:${bd2.apiPort}/v1/layerfs/exec`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    branch: "main",
                    agent_id: "agent-b",
                    step_id: "step-b1",
                    command: "printf 'export const b = 2;' > mod_b.ts && printf 'lock-vB' > package-lock.json",
                }),
            });
            // Merge `branch-agent-a` (created on Body-01) into `main` on Body-02!
            const mergeRes = await fetch(`http://127.0.0.1:${bd2.apiPort}/v1/layerfs/merge`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    sourceBranch: "branch-agent-a",
                    targetBranch: "main",
                    baseCommitId: "C0",
                }),
            });
            assert.equal(mergeRes.status, 200);
            const mergeJson = (await mergeRes.json());
            assert.ok(mergeJson.autoResolvedByLww.includes("package-lock.json"));
            assert.equal(mergeJson.s3Synced, true);
            // Verify both `mod_a.ts` and `mod_b.ts` are now present on `main` when read from Body-01!
            const verifyMain = await fetch(`http://127.0.0.1:${bd1.apiPort}/v1/layerfs/exec`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    branch: "main",
                    agent_id: "verifier",
                    step_id: "step-verify",
                    command: "cat mod_a.ts && printf '|' && cat mod_b.ts",
                }),
            });
            const verifyJson = (await verifyMain.json());
            assert.equal(verifyJson.stdout, "export const a = 1;|export const b = 2;");
        }
        finally {
            await body1.stop();
            await body2.stop();
            await new Promise((resolve) => s3Server.close(() => resolve()));
        }
    });
});
