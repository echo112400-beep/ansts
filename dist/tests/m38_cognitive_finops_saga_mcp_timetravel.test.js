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
 * @fileoverview M38 Next-Batch Deep Optimization E2E Suite:
 * 1. [M38-1] Cognitive LLM Router Sticky Prefix-Cache Affinity + FinOps 80% Auto-Downgrade & 100% HTTP 402 Gate (`POST /v1beta1/router/completions`)
 * 2. [M38-2] Dynamic MCP Tool Hub Registration, Saga SHA-256 Idempotency, Human-in-the-Loop Approval Gate & LIFO Rollback (`POST /v1beta1/saga/*`)
 * 3. [M38-3] Turn WAL Fencing (`409` on Stale Fence), Deterministic Time-Travel Task + LayerFS Zero-Copy Forking & Cross-Node Heavy Sandbox Live Migration
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("M38 Next-Batch Deep Optimization E2E Suite (Cognitive Router + FinOps, MCP + Saga LIFO & Time-Travel + Live Migration)", () => {
    (0, node_test_1.it)("1. [M38-1] Routes LLM completions with sticky prefix-cache affinity, auto-downgrades model at 80% FinOps budget, and blocks with 402 at 100%", async () => {
        const srv = new index_js_1.MultiNodeDevServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-01",
            nodeRole: "gateway",
        });
        const { apiPort } = await srv.start(0, 0);
        try {
            // Configure tenant FinOps budget: $10 daily limit, 80% ($8) soft downgrade from DeepSeek -> Qwen-3.8
            await fetch(`http://127.0.0.1:${apiPort}/v1beta1/router/completions`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "setBudgetPolicy",
                    tenantId: "tenant-finops",
                    dailyBudgetUsd: 10.0,
                    softDowngradeThresholdRatio: 0.8,
                    preferredModel: "DeepSeek",
                    fallbackEconomyModel: "Qwen-3.8",
                }),
            });
            // Call #1 ($8.50 spend): starts at $0 spent -> uses preferredModel `DeepSeek`, prefixCacheHit=false
            const c1 = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/router/completions`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    tenantId: "tenant-finops",
                    systemPrompt: "You are a kernel engineer",
                    costUsd: 8.5,
                }),
            });
            assert.equal(c1.status, 200);
            const c1Json = (await c1.json());
            assert.equal(c1Json.selectedModel, "DeepSeek");
            assert.equal(c1Json.downgraded, false);
            assert.equal(c1Json.prefixCacheHit, false);
            // Call #2 ($2.00 spend, total now $8.50 >= 80%): auto-downgrades to `Qwen-3.8` AND hits sticky prefix cache!
            const c2 = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/router/completions`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    tenantId: "tenant-finops",
                    systemPrompt: "You are a kernel engineer",
                    costUsd: 2.0,
                }),
            });
            assert.equal(c2.status, 200);
            const c2Json = (await c2.json());
            assert.equal(c2Json.selectedModel, "Qwen-3.8");
            assert.equal(c2Json.downgraded, true);
            assert.equal(c2Json.prefixCacheHit, true);
            assert.equal(c2Json.endpointId, c1Json.endpointId);
            // Call #3 (total spent is $10.50 >= $10.00): rejected with HTTP 402 Payment Required!
            const c3 = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/router/completions`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    tenantId: "tenant-finops",
                    systemPrompt: "You are a kernel engineer",
                    costUsd: 0.5,
                }),
            });
            assert.equal(c3.status, 402);
            const c3Json = (await c3.json());
            assert.equal(c3Json.error, "FINOPS_BUDGET_EXCEEDED");
        }
        finally {
            await srv.stop();
        }
    });
    (0, node_test_1.it)("2. [M38-2] Registers dynamic MCP tools, deduplicates retries, enforces Human Approval Gate, and executes LIFO Saga rollback", async () => {
        const srv = new index_js_1.MultiNodeDevServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-01",
            nodeRole: "gateway",
        });
        const { apiPort } = await srv.start(0, 0);
        try {
            // 1. Dynamically register MCP tool `k8s_deploy`
            const regRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/saga/mcp/register`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    serverName: "mcp-ops",
                    tools: [
                        {
                            toolName: "k8s_deploy",
                            description: "Deploy service",
                            effectClass: "COMPENSATABLE_WRITE",
                        },
                    ],
                }),
            });
            assert.equal(regRes.status, 200);
            // 2. Execute `k8s_deploy` at turn 1 and verify idempotent retry deduplication
            const ex1 = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/saga/execute`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "task-saga-m38",
                    turnIndex: 1,
                    toolName: "k8s_deploy",
                    args: { version: "v2.5.0" },
                }),
            });
            const ex1Json = (await ex1.json());
            assert.equal(ex1Json.deduplicated, false);
            const ex1Retry = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/saga/execute`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "task-saga-m38",
                    turnIndex: 1,
                    toolName: "k8s_deploy",
                    args: { version: "v2.5.0" },
                }),
            });
            const ex1RetryJson = (await ex1Retry.json());
            assert.equal(ex1RetryJson.deduplicated, true);
            // 3. Stage high-risk tool at turn 2 requiring human approval (`202 PENDING_APPROVAL`)
            const stageRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/saga/execute`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "task-saga-m38",
                    turnIndex: 2,
                    toolName: "shell_exec",
                    args: { cmd: "systemctl reload nginx" },
                    requireApproval: true,
                }),
            });
            assert.equal(stageRes.status, 202);
            const stageJson = (await stageRes.json());
            assert.equal(stageJson.status, "PENDING_APPROVAL");
            // Approve the ticket -> commits the staged tool call!
            const appRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/saga/approve`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    ticketId: stageJson.ticket.ticketId,
                    taskId: "task-saga-m38",
                    approved: true,
                    decidedBy: "sre-lead",
                }),
            });
            assert.equal(appRes.status, 200);
            const appJson = (await appRes.json());
            assert.equal(appJson.ticket.status, "APPROVED");
            assert.equal(appJson.committedEffects.length, 1);
            // 4. Rollback task in reverse LIFO order
            const rbRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/saga/rollback`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "task-saga-m38",
                    upToTurnInclusive: 1,
                }),
            });
            assert.equal(rbRes.status, 200);
            const rbJson = (await rbRes.json());
            assert.equal(rbJson.rolledBackCount, 2);
        }
        finally {
            await srv.stop();
        }
    });
    (0, node_test_1.it)("3. [M38-3] Appends CRC32 Turn WAL frames (rejecting stale fences with 409), forks Time-Travel branches, and live-migrates heavy sandboxes", async () => {
        const srv = new index_js_1.MultiNodeDevServer({
            host: "127.0.0.1",
            nodeId: "body-host-01",
            nodeRole: "body",
        });
        const { apiPort } = await srv.start(0, 0);
        try {
            // 1. Append Turn 1 & Turn 2 WAL deltas
            const w1 = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/checkpoint/time-travel`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "appendTurnWal",
                    taskId: "task-tt-01",
                    turnIndex: 1,
                    fencingToken: 5,
                    highestSubmittedFencingToken: 5,
                    changedFiles: ["src/core.ts"],
                }),
            });
            assert.equal(w1.status, 200);
            // Stale fencing token (4 < 5) must be rejected with HTTP 409
            const wStale = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/checkpoint/time-travel`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "appendTurnWal",
                    taskId: "task-tt-01",
                    turnIndex: 2,
                    fencingToken: 4,
                    highestSubmittedFencingToken: 5,
                    changedFiles: ["src/zombie.ts"],
                }),
            });
            assert.equal(wStale.status, 409);
            // 2. Time-Travel fork `task-tt-01` at Turn 1 + zero-copy LayerFS commit `C0`
            const forkRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/checkpoint/time-travel`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "forkAtTurn",
                    taskId: "task-tt-01",
                    targetTurnIndex: 1,
                    newTaskId: "task-tt-01-replayed",
                    overriddenPrompt: "explore alternative MCTS path",
                    sourceCommitId: "C0",
                    newBranchName: "mcts-alt-branch",
                }),
            });
            assert.equal(forkRes.status, 200);
            const forkJson = (await forkRes.json());
            assert.equal(forkJson.forked.restoredTurnIndex, 1);
            assert.deepEqual(forkJson.forked.inheritedFiles, ["src/core.ts"]);
            assert.equal(forkJson.layerfsFork.branchName, "mcts-alt-branch");
            assert.equal(forkJson.layerfsFork.copiedFilesCount, 0);
            // 3. Live-migrate heavy task snapshot to `body-host-02`
            const migRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/runtime/migrate`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "task-tt-01-replayed",
                    turnIndex: 1,
                    branchName: "mcts-alt-branch",
                    sourceNodeId: "body-host-01",
                    targetNodeId: "body-host-02",
                    newFencingToken: 6,
                }),
            });
            assert.equal(migRes.status, 200);
            const migJson = (await migRes.json());
            assert.equal(migJson.sourceFrozen, true);
            assert.equal(migJson.targetRestored, true);
        }
        finally {
            await srv.stop();
        }
    });
});
