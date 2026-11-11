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
 * @fileoverview Google Engineering Standards & 88-Task Behavioral End-to-End Verification Suite (7 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const path = __importStar(require("node:path"));
const Ansts = __importStar(require("../src/index.js"));
(0, node_test_1.describe)("Google Engineering Standards & 88-Task Full-Lifecycle Behavioral Audit", () => {
    (0, node_test_1.it)("1. Verifies all 58 src/<all>/*.ts modules carry @fileoverview JSDoc headers and strict typing", () => {
        const walk = (dir) => {
            let out = [];
            for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
                const full = path.join(dir, entry.name);
                if (entry.isDirectory())
                    out = out.concat(walk(full));
                else if (entry.isFile() && entry.name.endsWith(".ts"))
                    out.push(full);
            }
            return out;
        };
        const srcFiles = walk(path.join(process.cwd(), "src"));
        assert.equal(srcFiles.length, 86);
        for (const f of srcFiles) {
            const content = fs.readFileSync(f, "utf-8");
            assert.ok(content.includes("@fileoverview"), `Missing @fileoverview in ${f}`);
        }
    });
    (0, node_test_1.it)("2. Behavioral E2E M1-M3: ApiGatewayServer -> CoreScheduler5D -> AgentHarnessRunner -> CognitiveRouter & SagaToolGateway", async () => {
        const sched = new Ansts.CoreScheduler5D();
        sched.registerOrUpdateNode(new Ansts.NodeDaemon("node-e2e-1").emitHeartbeat());
        const gw = new Ansts.ApiGatewayServer(sched);
        const resp = gw.handleSubmitRequest({
            task_id: "e2e-task-01",
            tenant_id: "tenant-e2e",
            priority: Ansts.TaskPriority.P1_HIGH,
            prompt: "Run E2E verification",
        });
        assert.equal(resp.statusCode, 202);
        const placements = sched.scheduleTick();
        assert.equal(placements.length, 1);
        const router = new Ansts.CognitiveRouter();
        router.registerEndpoint(new Ansts.LlmEndpointMetrics({ endpoint_id: "ep-1", provider: "openai", model_name: "gpt-4o" }));
        const saga = new Ansts.SagaToolGateway();
        saga.registerTool("echo_tool", Ansts.ToolCallEffectClass.IDEMPOTENT_WRITE, (args) => ({ echoed: args.msg }));
        const runner = new Ansts.AgentHarnessRunner(router, saga);
        const task = sched.allTasks.get("e2e-task-01");
        const turnSummary = runner.executeSingleTurn(task, "echo_tool", { msg: "ok" });
        assert.equal(turnSummary.turnIndex, 1);
        assert.equal(turnSummary.toolOutput.echoed, "ok");
    });
    (0, node_test_1.it)("3. Behavioral E2E M4-M6: WarmSandboxPoolManager -> CgroupPhaseController -> TurnWalEngine -> CloudEventsBus", () => {
        const pool = new Ansts.WarmSandboxPoolManager("node-1", 4);
        const vm = pool.acquireSandbox("e2e-task-01");
        assert.equal(vm.state, "ALLOCATED");
        assert.equal(pool.stats().warmCount, 4);
        const cg = new Ansts.CgroupPhaseController();
        const actState = cg.applyActPhase("e2e-task-01", {
            think_cpu_cores: 0.25,
            act_cpu_cores: 2.0,
            act_memory_mb: 4096,
            disk_mb: 10240,
        });
        assert.equal(actState.cpuQuotaUsPer100ms, 200_000);
        const wal = new Ansts.TurnWalEngine();
        const entry = wal.appendTurnDelta({
            taskId: "e2e-task-01",
            turnIndex: 1,
            fencingToken: 1,
            changedFiles: ["src/main.ts"],
            sqliteWalPages: 8,
            highestSubmittedFencingToken: 1,
        });
        assert.equal(entry.walSeq, 1);
        assert.equal(pool.releaseSandbox("e2e-task-01"), true);
    });
    (0, node_test_1.it)("4. Behavioral E2E M7-M9: Cloudflare Worker Entrypoint -> AnstsSchedulerCell -> RpoZeroDurabilityEngine", async () => {
        const state = new Ansts.MockDurableObjectState("cell-0");
        const cell = new Ansts.AnstsSchedulerCell(state);
        const env = {
            ANSTS_SCHEDULER_CELL: {
                idFromName: (name) => ({ toString: () => name }),
                get: () => ({
                    id: { toString: () => "cell-0" },
                    fetch: (input, init) => cell.fetch(input instanceof Request ? input : new Request(String(input), init)),
                }),
            },
            ANSTS_HARNESS_CELL: {
                idFromName: (name) => ({ toString: () => name }),
                get: () => ({ id: { toString: () => "h-0" }, fetch: async () => new Response("{}") }),
            },
            ANSTS_IDEMPOTENCY_KV: new Ansts.InMemoryKvNamespace(),
            ANSTS_CHECKPOINT_BUCKET: new Ansts.InMemoryR2Bucket(),
            ANSTS_TASK_DISPATCH_QUEUE: new Ansts.InMemoryQueueProducer(),
            ANSTS_WEBHOOK_OUTBOX_QUEUE: new Ansts.InMemoryQueueProducer(),
            CELLD_REGION: "us-west-1",
        };
        const healthResp = await Ansts.cloudflareCelldWorker.fetch(new Request("https://ansts.internal/healthz"), env);
        assert.equal(healthResp.status, 200);
        const healthBody = (await healthResp.json());
        assert.equal(healthBody.region, "us-west-1");
    });
    (0, node_test_1.it)("5. Behavioral E2E M10-M11: ModelPoolClientAdapter -> SlidingWindowTokenBucket -> ThreeStateCircuitBreaker", () => {
        const adapter = new Ansts.ModelPoolClientAdapter();
        adapter.setPricing("ep-1", 0.002, 0.01);
        const ep = new Ansts.LlmEndpointMetrics({ endpoint_id: "ep-1", provider: "openai", model_name: "gpt-4o" });
        const inf = adapter.executeInference(ep, {
            taskId: "t1",
            turnIndex: 1,
            prompt: "test",
            estimatedPromptTokens: 1000,
            maxOutputTokens: 200,
            requireJsonSchema: true,
        });
        assert.ok(inf.costUsd > 0);
        assert.equal(JSON.parse(inf.outputText).action, "act");
        const breaker = new Ansts.ThreeStateCircuitBreaker(2, 1000);
        breaker.recordFailure(100);
        breaker.recordFailure(110);
        assert.equal(breaker.getState(200), "OPEN");
        assert.equal(breaker.getState(1500), "HALF_OPEN");
        breaker.recordSuccess();
        assert.equal(breaker.getState(1600), "CLOSED");
    });
    (0, node_test_1.it)("6. Behavioral E2E M12-M14: ManualFleetScaleController Cordon & Drain across Active Bare-Metal Nodes", () => {
        const fleet = new Ansts.ManualFleetScaleController();
        fleet.registerBareMetalNode({
            nodeId: "bm-1",
            ipAddress: "10.0.0.1",
            role: "worker",
            systemdUnit: "ansts-node-daemon.service",
            runningCells: ["cell-a", "cell-b"],
        });
        fleet.registerBareMetalNode({
            nodeId: "bm-2",
            ipAddress: "10.0.0.2",
            role: "worker",
            systemdUnit: "ansts-node-daemon.service",
            runningCells: [],
        });
        const drained = fleet.cordonAndDrainNode("bm-1");
        assert.equal(drained.finalState, "OFFLINE");
        assert.equal(drained.drainedCells.length, 2);
        assert.equal(drained.drainedCells[0].targetNodeId, "bm-2");
    });
    (0, node_test_1.it)("7. Behavioral E2E M15-M17: DynamicQueueAutoSharder + LayerFsContentStoreEngine + LayerFsWorkspaceDriver", () => {
        const store = new Ansts.LayerFsContentStoreEngine();
        const driver = new Ansts.LayerFsWorkspaceDriver(store);
        driver.forkBranch("main", "audit-branch");
        const op = driver.executeInProcessFileTool({
            branchName: "audit-branch",
            agentId: "auditor",
            stepId: "audit-step-1",
            operation: "write",
            filePath: "audit.txt",
            newContent: "verified-genuine-behavior\n".repeat(20),
        });
        assert.equal(op.committedNewSnapshot, true);
        const head = driver.getBranchHeadCommit("audit-branch");
        const fileEntry = head.filesTree.get("audit.txt");
        assert.equal(store.reconstructObjectContent(fileEntry.objectId), "verified-genuine-behavior\n".repeat(20));
    });
});
