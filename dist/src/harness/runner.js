"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AgentHarnessRunner = void 0;
/**
 * @fileoverview M4 TASK-17 & M64-5: In-Sandbox Agent Harness ReAct Loop Runner
 * with Closed-Loop `CognitiveRouter.reportInvocationMetrics()` Token & Latency Accounting.
 */
const models_js_1 = require("../models.js");
const cognitive_router_js_1 = require("../cognitive_router.js");
const saga_gateway_js_1 = require("../saga_gateway.js");
const velocity_breaker_js_1 = require("./velocity_breaker.js");
class AgentHarnessRunner {
    router;
    sagaGateway;
    breaker;
    constructor(router, sagaGateway, breaker = new velocity_breaker_js_1.SyntacticAndVelocityBreaker()) {
        this.router = router;
        this.sagaGateway = sagaGateway;
        this.breaker = breaker;
    }
    executeSingleTurn(task, toolName, toolArgs, estimatedTokens = 1500) {
        if (task.current_turn >= task.max_turns) {
            task.state = models_js_1.TaskState.DEAD_LETTER;
            throw new Error(`Task '${task.task_id}' exceeded max_turns (${task.max_turns})`);
        }
        task.state = models_js_1.TaskState.RUNNING_THINK;
        const prefixHash = task.prefix_Cache_hash ||
            cognitive_router_js_1.CognitiveRouter.computePrefixHash("system:agent-v2", "tools:default", task.prompt);
        task.prefix_Cache_hash = prefixHash;
        const t0 = performance.now();
        const [endpoint, cacheHit] = this.router.selectEndpoint(prefixHash, estimatedTokens, task.priority);
        try {
            const nextTurn = task.current_turn + 1;
            const argsHash = saga_gateway_js_1.SagaToolGateway.generateIdempotencyKey(task.task_id, nextTurn, toolName, toolArgs);
            const check = this.breaker.recordAndCheckTurn(task.task_id, {
                turnIndex: nextTurn,
                toolName,
                argsHash,
                tokensConsumed: estimatedTokens,
                timestampMs: Date.now(),
            });
            if (check.tripped) {
                task.state = models_js_1.TaskState.SUSPENDED_APPROVAL;
                throw new Error(`Velocity breaker tripped for task '${task.task_id}': ${check.reason}`);
            }
            task.state = models_js_1.TaskState.RUNNING_ACT;
            const [toolOutput, deduplicated] = this.sagaGateway.invokeTool(task.task_id, nextTurn, toolName, toolArgs);
            const observedTtftMs = Math.max(1, Number((performance.now() - t0).toFixed(2)));
            this.router.reportInvocationMetrics(endpoint.endpoint_id, observedTtftMs, deduplicated ? 0 : estimatedTokens, false);
            task.current_turn = nextTurn;
            return {
                taskId: task.task_id,
                turnIndex: nextTurn,
                endpointId: endpoint.endpoint_id,
                cacheHit,
                toolOutput,
                deduplicatedTool: deduplicated,
            };
        }
        finally {
            this.router.releaseEndpoint(endpoint.endpoint_id);
        }
    }
    /**
     * [M73-1 / F7] Executes an Agent ReAct turn after automatically compacting long-horizon `priorTurns`
     * into LayerFS symbolic URI pointers via `WalCompactorAndPagedRestorer.compactWorkingMemoryContext`.
     */
    executeTurnWithHierarchicalMemory(params) {
        const compactionReport = params.compactor.compactWorkingMemoryContext({
            taskId: params.task.task_id,
            currentTurnIndex: params.task.current_turn,
            hotWindowTurns: params.hotWindowTurns,
            maxInlineCharsPerOutput: params.maxInlineCharsPerOutput,
            turns: params.priorTurns,
        });
        const turnSummary = this.executeSingleTurn(params.task, params.toolName, params.toolArgs, compactionReport.compactedContextTokensEstimate);
        return {
            turnSummary,
            compactionReport,
        };
    }
}
exports.AgentHarnessRunner = AgentHarnessRunner;
