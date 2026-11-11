"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.AgentHarnessRunner = void 0;



var _models = require("../models.js");
var _cognitive_router = require("../cognitive_router.js");
var _saga_gateway = require("../saga_gateway.js");
var _velocity_breaker = require("./velocity_breaker.js"); /**
 * @fileoverview M4 TASK-17 & M64-5: In-Sandbox Agent Harness ReAct Loop Runner
 * with Closed-Loop `CognitiveRouter.reportInvocationMetrics()` Token & Latency Accounting.
 */








class AgentHarnessRunner {
  router;
  sagaGateway;
  breaker;

  constructor(
  router,
  sagaGateway,
  breaker = new _velocity_breaker.SyntacticAndVelocityBreaker())
  {
    this.router = router;
    this.sagaGateway = sagaGateway;
    this.breaker = breaker;
  }

  executeSingleTurn(
  task,
  toolName,
  toolArgs,
  estimatedTokens = 1500)
  {
    if (task.current_turn >= task.max_turns) {
      task.state = _models.TaskState.DEAD_LETTER;
      throw new Error(`Task '${task.task_id}' exceeded max_turns (${task.max_turns})`);
    }
    task.state = _models.TaskState.RUNNING_THINK;
    const prefixHash =
    task.prefix_Cache_hash ||
    _cognitive_router.CognitiveRouter.computePrefixHash("system:agent-v2", "tools:default", task.prompt);
    task.prefix_Cache_hash = prefixHash;

    const t0 = performance.now();
    const [endpoint, cacheHit] = this.router.selectEndpoint(prefixHash, estimatedTokens, task.priority);
    try {
      const nextTurn = task.current_turn + 1;
      const argsHash = _saga_gateway.SagaToolGateway.generateIdempotencyKey(task.task_id, nextTurn, toolName, toolArgs);
      const check = this.breaker.recordAndCheckTurn(task.task_id, {
        turnIndex: nextTurn,
        toolName,
        argsHash,
        tokensConsumed: estimatedTokens,
        timestampMs: Date.now()
      });
      if (check.tripped) {
        task.state = _models.TaskState.SUSPENDED_APPROVAL;
        throw new Error(`Velocity breaker tripped for task '${task.task_id}': ${check.reason}`);
      }

      task.state = _models.TaskState.RUNNING_ACT;
      const [toolOutput, deduplicated] = this.sagaGateway.invokeTool(
      task.task_id,
      nextTurn,
      toolName,
      toolArgs);

      const observedTtftMs = Math.max(1, Number((performance.now() - t0).toFixed(2)));
      this.router.reportInvocationMetrics(
      endpoint.endpoint_id,
      observedTtftMs,
      deduplicated ? 0 : estimatedTokens,
      false);

      task.current_turn = nextTurn;
      return {
        taskId: task.task_id,
        turnIndex: nextTurn,
        endpointId: endpoint.endpoint_id,
        cacheHit,
        toolOutput,
        deduplicatedTool: deduplicated
      };
    } finally {
      this.router.releaseEndpoint(endpoint.endpoint_id);
    }
  }

  /**
   * [M73-1 / F7] Executes an Agent ReAct turn after automatically compacting long-horizon `priorTurns`
   * into LayerFS symbolic URI pointers via `WalCompactorAndPagedRestorer.compactWorkingMemoryContext`.
   */
  executeTurnWithHierarchicalMemory(params)
















  {
    const compactionReport = params.compactor.compactWorkingMemoryContext({
      taskId: params.task.task_id,
      currentTurnIndex: params.task.current_turn,
      hotWindowTurns: params.hotWindowTurns,
      maxInlineCharsPerOutput: params.maxInlineCharsPerOutput,
      turns: params.priorTurns
    });
    const turnSummary = this.executeSingleTurn(
    params.task,
    params.toolName,
    params.toolArgs,
    compactionReport.compactedContextTokensEstimate);

    return {
      turnSummary,
      compactionReport
    };
  }
}exports.AgentHarnessRunner = AgentHarnessRunner;