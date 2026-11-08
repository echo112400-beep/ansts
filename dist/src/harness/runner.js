"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.AgentHarnessRunner = void 0;


var _models = require("../models.js");
var _cognitive_router = require("../cognitive_router.js");
var _saga_gateway = require("../saga_gateway.js");
var _velocity_breaker = require("./velocity_breaker.js"); /**
 * @fileoverview M4 TASK-17: In-Sandbox Agent Harness ReAct Loop Runner.
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
}exports.AgentHarnessRunner = AgentHarnessRunner;