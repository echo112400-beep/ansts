/**
 * @fileoverview M4 TASK-17: In-Sandbox Agent Harness ReAct Loop Runner.
 */
import { TaskSpec, TaskState } from "../models.js";
import { CognitiveRouter } from "../cognitive_router.js";
import { SagaToolGateway } from "../saga_gateway.js";
import { SyntacticAndVelocityBreaker } from "./velocity_breaker.js";

export interface TurnExecutionSummary {
  taskId: string;
  turnIndex: number;
  endpointId: string;
  cacheHit: boolean;
  toolOutput: Record<string, unknown>;
  deduplicatedTool: boolean;
}

export class AgentHarnessRunner {
  private readonly router: CognitiveRouter;
  private readonly sagaGateway: SagaToolGateway;
  private readonly breaker: SyntacticAndVelocityBreaker;

  constructor(
    router: CognitiveRouter,
    sagaGateway: SagaToolGateway,
    breaker = new SyntacticAndVelocityBreaker()
  ) {
    this.router = router;
    this.sagaGateway = sagaGateway;
    this.breaker = breaker;
  }

  public executeSingleTurn(
    task: TaskSpec,
    toolName: string,
    toolArgs: Record<string, unknown>,
    estimatedTokens = 1500
  ): TurnExecutionSummary {
    if (task.current_turn >= task.max_turns) {
      task.state = TaskState.DEAD_LETTER;
      throw new Error(`Task '${task.task_id}' exceeded max_turns (${task.max_turns})`);
    }
    task.state = TaskState.RUNNING_THINK;
    const prefixHash =
      task.prefix_Cache_hash ||
      CognitiveRouter.computePrefixHash("system:agent-v2", "tools:default", task.prompt);
    task.prefix_Cache_hash = prefixHash;

    const [endpoint, cacheHit] = this.router.selectEndpoint(prefixHash, estimatedTokens, task.priority);
    try {
      const nextTurn = task.current_turn + 1;
      const argsHash = SagaToolGateway.generateIdempotencyKey(task.task_id, nextTurn, toolName, toolArgs);
      const check = this.breaker.recordAndCheckTurn(task.task_id, {
        turnIndex: nextTurn,
        toolName,
        argsHash,
        tokensConsumed: estimatedTokens,
        timestampMs: Date.now(),
      });
      if (check.tripped) {
        task.state = TaskState.SUSPENDED_APPROVAL;
        throw new Error(`Velocity breaker tripped for task '${task.task_id}': ${check.reason}`);
      }

      task.state = TaskState.RUNNING_ACT;
      const [toolOutput, deduplicated] = this.sagaGateway.invokeTool(
        task.task_id,
        nextTurn,
        toolName,
        toolArgs
      );
      task.current_turn = nextTurn;
      return {
        taskId: task.task_id,
        turnIndex: nextTurn,
        endpointId: endpoint.endpoint_id,
        cacheHit,
        toolOutput,
        deduplicatedTool: deduplicated,
      };
    } finally {
      this.router.releaseEndpoint(endpoint.endpoint_id);
    }
  }
}
