"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SagaToolGateway = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));
var _models = require("./models.js");
var _compensator = require("./saga/compensator.js");
var _staging = require("./saga/staging.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M3 TASK-11 & M53-1: Unified Idempotent Tool Proxy & Saga Transaction Gateway
 * with Post-Rollback Idempotency Cache Invalidation.
 */
class SagaToolGateway {
  executionJournal = new Map();
  toolRegistry = new Map();
  compensator;
  stagingGate;

  constructor(
  compensator = new _compensator.SagaCompensatorEngine(),
  stagingGate = new _staging.StagingCommitAndApprovalGate())
  {
    this.compensator = compensator;
    this.stagingGate = stagingGate;
    this.compensator.setOnStepCompensated((step) => {
      if (step.idempotencyKey) {
        this.executionJournal.delete(step.idempotencyKey);
      }
      this.invalidateTaskTurnsFrom(step.taskId, step.turnIndex);
    });
  }

  registerTool(
  toolName,
  effectClass,
  handler,
  compensateHandler)
  {
    this.toolRegistry.set(toolName, [effectClass, handler, compensateHandler]);
  }

  static generateIdempotencyKey(
  taskId,
  turnIndex,
  toolName,
  canonicalArgs)
  {
    const sortedKeys = Object.keys(canonicalArgs).sort();
    const sortedObj = {};
    for (const k of sortedKeys) {
      sortedObj[k] = canonicalArgs[k];
    }
    const payload = `${taskId}:${turnIndex}:${toolName}:${JSON.stringify(sortedObj)}`;
    return crypto.createHash("sha256").update(payload, "utf-8").digest("hex");
  }

  /**
   * [M53-1] Purges stale idempotency journal records for `taskId` at or after `fromTurnInclusive`
   * when a task is rolled back or time-travel forked.
   */
  invalidateTaskTurnsFrom(taskId, fromTurnInclusive = 0) {
    let invalidated = 0;
    for (const [k, log] of this.executionJournal.entries()) {
      if (log.task_id === taskId && log.turn_index >= fromTurnInclusive) {
        this.executionJournal.delete(k);
        invalidated += 1;
      }
    }
    return invalidated;
  }

  /**
   * [M53-1] Inline tool execution helper that registers or invokes a tool with idempotency caching and Saga registration.
   */
  executeTool(params)







  {
    this.registerTool(
    params.toolName,
    params.effectClass,
    () => params.actionFn(),
    params.compensationFn ?
    () => {
      params.compensationFn();
      return { compensated: true };
    } :
    undefined);

    const [result] = this.invokeTool(params.taskId, params.turnIndex, params.toolName, params.args);
    return result;
  }

  /**
   * [M53-1] Rolls back all Saga steps for `taskId` at or after `upToTurnInclusive` in LIFO order,
   * isolates per-step compensation errors, and invalidates corresponding `executionJournal` idempotency keys.
   */
  rollbackAndInvalidateTask(
  taskId,
  upToTurnInclusive = 0)




  {
    const invalidatedIdemKeys = [];
    for (const [k, log] of this.executionJournal.entries()) {
      if (log.task_id === taskId && log.turn_index >= upToTurnInclusive) {
        invalidatedIdemKeys.push(k);
      }
    }
    const rolledBackSteps = this.compensator.rollbackTask(taskId, upToTurnInclusive);
    this.invalidateTaskTurnsFrom(taskId, upToTurnInclusive);
    const compensationErrors = rolledBackSteps.
    filter((s) => Boolean(s.compensationError)).
    map((s) => ({
      stepId: s.stepId,
      turnIndex: s.turnIndex,
      error: s.compensationError
    }));
    return {
      rolledBackSteps,
      invalidatedIdemKeys,
      compensationErrors
    };
  }

  invokeTool(
  taskId,
  turnIndex,
  toolName,
  args)
  {
    const entry = this.toolRegistry.get(toolName);
    if (!entry) {
      throw new Error(`Tool '${toolName}' is not registered in SagaToolGateway`);
    }
    const [effectClass, handler, compensateHandler] = entry;
    const idemKey = SagaToolGateway.generateIdempotencyKey(taskId, turnIndex, toolName, args);

    if (effectClass !== _models.ToolCallEffectClass.READ_ONLY) {
      const existing = this.executionJournal.get(idemKey);
      if (existing && existing.status === "SUCCESS") {
        return [existing.result_payload, true];
      }
    }

    const result = handler(args);
    const logEntry = {
      idempotency_key: idemKey,
      task_id: taskId,
      turn_index: turnIndex,
      tool_name: toolName,
      effect_class: effectClass,
      status: "SUCCESS",
      result_payload: result,
      compensation_tool: compensateHandler ? `${toolName}_compensate` : undefined,
      executed_at: Date.now() / 1000
    };
    this.executionJournal.set(idemKey, logEntry);

    if (compensateHandler) {
      this.compensator.recordForwardStep({
        stepId: idemKey.slice(0, 16),
        taskId,
        turnIndex,
        toolName,
        idempotencyKey: idemKey,
        forwardPayload: { args, result },
        compensateHandler
      });
    }

    return [result, false];
  }
}exports.SagaToolGateway = SagaToolGateway;