"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SagaToolGateway = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));
var _models = require("./models.js");
var _compensator = require("./saga/compensator.js");
var _staging = require("./saga/staging.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M3 TASK-11: Unified Idempotent Tool Proxy & Saga Transaction Gateway.
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

    if (effectClass === _models.ToolCallEffectClass.COMPENSATABLE && compensateHandler) {
      this.compensator.recordForwardStep({
        stepId: idemKey.slice(0, 16),
        taskId,
        turnIndex,
        toolName,
        forwardPayload: { args, result },
        compensateHandler
      });
    }

    return [result, false];
  }
}exports.SagaToolGateway = SagaToolGateway;