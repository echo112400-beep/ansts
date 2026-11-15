"use strict";


var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));
var child_process = _interopRequireWildcard(require("node:child_process"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M94: Git Branch Merge (`main` a9ccc0e + `feat/celld-v25` M16-M93) & Unified Contract Verification Suite
 */












(0, _nodeTest.describe)("M94 Git Branch Merge & Unified Contract Suite", () => {
  (0, _nodeTest.it)("verifies unified Git ancestry of main (a9ccc0e) and feat/celld-v25 (a9235d8) and preserves all architecture & console docs", () => {
    const requiredDocs = [
    "docs/agent_sandbox_scheduler_dashboard.html",
    "docs/ansts_production_deployment_architecture_design.md",
    "docs/ansts_v2_implementation_backlog.md",
    "docs/architecture_design_doc.md",
    "docs/celld_persistent_recoverable_ha_scheduler_design.md",
    "docs/microservices_architecture.md",
    "docs/index.html",
    "docs/ansts_console_management.html",
    "docs/console-management-design.md",
    "docs/ansts_full_improvements_and_bugfixes_report.md",
    "docs/google_engineering_implementation_report.md"];

    for (const rel of requiredDocs) {
      _strict.default.equal(
      fs.existsSync(path.resolve(process.cwd(), rel)),
      true,
      `Expected merged repository to contain ${rel}`);

    }

    // Verify both original main (a9ccc0e) and M93 (a9235d8) exist in the Git object store
    const revA = child_process.execSync("git rev-parse --verify a9ccc0e", { encoding: "utf-8" }).trim();
    const revB = child_process.execSync("git rev-parse --verify a9235d8", { encoding: "utf-8" }).trim();
    _strict.default.ok(revA.startsWith("a9ccc0e"));
    _strict.default.ok(revB.startsWith("a9235d8"));
  });

  (0, _nodeTest.it)("verifies unified M1-M15 and M16-M94 exported classes, aliases, and binary framing helpers", () => {
    _strict.default.equal(_index.AgentTask, _index.TaskSpec);
    _strict.default.equal(_index.ToolEffectClass, _index.ToolCallEffectClass);
    _strict.default.equal(_index.BayesianTaskProfiler, _index.TaskResourceProfiler);
    _strict.default.equal(_index.MultiTenantQueueManager, _index.HierarchicalQueueManager);
    _strict.default.equal(_index.EFFORT_MULTIPLIERS.high, 1.6);

    const task = new _index.AgentTask({
      task_id: "merged-task-01",
      tenant_id: "Northstar",
      priority: 0,
      prompt: "Verify merged AgentTask alias"
    });
    const qm = new _index.MultiTenantQueueManager();
    qm.enqueue(task);
    _strict.default.equal(qm.totalQueued(), 1);
    const popped = qm.popNext();
    _strict.default.equal(popped?.task_id, "merged-task-01");

    const profiler = new _index.BayesianTaskProfiler();
    profiler.recordExecutionMetrics(task, 2.0, 2048);
    _strict.default.equal(profiler.listTenantProfiles().length, 1);

    const frame1 = (0, _index.encodeLengthPrefixedCrcFrame)({ requestId: "req-1", ok: true });
    const frame2 = (0, _index.encodeLengthPrefixedCrcFrame)({ requestId: "req-2", count: 42 });
    const decoded = (0, _index.decodeLengthPrefixedCrcFrames)(Buffer.concat([frame1, frame2]));
    _strict.default.equal(decoded.frames.length, 2);
    _strict.default.equal(decoded.frames[0].requestId, "req-1");
    _strict.default.equal(decoded.frames[1].count, 42);
    _strict.default.equal(decoded.remaining.length, 0);
    _strict.default.ok((0, _index.computeCrc32)(Buffer.from("ansts-v25")) > 0);
  });
});