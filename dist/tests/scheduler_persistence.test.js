"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview feat/celld-v25 Single-Write-Path Scheduler Persistence & Queue Deduplication Suite (`b566a53`).
 */




(0, _nodeTest.describe)("feat/celld-v25 Single-Write-Path Scheduler Persistence Suite", () => {
  (0, _nodeTest.it)("Commits tasks via single write path and deduplicates repeated idempotency keys", () => {
    const sql = new _index.InMemoryCellSqlStorage();
    const store = new _index.SchedulerV25SingleWritePathStore(sql);

    const task = new _index.TaskSpec({
      task_id: "v25-task-001",
      tenant_id: "tenant-alpha",
      priority: _index.TaskPriority.P1_HIGH,
      idempotency_key: "idem-v25-001",
      prompt: "Verify v2.5 single write path"
    });

    const first = store.commitTaskSingleWritePath(task);
    const second = store.commitTaskSingleWritePath(task);
    assert.equal(first.deduplicated, false);
    assert.equal(second.deduplicated, true);
    assert.equal(store.listPersistedTasks().length, 1);
  });
});