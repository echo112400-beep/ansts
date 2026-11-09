"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview feat/celld-v25 Scheduler Authority & 256-Slot HaSchedulerShardCoordinator Suite.
 */(0, _nodeTest.describe)("feat/celld-v25 Scheduler Shard Authority Suite", () => {
  (0, _nodeTest.it)("Maps tasks across 256 virtual slots to registered celld-brain-01..03 nodes with monotonic epochs", () => {
    const coord = new _index.HaSchedulerShardCoordinator(256, 8);
    assert.equal(coord.registerBrainShardNode("brain-node-01"), 1);
    assert.equal(coord.registerBrainShardNode("brain-node-02"), 1);
    assert.equal(coord.registerBrainShardNode("brain-node-03"), 1);

    const res = coord.resolveBrainForTask("tenant-alpha", "task-distributed-001");
    assert.ok(res.slotIndex >= 0 && res.slotIndex < 256);
    assert.ok(["brain-node-01", "brain-node-02", "brain-node-03"].includes(res.brainNodeId));
    assert.equal(res.epoch, 1);
  });
});