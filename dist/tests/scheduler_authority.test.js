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
 * @fileoverview feat/celld-v25 Scheduler Authority & 256-Slot HaSchedulerShardCoordinator Suite.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("feat/celld-v25 Scheduler Shard Authority Suite", () => {
    (0, node_test_1.it)("Maps tasks across 256 virtual slots to registered celld-brain-01..03 nodes with monotonic epochs", () => {
        const coord = new index_js_1.HaSchedulerShardCoordinator(256, 8);
        assert.equal(coord.registerBrainShardNode("brain-node-01"), 1);
        assert.equal(coord.registerBrainShardNode("brain-node-02"), 1);
        assert.equal(coord.registerBrainShardNode("brain-node-03"), 1);
        const res = coord.resolveBrainForTask("tenant-alpha", "task-distributed-001");
        assert.ok(res.slotIndex >= 0 && res.slotIndex < 256);
        assert.ok(["brain-node-01", "brain-node-02", "brain-node-03"].includes(res.brainNodeId));
        assert.equal(res.epoch, 1);
    });
});
