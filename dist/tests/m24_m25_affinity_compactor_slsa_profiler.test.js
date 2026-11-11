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
 * @fileoverview feat/celld-v25 M24 & M25 SLSA Attestor, UFFD Lazy Restorer, Deterministic Envelope & Distributed GC Sweeper Suite.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("feat/celld-v25 M24 & M25 SLSA, UFFD, Determinism & CAS GC Suite", () => {
    (0, node_test_1.it)("1. SlsaProvenanceAttestor signs SLSA v1.0 provenance statement", () => {
        const slsa = new index_js_1.SlsaProvenanceAttestor();
        const stmt = slsa.createSignedProvenanceStatement({
            imageReference: "ghcr.io/ansts/sandbox:v2.5",
            sha256Digest: "sha256:abcdef1234567890",
            builderId: "https://github.com/echo112400-beep/ansts/actions",
        });
        assert.equal(stmt.predicateType, "https://slsa.dev/provenance/v1");
        assert.equal(stmt.signatureHex.length, 64);
    });
    (0, node_test_1.it)("2. UffdLazyPageFaultRestorer & DeterministicExecutionEnvelope provide reproducible execution", () => {
        const uffd = new index_js_1.UffdLazyPageFaultRestorer();
        assert.equal(uffd.handlePageFault(42).fetchedFromRemote, true);
        assert.equal(uffd.handlePageFault(42).fetchedFromRemote, false);
        const env1 = new index_js_1.DeterministicExecutionEnvelope(12345);
        const env2 = new index_js_1.DeterministicExecutionEnvelope(12345);
        assert.equal(env1.nextDeterministicRandom(), env2.nextDeterministicRandom());
    });
    (0, node_test_1.it)("3. DistributedCasGcSweeper retains active commit root blobs and sweeps unreferenced blobs", () => {
        const gc = new index_js_1.DistributedCasGcSweeper();
        const res = gc.sweepUnreferencedBlobs({
            allBlobIds: ["b1", "b2", "b3", "b4"],
            activeCommitRoots: new Map([["C1", ["b1", "b3"]]]),
        });
        assert.deepEqual(res.retainedBlobIds, ["b1", "b3"]);
        assert.deepEqual(res.sweptBlobIds, ["b2", "b4"]);
    });
});
