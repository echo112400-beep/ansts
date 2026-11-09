"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview feat/celld-v25 M24 & M25 SLSA Attestor, UFFD Lazy Restorer, Deterministic Envelope & Distributed GC Sweeper Suite.
 */




(0, _nodeTest.describe)("feat/celld-v25 M24 & M25 SLSA, UFFD, Determinism & CAS GC Suite", () => {
  (0, _nodeTest.it)("1. SlsaProvenanceAttestor signs SLSA v1.0 provenance statement", () => {
    const slsa = new _index.SlsaProvenanceAttestor();
    const stmt = slsa.createSignedProvenanceStatement({
      imageReference: "ghcr.io/ansts/sandbox:v2.5",
      sha256Digest: "sha256:abcdef1234567890",
      builderId: "https://github.com/echo112400-beep/ansts/actions"
    });
    assert.equal(stmt.predicateType, "https://slsa.dev/provenance/v1");
    assert.equal(stmt.signatureHex.length, 64);
  });

  (0, _nodeTest.it)("2. UffdLazyPageFaultRestorer & DeterministicExecutionEnvelope provide reproducible execution", () => {
    const uffd = new _index.UffdLazyPageFaultRestorer();
    assert.equal(uffd.handlePageFault(42).fetchedFromRemote, true);
    assert.equal(uffd.handlePageFault(42).fetchedFromRemote, false);

    const env1 = new _index.DeterministicExecutionEnvelope(12345);
    const env2 = new _index.DeterministicExecutionEnvelope(12345);
    assert.equal(env1.nextDeterministicRandom(), env2.nextDeterministicRandom());
  });

  (0, _nodeTest.it)("3. DistributedCasGcSweeper retains active commit root blobs and sweeps unreferenced blobs", () => {
    const gc = new _index.DistributedCasGcSweeper();
    const res = gc.sweepUnreferencedBlobs({
      allBlobIds: ["b1", "b2", "b3", "b4"],
      activeCommitRoots: new Map([["C1", ["b1", "b3"]]])
    });
    assert.deepEqual(res.retainedBlobIds, ["b1", "b3"]);
    assert.deepEqual(res.sweptBlobIds, ["b2", "b4"]);
  });
});