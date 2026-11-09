/**
 * @fileoverview feat/celld-v25 M24 & M25 SLSA Attestor, UFFD Lazy Restorer, Deterministic Envelope & Distributed GC Sweeper Suite.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  SlsaProvenanceAttestor,
  UffdLazyPageFaultRestorer,
  DeterministicExecutionEnvelope,
  DistributedCasGcSweeper,
} from "../src/index.js";

describe("feat/celld-v25 M24 & M25 SLSA, UFFD, Determinism & CAS GC Suite", () => {
  it("1. SlsaProvenanceAttestor signs SLSA v1.0 provenance statement", () => {
    const slsa = new SlsaProvenanceAttestor();
    const stmt = slsa.createSignedProvenanceStatement({
      imageReference: "ghcr.io/ansts/sandbox:v2.5",
      sha256Digest: "sha256:abcdef1234567890",
      builderId: "https://github.com/echo112400-beep/ansts/actions",
    });
    assert.equal(stmt.predicateType, "https://slsa.dev/provenance/v1");
    assert.equal(stmt.signatureHex.length, 64);
  });

  it("2. UffdLazyPageFaultRestorer & DeterministicExecutionEnvelope provide reproducible execution", () => {
    const uffd = new UffdLazyPageFaultRestorer();
    assert.equal(uffd.handlePageFault(42).fetchedFromRemote, true);
    assert.equal(uffd.handlePageFault(42).fetchedFromRemote, false);

    const env1 = new DeterministicExecutionEnvelope(12345);
    const env2 = new DeterministicExecutionEnvelope(12345);
    assert.equal(env1.nextDeterministicRandom(), env2.nextDeterministicRandom());
  });

  it("3. DistributedCasGcSweeper retains active commit root blobs and sweeps unreferenced blobs", () => {
    const gc = new DistributedCasGcSweeper();
    const res = gc.sweepUnreferencedBlobs({
      allBlobIds: ["b1", "b2", "b3", "b4"],
      activeCommitRoots: new Map([["C1", ["b1", "b3"]]]),
    });
    assert.deepEqual(res.retainedBlobIds, ["b1", "b3"]);
    assert.deepEqual(res.sweptBlobIds, ["b2", "b4"]);
  });
});
