"use strict";





var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var http = _interopRequireWildcard(require("node:http"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M37 Next-Batch Deep Optimization E2E Suite:
 * 1. [M37-1] Geo-Sovereignty Admission Routing (`200` lowest-latency vs `451` Data Sovereignty Lock) & Cross-Region S3 LTX Replication
 * 2. [M37-2] Live Host Preflight Readiness Probe (`statfsSync`, `os.freemem`, `/dev/shm`) & Automated Network Partition + Split-Brain Fencing Drill
 * 3. [M37-3] Deterministic Execution Envelope (`xorshift32` PRNG + Virtual Clock) & Parallel CoW Speculative Branch Forking (`POST /v1beta1/runtime/deterministic-fork`)
 */(0, _nodeTest.describe)("M37 Next-Batch Deep Optimization E2E Suite (Geo-Sovereignty & S3 Replication, Chaos Drills & Deterministic CoW Forking)", () => {(0, _nodeTest.it)("1. [M37-1] Enforces GDPR Geo-Sovereignty routing (200 vs 451 legal lock) and replicates LTX objects to DR S3 region", async () => {const s3Objects = new Map();const s3Server = http.createServer((req, res) => {
      const u = new URL(req.url ?? "/", "http://127.0.0.1");
      if (req.method === "PUT") {
        const chunks = [];
        req.on("data", (c) => chunks.push(c));
        req.on("end", () => {
          s3Objects.set(u.pathname, Buffer.concat(chunks));
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ stored: true }));
        });
        return;
      }
      res.writeHead(404).end();
    });
    const s3Port = await new Promise((resolve) => {
      s3Server.listen(0, "127.0.0.1", () => {
        const addr = s3Server.address();
        resolve(typeof addr === "object" && addr ? addr.port : 0);
      });
    });
    const s3Endpoint = `http://127.0.0.1:${s3Port}`;

    const gw = new _index.MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      s3Endpoint
    });
    const { apiPort } = await gw.start(0, 0);

    try {
      // 1. Route GDPR_EU request and replicate LTX object to eu-west-1 DR region
      const res1 = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/cluster/geo-route`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          jurisdiction: "GDPR_EU",
          replicateObjectKey: "ltx-gdpr-001",
          drTargetRegion: "eu-west-1",
          payload: { tenantId: "eu-bank-01" }
        })
      });
      assert.equal(res1.status, 200);
      const json1 = await res1.json();



      assert.equal(json1.selectedRegion.regionId, "eu-central-1");
      assert.equal(json1.selectedRegion.jurisdiction, "GDPR_EU");
      assert.equal(json1.replication.s3Stored, true);
      assert.equal(json1.replication.replicatedCount, 1);

      // 2. Mark both GDPR_EU regions unavailable -> must return HTTP 451 DATA_SOVEREIGNTY_LOCK (never leak to US/APAC!)
      await fetch(`http://127.0.0.1:${apiPort}/v1beta1/cluster/geo-route`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "setRegionAvailability",
          regionId: "eu-central-1",
          capacityAvailable: false
        })
      });
      await fetch(`http://127.0.0.1:${apiPort}/v1beta1/cluster/geo-route`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "setRegionAvailability",
          regionId: "eu-west-1",
          capacityAvailable: false
        })
      });

      const resLock = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/cluster/geo-route`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ jurisdiction: "GDPR_EU" })
      });
      assert.equal(resLock.status, 451);
      const lockJson = await resLock.json();
      assert.equal(lockJson.error, "DATA_SOVEREIGNTY_LOCK");
    } finally {
      await gw.stop();
      await new Promise((resolve) => s3Server.close(() => resolve()));
    }
  });

  (0, _nodeTest.it)("2. [M37-2 & M37-3] Executes live Host Preflight + Split-Brain Chaos Drill and verifies Deterministic Speculative CoW Branch Forking", async () => {
    const srv = new _index.MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "body-det-01",
      nodeRole: "body"
    });
    const { apiPort } = await srv.start(0, 0);

    try {
      // 1. Run live Chaos Drill & Host Preflight Gate
      const drillRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/cluster/chaos-drill`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          targetHost: "body-host-02",
          oldNodeFencingToken: 7,
          newNodeFencingToken: 8
        })
      });
      assert.equal(drillRes.status, 200);
      const drillJson = await drillRes.json();



      assert.equal(drillJson.preflight.ready, true);
      assert.ok(drillJson.preflight.freeMemoryMb > 0);
      assert.equal(drillJson.drill.partitionBlockedTraffic, true);
      assert.equal(drillJson.drill.partitionHealed, true);
      assert.equal(drillJson.drill.rpoZeroVerified, true);

      // 2. Run Deterministic Speculative CoW Branches with identical seed (777) vs different seed (888)
      const detRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/runtime/deterministic-fork`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sourceBranch: "main",
          branches: [
          {
            branchName: "spec-run-a",
            seed: 777,
            command: "printf '%s:%s' \"$ANSTS_VIRTUAL_CLOCK_MS\" \"$ANSTS_DET_RAND\" | tee det_out.txt"
          },
          {
            branchName: "spec-run-b",
            seed: 777,
            command: "printf '%s:%s' \"$ANSTS_VIRTUAL_CLOCK_MS\" \"$ANSTS_DET_RAND\" | tee det_out.txt"
          },
          {
            branchName: "spec-run-c",
            seed: 888,
            command: "printf '%s:%s' \"$ANSTS_VIRTUAL_CLOCK_MS\" \"$ANSTS_DET_RAND\" | tee det_out.txt"
          }]

        })
      });
      assert.equal(detRes.status, 200);
      const detJson = await detRes.json();


      assert.equal(detJson.results.length, 3);
      // Same seed (777) produces bit-identical output across independent CoW branches!
      assert.equal(detJson.results[0].stdout, detJson.results[1].stdout);
      assert.notEqual(detJson.results[0].stdout, detJson.results[2].stdout);
    } finally {
      await srv.stop();
    }
  });
});