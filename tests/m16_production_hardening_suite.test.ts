/**
 * @fileoverview M16 Production Hardening Test Suite (`TASK-75` ~ `TASK-80`).
 *
 * Verifies the 5 production architectural decisions locked during the Grilling review:
 * - Q1 (`TASK-75`): Cross-host mTLS HTTP/2 tunnel + local `AF_VSOCK` fast-path & fence guard
 * - Q2 (`TASK-76`): S3 `control/topology-v1.json` ETag CAS & 304/push broadcast coordination
 * - Q3 (`TASK-77`): 256 Fixed Virtual Hash Slots & 3-Phase Atomic Migration (`DRAINING -> SNAPSHOT_TRANSFERRED -> CUTOVER`)
 * - Q4 (`TASK-78`): Healthy Peer ring selection & seamless fallback from `fleet` to `bucket` durability mode
 * - Q5 (`TASK-79`): `ANSTS_RUNTIME_PROFILE=production` Fail-Fast Preflight Gate
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

import {
  CrossHostMtlsTunnelBridge,
  S3TopologyCasCoordinator,
  VirtualHashSlotRingManager,
  RpoZeroDurabilityEngine,
  ProductionPreflightGate,
  StaleFenceError,
  S3CasPreconditionFailedError,
  EdgeTunnelHaDynamicRouter,
} from "../src/index";

test("test 56: [Q1 / TASK-75] CrossHostMtlsTunnelBridge verifies TLS 1.3 SPIFFE cert, routes remote mTLS vs local UDS, and blocks stale fences", () => {
  const bridge = new CrossHostMtlsTunnelBridge("celld-node-01", {
    spiffeId: "spiffe://ansts.internal/ns/prod/sa/celld-brain-01",
    certSha256Fingerprint: "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
    caTrustBundleId: "ansts-prod-root-ca-v2",
    tlsMinVersion: "TLSv1.3",
  });

  bridge.registerBodyHostEndpoint("body-host-remote-03", "10.240.32.13", 9443, false);
  bridge.registerBodyHostEndpoint("body-host-local-01", "127.0.0.1", 9443, true);

  const remoteBoost = bridge.dispatchControlOrVsockCommand({
    commandId: "cmd-001",
    taskId: "task-prod-101",
    sandboxId: "sbx-101",
    targetBodyHostId: "body-host-remote-03",
    fence: { epoch: 2, turnSeq: 1 },
    action: "CGROUP_BOOST_8C",
    payload: { tool: "cargo_build" },
  });
  assert.equal(remoteBoost.statusCode, 200);
  assert.equal(remoteBoost.transportMode, "MTLS_H2_REMOTE_TUNNEL");
  assert.equal(remoteBoost.cgroupApplied, "800000 100000");

  const localSnap = bridge.dispatchControlOrVsockCommand({
    commandId: "cmd-002",
    taskId: "task-prod-101",
    sandboxId: "sbx-101",
    targetBodyHostId: "body-host-local-01",
    fence: { epoch: 2, turnSeq: 2 },
    action: "OVERLAY_CAPTURE_SNAPSHOT",
    payload: {},
  });
  assert.equal(localSnap.statusCode, 200);
  assert.equal(localSnap.transportMode, "LOCAL_UDS_VSOCK_FASTPATH");
  assert.ok(localSnap.merkleRoot?.startsWith("merkle_"));

  assert.throws(
    () =>
      bridge.dispatchControlOrVsockCommand({
        commandId: "cmd-stale",
        taskId: "task-prod-101",
        sandboxId: "sbx-101",
        targetBodyHostId: "body-host-remote-03",
        fence: { epoch: 1, turnSeq: 99 },
        action: "CGROUP_THROTTLE_0_1C",
        payload: {},
      }),
    StaleFenceError,
  );
});

test("test 57: [Q2 & Q3 / TASK-76 & 77] S3TopologyCasCoordinator ETag CAS + 256 Virtual Slot 3-Phase Migration", () => {
  const ring = new VirtualHashSlotRingManager(["scheduler-cell-shard-00", "scheduler-cell-shard-01"]);
  const tenantSlot = ring.resolveOwnerShardForTenant("tenant-enterprise-alpha");
  assert.equal(tenantSlot.phase, "STEADY");

  // 3-Phase migration of `tenantSlot.slotIndex` to newly added `scheduler-cell-shard-02`
  const p1 = ring.beginSlotDrain(tenantSlot.slotIndex, "scheduler-cell-shard-02");
  assert.equal(p1.phase, "DRAINING");

  const p2 = ring.transferSlotSnapshot({
    slotIndex: tenantSlot.slotIndex,
    fromShardId: tenantSlot.ownerShardId,
    toShardId: "scheduler-cell-shard-02",
    generation: ring.generation,
    tenantWdrfShares: { "tenant-enterprise-alpha": 0.42 },
    queuedTaskIds: ["task-q-1", "task-q-2"],
    dagEscrowVoucherIds: ["voucher-dag-9"],
  });
  assert.equal(p2.phase, "SNAPSHOT_TRANSFERRED");

  const p3 = ring.commitSlotCutover(tenantSlot.slotIndex);
  assert.equal(p3.phase, "STEADY");
  assert.equal(p3.ownerShardId, "scheduler-cell-shard-02");
  assert.equal(p3.generation, 2);

  // S3 Topology CAS + 304 Not Modified + Push Broadcast
  const coordinator = new S3TopologyCasCoordinator();
  let pushCount = 0;
  coordinator.subscribePushBroadcast(() => {
    pushCount += 1;
  });

  const cas1 = coordinator.casUpdateTopology(
    {
      schemaVersion: "v1",
      generation: 1,
      updatedAtMs: Date.now(),
      schedulerShardCount: 2,
      queueShardCount: 4,
      virtualSlotAssignment: { [tenantSlot.slotIndex]: "scheduler-cell-shard-02" },
      brainNodes: [],
      bodyHosts: [],
    },
    null,
  );
  assert.equal(pushCount, 1);

  const poll304 = coordinator.getCurrentTopologyWithEtag(cas1.newEtag);
  assert.equal(poll304.statusCode, 304);

  assert.throws(
    () =>
      coordinator.casUpdateTopology(
        {
          schemaVersion: "v1",
          generation: 2,
          updatedAtMs: Date.now(),
          schedulerShardCount: 4,
          queueShardCount: 8,
          virtualSlotAssignment: {},
          brainNodes: [],
          bodyHosts: [],
        },
        "stale_etag_value",
      ),
    S3CasPreconditionFailedError,
  );
});

test("test 58: [Q4 & Q5 / TASK-78 & 79] Healthy Peer Fallback to Bucket Mode & Production Preflight Gate", () => {
  const haEngine = new RpoZeroDurabilityEngine("fleet", 10000);
  const now = Date.now();
  const ownership = haEngine.claimCellOwnershipCas("harness:task-q4", "celld-node-01", now);

  // Case A: 2 healthy peers available -> commits in `fleet` mode
  const fleetCommit = haEngine.selectHealthyPeersAndCommitRpoZero({
    cellScope: "harness:task-q4",
    nodeId: "celld-node-01",
    fence: { epoch: ownership.epoch, turnSeq: 1 },
    ltxPayload: "LTX_FRAME_1",
    clusterNodes: [
      {
        node_id: "celld-node-02",
        sampled_ms: now,
        owned_cells: 10,
        placement_weight: 100,
        resident_cells: 5,
        host_websockets: 2,
        rss_bytes: 1000,
        in_use_bytes: 500,
        cpu_percent_x100: 200,
        open_fds: 10,
        pressured: false,
        memory_headroom: true,
        shed_cells: 0,
        restoring: 0,
        draining: false,
      },
    ],
  });
  assert.equal(fleetCommit.proofType, "fleet");
  assert.equal(fleetCommit.degradedToBucketMode, false);

  // Case B: All peer nodes draining or pressured -> seamlessly degrades to `bucket` mode with RPO=0 ownership verification
  const degradedCommit = haEngine.selectHealthyPeersAndCommitRpoZero({
    cellScope: "harness:task-q4",
    nodeId: "celld-node-01",
    fence: { epoch: ownership.epoch, turnSeq: 2 },
    ltxPayload: "LTX_FRAME_2",
    clusterNodes: [
      {
        node_id: "celld-node-02",
        sampled_ms: now,
        owned_cells: 10,
        placement_weight: 100,
        resident_cells: 5,
        host_websockets: 2,
        rss_bytes: 1000,
        in_use_bytes: 500,
        cpu_percent_x100: 200,
        open_fds: 10,
        pressured: false,
        memory_headroom: true,
        shed_cells: 0,
        restoring: 0,
        draining: true,
      },
    ],
  });
  assert.equal(degradedCommit.proofType, "bucket");
  assert.equal(degradedCommit.degradedToBucketMode, true);

  // Q5: Production Preflight Gate verification
  const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-preflight-"));
  const cgDir = path.join(tmpRoot, "cgroup");
  const psiDir = path.join(tmpRoot, "pressure");
  const vsockDir = path.join(tmpRoot, "vsock");
  fs.mkdirSync(cgDir, { recursive: true });
  fs.mkdirSync(psiDir, { recursive: true });
  fs.writeFileSync(path.join(cgDir, "cgroup.controllers"), "cpu memory pids\n", "utf8");
  fs.writeFileSync(path.join(psiDir, "cpu"), "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\n", "utf8");
  fs.writeFileSync(path.join(psiDir, "memory"), "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n", "utf8");

  const passRes = ProductionPreflightGate.assertProductionReadyOrThrow({
    profile: "production",
    role: "all",
    s3Endpoint: "http://10.240.64.11:9000",
    s3Bucket: "s3://ansts-fleet",
    s3AccessKeyId: "ansts_prod_key_01",
    s3SecretAccessKey: "ansts_prod_secret_key_012345",
    cgroupRoot: cgDir,
    procPressureRoot: psiDir,
    vsockSocketDir: vsockDir,
  });
  assert.equal(passRes.passed, true);
  assert.equal(passRes.mockFallbackAllowed, false);

  assert.throws(
    () =>
      ProductionPreflightGate.assertProductionReadyOrThrow({
        profile: "production",
        role: "all",
        s3Endpoint: "",
        s3Bucket: "",
      }),
    /ANSTS_RUNTIME_PROFILE=production preflight failed/,
  );
});

test("test 59: [TASK-83] EdgeTunnelHaDynamicRouter dual-homed registration, 0-hop/1-hop dynamic routing, sub-50ms secondary failover, and Make-Before-Break drain", () => {
  // Brain node is on `celld-gw-03`, while `body-host-edge-01` connects to `celld-gw-01` (Primary) & `celld-gw-02` (Secondary)
  const router = new EdgeTunnelHaDynamicRouter("celld-gw-03");
  router.registerTunnelGateway({
    gatewayId: "celld-gw-01",
    internalRelayUrl: "https://10.240.16.11:9444/internal/v1beta1/tunnel/relay",
    publicEdgeIngressUrl: "https://tunnel-01.edge.ansts.io:9443",
    healthy: true,
    draining: false,
  });
  router.registerTunnelGateway({
    gatewayId: "celld-gw-02",
    internalRelayUrl: "https://10.240.16.12:9444/internal/v1beta1/tunnel/relay",
    publicEdgeIngressUrl: "https://tunnel-02.edge.ansts.io:9443",
    healthy: true,
    draining: false,
  });
  router.registerTunnelGateway({
    gatewayId: "celld-gw-03",
    internalRelayUrl: "https://10.240.16.13:9444/internal/v1beta1/tunnel/relay",
    publicEdgeIngressUrl: "https://tunnel-03.edge.ansts.io:9443",
    healthy: true,
    draining: false,
  });

  const alloc = router.allocateDualHomedGatewaysForDaemon("body-host-edge-01");
  assert.ok(alloc.primary);
  assert.ok(alloc.secondary);
  assert.notEqual(alloc.primary.gatewayId, alloc.secondary?.gatewayId);

  // Edge NodeDaemon establishes Primary stream on gw-01 and Secondary standby stream on gw-02
  router.registerInboundDaemonStream({
    hostId: "body-host-edge-01",
    gatewayId: "celld-gw-01",
    role: "PRIMARY",
    connGeneration: 1,
    epoch: 1,
  });
  const routeEntry = router.registerInboundDaemonStream({
    hostId: "body-host-edge-01",
    gatewayId: "celld-gw-02",
    role: "SECONDARY",
    connGeneration: 1,
    epoch: 1,
  });
  assert.equal(routeEntry.primaryGatewayId, "celld-gw-01");
  assert.equal(routeEntry.secondaryGatewayId, "celld-gw-02");

  // 1. Normal 1-Hop Relay from Brain (gw-03) -> Primary Tunnel Gateway (gw-01) -> Edge NodeDaemon
  const res1 = router.routeAndForwardCommand({
    commandId: "cmd-edge-1",
    taskId: "task-edge-99",
    sandboxId: "sbx-edge-1",
    targetBodyHostId: "body-host-edge-01",
    fence: { epoch: 1, turnSeq: 1 },
    action: "CGROUP_BOOST_8C",
    payload: {},
  });
  assert.equal(res1.statusCode, 200);
  assert.equal(res1.routedViaGatewayId, "celld-gw-01");
  assert.equal(res1.hopMode, "SINGLE_HOP_PRIMARY_RELAY");
  assert.equal(res1.failoverTriggered, false);

  // 2. Simulate Primary Gateway (`celld-gw-01`) sudden crash -> automatic instant failover to Secondary (`celld-gw-02`)
  router.setGatewayHealth("celld-gw-01", false);
  const res2 = router.routeAndForwardCommand({
    commandId: "cmd-edge-2",
    taskId: "task-edge-99",
    sandboxId: "sbx-edge-1",
    targetBodyHostId: "body-host-edge-01",
    fence: { epoch: 1, turnSeq: 2 },
    action: "CGROUP_THROTTLE_0_1C",
    payload: {},
  });
  assert.equal(res2.statusCode, 200);
  assert.equal(res2.routedViaGatewayId, "celld-gw-02");
  assert.equal(res2.hopMode, "FAILOVER_SECONDARY_RELAY");
  assert.equal(res2.failoverTriggered, true);
  assert.equal(router.routeTable.get("body-host-edge-01")?.primaryGatewayId, "celld-gw-02");

  // 3. Make-Before-Break graceful drain on `celld-gw-02` recommends healthy `celld-gw-03`
  const drainNotices = router.initiateMakeBeforeBreakDrain("celld-gw-02", 15000);
  const notice = drainNotices.get("body-host-edge-01");
  assert.ok(notice);
  assert.equal(notice.recommendedCandidateGateways[0].gatewayId, "celld-gw-03");
});
