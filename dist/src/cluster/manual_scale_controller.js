"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.VirtualHashSlotRingManager = exports.ManualFleetScaleController = exports.CelldStateAwareFleetAutoscaler = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M13 TASK-62 & M53-4: Non-K8s Bare-Metal/VM Systemd & Docker Compose Manual Fleet Scale-Out/Scale-In Controller
 * with Drain-Token Lease TTL Auto-Expiration to prevent rolling-shutdown deadlocks.
 */ /**
 * Consistent Hash Virtual Slot Ring Manager (`VirtualHashSlotRingManager`) for Non-K8s Bare-Metal Manual Scale-Out/In.
 * Maps keys onto a ring of `totalVirtualSlots` (default 1024) with configurable virtual nodes per physical node.
 */class VirtualHashSlotRingManager {
  totalVirtualSlots;
  vnodesPerNode;
  ring = [];
  activeNodeIds = new Set();

  constructor(totalVirtualSlots = 1024, vnodesPerNode = 16) {
    this.totalVirtualSlots = totalVirtualSlots;
    this.vnodesPerNode = vnodesPerNode;
  }

  static hashString(input) {
    return crypto.createHash("sha256").update(input, "utf-8").digest().readUInt32BE(0);
  }

  addNode(nodeId) {
    if (this.activeNodeIds.has(nodeId)) return;
    this.activeNodeIds.add(nodeId);
    for (let v = 0; v < this.vnodesPerNode; v++) {
      const tokenHash = VirtualHashSlotRingManager.hashString(`${nodeId}#vnode-${v}`);
      this.ring.push({ tokenHash, nodeId });
    }
    this.ring.sort((a, b) => a.tokenHash - b.tokenHash);
  }

  removeNode(nodeId) {
    if (!this.activeNodeIds.has(nodeId)) return;
    this.activeNodeIds.delete(nodeId);
    for (let i = this.ring.length - 1; i >= 0; i--) {
      if (this.ring[i].nodeId === nodeId) {
        this.ring.splice(i, 1);
      }
    }
  }

  getOwnerNodeForSlot(slotIndex) {
    if (this.ring.length === 0) return null;
    const slotHash = VirtualHashSlotRingManager.hashString(`slot:${slotIndex % this.totalVirtualSlots}`);
    let low = 0;
    let high = this.ring.length - 1;
    while (low <= high) {
      const mid = low + high >> 1;
      if (this.ring[mid].tokenHash >= slotHash) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    const chosenIdx = low < this.ring.length ? low : 0;
    return this.ring[chosenIdx].nodeId;
  }

  getOwnerNodeForKey(key) {
    const slotIndex = VirtualHashSlotRingManager.hashString(key) % this.totalVirtualSlots;
    return {
      slotIndex,
      nodeId: this.getOwnerNodeForSlot(slotIndex)
    };
  }

  computeSlotDistribution() {
    const counts = new Map();
    for (const n of this.activeNodeIds) counts.set(n, 0);
    if (this.ring.length === 0) return counts;
    for (let s = 0; s < this.totalVirtualSlots; s++) {
      const owner = this.getOwnerNodeForSlot(s);
      counts.set(owner, (counts.get(owner) ?? 0) + 1);
    }
    return counts;
  }
}exports.VirtualHashSlotRingManager = VirtualHashSlotRingManager;












class ManualFleetScaleController {
  nodes = new Map();
  slotRing = new VirtualHashSlotRingManager(1024, 16);

  registerBareMetalNode(node) {
    const full = {
      ...node,
      state: "ACTIVE",
      runningCells: node.runningCells ? [...node.runningCells] : []
    };
    this.nodes.set(node.nodeId, full);
    this.slotRing.addNode(node.nodeId);
    return full;
  }

  scaleOutNode(nodeId, ipAddress, role = "worker")


  {
    const newNode = this.registerBareMetalNode({
      nodeId,
      ipAddress,
      role,
      systemdUnit: role === "control-plane" ? "celld.service" : "ansts-node-daemon.service"
    });
    const rebalancedCells = [];
    for (const existing of this.nodes.values()) {
      if (existing.nodeId !== nodeId && existing.state === "ACTIVE" && existing.runningCells.length > 2) {
        const moved = existing.runningCells.pop();
        newNode.runningCells.push(moved);
        rebalancedCells.push(moved);
      }
    }
    return { node: newNode, rebalancedCells };
  }

  cordonAndDrainNode(nodeId)


  {
    const source = this.nodes.get(nodeId);
    if (!source) {
      throw new Error(`Node '${nodeId}' not found in ManualFleetScaleController`);
    }
    source.state = "CORDONED";
    const activeTargets = Array.from(this.nodes.values()).filter(
    (n) => n.nodeId !== nodeId && n.state === "ACTIVE" && n.role === source.role);

    if (activeTargets.length === 0 && source.runningCells.length > 0) {
      throw new Error(`Cannot drain node '${nodeId}': no other ACTIVE nodes with role '${source.role}'`);
    }

    source.state = "DRAINING";
    const drainedCells = [];
    let idx = 0;
    while (source.runningCells.length > 0) {
      const cellId = source.runningCells.shift();
      const target = activeTargets[idx % activeTargets.length];
      target.runningCells.push(cellId);
      drainedCells.push({ cellId, targetNodeId: target.nodeId });
      idx += 1;
    }
    source.state = "OFFLINE";
    this.slotRing.removeNode(nodeId);
    return { drainedCells, finalState: source.state };
  }

  listNodes() {
    return Array.from(this.nodes.values());
  }
}

/**
 * M16 TASK-78 & M53-4: Real-Time `GET /state` & `fleet/capacity-v1.json` Autoscaler Evaluator
 * and Paced Drain-Token Shutdown Coordinator (`CELLD_SHUTDOWN_TOTAL_MS=40000`, `handoff=preserve`)
 * with Lease-TTL Auto-Reclamation if a draining node crashes mid-shutdown.
 */exports.ManualFleetScaleController = ManualFleetScaleController;














class CelldStateAwareFleetAutoscaler {
  activeDrainTokenHolder = null;
  drainTokenAcquiredAtMs = null;
  shutdownTotalMs;
  drainTokenWaitMs;
  handoffNoProgressMs;

  constructor(shutdownTotalMs = 40_000) {
    this.shutdownTotalMs = shutdownTotalMs;
    this.drainTokenWaitMs = Math.floor(shutdownTotalMs * 3 / 4);
    this.handoffNoProgressMs = Math.floor(shutdownTotalMs * 5 / 8);
  }

  evaluateFleetStateSamples(samples)




  {
    const anyRestoring = samples.some((s) => s.restoring > 0);
    const isolateMaintenanceWarning = samples.some(
    (s) => (s.live_empty_isolates_sec ?? 0) > 30 || (s.retiring_isolates_sec ?? 0) > 30);


    if (samples.some((s) => s.capacity_waiting > 0 || s.pressured)) {
      return {
        decision: "ADD_NODE",
        rollingUpgradeNextNodeAllowed: !anyRestoring,
        isolateMaintenanceWarning,
        reason: "Positive capacity_waiting or pressured node lease detected in /state"
      };
    }

    const allHaveHeadroomAndSmallBacklog =
    samples.length > 1 && samples.every((s) => s.memory_headroom && s.restoring <= 1);
    if (allHaveHeadroomAndSmallBacklog) {
      return {
        decision: "SCALE_DOWN_SAFE",
        rollingUpgradeNextNodeAllowed: !anyRestoring,
        isolateMaintenanceWarning,
        reason: "All survivor nodes report memory_headroom=true and low restoring backlog"
      };
    }

    return {
      decision: "HOLD_STEADY",
      rollingUpgradeNextNodeAllowed: !anyRestoring,
      isolateMaintenanceWarning,
      reason: "Fleet balanced within normal residency and memory watermarks"
    };
  }

  initiateNodeShutdown(params)











  {
    if (params.systemdTimeoutStopSec <= this.shutdownTotalMs / 1000) {
      throw new Error(
      `systemd TimeoutStopSec (${params.systemdTimeoutStopSec}s) must exceed CELLD_SHUTDOWN_TOTAL_MS (${this.shutdownTotalMs / 1000}s)`);

    }
    const nowMs = params.nowMs ?? Date.now();
    let expiredStaleHolder = null;
    // [M53-4] Auto-expire drain token if previous holder crashed mid-shutdown and exceeded shutdownTotalMs TTL
    if (
    this.activeDrainTokenHolder !== null &&
    this.activeDrainTokenHolder !== params.nodeId &&
    this.drainTokenAcquiredAtMs !== null &&
    nowMs - this.drainTokenAcquiredAtMs >= this.shutdownTotalMs)
    {
      expiredStaleHolder = this.activeDrainTokenHolder;
      this.activeDrainTokenHolder = null;
      this.drainTokenAcquiredAtMs = null;
    }

    const claimedDrainToken =
    this.activeDrainTokenHolder === null || this.activeDrainTokenHolder === params.nodeId;
    if (claimedDrainToken) {
      this.activeDrainTokenHolder = params.nodeId;
      this.drainTokenAcquiredAtMs = nowMs;
    }
    return {
      claimedDrainToken,
      expiredStaleHolder,
      ownershipPreservedForSameNodeRestart: params.handoffMode === "preserve",
      maxConcurrentReleases: 128,
      drainTokenWaitMs: this.drainTokenWaitMs,
      handoffNoProgressMs: this.handoffNoProgressMs
    };
  }

  releaseDrainToken(nodeId) {
    if (this.activeDrainTokenHolder === nodeId) {
      this.activeDrainTokenHolder = null;
      this.drainTokenAcquiredAtMs = null;
    }
  }
}exports.CelldStateAwareFleetAutoscaler = CelldStateAwareFleetAutoscaler;