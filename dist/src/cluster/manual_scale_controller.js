"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ManualFleetScaleController = exports.CelldStateAwareFleetAutoscaler = void 0; /**
 * @fileoverview M13 TASK-62: Non-K8s Bare-Metal/VM Systemd & Docker Compose Manual Fleet Scale-Out/Scale-In Controller.
 */











class ManualFleetScaleController {
  nodes = new Map();

  registerBareMetalNode(node) {
    const full = {
      ...node,
      state: "ACTIVE",
      runningCells: node.runningCells ? [...node.runningCells] : []
    };
    this.nodes.set(node.nodeId, full);
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
    return { drainedCells, finalState: source.state };
  }

  listNodes() {
    return Array.from(this.nodes.values());
  }
}


/**
 * M16 TASK-78: Real-Time `GET /state` & `fleet/capacity-v1.json` Autoscaler Evaluator
 * and Paced Drain-Token Shutdown Coordinator (`CELLD_SHUTDOWN_TOTAL_MS=40000`, `handoff=preserve`).
 */exports.ManualFleetScaleController = ManualFleetScaleController;














class CelldStateAwareFleetAutoscaler {
  activeDrainTokenHolder = null;
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
    const claimedDrainToken = this.activeDrainTokenHolder === null || this.activeDrainTokenHolder === params.nodeId;
    if (claimedDrainToken) {
      this.activeDrainTokenHolder = params.nodeId;
    }
    return {
      claimedDrainToken,
      ownershipPreservedForSameNodeRestart: params.handoffMode === "preserve",
      maxConcurrentReleases: 128,
      drainTokenWaitMs: this.drainTokenWaitMs,
      handoffNoProgressMs: this.handoffNoProgressMs
    };
  }

  releaseDrainToken(nodeId) {
    if (this.activeDrainTokenHolder === nodeId) {
      this.activeDrainTokenHolder = null;
    }
  }
}exports.CelldStateAwareFleetAutoscaler = CelldStateAwareFleetAutoscaler;