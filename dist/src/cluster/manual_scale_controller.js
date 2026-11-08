"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ManualFleetScaleController = void 0; /**
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
}exports.ManualFleetScaleController = ManualFleetScaleController;