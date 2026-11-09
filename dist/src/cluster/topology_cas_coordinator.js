"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TopologyCasCoordinator = void 0; /**
 * @fileoverview Cluster Topology Compare-And-Swap (CAS) Membership Coordinator.
 */
class TopologyCasCoordinator {
  generation = 0;
  members = [];

  updateTopologyCas(expectedGeneration, nextMembers)


  {
    if (expectedGeneration !== this.generation) {
      throw new Error(`Topology CAS conflict: expected gen=${expectedGeneration}, actual=${this.generation}`);
    }
    this.generation += 1;
    this.members = [...nextMembers];
    return { generation: this.generation, members: [...this.members] };
  }
}exports.TopologyCasCoordinator = TopologyCasCoordinator;