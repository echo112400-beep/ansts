/**
 * @fileoverview Cluster Topology Compare-And-Swap (CAS) Membership Coordinator.
 */
export class TopologyCasCoordinator {
  private generation = 0;
  private members: string[] = [];

  public updateTopologyCas(expectedGeneration: number, nextMembers: string[]): {
    generation: number;
    members: string[];
  } {
    if (expectedGeneration !== this.generation) {
      throw new Error(`Topology CAS conflict: expected gen=${expectedGeneration}, actual=${this.generation}`);
    }
    this.generation += 1;
    this.members = [...nextMembers];
    return { generation: this.generation, members: [...this.members] };
  }
}
