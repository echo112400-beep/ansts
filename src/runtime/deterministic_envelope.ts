/**
 * @fileoverview Deterministic Execution Envelope (PRNG Seed, Virtual Clock & Syscall Recording).
 */
export class DeterministicExecutionEnvelope {
  private state: number;
  public readonly virtualClockStartMs: number;

  constructor(seed = 42, virtualClockStartMs = 1_700_000_000_000) {
    this.state = seed >>> 0;
    this.virtualClockStartMs = virtualClockStartMs;
  }

  public nextDeterministicRandom(): number {
    let x = this.state;
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    this.state = x >>> 0;
    return this.state / 0xffffffff;
  }
}
