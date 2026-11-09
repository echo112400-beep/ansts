/**
 * @fileoverview Deterministic Execution Envelope (xorshift32 PRNG Seed, Virtual Clock & Environment Builder).
 */
export class DeterministicExecutionEnvelope {
  private state: number;
  public readonly seed: number;
  public readonly virtualClockStartMs: number;

  constructor(seed = 42, virtualClockStartMs = 1_700_000_000_000) {
    this.seed = seed >>> 0;
    this.state = (seed >>> 0) || 1;
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

  public buildDeterministicShellExportPrefix(stepOffsetMs = 0): string {
    const randVal = Math.floor(this.nextDeterministicRandom() * 1_000_000_000);
    const virtualNow = this.virtualClockStartMs + stepOffsetMs;
    return `export ANSTS_DETERMINISTIC_SEED=${this.seed} ANSTS_VIRTUAL_CLOCK_MS=${virtualNow} ANSTS_DET_RAND=${randVal} TZ=UTC LC_ALL=C; `;
  }
}
