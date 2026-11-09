"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.DeterministicExecutionEnvelope = void 0; /**
 * @fileoverview Deterministic Execution Envelope (xorshift32 PRNG Seed, Virtual Clock & Environment Builder).
 */
class DeterministicExecutionEnvelope {
  state;
  seed;
  virtualClockStartMs;

  constructor(seed = 42, virtualClockStartMs = 1_700_000_000_000) {
    this.seed = seed >>> 0;
    this.state = seed >>> 0 || 1;
    this.virtualClockStartMs = virtualClockStartMs;
  }

  nextDeterministicRandom() {
    let x = this.state;
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    this.state = x >>> 0;
    return this.state / 0xffffffff;
  }

  buildDeterministicShellExportPrefix(stepOffsetMs = 0) {
    const randVal = Math.floor(this.nextDeterministicRandom() * 1_000_000_000);
    const virtualNow = this.virtualClockStartMs + stepOffsetMs;
    return `export ANSTS_DETERMINISTIC_SEED=${this.seed} ANSTS_VIRTUAL_CLOCK_MS=${virtualNow} ANSTS_DET_RAND=${randVal} TZ=UTC LC_ALL=C; `;
  }
}exports.DeterministicExecutionEnvelope = DeterministicExecutionEnvelope;