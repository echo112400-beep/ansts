/**
 * @fileoverview Deterministic replay envelope: virtual clock and seeded randomness.
 */
import * as crypto from "node:crypto";
export interface ReplayEnvelopeState {
  envelopeId: string;
  virtualBaseTimestampMs: number;
  virtualClockTickOffsetMs: number;
  randomSeedHex: string;
  consumedRandomBytesCount: number;
}
export class DeterministicReplayEnvelope {
  private clockOffsetMs = 0;
  private randomBytesPool: Buffer;
  private randomCursor = 0;
  constructor(public readonly state: ReplayEnvelopeState) {
    this.clockOffsetMs = state.virtualClockTickOffsetMs;
    const masterHash = crypto.createHash("sha256").update(state.randomSeedHex, "utf8").digest();
    this.randomBytesPool = Buffer.alloc(65536);
    let curr = masterHash;
    for (let i = 0; i < 65536; i += 32) {
      curr = crypto.createHash("sha256").update(curr).digest();
      curr.copy(this.randomBytesPool, i);
    }
    this.randomCursor = state.consumedRandomBytesCount;
  }
  public getDeterministicNowMs(): number {
    this.clockOffsetMs += 10;
    return this.state.virtualBaseTimestampMs + this.clockOffsetMs;
  }
  public getDeterministicRandom(): number {
    if (this.randomCursor + 8 > this.randomBytesPool.length) {
      this.randomCursor = 0;
    }
    const val = this.randomBytesPool.readUInt32LE(this.randomCursor);
    this.randomCursor += 4;
    return val / 4294967296.0;
  }
}
