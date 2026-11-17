/** Execution slots are reserved synchronously before asynchronous admission. */
export class BodyCapacity {
  private readonly reservations = new Set<string>();
  constructor(readonly slots = 1) {
    if (!Number.isSafeInteger(slots) || slots < 1 || slots > 256) throw new Error("INVALID_BODY_SLOTS");
  }
  reserve(identity: string): boolean {
    if (!identity || this.reservations.has(identity) || this.reservations.size >= this.slots) return false;
    this.reservations.add(identity); return true;
  }
  release(identity: string): boolean { return this.reservations.delete(identity); }
  snapshot(): { slots: number; reserved: number; available: number } {
    return { slots: this.slots, reserved: this.reservations.size, available: this.slots - this.reservations.size };
  }
}