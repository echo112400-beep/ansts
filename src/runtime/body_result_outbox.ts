/** @fileoverview Durable result delivery queue, retaining results until control-plane confirmation. */
import * as fs from "node:fs";
import * as path from "node:path";
import { executionDigest } from "../cloudflare/execution_attempt.js";

export interface BodyResultDelivery {
  lease: Record<string, unknown>;
  expected_version: number;
  result: Record<string, unknown>;
  attempts: number;
  next_retry_at: number;
}
export class BodyResultOutbox {
  constructor(private readonly directory: string) { fs.mkdirSync(directory, { recursive: true }); }
  private file(lease: Record<string, unknown>): string {
    return path.join(this.directory, executionDigest([lease.tenant_id, lease.task_id, lease.attempt_id]) + ".json");
  }
  put(delivery: BodyResultDelivery): void {
    const file = this.file(delivery.lease);
    if (fs.existsSync(file)) {
      const old = JSON.parse(fs.readFileSync(file, "utf8")) as BodyResultDelivery;
      if (executionDigest(old.result) !== executionDigest(delivery.result)) throw new Error("OUTBOX_RESULT_CONFLICT");
    }
    const temporary = file + ".tmp";
    const fd = fs.openSync(temporary, "w", 0o600);
    try { fs.writeFileSync(fd, JSON.stringify(delivery)); fs.fsyncSync(fd); }
    finally { fs.closeSync(fd); }
    fs.renameSync(temporary, file);
    const directory = fs.openSync(this.directory, "r");
    try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  }
  pending(): BodyResultDelivery[] {
    return fs.readdirSync(this.directory).filter(name => name.endsWith(".json")).map(name => JSON.parse(fs.readFileSync(path.join(this.directory, name), "utf8")) as BodyResultDelivery);
  }
  remove(lease: Record<string, unknown>): void { fs.unlinkSync(this.file(lease)); }
  defer(delivery: BodyResultDelivery): void {
    delivery.attempts++;
    delivery.next_retry_at = Date.now() + Math.min(60000, 1000 * 2 ** Math.min(delivery.attempts, 6)) + Math.floor(Math.random() * 1000);
    this.put(delivery);
  }
}
