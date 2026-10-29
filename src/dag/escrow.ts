/**
 * TASK-08: Multi-Agent DAG State Machine, HMAC-Signed Quota Escrow Vouchers & Straggler Acceleration.
 */

import * as crypto from "crypto";
import { AgentTask, EscrowVoucher, SandboxPhase, TaskState } from "../models";

export class DagEscrowCoordinator {
  private secret: Buffer;
  private signatures: Map<string, string> = new Map();

  constructor(clusterSecret: string = "ansts-escrow-hmac-secret-v2") {
    this.secret = Buffer.from(clusterSecret, "utf8");
  }

  public signVoucher(voucher: EscrowVoucher): string {
    const msg = `${voucher.parentTaskId}:${voucher.parentFence.epoch}:${voucher.parentFence.turnSeq}:${voucher.childTaskId}:${voucher.delegatedTps}:${voucher.delegatedTokens}`;
    const sig = crypto.createHmac("sha256", this.secret).update(msg, "utf8").digest("hex");
    this.signatures.set(voucher.childTaskId, sig);
    return sig;
  }

  public verifyVoucher(voucher: EscrowVoucher, signature: string): boolean {
    const expected = this.signVoucher(voucher);
    return crypto.timingSafeEqual(Buffer.from(expected, "utf8"), Buffer.from(signature, "utf8"));
  }

  public yieldParentAndIssueVouchers(parent: AgentTask, children: AgentTask[]): EscrowVoucher[] {
    parent.state = TaskState.HIBERNATED_YIELD;
    parent.phase = SandboxPhase.IDLE;
    parent.activeSessions = 0;

    const m = Math.max(1, children.length);
    const perTps = Math.max(15, Math.floor(parent.reservedTps / m));
    const remTok = Math.max(10000, parent.maxTotalTokens - parent.usedTokens);
    const perTok = Math.floor(remTok / m);

    const vouchers: EscrowVoucher[] = [];
    for (const child of children) {
      child.reservedTps = perTps;
      child.maxTotalTokens = perTok;
      const v: EscrowVoucher = {
        parentTaskId: parent.taskId,
        parentFence: parent.fence,
        childTaskId: child.taskId,
        delegatedTps: perTps,
        delegatedTokens: perTok,
      };
      this.signVoucher(v);
      vouchers.push(v);
    }
    return vouchers;
  }
}
