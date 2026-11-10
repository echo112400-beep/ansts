/**
 * @fileoverview v2.5, M52-4 & M61-4: Dual-Homed Reverse Tunnel Gateway Cell (`TunnelGatewayCell`).
 * Supports NAT-traversal outbound reverse tunnel command queueing, long-poll retrieval,
 * command execution acknowledgement, stale-heartbeat tunnel auto-demotion, in-flight command re-queueing,
 * Idempotent In-Flight/Completed Command Coalescing, and SQLite Persistence.
 */
import { SqlStorageLike } from "./env.js";

export interface ReverseTunnelBinding {
  hostId: string;
  gatewayId: string;
  role: "PRIMARY" | "SECONDARY";
  connGeneration: number;
  epoch: number;
  healthy: boolean;
  lastHeartbeatMs?: number;
}

export interface QueuedTunnelCommand {
  commandId: string;
  hostId: string;
  gatewayId: string;
  role: "PRIMARY" | "SECONDARY";
  payload: Record<string, unknown>;
  enqueuedAtMs: number;
  retryCount?: number;
  idempotencyKey?: string;
}

export class TunnelGatewayCell {
  private readonly tunnelsByHost: Map<string, ReverseTunnelBinding[]> = new Map();
  private readonly pendingCommandsByHost: Map<string, QueuedTunnelCommand[]> = new Map();
  private readonly inFlightCommands: Map<string, QueuedTunnelCommand> = new Map();
  private readonly commandWaiters: Map<
    string,
    {
      promise: Promise<Record<string, unknown>>;
      resolve: (result: Record<string, unknown>) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  > = new Map();
  private readonly commandByIdempotencyKey: Map<string, QueuedTunnelCommand> = new Map();
  private readonly completedResultsByIdempotencyKey: Map<string, Record<string, unknown>> = new Map();
  private cmdSeq = 0;
  private sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M61-4] Binds SQLite storage so tunnel bindings, queued commands, and completed idempotent command results
   * survive Gateway Cell restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): { tunnelsRestored: number; commandsRestored: number } {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS tunnel_gateway_bindings (host_id TEXT, gateway_id TEXT, payload_json TEXT, PRIMARY KEY (host_id, gateway_id))"
    );
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS tunnel_gateway_commands (command_id TEXT PRIMARY KEY, host_id TEXT, status TEXT, idempotency_key TEXT, payload_json TEXT, result_json TEXT)"
    );
    let tunnelsRestored = 0;
    for (const r of this.sql.exec<Record<string, unknown>>("SELECT * FROM tunnel_gateway_bindings").toArray()) {
      const b = JSON.parse(String(r.payload_json ?? "{}")) as ReverseTunnelBinding;
      if (b && b.hostId && b.gatewayId) {
        const list = (this.tunnelsByHost.get(b.hostId) ?? []).filter((x) => x.gatewayId !== b.gatewayId);
        list.push(b);
        this.tunnelsByHost.set(b.hostId, list);
        tunnelsRestored += 1;
      }
    }
    let commandsRestored = 0;
    for (const r of this.sql.exec<Record<string, unknown>>("SELECT * FROM tunnel_gateway_commands").toArray()) {
      const status = String(r.status ?? "PENDING");
      const cmd = JSON.parse(String(r.payload_json ?? "{}")) as QueuedTunnelCommand;
      const idemKey = r.idempotency_key ? String(r.idempotency_key) : cmd.idempotencyKey;
      if (status === "COMPLETED" && idemKey) {
        this.completedResultsByIdempotencyKey.set(
          `${cmd.hostId}::${idemKey}`,
          JSON.parse(String(r.result_json ?? "{}")) as Record<string, unknown>
        );
        commandsRestored += 1;
      } else if (status === "PENDING" || status === "IN_FLIGHT") {
        const q = this.pendingCommandsByHost.get(cmd.hostId) ?? [];
        if (!q.some((x) => x.commandId === cmd.commandId)) {
          q.push(cmd);
          this.pendingCommandsByHost.set(cmd.hostId, q);
        }
        if (idemKey) {
          this.commandByIdempotencyKey.set(`${cmd.hostId}::${idemKey}`, cmd);
        }
        commandsRestored += 1;
      }
    }
    return { tunnelsRestored, commandsRestored };
  }

  private persistTunnel(b: ReverseTunnelBinding): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO tunnel_gateway_bindings (host_id, gateway_id, payload_json) VALUES (?, ?, ?)",
      b.hostId,
      b.gatewayId,
      JSON.stringify(b)
    );
  }

  private persistCommand(
    cmd: QueuedTunnelCommand,
    status: "PENDING" | "IN_FLIGHT" | "COMPLETED",
    result?: Record<string, unknown>
  ): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO tunnel_gateway_commands (command_id, host_id, status, idempotency_key, payload_json, result_json) VALUES (?, ?, ?, ?, ?, ?)",
      cmd.commandId,
      cmd.hostId,
      status,
      cmd.idempotencyKey ?? "",
      JSON.stringify(cmd),
      result ? JSON.stringify(result) : ""
    );
  }

  public registerTunnel(binding: Omit<ReverseTunnelBinding, "healthy">, nowMs = Date.now()): ReverseTunnelBinding {
    const full: ReverseTunnelBinding = {
      ...binding,
      healthy: true,
      lastHeartbeatMs: binding.lastHeartbeatMs ?? nowMs,
    };
    const list = (this.tunnelsByHost.get(binding.hostId) ?? []).filter(
      (b) => b.gatewayId !== binding.gatewayId
    );
    list.push(full);
    this.tunnelsByHost.set(binding.hostId, list);
    this.persistTunnel(full);
    return full;
  }

  public markGatewayHealth(hostId: string, gatewayId: string, healthy: boolean, nowMs = Date.now()): void {
    const list = this.tunnelsByHost.get(hostId) ?? [];
    for (const b of list) {
      if (b.gatewayId === gatewayId) {
        b.healthy = healthy;
        if (healthy) b.lastHeartbeatMs = nowMs;
        this.persistTunnel(b);
      }
    }
  }

  /**
   * [M52-4] Automatically demotes any tunnel binding whose `lastHeartbeatMs` has exceeded `heartbeatTtlMs`,
   * and re-queues any unacknowledged in-flight commands onto the surviving healthy tunnel.
   */
  public evictStaleTunnelsAndRequeueInFlight(
    nowMs = Date.now(),
    heartbeatTtlMs = 30_000
  ): { demotedGateways: Array<{ hostId: string; gatewayId: string }>; requeuedCommandIds: string[] } {
    const demotedGateways: Array<{ hostId: string; gatewayId: string }> = [];
    for (const [hostId, list] of this.tunnelsByHost.entries()) {
      for (const b of list) {
        if (b.healthy && b.lastHeartbeatMs !== undefined && nowMs - b.lastHeartbeatMs > heartbeatTtlMs) {
          b.healthy = false;
          this.persistTunnel(b);
          demotedGateways.push({ hostId, gatewayId: b.gatewayId });
        }
      }
    }

    const requeuedCommandIds: string[] = [];
    for (const [cmdId, cmd] of this.inFlightCommands.entries()) {
      const hostTunnels = this.tunnelsByHost.get(cmd.hostId) ?? [];
      const origTunnel = hostTunnels.find((b) => b.gatewayId === cmd.gatewayId);
      if (!origTunnel || !origTunnel.healthy) {
        this.inFlightCommands.delete(cmdId);
        const active = this.selectActiveTunnel(cmd.hostId);
        const requeued: QueuedTunnelCommand = {
          ...cmd,
          gatewayId: active.gatewayId,
          role: active.role,
          retryCount: (cmd.retryCount ?? 0) + 1,
        };
        const q = this.pendingCommandsByHost.get(cmd.hostId) ?? [];
        q.push(requeued);
        this.pendingCommandsByHost.set(cmd.hostId, q);
        this.persistCommand(requeued, "PENDING");
        requeuedCommandIds.push(cmdId);
      }
    }

    return { demotedGateways, requeuedCommandIds };
  }

  public selectActiveTunnel(hostId: string): ReverseTunnelBinding {
    const list = (this.tunnelsByHost.get(hostId) ?? []).filter((b) => b.healthy);
    if (list.length === 0) {
      throw new Error(`No healthy reverse tunnel available for body host '${hostId}'`);
    }
    const primary = list.find((b) => b.role === "PRIMARY");
    return primary ?? list[0];
  }

  /**
   * [M61-4] Supports idempotent & coalesced command queueing via `options.idempotencyKey` (or `payload.idempotency_key`):
   * - If the command already completed, returns the cached completion result immediately (`deduplicated: true`).
   * - If the command is currently pending or in-flight, coalesces onto the existing command's `completionPromise`
   *   without enqueueing a duplicate command to the Body host.
   */
  public enqueueCommandForHost(
    hostId: string,
    payload: Record<string, unknown>,
    timeoutMs = 5000,
    options?: { idempotencyKey?: string }
  ): {
    command: QueuedTunnelCommand;
    completionPromise: Promise<Record<string, unknown>>;
    deduplicated?: boolean;
  } {
    const rawIdemKey =
      options?.idempotencyKey ??
      (typeof payload.idempotency_key === "string" ? payload.idempotency_key : undefined);
    const scopedIdem = rawIdemKey ? `${hostId}::${rawIdemKey}` : undefined;

    if (scopedIdem) {
      const completedRes = this.completedResultsByIdempotencyKey.get(scopedIdem);
      const prevCmd = this.commandByIdempotencyKey.get(scopedIdem);
      if (completedRes && prevCmd) {
        return {
          command: prevCmd,
          completionPromise: Promise.resolve(completedRes),
          deduplicated: true,
        };
      }
      if (prevCmd) {
        const existingWaiter = this.commandWaiters.get(prevCmd.commandId);
        if (existingWaiter) {
          return {
            command: prevCmd,
            completionPromise: existingWaiter.promise,
            deduplicated: true,
          };
        }
      }
    }

    const active = this.selectActiveTunnel(hostId);
    this.cmdSeq += 1;
    const commandId = `tcmd-${Date.now()}-${this.cmdSeq}`;
    const command: QueuedTunnelCommand = {
      commandId,
      hostId,
      gatewayId: active.gatewayId,
      role: active.role,
      payload,
      enqueuedAtMs: Date.now(),
      retryCount: 0,
      idempotencyKey: rawIdemKey,
    };
    const q = this.pendingCommandsByHost.get(hostId) ?? [];
    q.push(command);
    this.pendingCommandsByHost.set(hostId, q);
    if (scopedIdem) {
      this.commandByIdempotencyKey.set(scopedIdem, command);
    }
    this.persistCommand(command, "PENDING");

    let resolveFn!: (result: Record<string, unknown>) => void;
    let timerRef!: ReturnType<typeof setTimeout>;
    const completionPromise = new Promise<Record<string, unknown>>((resolve, reject) => {
      resolveFn = resolve;
      timerRef = setTimeout(() => {
        this.commandWaiters.delete(commandId);
        this.inFlightCommands.delete(commandId);
        reject(new Error(`Reverse tunnel command '${commandId}' for host '${hostId}' timed out after ${timeoutMs}ms`));
      }, timeoutMs);
    });
    this.commandWaiters.set(commandId, {
      promise: completionPromise,
      resolve: resolveFn,
      timer: timerRef,
    });

    return { command, completionPromise, deduplicated: false };
  }

  public pollPendingCommands(hostId: string, gatewayId?: string, nowMs = Date.now()): QueuedTunnelCommand[] {
    const list = this.tunnelsByHost.get(hostId) ?? [];
    for (const b of list) {
      if (!gatewayId || b.gatewayId === gatewayId) {
        b.lastHeartbeatMs = nowMs;
        this.persistTunnel(b);
      }
    }
    const q = this.pendingCommandsByHost.get(hostId) ?? [];
    if (q.length === 0) return [];
    this.pendingCommandsByHost.set(hostId, []);
    for (const cmd of q) {
      this.inFlightCommands.set(cmd.commandId, cmd);
      this.persistCommand(cmd, "IN_FLIGHT");
    }
    return q;
  }

  public acknowledgeCommand(commandId: string, result: Record<string, unknown>): boolean {
    const cmd = this.inFlightCommands.get(commandId);
    this.inFlightCommands.delete(commandId);
    if (cmd) {
      if (cmd.idempotencyKey) {
        const scopedIdem = `${cmd.hostId}::${cmd.idempotencyKey}`;
        this.completedResultsByIdempotencyKey.set(scopedIdem, result);
        this.commandByIdempotencyKey.set(scopedIdem, cmd);
      }
      this.persistCommand(cmd, "COMPLETED", result);
    }
    const waiter = this.commandWaiters.get(commandId);
    if (!waiter) return Boolean(cmd);
    clearTimeout(waiter.timer);
    this.commandWaiters.delete(commandId);
    waiter.resolve(result);
    return true;
  }

  public listHostTunnels(hostId: string): ReverseTunnelBinding[] {
    return [...(this.tunnelsByHost.get(hostId) ?? [])];
  }
}
