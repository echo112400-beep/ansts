"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TunnelGatewayCell = void 0; /**
 * @fileoverview v2.5, M52-4, M61-4, M65-4 & M68-1: Dual-Homed Reverse Tunnel Gateway Cell (`TunnelGatewayCell`).
 * Supports NAT-traversal outbound reverse tunnel command queueing, long-poll retrieval,
 * command execution acknowledgement, stale-heartbeat tunnel auto-demotion, in-flight command re-queueing,
 * Idempotent In-Flight/Completed Command Coalescing, Timed-Out Command Queue/Idempotency Eviction,
 * Non-Regressive Epoch/Generation Registration Guard, Bounded Idempotency Retention, SQLite Persistence & Durable Object `fetch` API.
 */























function isDurableObjectStateLike(x) {
  return Boolean(x && typeof x === "object" && "storage" in x && x.storage?.sql);
}

class TunnelGatewayCell {
  tunnelsByHost = new Map();
  pendingCommandsByHost = new Map();
  inFlightCommands = new Map();
  commandWaiters =






  new Map();
  commandByIdempotencyKey = new Map();
  completedResultsByIdempotencyKey = new Map();
  maxRetainedIdempotentCommands;
  cmdSeq = 0;
  sql;

  constructor(
  sqlOrState,
  maxRetainedIdempotentCommands = 2048)
  {
    this.maxRetainedIdempotentCommands =
    Number.isFinite(maxRetainedIdempotentCommands) && maxRetainedIdempotentCommands >= 1 ?
    Math.floor(maxRetainedIdempotentCommands) :
    2048;
    if (sqlOrState) {
      this.bindSqlStorage(isDurableObjectStateLike(sqlOrState) ? sqlOrState.storage.sql : sqlOrState);
    }
  }

  /**
   * [M61-4] Binds SQLite storage so tunnel bindings, queued commands, and completed idempotent command results
   * survive Gateway Cell restarts.
   */
  bindSqlStorage(sql) {
    this.sql = sql;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS tunnel_gateway_bindings (host_id TEXT, gateway_id TEXT, payload_json TEXT, PRIMARY KEY (host_id, gateway_id))");

    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS tunnel_gateway_commands (command_id TEXT PRIMARY KEY, host_id TEXT, status TEXT, idempotency_key TEXT, payload_json TEXT, result_json TEXT)");

    let tunnelsRestored = 0;
    for (const r of this.sql.exec("SELECT * FROM tunnel_gateway_bindings").toArray()) {
      const b = JSON.parse(String(r.payload_json ?? "{}"));
      if (b && b.hostId && b.gatewayId) {
        const list = (this.tunnelsByHost.get(b.hostId) ?? []).filter((x) => x.gatewayId !== b.gatewayId);
        list.push(b);
        this.tunnelsByHost.set(b.hostId, list);
        tunnelsRestored += 1;
      }
    }
    let commandsRestored = 0;
    for (const r of this.sql.exec("SELECT * FROM tunnel_gateway_commands").toArray()) {
      const status = String(r.status ?? "PENDING");
      const cmd = JSON.parse(String(r.payload_json ?? "{}"));
      const idemKey = r.idempotency_key ? String(r.idempotency_key) : cmd.idempotencyKey;
      if (status === "COMPLETED" && idemKey) {
        const scopedIdem = `${cmd.hostId}::${idemKey}`;
        this.completedResultsByIdempotencyKey.set(
        scopedIdem,
        JSON.parse(String(r.result_json ?? "{}")));

        this.commandByIdempotencyKey.set(scopedIdem, cmd);
        this.enforceIdempotencyCacheBound();
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

  persistTunnel(b) {
    if (!this.sql) return;
    this.sql.exec(
    "INSERT OR REPLACE INTO tunnel_gateway_bindings (host_id, gateway_id, payload_json) VALUES (?, ?, ?)",
    b.hostId,
    b.gatewayId,
    JSON.stringify(b));

  }

  persistCommand(
  cmd,
  status,
  result)
  {
    if (!this.sql) return;
    this.sql.exec(
    "INSERT OR REPLACE INTO tunnel_gateway_commands (command_id, host_id, status, idempotency_key, payload_json, result_json) VALUES (?, ?, ?, ?, ?, ?)",
    cmd.commandId,
    cmd.hostId,
    status,
    cmd.idempotencyKey ?? "",
    JSON.stringify(cmd),
    result ? JSON.stringify(result) : "");

  }

  deletePersistedCommand(commandId) {
    if (!this.sql) return;
    this.sql.exec("DELETE FROM tunnel_gateway_commands WHERE command_id = ?", commandId);
  }

  /**
   * [M68-1] Bounds `completedResultsByIdempotencyKey` and `commandByIdempotencyKey` to `maxRetainedIdempotentCommands`
   * using FIFO eviction so long-lived Gateway Cells never leak unbounded memory.
   */
  enforceIdempotencyCacheBound() {
    while (this.completedResultsByIdempotencyKey.size > this.maxRetainedIdempotentCommands) {
      const oldestScopedKey = this.completedResultsByIdempotencyKey.keys().next().value;
      if (oldestScopedKey === undefined) break;
      this.completedResultsByIdempotencyKey.delete(oldestScopedKey);
      const evictedCmd = this.commandByIdempotencyKey.get(oldestScopedKey);
      this.commandByIdempotencyKey.delete(oldestScopedKey);
      if (evictedCmd) {
        this.deletePersistedCommand(evictedCmd.commandId);
      }
    }
  }

  /**
   * [M68-1] Non-regressive tunnel registration guard: prevents out-of-order network registration packets
   * with an older `(epoch, connGeneration)` tuple from downgrading an already-registered `(hostId, gatewayId)` binding.
   */
  registerTunnel(binding, nowMs = Date.now()) {
    const safeEpoch =
    Number.isFinite(binding.epoch) && binding.epoch >= 1 ? Math.floor(binding.epoch) : 1;
    const safeGen =
    Number.isFinite(binding.connGeneration) && binding.connGeneration >= 1 ?
    Math.floor(binding.connGeneration) :
    1;

    const existingList = this.tunnelsByHost.get(binding.hostId) ?? [];
    const existing = existingList.find((b) => b.gatewayId === binding.gatewayId);
    if (
    existing && (
    safeEpoch < existing.epoch ||
    safeEpoch === existing.epoch && safeGen < existing.connGeneration))
    {
      return existing;
    }

    const full = {
      ...binding,
      epoch: safeEpoch,
      connGeneration: safeGen,
      healthy: true,
      lastHeartbeatMs: binding.lastHeartbeatMs ?? nowMs
    };
    const list = existingList.filter((b) => b.gatewayId !== binding.gatewayId);
    list.push(full);
    this.tunnelsByHost.set(binding.hostId, list);
    this.persistTunnel(full);
    return full;
  }

  markGatewayHealth(hostId, gatewayId, healthy, nowMs = Date.now()) {
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
  evictStaleTunnelsAndRequeueInFlight(
  nowMs = Date.now(),
  heartbeatTtlMs = 30_000)
  {
    const demotedGateways = [];
    for (const [hostId, list] of this.tunnelsByHost.entries()) {
      for (const b of list) {
        if (b.healthy && b.lastHeartbeatMs !== undefined && nowMs - b.lastHeartbeatMs > heartbeatTtlMs) {
          b.healthy = false;
          this.persistTunnel(b);
          demotedGateways.push({ hostId, gatewayId: b.gatewayId });
        }
      }
    }

    const requeuedCommandIds = [];
    for (const [cmdId, cmd] of this.inFlightCommands.entries()) {
      const hostTunnels = this.tunnelsByHost.get(cmd.hostId) ?? [];
      const origTunnel = hostTunnels.find((b) => b.gatewayId === cmd.gatewayId);
      if (!origTunnel || !origTunnel.healthy) {
        this.inFlightCommands.delete(cmdId);
        const active = this.selectActiveTunnel(cmd.hostId);
        const requeued = {
          ...cmd,
          gatewayId: active.gatewayId,
          role: active.role,
          retryCount: (cmd.retryCount ?? 0) + 1
        };
        const q = this.pendingCommandsByHost.get(cmd.hostId) ?? [];
        q.push(requeued);
        this.pendingCommandsByHost.set(cmd.hostId, q);
        if (requeued.idempotencyKey) {
          this.commandByIdempotencyKey.set(`${requeued.hostId}::${requeued.idempotencyKey}`, requeued);
        }
        this.persistCommand(requeued, "PENDING");
        requeuedCommandIds.push(cmdId);
      }
    }

    return { demotedGateways, requeuedCommandIds };
  }

  selectActiveTunnel(hostId) {
    const list = (this.tunnelsByHost.get(hostId) ?? []).filter((b) => b.healthy);
    if (list.length === 0) {
      throw new Error(`No healthy reverse tunnel available for body host '${hostId}'`);
    }
    const primary = list.find((b) => b.role === "PRIMARY");
    return primary ?? list[0];
  }

  /**
   * [M61-4 & M68-1] Supports idempotent & coalesced command queueing via `options.idempotencyKey` (or `payload.idempotency_key`):
   * - If the command already completed, returns the cached completion result immediately (`deduplicated: true`).
   * - If the command is currently pending or in-flight, coalesces onto the existing command's `completionPromise`
   *   without enqueueing a duplicate command to the Body host.
   * - [M68-1] When a command times out before completion, evicts it from `pendingCommandsByHost`, `inFlightCommands`,
   *   `commandByIdempotencyKey`, and SQLite so timed-out unpolled commands never execute as ghost commands on a later
   *   host poll and subsequent retries with the same `idempotencyKey` can cleanly re-enqueue.
   */
  enqueueCommandForHost(
  hostId,
  payload,
  timeoutMs = 5000,
  options)




  {
    const rawIdemKey =
    options?.idempotencyKey ?? (
    typeof payload.idempotency_key === "string" ? payload.idempotency_key : undefined);
    const scopedIdem = rawIdemKey ? `${hostId}::${rawIdemKey}` : undefined;

    if (scopedIdem) {
      const completedRes = this.completedResultsByIdempotencyKey.get(scopedIdem);
      const prevCmd = this.commandByIdempotencyKey.get(scopedIdem);
      if (completedRes && prevCmd) {
        return {
          command: prevCmd,
          completionPromise: Promise.resolve(completedRes),
          deduplicated: true
        };
      }
      if (prevCmd) {
        const existingWaiter = this.commandWaiters.get(prevCmd.commandId);
        if (existingWaiter) {
          return {
            command: prevCmd,
            completionPromise: existingWaiter.promise,
            deduplicated: true
          };
        }
        // Stale or timed-out previous command without an active waiter or completed result: clean up before re-enqueueing
        this.commandByIdempotencyKey.delete(scopedIdem);
      }
    }

    const active = this.selectActiveTunnel(hostId);
    this.cmdSeq += 1;
    const commandId = `tcmd-${Date.now()}-${this.cmdSeq}`;
    const command = {
      commandId,
      hostId,
      gatewayId: active.gatewayId,
      role: active.role,
      payload,
      enqueuedAtMs: Date.now(),
      retryCount: 0,
      idempotencyKey: rawIdemKey
    };
    const q = this.pendingCommandsByHost.get(hostId) ?? [];
    q.push(command);
    this.pendingCommandsByHost.set(hostId, q);
    if (scopedIdem) {
      this.commandByIdempotencyKey.set(scopedIdem, command);
    }
    this.persistCommand(command, "PENDING");

    const safeTimeoutMs = Number.isFinite(timeoutMs) && timeoutMs > 0 ? timeoutMs : 5000;
    let resolveFn;
    let timerRef;
    const completionPromise = new Promise((resolve, reject) => {
      resolveFn = resolve;
      timerRef = setTimeout(() => {
        this.commandWaiters.delete(commandId);
        this.inFlightCommands.delete(commandId);
        const hostQueue = this.pendingCommandsByHost.get(hostId);
        if (hostQueue) {
          const filtered = hostQueue.filter((c) => c.commandId !== commandId);
          if (filtered.length > 0) {
            this.pendingCommandsByHost.set(hostId, filtered);
          } else {
            this.pendingCommandsByHost.delete(hostId);
          }
        }
        if (
        scopedIdem &&
        this.commandByIdempotencyKey.get(scopedIdem)?.commandId === commandId &&
        !this.completedResultsByIdempotencyKey.has(scopedIdem))
        {
          this.commandByIdempotencyKey.delete(scopedIdem);
        }
        this.deletePersistedCommand(commandId);
        reject(new Error(`Reverse tunnel command '${commandId}' for host '${hostId}' timed out after ${safeTimeoutMs}ms`));
      }, safeTimeoutMs);
    });
    this.commandWaiters.set(commandId, {
      promise: completionPromise,
      resolve: resolveFn,
      timer: timerRef
    });

    return { command, completionPromise, deduplicated: false };
  }

  pollPendingCommands(hostId, gatewayId, nowMs = Date.now()) {
    const list = this.tunnelsByHost.get(hostId) ?? [];
    for (const b of list) {
      if (!gatewayId || b.gatewayId === gatewayId) {
        b.lastHeartbeatMs = nowMs;
        this.persistTunnel(b);
      }
    }
    const q = this.pendingCommandsByHost.get(hostId) ?? [];
    if (q.length === 0) return [];
    this.pendingCommandsByHost.delete(hostId);
    for (const cmd of q) {
      this.inFlightCommands.set(cmd.commandId, cmd);
      this.persistCommand(cmd, "IN_FLIGHT");
    }
    return q;
  }

  acknowledgeCommand(commandId, result) {
    const cmd = this.inFlightCommands.get(commandId);
    this.inFlightCommands.delete(commandId);
    if (cmd) {
      if (cmd.idempotencyKey) {
        const scopedIdem = `${cmd.hostId}::${cmd.idempotencyKey}`;
        this.completedResultsByIdempotencyKey.set(scopedIdem, result);
        this.commandByIdempotencyKey.set(scopedIdem, cmd);
        this.enforceIdempotencyCacheBound();
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

  listHostTunnels(hostId) {
    return [...(this.tunnelsByHost.get(hostId) ?? [])];
  }

  /**
   * [M65-4] Durable Object HTTP `fetch` entrypoint supporting `/register`, `/poll`, `/ack`, `/health`, and `/evict-stale`
   * (with or without `/v1/tunnel` or `/internal/v1beta1/tunnel` prefix).
   */
  async fetch(request) {
    const url = new URL(request.url);
    const normalizedPath =
    url.pathname.
    replace(/^\/internal\/v1beta1\/tunnel(?=\/|$)/, "").
    replace(/^\/v1\/tunnel(?=\/|$)/, "") || "/";

    if (request.method === "POST" && normalizedPath === "/register") {
      const body = await request.json();
      const hostId = String(body.hostId ?? "body-host-01");
      const gatewayId = String(body.gatewayId ?? "celld-gw-01");
      const role = body.role === "SECONDARY" ? "SECONDARY" : "PRIMARY";
      const connGeneration = Number(body.connGeneration ?? 1);
      const epoch = Number(body.epoch ?? 1);
      this.registerTunnel({ hostId, gatewayId, role, connGeneration, epoch });
      const active = this.selectActiveTunnel(hostId);
      return new Response(
      JSON.stringify({
        registered: true,
        hostId,
        gatewayId,
        role,
        activeGatewayId: active.gatewayId
      }),
      { status: 200, headers: { "content-type": "application/json" } });

    }

    if (request.method === "GET" && normalizedPath === "/poll") {
      const hostId = url.searchParams.get("hostId") ?? "body-host-01";
      const gatewayId = url.searchParams.get("gatewayId") ?? undefined;
      const commands = this.pollPendingCommands(hostId, gatewayId);
      return new Response(JSON.stringify({ hostId, commands }), {
        status: 200,
        headers: { "content-type": "application/json" }
      });
    }

    if (request.method === "POST" && normalizedPath === "/ack") {
      const body = await request.json();
      const commandId = String(body.commandId ?? "");
      const result = body.result ?? {};
      const acknowledged = this.acknowledgeCommand(commandId, result);
      return new Response(JSON.stringify({ acknowledged, commandId }), {
        status: 200,
        headers: { "content-type": "application/json" }
      });
    }

    if (request.method === "POST" && normalizedPath === "/health") {
      const body = await request.json();
      const hostId = String(body.hostId ?? "body-host-01");
      const gatewayId = String(body.gatewayId ?? "celld-gw-01");
      const healthy = Boolean(body.healthy ?? true);
      this.markGatewayHealth(hostId, gatewayId, healthy);
      const active = this.selectActiveTunnel(hostId);
      return new Response(
      JSON.stringify({ hostId, activeGatewayId: active.gatewayId, activeRole: active.role }),
      { status: 200, headers: { "content-type": "application/json" } });

    }

    if (request.method === "POST" && normalizedPath === "/evict-stale") {
      const body = await request.json();
      const out = this.evictStaleTunnelsAndRequeueInFlight(
      body.nowMs !== undefined ? Number(body.nowMs) : Date.now(),
      body.heartbeatTtlMs !== undefined ? Number(body.heartbeatTtlMs) : 30_000);

      return new Response(JSON.stringify(out), {
        status: 200,
        headers: { "content-type": "application/json" }
      });
    }

    return new Response(JSON.stringify({ error: "NOT_FOUND" }), { status: 404 });
  }
}exports.TunnelGatewayCell = TunnelGatewayCell;