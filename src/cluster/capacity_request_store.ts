import { createHash } from 'node:crypto';
import { capacityId, capacityResources, type CapacityResources } from '../control/capacity_report.js';
import { assertExpectedVersion, ControlError } from '../control/contracts.js';
import type { AdmissionReservation } from '../control/admission_reservations.js';
import type { CapacityInventory } from './capacity_inventory.js';
import type { DirectoryStorage } from '../control/tenant_directory.js';
import { selectCapacityRequest, type CapacityQueueEntry } from './capacity_queue.js';

const dimensions = ['cpuMilliCores','memoryMiB','slots'] as const;

interface CapacityRequest extends CapacityQueueEntry {
  state: 'QUEUED' | 'CANCELLED' | 'GRANTED' | 'RELEASED';
  grantExpiresAt?: number;
}

interface RequestRow {
  id: string;
  tenant_id: string;
  resources: string;
  enqueued_at: number;
  priority: number;
  state: CapacityRequest['state'];
  grant_expires_at?: number | null;
}

function deserialize(row: RequestRow): CapacityRequest {
  return {
    id: row.id,
    tenantId: row.tenant_id,
    resources: capacityResources(JSON.parse(row.resources), true),
    enqueuedAt: row.enqueued_at,
    priority: row.priority,
    state: row.state,
    ...(row.grant_expires_at == null ? {} : { grantExpiresAt: row.grant_expires_at }),
  };
}

/** Persistence for trusted pool handlers. Handlers supply the authenticated tenant and policy priority. */
export class CapacityRequestStore {
  constructor(private readonly storage: DirectoryStorage, private readonly poolId: string) {
    capacityId(poolId);
    if (typeof storage.transactionSync !== 'function') {
      throw new ControlError('DURABLE_TRANSACTION_REQUIRED', 503);
    }
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS capacity_requests (
      pool_id TEXT NOT NULL,
      id TEXT NOT NULL,
      tenant_id TEXT NOT NULL,
      resources TEXT NOT NULL,
      enqueued_at INTEGER NOT NULL,
      priority INTEGER NOT NULL,
      state TEXT NOT NULL CHECK (state IN ('QUEUED', 'CANCELLED')),
      PRIMARY KEY (pool_id, id)
    )`);
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS capacity_request_grants (
      pool_id TEXT NOT NULL,
      request_id TEXT NOT NULL,
      host_id TEXT NOT NULL,
      policy_version INTEGER NOT NULL,
      PRIMARY KEY (pool_id, request_id)
    )`);
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS capacity_grant_bindings (
      pool_id TEXT NOT NULL,
      request_id TEXT NOT NULL,
      tenant_id TEXT NOT NULL,
      reservation_id TEXT NOT NULL,
      attempt_id TEXT NOT NULL,
      generation INTEGER NOT NULL,
      fencing INTEGER NOT NULL,
      PRIMARY KEY (pool_id, request_id),
      UNIQUE (tenant_id, reservation_id),
      UNIQUE (tenant_id, attempt_id)
    )`);
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS capacity_grant_expiry (
      pool_id TEXT NOT NULL,
      request_id TEXT NOT NULL,
      expires_at INTEGER NOT NULL,
      PRIMARY KEY (pool_id, request_id)
    )`);
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS capacity_queue_policy (
      pool_id TEXT PRIMARY KEY, version INTEGER NOT NULL, wait_ms INTEGER NOT NULL, weights_json TEXT NOT NULL
    )`);
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS capacity_allocation_control (
      pool_id TEXT PRIMARY KEY, version INTEGER NOT NULL, enabled INTEGER NOT NULL
    )`);
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS capacity_grant_epochs (
      pool_id TEXT NOT NULL, request_id TEXT NOT NULL, daemon_id TEXT NOT NULL, epoch INTEGER NOT NULL,
      PRIMARY KEY (pool_id, request_id)
    )`);
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS capacity_guaranteed_shares (
      pool_id TEXT NOT NULL, tenant_id TEXT NOT NULL, resources_json TEXT NOT NULL,
      PRIMARY KEY (pool_id,tenant_id)
    )`);
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS capacity_queue_priorities (
      pool_id TEXT PRIMARY KEY, priorities_json TEXT NOT NULL
    )`);
    storage.sql.exec(`CREATE INDEX IF NOT EXISTS capacity_requests_by_tenant
      ON capacity_requests (pool_id, tenant_id, enqueued_at, id)`);
  }

  readAllocationControl() {
    const row = this.storage.sql.exec<{version:number;enabled:number}>(
      'SELECT version, enabled FROM capacity_allocation_control WHERE pool_id = ?', this.poolId,
    ).toArray()[0];
    return {version:row?.version ?? 0,enabled:row ? row.enabled === 1 : true};
  }

  writeAllocationControl(expectedVersion: number, enabled: boolean) {
    if (typeof enabled !== 'boolean') throw new ControlError('INVALID_CAPACITY_INPUT',422);
    return this.storage.transactionSync(() => {
      const current = this.readAllocationControl();
      assertExpectedVersion(expectedVersion,current.version);
      this.storage.sql.exec('INSERT OR REPLACE INTO capacity_allocation_control VALUES (?, ?, ?)',
        this.poolId,current.version+1,enabled ? 1 : 0);
      return this.readAllocationControl();
    });
  }

  readPolicy() {
    const row = this.storage.sql.exec<{version:number;wait_ms:number;weights_json:string}>(
      'SELECT version, wait_ms, weights_json FROM capacity_queue_policy WHERE pool_id = ?', this.poolId,
    ).toArray()[0];
    const configured=this.storage.sql.exec<{priorities_json:string}>(
      'SELECT priorities_json FROM capacity_queue_priorities WHERE pool_id=?',this.poolId,
    ).toArray()[0];
    const priorities=new Map<string,number>(configured?JSON.parse(configured.priorities_json):[]);
    if (!row) return {version:0,maxWaitMs:60_000,weights:new Map<string,number>(),priorities};
    return {version:row.version,maxWaitMs:row.wait_ms,weights:new Map<string,number>(JSON.parse(row.weights_json)),priorities};
  }

  readGuaranteedShares(): Record<string,CapacityResources> {
    return Object.fromEntries(this.storage.sql.exec<{tenant_id:string;resources_json:string}>(
      'SELECT tenant_id,resources_json FROM capacity_guaranteed_shares WHERE pool_id=?',this.poolId,
    ).toArray().map(row=>[row.tenant_id,capacityResources(JSON.parse(row.resources_json))]));
  }

  writePolicy(expectedVersion: number, maxWaitMs: number, weights: Record<string, number>,
    guarantee?: {shares:Record<string,CapacityResources>;limit:CapacityResources}, priorities?:Record<string,number>) {
    if (!Number.isSafeInteger(maxWaitMs) || maxWaitMs < 1_000 || maxWaitMs > 3_600_000 ||
        !weights || typeof weights !== 'object' || Array.isArray(weights)) {
      throw new ControlError('INVALID_CAPACITY_QUEUE_POLICY',422);
    }
    const entries = Object.entries(weights);
    for (const [tenant,weight] of entries) {
      capacityId(tenant);
      if (!Number.isSafeInteger(weight) || weight < 1 || weight > 1000) {
        throw new ControlError('INVALID_CAPACITY_QUEUE_POLICY',422);
      }
    }
    return this.storage.transactionSync(() => {
      const current = this.readPolicy();
      assertExpectedVersion(expectedVersion,current.version);
      this.storage.sql.exec('INSERT OR REPLACE INTO capacity_queue_policy VALUES (?, ?, ?, ?)',
        this.poolId,current.version+1,maxWaitMs,JSON.stringify(entries));
      if (guarantee) {
        if (!guarantee.shares || typeof guarantee.shares!=='object' || Array.isArray(guarantee.shares))
          throw new ControlError('INVALID_CAPACITY_QUEUE_POLICY',422);
        const entries=Object.entries(guarantee.shares).map(([tenant,resources])=>{
          capacityId(tenant);
          return [tenant,capacityResources(resources)] as const;
        });
        if (dimensions.some(dimension=>entries.reduce((total,[,share])=>total+share[dimension],0)>guarantee.limit[dimension]))
          throw new ControlError('CAPACITY_GUARANTEES_EXCEED_POOL',422);
        this.storage.sql.exec('DELETE FROM capacity_guaranteed_shares WHERE pool_id=?',this.poolId);
        for (const [tenant,share] of entries) {
          this.storage.sql.exec('INSERT INTO capacity_guaranteed_shares VALUES (?,?,?)',
            this.poolId,tenant,JSON.stringify(share));
        }
      }
      if (priorities!==undefined) {
        if (!priorities || typeof priorities!=='object' || Array.isArray(priorities))
          throw new ControlError('INVALID_CAPACITY_QUEUE_POLICY',422);
        const entries=Object.entries(priorities);
        for (const [tenant,priority] of entries) {
          capacityId(tenant);
          if (!Number.isSafeInteger(priority)||priority<0||priority>100)
            throw new ControlError('INVALID_CAPACITY_QUEUE_POLICY',422);
        }
        this.storage.sql.exec('INSERT OR REPLACE INTO capacity_queue_priorities VALUES (?,?)',
          this.poolId,JSON.stringify(entries));
      }
      return this.readPolicy();
    });
  }

  submit(tenantId: string, key: string, demand: CapacityResources, priority: number, now: number, queueLimit?: number | null): CapacityRequest {
    capacityId(tenantId);
    capacityId(key);
    const resources = JSON.stringify(capacityResources(demand, true));
    if (!Number.isSafeInteger(priority) || !Number.isSafeInteger(now) || now < 0) {
      throw new ControlError('INVALID_CAPACITY_INPUT', 422);
    }
    const id = createHash('sha256').update(JSON.stringify([this.poolId, tenantId, key])).digest('hex');
    return this.storage.transactionSync(() => {
      const previous = this.find(tenantId, id);
      if (previous) {
        if (previous.resources !== resources) throw new ControlError('IDEMPOTENCY_CONFLICT', 409);
        return deserialize(previous);
      }
      if(queueLimit!==undefined){
        if(queueLimit===null||!Number.isSafeInteger(queueLimit)||queueLimit<0)
          throw new ControlError('CAPACITY_QUOTA_UNAVAILABLE',503);
        const [{queued}]=this.storage.sql.exec<{queued:number}>(`
          SELECT COUNT(*) AS queued FROM capacity_requests r
          WHERE r.tenant_id=? AND r.state='QUEUED'
            AND NOT EXISTS (SELECT 1 FROM capacity_request_grants g WHERE g.pool_id=r.pool_id AND g.request_id=r.id)
            AND NOT EXISTS (SELECT 1 FROM capacity_grant_bindings b WHERE b.pool_id=r.pool_id AND b.request_id=r.id)
        `,tenantId).toArray();
        if(queued>=queueLimit)throw new ControlError('QUEUE_QUOTA_EXCEEDED',409);
      }
      this.storage.sql.exec(
        `INSERT INTO capacity_requests VALUES (?, ?, ?, ?, ?, ?, 'QUEUED')`,
        this.poolId, id, tenantId, resources, now, priority,
      );
      return deserialize({ id, tenant_id: tenantId, resources, enqueued_at: now, priority, state: 'QUEUED' });
    });
  }

  list(tenantId: string): CapacityRequest[] {
    capacityId(tenantId);
    return this.storage.sql.exec<RequestRow>(
      `SELECT r.*, CASE WHEN g.request_id IS NULL THEN CASE WHEN b.request_id IS NULL THEN r.state ELSE 'RELEASED' END ELSE 'GRANTED' END AS state,
       CASE WHEN g.request_id IS NOT NULL AND b.request_id IS NULL THEN e.expires_at END AS grant_expires_at
       FROM capacity_requests r
       LEFT JOIN capacity_request_grants g ON g.pool_id = r.pool_id AND g.request_id = r.id
       LEFT JOIN capacity_grant_bindings b ON b.pool_id = r.pool_id AND b.request_id = r.id
       LEFT JOIN capacity_grant_expiry e ON e.pool_id = r.pool_id AND e.request_id = r.id
       WHERE r.pool_id = ? AND r.tenant_id = ? ORDER BY r.enqueued_at, r.id`,
      this.poolId, tenantId,
    ).toArray().map(deserialize);
  }

  select(
    snapshot: Parameters<typeof selectCapacityRequest>[1],
    policy: Parameters<typeof selectCapacityRequest>[2],
    now: number,
  ): ReturnType<typeof selectCapacityRequest> {
    const queued = this.storage.sql.exec<RequestRow>(
      `SELECT r.* FROM capacity_requests r WHERE r.pool_id = ? AND r.state = 'QUEUED'
       AND NOT EXISTS (SELECT 1 FROM capacity_request_grants g WHERE g.pool_id = r.pool_id AND g.request_id = r.id)`,
      this.poolId,
    ).toArray().map(deserialize);
    return selectCapacityRequest(queued, snapshot, policy, now);
  }
  selectFromInventory(
    inventory: ReturnType<CapacityInventory['snapshot']>,
    tenants: Pick<Parameters<typeof selectCapacityRequest>[1], 'tenantRemaining' | 'tenantUsage' | 'tenantPoolAvailable'>,
    policy: Parameters<typeof selectCapacityRequest>[2],
    now: number,
  ): ReturnType<typeof selectCapacityRequest> {
    const pool = inventory.pools.find(candidate => candidate.id === this.poolId);
    if (!pool || pool.status !== 'ACTIVE' || inventory.asOf !== now) return null;

    const hosts = inventory.hosts
      .filter(host => host.poolId === this.poolId && host.health === 'HEALTHY')
      .map(host => ({ id: host.id, capacity: host.lastKnownCapacity, available: host.available }));

    return this.select({
      ...tenants,
      poolCapacity: pool.capacity,
      poolAvailable: pool.available,
      hosts,
    }, policy, now);
  }

  accountForGrants(inventory: ReturnType<CapacityInventory['snapshot']>) {
    const snapshot = structuredClone(inventory);
    const pool = snapshot.pools.find(candidate => candidate.id === this.poolId);
    const grants = this.storage.sql.exec<RequestRow & {host_id: string; reservation_id: string | null}>(
      `SELECT r.*, g.host_id, b.reservation_id FROM capacity_request_grants g
       JOIN capacity_requests r ON r.pool_id = g.pool_id AND r.id = g.request_id
       LEFT JOIN capacity_grant_bindings b ON b.pool_id = g.pool_id AND b.request_id = g.request_id
       WHERE g.pool_id = ?`, this.poolId,
    ).toArray();
    const borrowed = {cpuMilliCores:0,memoryMiB:0,slots:0};
    if (pool) for (const row of grants) {
      const {resources} = deserialize(row);
      const host = snapshot.hosts.find(candidate => candidate.id === row.host_id);
      for (const dimension of dimensions) {
        borrowed[dimension] += resources[dimension];
        pool.available[dimension] = Math.max(0,pool.available[dimension]-resources[dimension]);
        if (host) host.available[dimension] = Math.max(0,host.available[dimension]-resources[dimension]);
      }
    }
    return {snapshot,grants,borrowed};
  }

  poolAvailabilityFor(poolAvailable:CapacityResources, grants:readonly RequestRow[]) {
    const unused = new Map(Object.entries(this.readGuaranteedShares()).map(([tenant,share])=>[tenant,{...share}]));
    for (const row of grants) {
      const share = unused.get(row.tenant_id);
      if (!share) continue;
      const resources = deserialize(row).resources;
      for (const dimension of dimensions) share[dimension] = Math.max(0,share[dimension]-resources[dimension]);
    }
    const protectedCapacity = {cpuMilliCores:0,memoryMiB:0,slots:0};
    for (const share of unused.values()) {
      for (const dimension of dimensions) protectedCapacity[dimension] += share[dimension];
    }
    return (tenantId:string):CapacityResources => {
      const available = {...poolAvailable};
      for (const dimension of dimensions) {
        available[dimension] = Math.max(0,available[dimension]-protectedCapacity[dimension]+(unused.get(tenantId)?.[dimension]??0));
      }
      return available;
    };
  }

  /** Reserve pool capacity locally. The tenant authority must still authorize execution. */
  issueGrant(
    inventory: ReturnType<CapacityInventory['snapshot']>,
    tenants: Pick<Parameters<typeof selectCapacityRequest>[1], 'tenantRemaining' | 'tenantUsage'> & {activeReservationIds?:ReadonlyMap<string,ReadonlySet<string>>},
    policy: Parameters<typeof selectCapacityRequest>[2],
    now: number,
  ): ReturnType<typeof selectCapacityRequest> {
    return this.storage.transactionSync(() => {
      this.expireUnboundGrants(inventory,now);
      if (!this.readAllocationControl().enabled) return null;
      const {snapshot,grants}=this.accountForGrants(inventory);
      const usage = new Map(tenants.tenantUsage);
      const remaining = new Map(tenants.tenantRemaining);
      for (const row of grants) {
        const request = deserialize(row);
        if (row.reservation_id && tenants.activeReservationIds?.get(request.tenantId)?.has(row.reservation_id)) continue;
        const held = {...(usage.get(request.tenantId) ?? {cpuMilliCores:0,memoryMiB:0,slots:0})};
        const quota = remaining.get(request.tenantId);
        const balance = quota && {...quota};
        for (const dimension of dimensions) {
          held[dimension] += request.resources[dimension];
          if (balance) balance[dimension] = Math.max(0,balance[dimension]-request.resources[dimension]);
        }
        usage.set(request.tenantId,held);
        if (balance) remaining.set(request.tenantId,balance);
      }
      const tenantPoolAvailable = new Map<string,CapacityResources>();
      const pool = snapshot.pools.find(item=>item.id===this.poolId);
      if (!pool) return null;
      const availableFor = this.poolAvailabilityFor(pool.available,grants);
      for (const tenant of remaining.keys()) tenantPoolAvailable.set(tenant,availableFor(tenant));
      const grant = this.selectFromInventory(snapshot,{
        tenantUsage:usage,tenantRemaining:remaining,tenantPoolAvailable,
      },policy,now);
      if (grant) {
        const host = snapshot.hosts.find(item => item.id === grant.hostId);
        const report = host?.reports.find(item => item.daemonId === host.daemonIds[0]);
        if (!report || report.epoch === null) return null;
        this.storage.sql.exec('INSERT INTO capacity_grant_epochs VALUES (?, ?, ?, ?)',
          this.poolId, grant.requestId, report.daemonId, report.epoch,
        );
        this.storage.sql.exec(
          'INSERT INTO capacity_request_grants VALUES (?, ?, ?, ?)',
          this.poolId, grant.requestId, grant.hostId, grant.policyVersion,
        );
        this.storage.sql.exec('INSERT INTO capacity_grant_expiry VALUES (?, ?, ?)',
          this.poolId, grant.requestId, now + 60_000,
        );
      }
      return grant;
    });
  }

  /** Called with a reservation read from the tenant authority. */
  bindGrant(requestId: string, reservation: Pick<AdmissionReservation, 'id' | 'attemptId' | 'tenantId' | 'generation' | 'fencing' | 'phase' | 'expiresAt' | 'resources'>, now: number): string {
    capacityId(requestId);
    return this.storage.transactionSync(() => {
      const expiry = this.storage.sql.exec<{expires_at: number}>(
        'SELECT expires_at FROM capacity_grant_expiry WHERE pool_id = ? AND request_id = ?',
        this.poolId, requestId,
      ).toArray()[0];
      const request = this.find(reservation.tenantId, requestId);
      if (!request || request.state !== 'GRANTED') throw new ControlError('CAPACITY_GRANT_NOT_FOUND', 404);
      const demand = deserialize(request).resources;
      const existing = this.storage.sql.exec<{
        pool_id: string; request_id: string; reservation_id: string;
        attempt_id: string; generation: number; fencing: number;
      }>(`SELECT * FROM capacity_grant_bindings
          WHERE (pool_id = ? AND request_id = ?) OR (tenant_id = ? AND (reservation_id = ? OR attempt_id = ?))`,
        this.poolId, requestId, reservation.tenantId, reservation.id, reservation.attemptId,
      ).toArray();
      const receipt = 'capacity-grant:'+requestId;
      if (existing.length) {
        if (existing.length === 1 && existing[0].pool_id === this.poolId &&
            existing[0].request_id === requestId && existing[0].reservation_id === reservation.id &&
            existing[0].attempt_id === reservation.attemptId && existing[0].generation === reservation.generation &&
            existing[0].fencing === reservation.fencing) return receipt;
        throw new ControlError('CAPACITY_GRANT_BINDING_CONFLICT', 409);
      }
      if (!expiry || expiry.expires_at <= now || reservation.phase !== 'PREPARING' || reservation.expiresAt <= now ||
          demand.cpuMilliCores !== reservation.resources.cpuMilliCores ||
          demand.memoryMiB !== reservation.resources.memoryMiB ||
          demand.slots !== reservation.resources.concurrentAttempts) {
        throw new ControlError('CAPACITY_RESERVATION_MISMATCH', 409);
      }
      this.storage.sql.exec('INSERT INTO capacity_grant_bindings VALUES (?, ?, ?, ?, ?, ?, ?)',
        this.poolId, requestId, reservation.tenantId, reservation.id, reservation.attemptId,
        reservation.generation, reservation.fencing,
      );
      return receipt;
    });
  }

  /** Release from a terminal reservation read from the trusted tenant authority. */
  releaseTerminatedGrant(requestId: string, reservation: Pick<AdmissionReservation,
    'tenantId' | 'id' | 'attemptId' | 'generation' | 'fencing' | 'phase' | 'terminalEvidence' | 'capacityReceipt' | 'executionObserved' | 'startAuthorized'
  >): void {
    capacityId(requestId);
    this.storage.transactionSync(() => {
      // The tenant authority advances generation when accepting authenticated NOT_STARTED evidence.
      const neverStarted = reservation.phase === 'CANCELLED' && !reservation.executionObserved;
      if ((!neverStarted && !['SETTLING', 'SETTLED'].includes(reservation.phase)) || (!reservation.terminalEvidence && !(neverStarted && !reservation.startAuthorized)) ||
          reservation.capacityReceipt !== 'capacity-grant:'+requestId) {
        throw new ControlError('CAPACITY_EXIT_EVIDENCE_REQUIRED', 409);
      }
      const bound = this.storage.sql.exec<{request_id: string}>(
        `SELECT request_id FROM capacity_grant_bindings WHERE pool_id = ? AND request_id = ?
          AND tenant_id = ? AND reservation_id = ? AND attempt_id = ? AND generation = ? AND fencing = ?`,
        this.poolId, requestId, reservation.tenantId, reservation.id, reservation.attemptId,
        reservation.generation - (neverStarted ? 1 : 0), reservation.fencing,
      ).toArray()[0];
      if (!bound) {
        const request = this.find(reservation.tenantId, requestId);
        const other = this.storage.sql.exec(
          'SELECT request_id FROM capacity_grant_bindings WHERE pool_id=? AND request_id=?',
          this.poolId, requestId,
        ).toArray()[0];
        if (!neverStarted || reservation.startAuthorized || !request || other) {
          throw new ControlError('CAPACITY_GRANT_BINDING_CONFLICT', 409);
        }
        // Cancel the original request before a delayed bind can acquire it.
        this.storage.sql.exec('DELETE FROM capacity_grant_epochs WHERE pool_id=? AND request_id=?',
          this.poolId, requestId);
      }

      this.storage.sql.exec("UPDATE capacity_requests SET state = 'CANCELLED' WHERE pool_id = ? AND id = ?",
        this.poolId, requestId);
      this.storage.sql.exec('DELETE FROM capacity_request_grants WHERE pool_id = ? AND request_id = ?',
        this.poolId, requestId);
      this.storage.sql.exec('DELETE FROM capacity_grant_expiry WHERE pool_id = ? AND request_id = ?',
        this.poolId, requestId);
    });
  }

  cancel(tenantId: string, id: string): CapacityRequest {
    capacityId(tenantId);
    capacityId(id);
    return this.storage.transactionSync(() => {
      const row = this.find(tenantId, id);
      if (row?.state === 'GRANTED' || row?.state === 'RELEASED') throw new ControlError('CAPACITY_REQUEST_ALREADY_GRANTED', 409);
      if (!row) throw new ControlError('CAPACITY_REQUEST_NOT_FOUND', 404);
      this.storage.sql.exec(
        "UPDATE capacity_requests SET state = 'CANCELLED' WHERE pool_id = ? AND tenant_id = ? AND id = ?",
        this.poolId, tenantId, id,
      );
      return deserialize({ ...row, state: 'CANCELLED' });
    });
  }

  private expireUnboundGrants(inventory: ReturnType<CapacityInventory['snapshot']>, now: number): void {
    if (inventory.asOf !== now) return;
    const epochs = new Map(inventory.hosts.filter(host => host.poolId === this.poolId)
      .flatMap(host => host.reports.map(report => [report.daemonId, report.epoch] as const)));
    const expired = this.storage.sql.exec<{request_id:string; expires_at:number; daemon_id:string|null; epoch:number|null}>(
      `SELECT e.request_id,e.expires_at,f.daemon_id,f.epoch FROM capacity_grant_expiry e
       LEFT JOIN capacity_grant_epochs f ON f.pool_id=e.pool_id AND f.request_id=e.request_id
       WHERE e.pool_id = ? AND NOT EXISTS (
         SELECT 1 FROM capacity_grant_bindings b WHERE b.pool_id = e.pool_id AND b.request_id = e.request_id
       )`, this.poolId,
    ).toArray();
    for (const {request_id: id, expires_at, daemon_id, epoch} of expired) {
      const currentEpoch = daemon_id === null ? null : epochs.get(daemon_id);
      if (expires_at > now && !(currentEpoch != null && epoch != null && currentEpoch > epoch)) continue;
      this.storage.sql.exec('DELETE FROM capacity_grant_epochs WHERE pool_id = ? AND request_id = ?', this.poolId, id);
      this.storage.sql.exec("UPDATE capacity_requests SET state = 'CANCELLED' WHERE pool_id = ? AND id = ?", this.poolId, id);
      this.storage.sql.exec('DELETE FROM capacity_request_grants WHERE pool_id = ? AND request_id = ?', this.poolId, id);
      this.storage.sql.exec('DELETE FROM capacity_grant_expiry WHERE pool_id = ? AND request_id = ?', this.poolId, id);
    }
  }

  private find(tenantId: string, id: string): RequestRow | undefined {
    return this.storage.sql.exec<RequestRow>(
      `SELECT r.*, CASE WHEN g.request_id IS NULL THEN CASE WHEN b.request_id IS NULL THEN r.state ELSE 'RELEASED' END ELSE 'GRANTED' END AS state,
       CASE WHEN g.request_id IS NOT NULL AND b.request_id IS NULL THEN e.expires_at END AS grant_expires_at
       FROM capacity_requests r
       LEFT JOIN capacity_request_grants g ON g.pool_id = r.pool_id AND g.request_id = r.id
       LEFT JOIN capacity_grant_bindings b ON b.pool_id = r.pool_id AND b.request_id = r.id
       LEFT JOIN capacity_grant_expiry e ON e.pool_id = r.pool_id AND e.request_id = r.id
       WHERE r.pool_id = ? AND r.tenant_id = ? AND r.id = ?`,
      this.poolId, tenantId, id,
    ).toArray()[0];
  }
}
























