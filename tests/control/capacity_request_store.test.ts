import { CapacityInventory } from '../../src/cluster/capacity_inventory.js';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { CapacityRequestStore } from '../../src/cluster/capacity_request_store.js';
import type { DirectoryStorage } from '../../src/control/tenant_directory.js';

const { DatabaseSync } = createRequire(join(process.cwd(), 'package.json'))('node:sqlite');
const demand = { cpuMilliCores: 100, memoryMiB: 200, slots: 1 };

for (const mode of ['bound', 'expiry', 'restart', 'not-started', 'cancel-before-start', 'cancel-before-bind']) test('capacity grant recovery: '+mode, () => {
  const dir = mkdtempSync(join(tmpdir(), 'ansts-capacity-requests-'));
  const path = join(dir, 'requests.sqlite');
  let db = new DatabaseSync(path);
  const storage: DirectoryStorage = {
    sql: {
      exec<T>(query: string, ...bindings: unknown[]) {
        const rows = db.prepare(query).all(...bindings) as T[];
        return { toArray: () => rows };
      },
    },
    transactionSync<T>(callback: () => T) {
      db.exec('BEGIN IMMEDIATE');
      try {
        const result = callback();
        db.exec('COMMIT');
        return result;
      } catch (error) {
        db.exec('ROLLBACK');
        throw error;
      }
    },
  };
  try {
    const first = new CapacityRequestStore(storage, 'pool-a');
    const original = first.submit('tenant-a', 'key', demand, 3, 10);
    const second = first.submit('tenant-b', 'key', demand, 1, 11);
    assert.notEqual(original.id, second.id);
    db.close();
    db = new DatabaseSync(path);

    const reopened = new CapacityRequestStore(storage, 'pool-a');
    assert.deepEqual(reopened.submit('tenant-a', 'key', demand, 0, 20), original);
    assert.throws(() => reopened.submit('tenant-a', 'key', { ...demand, slots: 2 }, 3, 20),
      { code: 'IDEMPOTENCY_CONFLICT' });
    assert.deepEqual(reopened.list('tenant-b'), [second]);
    assert.deepEqual(new CapacityRequestStore(storage, 'pool-b').list('tenant-a'), []);
    assert.throws(() => reopened.cancel('tenant-b', original.id), { code: 'CAPACITY_REQUEST_NOT_FOUND' });

    if(mode==='restart'){
      const otherPool=new CapacityRequestStore(storage,'pool-b');
      assert.deepEqual(reopened.submit('tenant-a','key',demand,0,20,null),original);
      assert.throws(()=>otherPool.submit('tenant-a','new',demand,0,20,1),{code:'QUEUE_QUOTA_EXCEEDED'});
      assert.throws(()=>otherPool.submit('tenant-a','new',demand,0,20,null),{code:'CAPACITY_QUOTA_UNAVAILABLE'});
      const shortQueue=otherPool.submit('tenant-c','short',demand,0,20,1);
      otherPool.cancel('tenant-c',shortQueue.id);
      assert.equal(reopened.submit('tenant-c','replacement',demand,0,21,1).state,'QUEUED');
    }
    const snapshot = {
      poolCapacity: demand,
      poolAvailable: demand,
      hosts: [{ id: 'host', capacity: demand, available: demand }],
      tenantUsage: new Map(),
      tenantRemaining: new Map([['tenant-a', demand], ['tenant-b', demand]]),
    };
    const policy = { version: 1, maxWaitMs: 100, weights: new Map() };
    assert.equal(reopened.select(snapshot, policy, 20)?.requestId, original.id);
    assert.equal(reopened.cancel('tenant-a', original.id).state, 'CANCELLED');
    assert.equal(reopened.submit('tenant-a', 'key', demand, 3, 21).state, 'CANCELLED');
    assert.equal(reopened.select(snapshot, policy, 21)?.requestId, second.id);

    const now = 30;
    const admin = {sub:'test-admin',role:'SRE_ADMIN' as const,scopeTenant:'all',scopeRegion:'all'};
    const inventory = new CapacityInventory(storage);
    const mutation = {expectedVersion:0,reason:'fixture',requestId:'test',idempotencyKey:'pool'};
    inventory.writePool(admin,{...mutation,id:'pool-a',name:'Test',region:'dev',status:'ACTIVE',limit:demand},now);
    inventory.writeHost(admin,{
      ...mutation,idempotencyKey:'host',id:'host',poolId:'pool-a',daemonIds:['daemon'],status:'ACTIVE',limit:demand,
    },now);
    inventory.report('daemon',{
      schemaVersion:1,purpose:'ansts-capacity-report-v1',hostId:'host',daemonId:'daemon',
      epoch:1,sequence:1,observedAt:now,total:demand,reserved:{cpuMilliCores:0,memoryMiB:0,slots:0},
      draining:false,recoveryRequired:false,
    },now);
    const third = reopened.submit('tenant-a','another',demand,0,now);
    const live = inventory.snapshot(admin,now);
    assert.deepEqual(reopened.writeAllocationControl(0,false),{version:1,enabled:false});
    assert.equal(reopened.issueGrant(live,snapshot,policy,now),null);
    assert.throws(() => reopened.writeAllocationControl(0,true),{code:'VERSION_CONFLICT'});
    assert.deepEqual(reopened.writeAllocationControl(1,true),{version:2,enabled:true});
    assert.equal(reopened.issueGrant(live,snapshot,policy,now)?.requestId,second.id);
    assert.equal(reopened.list('tenant-b')[0]?.state,'GRANTED');
    assert.throws(() => reopened.cancel('tenant-b',second.id),{code:'CAPACITY_REQUEST_ALREADY_GRANTED'});
    db.close();
    db = new DatabaseSync(path);
    const recovered = new CapacityRequestStore(storage,'pool-a');
    assert.equal(recovered.issueGrant(live,snapshot,policy,now),null);
    assert.deepEqual(recovered.list('tenant-a').find(item=>item.id===third.id),third);
    const reservation: Parameters<typeof recovered.bindGrant>[1] = {
      id:'reservation', attemptId:'attempt', tenantId:'tenant-b', generation:1, fencing:7,
      phase:'PREPARING', expiresAt:100,
      resources:{cpuMilliCores:demand.cpuMilliCores, memoryMiB:demand.memoryMiB, concurrentAttempts:1, queuedTasks:0},
    };
    assert.throws(() => recovered.bindGrant(second.id,{...reservation,expiresAt:now},now),
      {code:'CAPACITY_RESERVATION_MISMATCH'});
    assert.throws(() => recovered.bindGrant(second.id,{...reservation,tenantId:'tenant-a'},now),
      {code:'CAPACITY_GRANT_NOT_FOUND'});
    const expiry = now + 60_000;
    function refreshedSnapshot(at: number, sequence: number, epoch = 1) {
      inventory.report('daemon',{
        schemaVersion:1,purpose:'ansts-capacity-report-v1',hostId:'host',daemonId:'daemon',
        epoch,sequence,observedAt:at,total:demand,reserved:{cpuMilliCores:0,memoryMiB:0,slots:0},
        draining:false,recoveryRequired:false,
      },at);
      return inventory.snapshot(admin,at);
    }
    if (mode === 'cancel-before-bind') {
      const cancelled = {
        ...reservation, phase:'CANCELLED' as const, generation:reservation.generation+1,
        startAuthorized:false, executionObserved:false, terminalEvidence:null,
        capacityReceipt:'capacity-grant:'+second.id,
      };
      recovered.releaseTerminatedGrant(second.id,cancelled);
      assert.equal(recovered.list('tenant-b')[0]?.state,'CANCELLED');
      assert.throws(() => recovered.bindGrant(second.id,reservation,now),{code:'CAPACITY_GRANT_NOT_FOUND'});
      assert.equal(recovered.issueGrant(live,snapshot,policy,now)?.requestId,third.id);
      recovered.releaseTerminatedGrant(second.id,cancelled);
      assert.equal(recovered.list('tenant-a').find(item=>item.id===third.id)?.state,'GRANTED');
      return;
    }
    if (mode === 'restart') {
      assert.equal(recovered.issueGrant(refreshedSnapshot(now+1,1,2),snapshot,policy,now+1)?.requestId,third.id);
      assert.equal(recovered.list('tenant-b')[0]?.state,'CANCELLED');
      assert.throws(() => recovered.bindGrant(second.id,reservation,now+1),{code:'CAPACITY_GRANT_NOT_FOUND'});
      return;
    }
    if (mode === 'expiry') {
      assert.equal(recovered.issueGrant(refreshedSnapshot(expiry-1,2),snapshot,policy,expiry-1),null);
      assert.throws(() => recovered.bindGrant(second.id,{...reservation,expiresAt:expiry+1},expiry),
        {code:'CAPACITY_RESERVATION_MISMATCH'});
      assert.equal(recovered.issueGrant(refreshedSnapshot(expiry,3),snapshot,policy,expiry)?.requestId,third.id);
      assert.equal(recovered.list('tenant-b')[0]?.state,'CANCELLED');
      return;
    }
    const receipt = recovered.bindGrant(second.id,reservation,now);
    db.close();
    db = new DatabaseSync(path);
    const afterBinding = new CapacityRequestStore(storage,'pool-a');
    assert.equal(afterBinding.bindGrant(second.id,{...reservation,phase:'RESERVED'},now+1),receipt);
    assert.equal(afterBinding.issueGrant(refreshedSnapshot(expiry,2),snapshot,policy,expiry),null);
    const doubled = {cpuMilliCores:demand.cpuMilliCores*2,memoryMiB:demand.memoryMiB*2,slots:2};
    inventory.writePool(admin,{
      ...mutation,expectedVersion:1,idempotencyKey:'pool-grow',id:'pool-a',name:'Test',
      region:'dev',status:'ACTIVE',limit:doubled,
    },expiry);
    inventory.writeHost(admin,{
      ...mutation,expectedVersion:1,idempotencyKey:'host-grow',id:'host',poolId:'pool-a',
      daemonIds:['daemon'],status:'ACTIVE',limit:doubled,
    },expiry);
    inventory.report('daemon',{
      schemaVersion:1,purpose:'ansts-capacity-report-v1',hostId:'host',daemonId:'daemon',
      epoch:1,sequence:3,observedAt:expiry,total:doubled,reserved:{cpuMilliCores:0,memoryMiB:0,slots:0},
      draining:false,recoveryRequired:false,
    },expiry);
    const sibling=afterBinding.submit('tenant-b','sibling',demand,0,expiry);
    const headroom={
      tenantRemaining:new Map([['tenant-b',demand]]),
      tenantUsage:new Map([['tenant-b',demand]]),
    };
    const expanded=inventory.snapshot(admin,expiry);
    assert.equal(afterBinding.issueGrant(expanded,headroom,policy,expiry),null);
    assert.equal(afterBinding.issueGrant(expanded,{
      ...headroom,activeReservationIds:new Map([['tenant-b',new Set([reservation.id])]]),
    },policy,expiry)?.requestId,sibling.id);
    for (const change of [{fencing:6}, {generation:2}, {attemptId:'different'}, {id:'different'}]) {
      assert.throws(() => afterBinding.bindGrant(second.id,{...reservation,...change},now+1),
        {code:'CAPACITY_GRANT_BINDING_CONFLICT'});
    }
    const terminal = {
      ...reservation, phase:'SETTLING' as const, capacityReceipt:receipt, terminalEvidence:'exit-proof', executionObserved:true, startAuthorized:true,
    };
    for (const input of [{...terminal,phase:'UNCERTAIN' as const},{...terminal,terminalEvidence:null}]) {
      assert.throws(() => afterBinding.releaseTerminatedGrant(second.id,input),
        {code:'CAPACITY_EXIT_EVIDENCE_REQUIRED'});
    }
    assert.throws(() => afterBinding.releaseTerminatedGrant(second.id,{...terminal,fencing:6}),
      {code:'CAPACITY_GRANT_BINDING_CONFLICT'});
    assert.equal(afterBinding.list('tenant-b')[0]?.state,'GRANTED');
    afterBinding.releaseTerminatedGrant(second.id,mode==='not-started'
      ? {...terminal,phase:'CANCELLED',executionObserved:false,generation:reservation.generation+1}
      : mode==='cancel-before-start'
        ? {...terminal,phase:'CANCELLED',executionObserved:false,startAuthorized:false,terminalEvidence:null,generation:reservation.generation+1}
        : terminal);
    assert.equal(afterBinding.list('tenant-b')[0]?.state,'RELEASED');
    assert.equal(afterBinding.issueGrant(inventory.snapshot(admin,expiry+1),snapshot,policy,expiry+1)?.requestId,third.id);
    afterBinding.releaseTerminatedGrant(second.id,mode==='not-started'
      ? {...terminal,phase:'CANCELLED',executionObserved:false,generation:reservation.generation+1}
      : mode==='cancel-before-start'
        ? {...terminal,phase:'CANCELLED',executionObserved:false,startAuthorized:false,terminalEvidence:null,generation:reservation.generation+1}
        : terminal);
    assert.equal(afterBinding.list('tenant-a').find(item=>item.id===third.id)?.state,'GRANTED');
    assert.throws(() => afterBinding.bindGrant(second.id,reservation,now),
      {code:'CAPACITY_GRANT_NOT_FOUND'});
  } finally {
    db.close();
    rmSync(dir, { recursive: true, force: true });
  }
});













