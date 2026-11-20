import assert from 'node:assert/strict';
import { test } from 'node:test';
import { selectCapacityRequest, type CapacityQueueEntry } from '../../src/cluster/capacity_queue.js';

const resources = (cpuMilliCores: number, memoryMiB: number, slots = 1) =>
  ({ cpuMilliCores, memoryMiB, slots });

const entry = (id: string, demand: ReturnType<typeof resources>, enqueuedAt = 0): CapacityQueueEntry =>
  ({ id, tenantId: id, resources: demand, enqueuedAt, priority: 0 });

const cpu = entry('cpu', resources(80, 10));
const memory = entry('memory', resources(10, 70), 10);
const capacity = resources(100, 100, 4);
const policy = { version: 2, maxWaitMs: 100, weights: new Map([['cpu', 2]]) };
const snapshot = {
  poolCapacity: capacity,
  poolAvailable: capacity,
  hosts: [{ id: 'first', capacity, available: capacity }],
  tenantRemaining: new Map([['cpu', capacity], ['memory', capacity]]),
  tenantUsage: new Map<string, ReturnType<typeof resources>>(),
};

test('weighted dominant share determines the allocation order and includes current usage', () => {
  assert.deepEqual(selectCapacityRequest([memory, cpu], snapshot, policy, 20),
    { requestId: 'cpu', hostId: 'first', policyVersion: 2 });
  assert.equal(selectCapacityRequest([memory, cpu], {
    ...snapshot,
    tenantUsage: new Map([['cpu', resources(75, 0)]]),
  }, policy, 20)?.requestId, 'memory');
});

test('aggregate availability cannot satisfy a request spread over hosts', () => {
  const split = {
    ...snapshot,
    hosts: [
      { id: 'first', capacity, available: resources(80, 20) },
      { id: 'second', capacity, available: resources(20, 80) },
    ],
  };
  assert.equal(selectCapacityRequest([entry('cpu', resources(50, 50))], split, policy, 20), null);
});

test('an aged eligible request stops bypass until it fits, but a quota denial does not', () => {
  const constrained = {
    ...snapshot,
    hosts: [{ id: 'first', capacity, available: resources(30, 80) }],
  };
  assert.equal(selectCapacityRequest([memory, cpu], constrained, policy, 50)?.requestId, 'memory');
  assert.equal(selectCapacityRequest([memory, cpu], constrained, policy, 105), null);
  assert.equal(selectCapacityRequest([memory, cpu], snapshot, policy, 105)?.requestId, 'cpu');

  const quotaDenied = { ...constrained, tenantRemaining: new Map([['memory', capacity]]) };
  assert.equal(selectCapacityRequest([memory, cpu], quotaDenied, policy, 105)?.requestId, 'memory');
});


test('an aged request larger than every host cannot prevent a feasible request', () => {
  const fragmented = {
    ...snapshot,
    hosts: [{ id: 'small', capacity: resources(40, 80), available: resources(40, 80) }],
  };
  assert.equal(selectCapacityRequest([cpu, memory], fragmented, policy, 150)?.requestId, 'memory');
});

test('fixed mixed CPU and memory workload produces a versioned allocation trace', () => {
  const workload = [
    {...entry('cpu-1',resources(40,5)),tenantId:'cpu'},
    {...entry('cpu-2',resources(40,5),1),tenantId:'cpu'},
    {...entry('memory-1',resources(5,40),2),tenantId:'memory'},
    {...entry('memory-2',resources(5,40),3),tenantId:'memory'},
  ];
  const dimensions = ['cpuMilliCores','memoryMiB','slots'] as const;
  function trace(weights:Map<string,number>,version:number) {
    const queued=[...workload],available={...capacity};
    const tenantUsage=new Map<string,ReturnType<typeof resources>>();
    const tenantRemaining=new Map([['cpu',{...capacity}],['memory',{...capacity}]]);
    const selected:string[]=[];
    while(queued.length){
      const grant=selectCapacityRequest(queued,{
        ...snapshot,poolAvailable:available,hosts:[{id:'first',capacity,available}],tenantUsage,tenantRemaining,
      },{...policy,weights,version},20);
      assert.ok(grant);
      assert.equal(grant.policyVersion,version);
      const [request]=queued.splice(queued.findIndex(item=>item.id===grant.requestId),1);
      const used=tenantUsage.get(request.tenantId)??resources(0,0,0);
      const remaining=tenantRemaining.get(request.tenantId)!;
      for (const dimension of dimensions) {
        available[dimension]-=request.resources[dimension];
        used[dimension]+=request.resources[dimension];
        remaining[dimension]-=request.resources[dimension];
        assert.ok(available[dimension]>=0);
      }
      tenantUsage.set(request.tenantId,used);
      selected.push(request.id);
    }
    return selected;
  }
  assert.deepEqual(trace(new Map(),1),['cpu-1','memory-1','cpu-2','memory-2']);
  assert.deepEqual(trace(new Map([['cpu',2]]),2),['cpu-1','cpu-2','memory-1','memory-2']);
});

test('configured tenant priority applies before fairness until a request ages', () => {
  const configured={...policy,priorities:new Map([['memory',10]])};
  assert.equal(selectCapacityRequest([cpu,memory],snapshot,configured,20)?.requestId,'memory');
  assert.equal(selectCapacityRequest([cpu,memory],snapshot,configured,105)?.requestId,'cpu');
});
