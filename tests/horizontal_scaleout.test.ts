import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PartitionedSchedulerRouter,
  DynamicQueueAutoSharder,
  BrainBodyContainerPoolScaler,
} from '../src/cloudflare/horizontal_scaler.js';

test('Scale-Out #1: PartitionedSchedulerRouter P2C, Hot-Tenant Sub-Sharding & Online Shard Addition', () => {
  const router = new PartitionedSchedulerRouter({
    initialShardCount: 4,
    maxConcurrencyPerShard: 100,
    hotTenantThresholdInFlight: 20,
    maxSubShardsPerTenant: 4,
  });

  // 1. Normal tenant gets deterministic affinity
  const r1 = router.routeTaskAdmission('tenant-alpha', 'task-01', 2);
  const r2 = router.routeTaskAdmission('tenant-alpha', 'task-02', 2);
  assert.equal(r1.routingMode, 'AFFINITY');
  assert.equal(r1.targetShardId, r2.targetShardId);

  // 2. Hot tenant with 50 concurrent tasks splits across multiple shards via P2C
  const chosenShards = new Set<string>();
  for (let i = 0; i < 30; i++) {
    const res = router.routeTaskAdmission('tenant-whale', `whale-task-${i}`, 50);
    assert.equal(res.routingMode, 'HOT_TENANT_P2C');
    assert.ok(res.splitFactor >= 2);
    chosenShards.add(res.targetShardId);
    router.recordShardDispatchStart(res.targetShardId);
  }
  assert.ok(chosenShards.size >= 2, 'Hot tenant workload must distribute across multiple SchedulerCell shards');

  // 3. Online horizontal scale-out from 4 -> 8 shards
  const added = router.scaleOutSchedulerShards(4, 100);
  assert.equal(added.length, 4);
  assert.equal(router.getShardStates().length, 8);

  // 4. Lock-free quota gossip reconciliation
  const gossip = router.syncTenantQuotaGossip('tenant-whale', 200, {
    'scheduler-cell-shard-00': 15,
    'scheduler-cell-shard-01': 15,
  });
  assert.equal(gossip.totalUsed, 30);
  assert.equal(gossip.remainingGlobalQuota, 170);
});

test('Scale-Out #2: DynamicQueueAutoSharder scales beyond 256 concurrent producer cap per Queue Cell', () => {
  const autoSharder = new DynamicQueueAutoSharder({
    initialShards: 4, // 4 * 256 = 1024 producer capacity
    maxShards: 32,
    scaleUpUtilizationThreshold: 0.75,
  });

  // Simulate 850 concurrent producers across 4 shards (>75% of 1024)
  const scaleResult = autoSharder.evaluateAndScale({
    0: 220,
    1: 215,
    2: 210,
    3: 205,
  });

  assert.equal(scaleResult.scaledOut, true);
  assert.ok(scaleResult.newShards >= 8);
  assert.ok(scaleResult.maxConcurrentProducerCapacity >= 2048);
});

test('Scale-Out #3: Brain-Body Disaggregated Container Pool Autoscaler multiplexes 10,000 Brain Cells onto burst hosts', () => {
  const scaler = new BrainBodyContainerPoolScaler({
    targetWarmPoolBufferRatio: 0.25,
    maxBurstPerHost: 20,
  });

  // 5,000 active Brain cells (mostly in LLM-Wait / WebSocket Hibernation), 160 active tool burst tasks
  const plan = scaler.computeScalePlan(5000, 160, [
    {
      hostId: 'host-01',
      activeToolBurstSandboxes: 20,
      maxBurstSandboxesPerHost: 20,
      warmPoolIdleContainers: 2,
      psiCpuSomeAvg10: 0.12,
      psiMemoryFullAvg10: 0.02,
    },
    {
      hostId: 'host-02',
      activeToolBurstSandboxes: 20,
      maxBurstSandboxesPerHost: 20,
      warmPoolIdleContainers: 1,
      psiCpuSomeAvg10: 0.55, // PSI throttled!
      psiMemoryFullAvg10: 0.05,
    },
  ]);

  assert.equal(plan.brainToBodyMultiplexRatio, 31.25);
  assert.deepEqual(plan.psiThrottledHosts, ['host-02']);
  assert.equal(plan.recommendedHostCount, 10); // ceil(160 * 1.25 / 20) = 10 hosts
  assert.equal(plan.hostsToAdd, 9); // 10 needed - 1 healthy host
  assert.ok(plan.warmContainersToPrewarm > 0);
});
