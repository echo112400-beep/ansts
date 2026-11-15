"""Measure a fixed two-tenant workload; reports observations, never invents an SLO."""
import argparse
import json
import statistics
import time
from pathlib import Path
from verify_body_split import request, token
from verify_execution_lifecycle import IMAGE, BASE

parser = argparse.ArgumentParser()
parser.add_argument('--tasks', type=int, default=8)
parser.add_argument('--output', default='/tmp/ansts-capacity-measurement.json')
args = parser.parse_args()
assert 2 <= args.tasks <= 100
start = time.monotonic()
samples = []
for index in range(args.tasks):
    tenant = f'capacity-tenant-{index % 2}'
    identity = f'capacity-{time.time_ns()}'
    headers = {'Authorization': token(tenant)}
    submitted_at = time.monotonic()
    status, value = request(BASE + '/v1/tasks', {
        'tenant_id': tenant, 'task_id': identity, 'prompt': 'fixed capacity measurement', 'timeout_seconds': 30,
        'resource_profile': {'think_cpu_cores': 0.5, 'act_cpu_cores': 0.5, 'act_memory_mb': 128, 'disk_mb': 32},
        'execution': {'kind': 'container', 'image': IMAGE, 'argv': ['node', '-e', 'setTimeout(()=>console.log("measured"),100)'], 'timeoutMs': 10000, 'cpu': 0.5, 'memoryMb': 128},
    }, headers)
    assert status == 202, value
    samples.append({'tenant': tenant, 'url': BASE + f'/v1/tasks/{identity}?tenantId={tenant}&includeResult=1', 'headers': headers, 'submitted': submitted_at})
while any('latencySeconds' not in sample for sample in samples):
    assert time.monotonic() - start < 300, 'capacity workload timed out'
    for sample in samples:
        if 'latencySeconds' in sample:
            continue
        status, value = request(sample['url'], headers=sample['headers'])
        if status == 200 and value.get('phase') == 'COMPLETED':
            assert value['result']['executionOutcome'] == 'SUCCEEDED', value
            sample['latencySeconds'] = time.monotonic() - sample['submitted']
    time.sleep(0.2)
elapsed = time.monotonic() - start
latencies = sorted(sample['latencySeconds'] for sample in samples)
tenants = {tenant: {'completed': sum(sample['tenant'] == tenant for sample in samples), 'meanLatencySeconds': statistics.mean(sample['latencySeconds'] for sample in samples if sample['tenant'] == tenant)} for tenant in sorted({sample['tenant'] for sample in samples})}
result = {'tasks': args.tasks, 'elapsedSeconds': elapsed, 'throughputTasksPerSecond': args.tasks / elapsed, 'p95EndToEndSeconds': latencies[max(0, int(len(latencies) * 0.95 + 0.999) - 1)], 'tenants': tenants, 'workload': '2 tenants; 100ms timer; 0.5 configured CPU; 128MiB configured memory; includes polling/queue overhead', 'actualCpuMemoryUsage': 'not collected', 'slo': 'not established'}
Path(args.output).write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
