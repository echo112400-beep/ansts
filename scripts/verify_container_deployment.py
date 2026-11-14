"""Authenticated Gateway -> Scheduler -> Body -> real Engine smoke test."""
import json
import time
from verify_body_split import request, token, wait

tenant = 'container-smoke'
task_id = f'container-{time.time_ns()}'
headers = {'Authorization': token(tenant)}
url = f'http://127.0.0.1:18080/v1/tasks/{task_id}?tenantId={tenant}'
image = 'node@sha256:fe4f6d74e20370acb04ad889077d14d0f64a9c8ea6a42bc60cddf1e844699914'
status, submitted = request('http://127.0.0.1:18080/v1/tasks', {
    'tenant_id': tenant, 'task_id': task_id, 'prompt': 'structured container smoke',
    'timeout_seconds': 15,
    'resource_profile': {'think_cpu_cores': 0.5, 'act_cpu_cores': 0.5, 'act_memory_mb': 128, 'disk_mb': 32},
    'execution': {'kind': 'container', 'image': image, 'argv': ['node', '-e', "console.log('container-delivered')"], 'timeoutMs': 10000, 'cpu': 0.5, 'memoryMb': 128},
}, headers)
assert status in (200, 202), submitted
wait(lambda: request(url, headers=headers)[1].get('state') == 'COMPLETED')
def result_ready():
    status, value = request(url + '&includeResult=1', headers=headers)
    return value if status == 200 and value.get('phase') == 'COMPLETED' else None
result = wait(result_ready)['result']
assert result['executionKind'] == 'container', result
assert result['exitCode'] == 0 and 'container-delivered' in result['logs'], result
assert request(url + '&includeResult=1')[0] == 401
assert request(url + '&includeResult=1', headers={'Authorization': token('another-tenant')})[0] in (403, 404)
print(json.dumps({'taskId': task_id, 'execution': 'real container completed', 'logs': 'tenant authorized', 'exitCode': result['exitCode']}))
