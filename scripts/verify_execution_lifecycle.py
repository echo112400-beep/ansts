"""Real development lifecycle acceptance. Does not change credentials or unrelated tasks."""
import argparse
import base64
import json
import time
from verify_body_split import request, token, wait

IMAGE = 'node@sha256:fe4f6d74e20370acb04ad889077d14d0f64a9c8ea6a42bc60cddf1e844699914'
BASE = 'http://127.0.0.1:18080'
TENANT = 'lifecycle-acceptance'
HEADERS = {'Authorization': token(TENANT)}

def submit(code, timeout=15000):
    identity = f'lifecycle-{time.time_ns()}'
    status, value = request(BASE + '/v1/tasks', {
        'tenant_id': TENANT, 'task_id': identity, 'prompt': 'execution lifecycle acceptance',
        'timeout_seconds': int(timeout / 1000) + 30,
        'resource_profile': {'think_cpu_cores': 0.5, 'act_cpu_cores': 0.5, 'act_memory_mb': 128, 'disk_mb': 32},
        'execution': {'kind': 'container', 'image': IMAGE, 'argv': ['node', '-e', code], 'timeoutMs': timeout, 'cpu': 0.5, 'memoryMb': 128},
    }, HEADERS)
    assert status == 202, value
    return BASE + f'/v1/tasks/{identity}?tenantId={TENANT}'

def finished(url):
    status, value = request(url + '&includeResult=1', headers=HEADERS)
    return value if status == 200 and value.get('phase') == 'COMPLETED' else None

def run_round():
    url = submit("require('fs').writeFileSync('/workspace/result.bin',Buffer.from([0,255]));require('fs').symlinkSync('/etc/passwd','/workspace/link');console.log('lifecycle-ok')")
    result = wait(lambda: finished(url))['result']
    assert result['executionOutcome'] == 'SUCCEEDED' and 'lifecycle-ok' in result['logs'], result
    assert base64.b64decode(result['artifacts'][0]['contentBase64']) == bytes([0, 255]), result
    assert len(result['artifacts']) == 1 and result['artifactsTruncated'], result
    assert request(url + '&includeResult=1')[0] == 401
    assert request(url + '&includeResult=1', headers={'Authorization': token('other-tenant')})[0] in (403, 404)
    failed_url = submit('process.exit(7)')
    assert wait(lambda: finished(failed_url))['result']['executionOutcome'] == 'FAILED'
    timeout_url = submit('setInterval(()=>{},1000)', 1000)
    assert wait(lambda: finished(timeout_url))['result']['executionOutcome'] == 'TIMED_OUT'
    cancel_url = submit("console.log('running-before-cancel');setInterval(()=>{},1000)", 30000)
    wait(lambda: request(cancel_url + '&includeResult=1', headers=HEADERS)[1].get('runtimeLogs'))
    status, value = request(cancel_url.split('?')[0] + '/cancel?tenantId=' + TENANT, {'tenant_id': TENANT, 'reason': 'acceptance cancellation'}, HEADERS)
    assert status == 200 and value['cancel_requested']['actor'] == 'body-split-check', value
    assert wait(lambda: finished(cancel_url))['result']['executionOutcome'] == 'CANCELLED'
    print(json.dumps({'round': 'passed', 'binaryArtifacts': True, 'symlinkExcluded': True, 'tenantIsolation': True, 'nonzeroExit': True, 'timeout': True, 'liveLogs': True, 'namedCancellation': True}), flush=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rounds', type=int, default=3)
    args = parser.parse_args()
    assert 1 <= args.rounds <= 10
    for _ in range(args.rounds):
        run_round()
