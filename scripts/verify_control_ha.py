"""Verify shared Gateway/Body state and entry failover; always restores stopped development services."""
import json
import subprocess
import time
from verify_body_split import request, token, wait
from verify_execution_lifecycle import submit, finished

headers = {'Authorization': token('all')}
def state(port):
    status, value = request(f'http://127.0.0.1:{port}/v1beta1/gateway/state', headers=headers)
    assert status == 200, value
    return value
first, second = state(18080), state(18081)
assert first['cellId'] == second['cellId'], 'Gateways use different state authorities'
assert sorted(row['executionTaskId'] for row in first['dispatches']) == sorted(row['executionTaskId'] for row in second['dispatches'])
before = len(first['dispatches'])
try:
    subprocess.run(['docker', 'stop', 'ansts-gateway-01'], check=True, stdout=subprocess.DEVNULL)
    assert state(18081)['cellId'] == first['cellId']
finally:
    subprocess.run(['docker', 'start', 'ansts-gateway-01'], check=True, stdout=subprocess.DEVNULL)
wait(lambda: request('http://127.0.0.1:18080/readyz')[0] == 200)
assert len(state(18080)['dispatches']) == before
try:
    subprocess.run(['docker', 'stop', 'ansts-celld-brain-01'], check=True, stdout=subprocess.DEVNULL)
    wait(lambda: request('http://127.0.0.1:18081/readyz')[0] == 200)
    assert state(18081)['cellId'] == first['cellId']
    # The old Body listener is now a proxy, not an independent storage authority.
    assert request('http://127.0.0.1:18085/readyz')[0] == 200
    fixture = submit("console.log('ha-write-ok')")
    result = wait(lambda: finished(fixture))['result']
    assert result['executionOutcome'] == 'SUCCEEDED' and 'ha-write-ok' in result['logs'], result
finally:
    subprocess.run(['docker', 'start', 'ansts-celld-brain-01'], check=True, stdout=subprocess.DEVNULL)
wait(lambda: request('http://127.0.0.1:18082/healthz')[0] == 200)
try:
    subprocess.run(['docker', 'stop', 'ansts-body-celld'], check=True, stdout=subprocess.DEVNULL)
    for port in (19501, 19502):
        assert request(f'http://127.0.0.1:{port}/readyz')[0] == 200
    assert state(18081)['cellId'] == first['cellId']
finally:
    subprocess.run(['docker', 'start', 'ansts-body-celld'], check=True, stdout=subprocess.DEVNULL)
wait(lambda: request('http://127.0.0.1:18085/readyz')[0] == 200)
print(json.dumps({'gatewaySharedAuthority': True, 'gatewayRestartStatePreserved': True, 'firstBrainEntryFailure': 'reads, readiness and real task execution failed over', 'legacyBodyListenerFailure': 'native and Gateway paths stay ready', 'bodyAuthority': 'shared fleet', 'servicesRestored': True}))
