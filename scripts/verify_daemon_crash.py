"""Abrupt Daemon loss: retain uncertainty, recover orphan containers, reject stale completion."""
import json
import subprocess
from verify_body_split import ROOT, request, wait
from verify_execution_lifecycle import submit, HEADERS

url = submit("console.log('crash-fixture-running');setInterval(()=>{},1000)", 60000)
wait(lambda: request(url + '&includeResult=1', headers=HEADERS)[1].get('runtimeLogs'))
task = request(url, headers=HEADERS)[1]
host = task['assigned_node_id']
container = 'ansts-body-daemon-' + host[-2:]
port = 19501 if host.endswith('01') else 19502
try:
    subprocess.run(['docker', 'kill', container], check=True, stdout=subprocess.DEVNULL)
finally:
    subprocess.run(['docker', 'start', container], check=True, stdout=subprocess.DEVNULL)
wait(lambda: request(f'http://127.0.0.1:{port}/readyz')[0] == 200)
wait(lambda: request(url + '&includeResult=1', headers=HEADERS)[1].get('phase') == 'UNCERTAIN')
wait(lambda: request(url, headers=HEADERS)[1].get('state') == 'SUSPENDED_RECONCILIATION')
health = request(f'http://127.0.0.1:{port}/health')[1]
assert health['capacity']['reserved'] == 0 and health['resources']['reservations'] == 0, health
remaining = subprocess.check_output(['docker', 'ps', '-aq', '--filter', 'label=ansts.managed=sandbox', '--filter', f'label=ansts.owner={host}']).decode().strip()
assert not remaining, 'orphan sandbox left behind'
headers = {'Authorization': 'Body ' + (ROOT / f'.dev-secrets/{host}.token').read_text().strip(), 'x-ansts-body-id': host}
status, result = request('http://127.0.0.1:18082/v1/scheduler/complete', {'task_id': task['executionTaskId'], 'tenant_id': task['tenant_id'], 'node_id': host, 'fencing_token': 0}, headers)
assert status == 409, result
print(json.dumps({'daemonCrash': 'verified', 'task': task['task_id'], 'outcome': 'UNCERTAIN; no automatic replay', 'orphanContainers': 0, 'resourceReservations': 0, 'staleCompletion': 'rejected'}))
