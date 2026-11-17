"""Verify persistent drain on an idle development Daemon, then resume it."""
import json
import subprocess
from verify_body_split import ROOT, request, wait

base = 'http://127.0.0.1:19501'
headers = {'Authorization': 'Body ' + (ROOT / '.dev-secrets/body-host-01.token').read_text().strip(), 'x-ansts-body-id': 'body-host-01'}
health = request(base + '/health')[1]
assert health['activeSandboxes'] == 0 and not health.get('recoveryRequired'), health
assert not health.get('draining'), 'Already draining; preserve operator state'
try:
    status, result = request(base + '/v1/daemon/drain', {'draining': True}, headers)
    assert status == 200 and result['drained'], result
    assert request(base + '/readyz')[0] == 503
    assert request(base + '/v1/daemon/drain', {'draining': False})[0] == 401
    subprocess.run(['docker', 'restart', 'ansts-body-daemon-01'], check=True, stdout=subprocess.DEVNULL)
    wait(lambda: request(base + '/health')[1].get('draining'))
    assert request(base + '/readyz')[0] == 503
finally:
    wait(lambda: request(base + '/v1/daemon/drain', {'draining': False}, headers)[0] == 200)
wait(lambda: request(base + '/readyz')[0] == 200)
print(json.dumps({'drain': 'persisted across restart', 'anonymousMutation': 'rejected', 'resume': 'ready'}))
