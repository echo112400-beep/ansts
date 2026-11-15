"""Ten-minute execution across a short Daemon network outage, with guaranteed reconnection."""
import json
import subprocess
import time
import argparse
from verify_body_split import request
from verify_execution_lifecycle import submit, finished, HEADERS

parser = argparse.ArgumentParser()
parser.add_argument('--resume-url', help='Resume monitoring an existing fixture after a monitor interruption; does not submit or inject another outage.')
args = parser.parse_args()
url = args.resume_url or submit("console.log('long-task-started');setTimeout(()=>console.log('long-task-finished'),600000)", 660000)
deadline = time.monotonic() + 720
task = None
while time.monotonic() < deadline:
    try:
        _, state = request(url + '&includeResult=1', headers=HEADERS)
    except OSError:
        time.sleep(0.5)
        continue
    if state.get('runtimeLogs'):
        task = request(url, headers=HEADERS)[1]
        break
    time.sleep(0.5)
assert task, 'long task did not start'
container = 'ansts-body-daemon-' + task['assigned_node_id'][-2:]
networks = json.loads(subprocess.check_output(['docker', 'inspect', '--format', '{{json .NetworkSettings.Networks}}', container]))
assert len(networks) == 1, 'refuse ambiguous network fixture'
network = next(iter(networks))
print(json.dumps({'event': 'long-task-started', 'url': url, 'host': task['assigned_node_id']}), flush=True)
if not args.resume_url:
    try:
        subprocess.run(['docker', 'network', 'disconnect', network, container], check=True)
        time.sleep(4)
    finally:
        subprocess.run(['docker', 'network', 'connect', network, container], check=True)
    print(json.dumps({'event': 'network-restored', 'outageSeconds': 4}), flush=True)
last_report = 0
while time.monotonic() < deadline:
    try:
        result = finished(url)
        _, current = request(url + '&includeResult=1', headers=HEADERS)
    except OSError:
        time.sleep(1)
        continue
    assert current.get('phase') != 'UNCERTAIN', current
    if result:
        assert result['result']['executionOutcome'] == 'SUCCEEDED', result
        assert 'long-task-finished' in result['result']['logs'], result
        print(json.dumps({'event': 'long-task-passed', 'durationSeconds': 600, 'networkOutageSeconds': 4, 'outcome': 'SUCCEEDED'}), flush=True)
        break
    if time.monotonic() - last_report >= 30:
        print(json.dumps({'event': 'long-task-running', 'remainingBudgetSeconds': round(deadline - time.monotonic())}), flush=True)
        last_report = time.monotonic()
    time.sleep(2)
else:
    raise AssertionError('long task acceptance timed out')
