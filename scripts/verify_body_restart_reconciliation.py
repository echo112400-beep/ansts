"""Restart one development Body during a delayed attempt and verify conservative recovery.

The temporary Compose override is removed and normal Body settings restored in finally.
Credentials are read from ignored local files and never printed.
"""
import json
import base64
import hashlib
import hmac
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ['docker', 'compose', '-f', str(ROOT / 'deploy/dev-cluster/docker-compose.dev-cluster.yml')]
HOSTS = ('body-host-01', 'body-host-02')

def http(path, body=None, host=None, operator=False, base='http://127.0.0.1:18082'):
    headers = {'Content-Type': 'application/json'}
    if operator:
        headers['Authorization'] = 'Operator ' + (ROOT / '.dev-secrets/body-operator.token').read_text().strip()
    elif host:
        headers['Authorization'] = 'Body ' + (ROOT / f'.dev-secrets/{host}.token').read_text().strip()
        headers['x-ansts-body-id'] = host
    elif base.endswith(':18080'):
        encode = lambda value: base64.urlsafe_b64encode(json.dumps(value, separators=(',', ':')).encode()).decode().rstrip('=')
        now = int(time.time())
        header = encode({'alg': 'HS256', 'typ': 'JWT'})
        payload = encode({'sub': 'development-fault-test', 'role': 'SRE_ADMIN', 'scopeTenant': 'all', 'scopeRegion': 'all', 'iat': now, 'exp': now + 900})
        signing = f'{header}.{payload}'
        signature = base64.urlsafe_b64encode(hmac.new((ROOT / '.dev-secrets/user-signing.key').read_text().strip().encode(), signing.encode(), hashlib.sha256).digest()).decode().rstrip('=')
        headers['Authorization'] = f'Bearer {signing}.{signature}'
    request = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None, headers=headers)
    try:
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        raw = error.read().decode()
        try:
            return error.code, json.loads(raw)
        except json.JSONDecodeError:
            return error.code, {'error': raw[:200]}

def wait_for(predicate, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.5)
    raise AssertionError('timed out waiting for fault assertion')

def compose(*args, override=None):
    cmd = COMPOSE + (['-f', str(override)] if override else []) + list(args)
    subprocess.run(cmd, cwd=ROOT, check=True, stdout=subprocess.DEVNULL)

def main():
    task_id = f'body-restart-{time.time_ns()}'
    override = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as output:
            override = Path(output.name)
            output.write('services:\n  body-daemon-01:\n    environment:\n      ANSTS_BODY_EXECUTION_DELAY_MS: "20000"\n  body-daemon-02:\n    environment:\n      ANSTS_BODY_EXECUTION_DELAY_MS: "20000"\n')
        compose('up', '-d', 'body-daemon-01', 'body-daemon-02', override=override)
        time.sleep(3)
        status, submitted = http('/v1beta1/tasks/dispatch', {'tenantId': 'fault-test', 'taskId': task_id, 'promptText': 'must not automatically replay after restart'}, base='http://127.0.0.1:18080')
        assert status == 202, submitted
        host = submitted['targetBodyHostId']
        execution_task_id = submitted.get('executionTaskId', task_id)
        assert host in HOSTS, submitted
        def state():
            return http(f'/v1/body/state?taskId={execution_task_id}', host=host)[1].get('execution')
        wait_for(lambda: (value if value and value['phase'] == 'RUNNING' else None) if (value := state()) else None)
        # Allow the local journal to be admitted before restarting the container.
        time.sleep(1)
        subprocess.run(['docker', 'restart', 'ansts-body-daemon-' + host[-2:]], check=True, stdout=subprocess.DEVNULL)
        uncertain = wait_for(lambda: (value if value and value['phase'] == 'UNCERTAIN' else None) if (value := state()) else None)
        wait_for(lambda: http(f'/v1/tasks/{task_id}', base='http://127.0.0.1:18080')[1].get('state') == 'SUSPENDED_RECONCILIATION')
        review = {'tenant_id': uncertain['tenant_id'], 'task_id': execution_task_id, 'expected_version': uncertain['version'], 'action': 'retain_block', 'evidence': 'development restart during delayed execution; no completed result', 'reason': 'Fault verification must retain uncertainty'}
        assert http(f'/v1/body/reconcile?taskId={execution_task_id}', review, host=host)[0] == 401
        assert http(f'/v1/body/reconcile?taskId={execution_task_id}', {**review, 'expected_version': 0}, operator=True)[0] == 409
        status, result = http(f'/v1/body/reconcile?taskId={execution_task_id}', review, operator=True)
        assert status == 200, result
        assert result['execution']['phase'] == 'UNCERTAIN'
        assert result['execution']['audit'][-1]['action'] == 'retain_block'
        assert http(f'/v1/body/reconcile?taskId={execution_task_id}', review, operator=True)[0] == 200
        assert http(f'/v1/body/reconcile?taskId={execution_task_id}', {**review, 'reason': 'changed stale review'}, operator=True)[0] == 409
        time.sleep(3)
        assert state()['phase'] == 'UNCERTAIN'
        print(json.dumps({'taskId': task_id, 'host': host, 'restart': 'verified', 'automaticReplay': 'blocked', 'hostPrivilegeEscalation': 'rejected', 'staleReview': 'rejected', 'audit': 'persisted'}))
    finally:
        compose('up', '-d', 'body-daemon-01', 'body-daemon-02')
        if override:
            override.unlink(missing_ok=True)

if __name__ == '__main__':
    main()
