"""Verify deployed native/control process separation and independent restart recovery."""
import base64
import hashlib
import hmac
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
def token(tenant):
    encode = lambda value: base64.urlsafe_b64encode(json.dumps(value, separators=(',', ':')).encode()).decode().rstrip('=')
    now = int(time.time())
    payload = encode({'sub': 'body-split-check', 'role': 'SRE_ADMIN', 'scopeTenant': tenant, 'scopeRegion': 'all', 'iat': now, 'exp': now + 900})
    header = encode({'alg': 'HS256', 'typ': 'JWT'})
    content = f'{header}.{payload}'
    signature = base64.urlsafe_b64encode(hmac.new((ROOT / '.dev-secrets/user-signing.key').read_text().strip().encode(), content.encode(), hashlib.sha256).digest()).decode().rstrip('=')
    return f'Bearer {content}.{signature}'
def request(url, body=None, headers=None):
    request = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, headers={'Content-Type': 'application/json', **(headers or {})})
    try:
        with opener.open(request, timeout=15) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)
def wait(predicate):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.5)
    raise AssertionError('Body split verification timed out')
def docker(*args):
    subprocess.run(['docker', *args], check=True, stdout=subprocess.DEVNULL)

def main():
    for port in (19501, 19502):
        _, health = request(f'http://127.0.0.1:{port}/health')
        assert health['runtime'] == 'native-body-daemon', health
        assert health['controlPlane'] == 'remote-celld-body', health
    assert request('http://127.0.0.1:18085/readyz')[1]['controlPlane'] == 'celld-body'
    task_id = f'body-split-{time.time_ns()}'
    user_headers = {'Authorization': token('body-split-check')}
    status, submitted = request('http://127.0.0.1:18080/v1beta1/tasks/dispatch', {'tenantId': 'body-split-check', 'taskId': task_id, 'promptText': 'verify independently started Body control and native Daemon'}, user_headers)
    assert status == 202, submitted
    def completed():
        status, task = request(f'http://127.0.0.1:18080/v1/tasks/{task_id}?tenantId=body-split-check', headers=user_headers)
        return task if status == 200 and task.get('state') == 'COMPLETED' else None
    task = wait(completed)
    host = task['assigned_node_id']
    body_headers = {'Authorization': 'Body ' + (ROOT / f'.dev-secrets/{host}.token').read_text().strip(), 'x-ansts-body-id': host}
    query = urllib.parse.urlencode({'taskId': task['executionTaskId']})
    state_url = f'http://127.0.0.1:18085/v1/body/state?{query}'
    before = wait(lambda: (value if value and value['phase'] == 'COMPLETED' else None) if (value := request(state_url, headers=body_headers)[1].get('execution')) else None)
    try:
        docker('stop', 'ansts-body-celld')
        assert request('http://127.0.0.1:18082/healthz')[0] == 200
        assert request('http://127.0.0.1:19501/health')[0] == 200
        assert request('http://127.0.0.1:19501/readyz')[0] == 503
        assert request(f'http://127.0.0.1:18082/v1/body/state?{query}', headers=body_headers)[0] == 503
    finally:
        docker('start', 'ansts-body-celld')
    wait(lambda: request('http://127.0.0.1:18085/readyz')[0] == 200)
    after = request(state_url, headers=body_headers)[1]['execution']
    assert after == before, 'Body control state changed across independent restart'
    docker('restart', 'ansts-body-daemon-' + host[-2:])
    wait(lambda: request(f'http://127.0.0.1:{19501 if host.endswith("01") else 19502}/readyz')[0] == 200)
    assert request('http://127.0.0.1:18085/readyz')[0] == 200
    assert request(state_url, headers=body_headers)[1]['execution'] == before
    print(json.dumps({'taskId': task_id, 'execution': 'COMPLETED', 'bodyControlRestart': 'state preserved', 'daemonRestart': 'control unaffected', 'missingBodyControl': 'fails closed'}))
if __name__ == '__main__':
    main()
