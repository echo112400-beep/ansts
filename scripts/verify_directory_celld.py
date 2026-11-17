#!/usr/bin/env python3
"""Isolated native celld transaction/restart check; never mounts application secrets."""
import json
import base64
import hashlib
import hmac
from concurrent.futures import ThreadPoolExecutor
import pathlib
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
NAME = 'ansts-directory-check-' + uuid.uuid4().hex[:10]
TOKEN = 'isolated-verification-token-not-a-production-secret-123456'
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def command(*args):
    return subprocess.check_output(args, text=True).strip()

def address():
    return 'http://127.0.0.1:' + command('docker', 'port', NAME, '8080').split(':')[-1]

def call(path, body=None, token=TOKEN, headers=None, method=None):
    req = urllib.request.Request(address() + path, data=None if body is None else json.dumps(body).encode(),
        headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token, **(headers or {})}, method=method)
    try:
        with HTTP.open(req, timeout=10) as response:
            raw = response.read().decode()
            return response.status, json.loads(raw) if raw.startswith('{') else raw
    except urllib.error.HTTPError as error:
        raw = error.read().decode()
        return error.code, json.loads(raw) if raw.startswith('{') else raw

def ready():
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        if command('docker', 'inspect', '--format', '{{.State.Running}}', NAME) != 'true':
            raise RuntimeError('isolated celld exited before readiness')
        try:
            if call('/ping')[0] == 200:
                return
        except (OSError, ValueError, subprocess.CalledProcessError):
            pass
        time.sleep(.25)
    raise RuntimeError('isolated celld did not become ready')

with tempfile.TemporaryDirectory(prefix='ansts-directory-check-') as temporary:
    project = pathlib.Path(temporary)
    source = str(ROOT / 'src/cloudflare/tenant_directory_cell.ts')
    gateway = str(ROOT / 'src/cloudflare/gateway_cell.ts')
    (project / 'entry.ts').write_text('import {TenantDirectoryCell} from ' + json.dumps(source) + ';\n'
        'import {handleUnifiedGatewayRequest} from ' + json.dumps(gateway) + ';\n'
        'export {TenantDirectoryCell};\n'
        'export class DirectoryTestIngress {constructor(ctx,env){this.env=env;} '
        'async fetch(request){return await handleUnifiedGatewayRequest(request,this.env)??new Response("missing",{status:404});}}\n'
        'export default {fetch(request,env){const url=new URL(request.url);'
        'if(url.pathname==="/ping")return new Response("ok");'
        'const match=url.pathname.match(/^\\/(gateway-a|gateway-b)(\\/.*)$/);'
        'if(match){url.pathname=match[2];return env.INGRESS.get(env.INGRESS.idFromName(match[1])).fetch(new Request(url,request));}'
        'return env.ANSTS_TENANT_DIRECTORY_CELL.get(env.ANSTS_TENANT_DIRECTORY_CELL.idFromName("tenant-directory-v1")).fetch(request);}};\n')
    subprocess.run([str(ROOT / 'node_modules/.bin/esbuild'), str(project / 'entry.ts'), '--bundle',
        '--format=esm', '--platform=neutral', '--external:node:*', '--outfile=' + str(project / 'worker.js')], check=True)
    (project / 'wrangler.json').write_text(json.dumps({'name': NAME, 'main': 'worker.js',
        'compatibility_date': '2025-08-15', 'compatibility_flags': ['nodejs_compat'],
        'durable_objects': {'bindings': [{'name': 'ANSTS_TENANT_DIRECTORY_CELL', 'class_name': 'TenantDirectoryCell'}, {'name': 'INGRESS', 'class_name': 'DirectoryTestIngress'}]},
        'migrations': [{'tag': 'v1', 'new_sqlite_classes': ['TenantDirectoryCell', 'DirectoryTestIngress']}],
        'vars': {'ANSTS_DIRECTORY_SERVICE_TOKEN': TOKEN, 'ANSTS_USER_AUTH_SECRET': TOKEN}}))
    try:
        command('docker', 'run', '-d', '--user', str(os.getuid()) + ':' + str(os.getgid()), '--name', NAME, '-p', '127.0.0.1::8080', '-e', 'CELLD_ESBUILD=/tools/esbuild',
            '-v', str(ROOT / 'node_modules/@esbuild/linux-x64/bin/esbuild') + ':/tools/esbuild:ro',
            '-v', str(project) + ':/probe', 'ghcr.io/denoland/celld:0.6.3',
            'dev', '/probe', '--host', '0.0.0.0', '--port', '8080', '--no-watch')
        ready()
        actor = {'sub': 'github:1', 'role': 'SRE_ADMIN', 'scopeTenant': 'all', 'scopeRegion': 'all'}
        mutation = {'actor': actor, 'input': {'id': 'probe', 'region': 'dev', 'name': 'Probe', 'costCenter': 'eng',
            'status': 'ACTIVE', 'expectedVersion': 0, 'reason': 'isolated validation', 'requestId': 'probe-1', 'idempotencyKey': 'probe-1'}}
        assert call('/list', {'actor': actor}, token='wrong')[0] == 401
        assert call('/cost-center-mutate', {'actor': actor, 'input': {'id':'eng','name':'Engineering','status':'ACTIVE','expectedVersion':0,'reason':'test setup','requestId':'center','idempotencyKey':'center'}})[0] == 200
        status, first = call('/mutate', mutation)
        assert status == 200 and first['version'] == 1, (status, first)
        command('docker', 'restart', NAME)
        ready()
        assert call('/mutate', mutation) == (200, first)
        status, listing = call('/list', {'actor': actor})
        assert status == 200 and listing['items'] == [first]
        status, audit = call('/audit', {'actor': actor})
        assert status == 200 and len(audit['items']) == 2
        stale = json.loads(json.dumps(mutation))
        stale['input']['idempotencyKey'] = 'second-key'
        assert call('/mutate', stale)[0] == 409
        def encoded(value):
            return base64.urlsafe_b64encode(json.dumps(value, separators=(',', ':')).encode()).rstrip(b'=').decode()
        now = int(time.time())
        message = encoded({'alg': 'HS256', 'typ': 'JWT'}) + '.' + encoded({**actor, 'iat': now, 'exp': now + 300})
        jwt = message + '.' + base64.urlsafe_b64encode(hmac.new(TOKEN.encode(), message.encode(), hashlib.sha256).digest()).rstrip(b'=').decode()
        payload = {**mutation['input'], 'id': 'dual-entry'}
        def submit(gateway):
            return call('/' + gateway + '/v1/admin/tenants', payload, token=jwt, headers={'Idempotency-Key': 'dual-entry-key'})
        with ThreadPoolExecutor(max_workers=2) as pool:
            responses = list(pool.map(submit, ['gateway-a', 'gateway-b']))
        assert responses[0][0] == 200 and responses[0] == responses[1], responses
        status, audit = call('/audit', {'actor': actor})
        assert status == 200 and len(audit['items']) == 3
        def race_update(gateway):
            edit = {**payload, 'expectedVersion': 1, 'name': gateway}
            return call('/' + gateway + '/v1/admin/tenants/dual-entry', edit, token=jwt,
                headers={'Idempotency-Key': gateway + '-update'}, method='PUT')
        with ThreadPoolExecutor(max_workers=2) as pool:
            updates = list(pool.map(race_update, ['gateway-a', 'gateway-b']))
        assert sorted(result[0] for result in updates) == [200, 409], updates
        status, audit = call('/audit', {'actor': actor})
        assert status == 200 and len(audit['items']) == 4
        print('PASS: two independent Gateway ingress objects share one directory authority; concurrent replay commits once and competing CAS updates yield one winner.')
        print('PASS: native celld transactionSync, service authentication, restart persistence, replay deduplication, audit atomicity and CAS.')
    except Exception:
        subprocess.run(['docker', 'logs', '--tail', '20', NAME])
        raise
    finally:
        subprocess.run(['docker', 'rm', '-f', NAME], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
