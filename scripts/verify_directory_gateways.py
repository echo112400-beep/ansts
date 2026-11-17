#!/usr/bin/env python3
"""Three isolated celld processes exercise the real Gateway and Fleet entries."""
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import hmac
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]
PREFIX = 'ansts-dir-ha-' + uuid.uuid4().hex[:8]
SERVICE = 'isolated-service-token-no-production-value-12345678'
USER = 'isolated-user-signing-key-no-production-value-12345'
COORDINATOR = 'isolated-admission-coordinator-no-production-value-12345'
ADMISSION_KEY = 'a' * 64
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
CONTAINERS = []

def cmd(*args):
    return subprocess.check_output(args, text=True, stderr=subprocess.PIPE).strip()

def call(name, path, body=None, headers=None, method=None):
    port = cmd('docker', 'port', name, '8080').split(':')[-1]
    req = urllib.request.Request('http://127.0.0.1:' + port + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Content-Type': 'application/json', **(headers or {})}, method=method)
    try:
        with HTTP.open(req, timeout=12) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        raw = error.read()
        return error.code, json.loads(raw) if raw.startswith(b'{') else raw.decode()

def ready(name):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if cmd('docker', 'inspect', '--format', '{{.State.Running}}', name) != 'true':
            raise RuntimeError(name + ' exited')
        try:
            if call(name, '/healthz')[0] == 200:
                return
        except (OSError, ValueError, subprocess.CalledProcessError):
            pass
        time.sleep(.25)
    raise RuntimeError(name + ' readiness timeout')

def token(role='SRE_ADMIN', tenant='all'):
    def enc(value):
        return base64.urlsafe_b64encode(json.dumps(value, separators=(',', ':')).encode()).rstrip(b'=').decode()
    now = int(time.time())
    text = enc({'alg': 'HS256', 'typ': 'JWT'}) + '.' + enc({'sub': 'github:1', 'role': role,
        'scopeTenant': tenant, 'scopeRegion': 'all', 'iat': now, 'exp': now + 300})
    return text + '.' + base64.urlsafe_b64encode(hmac.new(USER.encode(), text.encode(), hashlib.sha256).digest()).rstrip(b'=').decode()

with tempfile.TemporaryDirectory(prefix=PREFIX) as scratch:
    cmd('docker', 'network', 'create', PREFIX)
    try:
        for label, entry in [('authority', 'worker.ts'), ('gateway-a', 'gateway_worker.ts'), ('gateway-b', 'gateway_worker.ts')]:
            project = Path(scratch) / label
            project.mkdir()
            subprocess.run([str(ROOT / 'node_modules/.bin/esbuild'), str(ROOT / 'src/cloudflare' / entry),
                '--bundle', '--format=esm', '--platform=neutral', '--external:node:*', '--outfile=' + str(project / 'worker.js')], check=True)
            config = {'name': PREFIX + '-' + label, 'main': 'worker.js', 'compatibility_date': '2025-08-15',
                'compatibility_flags': ['nodejs_compat'], 'vars': {'ANSTS_DIRECTORY_SERVICE_TOKEN': SERVICE}}
            if label == 'authority':
                config['vars'].update({'ANSTS_ADMISSION_SERVICE_TOKEN':COORDINATOR,'ANSTS_ADMISSION_SIGNING_KEY':ADMISSION_KEY})
                config.update({'durable_objects': {'bindings': [{'name': 'ANSTS_TENANT_DIRECTORY_CELL', 'class_name': 'TenantDirectoryCell'}, {'name':'ANSTS_TENANT_CONTROL_CELL','class_name':'TenantControlCell'},{'name':'ANSTS_ADMISSION_MAILBOX','class_name':'AdmissionShadowMailboxCell'}]},
                    'migrations': [{'tag': 'v1', 'new_sqlite_classes': ['TenantDirectoryCell','TenantControlCell','AdmissionShadowMailboxCell']}]})
            else:
                config.update({'durable_objects': {'bindings': [{'name': 'ANSTS_GATEWAY_CELL', 'class_name': 'AnstsGatewayCell'}]}, 'migrations': [{'tag': 'v1', 'new_sqlite_classes': ['AnstsGatewayCell']}]})
                config['vars'].update({'ANSTS_USER_AUTH_SECRET': USER, 'ANSTS_DIRECTORY_URL': 'http://' + PREFIX + '-authority:8080'})
            (project / 'wrangler.json').write_text(json.dumps(config))
            name = PREFIX + '-' + label
            cmd('docker', 'run', '-d', '--user', str(os.getuid()) + ':' + str(os.getgid()), '--name', name,
                '--network', PREFIX, '-p', '127.0.0.1::8080', '-e', 'CELLD_ESBUILD=/tools/esbuild',
                '-v', str(ROOT / 'node_modules/@esbuild/linux-x64/bin/esbuild') + ':/tools/esbuild:ro',
                '-v', str(project) + ':/probe', 'ghcr.io/denoland/celld:0.6.3',
                'dev', '/probe', '--host', '0.0.0.0', '--port', '8080', '--no-watch')
            CONTAINERS.append(name)
            ready(name)
        a, b = PREFIX + '-gateway-a', PREFIX + '-gateway-b'
        headers = {'Authorization': 'Bearer ' + token(), 'Idempotency-Key': 'shared-create'}
        body = {'id': 'shared', 'region': 'dev', 'name': 'Shared', 'costCenter': 'eng', 'status': 'ACTIVE', 'reason': 'isolated HA validation'}
        assert call(a, '/v1/admin/cost-centers', {'id':'eng','name':'Engineering','status':'ACTIVE','reason':'test setup'}, headers)[0] == 200
        assert call(a, '/v1/admin/tenants')[0] == 401
        with ThreadPoolExecutor(max_workers=2) as pool:
            created = list(pool.map(lambda name: call(name, '/v1/admin/tenants', body, headers), [a, b]))
        assert created[0][0] == 200 and created[0] == created[1], created
        def update(name):
            return call(name, '/v1/admin/tenants/shared', {**body, 'name': name, 'expectedVersion': 1},
                {**headers, 'Idempotency-Key': name}, method='PUT')
        with ThreadPoolExecutor(max_workers=2) as pool:
            updated = list(pool.map(update, [a, b]))
        assert sorted(response[0] for response in updated) == [200, 409], updated
        quota={'expectedVersion':0,'resources':{'concurrentAttempts':1,'cpuMilliCores':1000,'memoryMiB':512,'queuedTasks':10},'budget':{'period':'UTC_DAY','limitCreditUnits':'9007199254740993','softThresholdBps':8000},'reason':'internal policy verification'}
        initial=call(a,'/v1/admin/tenants/shared/quota',headers=headers)
        assert initial[0] == 200 and initial[1]['current'] is None and initial[1]['enforcement']=='NOT_CONNECTED',initial
        def write_quota(gateway):
            return call(gateway,'/v1/admin/tenants/shared/quota',quota,{**headers,'Idempotency-Key':'quota-create'},method='PUT')
        with ThreadPoolExecutor(max_workers=2) as pool:
            policies=list(pool.map(write_quota,[a,b]))
        assert policies[0][0]==200 and policies[0]==policies[1],policies
        assert call(a,'/v1/admin/tenants/shared/quota',quota,{**headers,'Idempotency-Key':'quota-stale'},method='PUT')[0]==409
        assert call(b,'/v1/admin/tenants/shared/quota',headers={'Authorization':'Bearer '+token('VIEWER','other')})[0]==404
        assert call(b,'/v1/admin/tenants/shared/quota',quota,{'Authorization':'Bearer '+token('OPERATOR','shared'),'Idempotency-Key':'operator'},method='PUT')[0]==403
        authority=PREFIX+'-authority'
        admission_actor={'sub':'github:1','role':'SRE_ADMIN','scopeTenant':'all','scopeRegion':'all'}
        def admission(action,values=None,credential=COORDINATOR):
            return call(authority,'/internal/v1/tenant-control/admission-'+action,{'actor':admission_actor,'tenantId':'shared',**(values or {})},{'Authorization':'Bearer '+credential})
        paused=admission('delivery-control',{'control':{'expectedVersion':0,'enabled':False,'reason':'Manual delivery validation fixture','idempotencyKey':'manual-delivery-mode'}});assert paused[0]==200,paused
        expires=int(time.time()*1000)+300000
        def admission_prepare(index):
            payload={'reservationId':'shadow-'+str(index),'taskId':'shadow-task-'+str(index),'attemptId':'shadow-attempt-'+str(index),'fencing':7,'expectedPolicyVersion':1,'resources':{'concurrentAttempts':1,'cpuMilliCores':500,'memoryMiB':256,'queuedTasks':0},'estimatedCreditUnits':'1','expiresAt':expires}
            return payload,admission('prepare',{'input':payload,'idempotencyKey':payload['reservationId']})
        assert admission('read',{'reservationId':'shadow-0'},SERVICE)[0]==401
        with ThreadPoolExecutor(max_workers=2) as pool:
            reservations=list(pool.map(admission_prepare,[0,1]))
        assert sorted(result[1][0] for result in reservations)==[200,409],reservations
        prepared_input,prepared=next(result for result in reservations if result[1][0]==200)
        assert prepared[1]['protocolMode']=='SHADOW' and prepared[1]['enforcement']=='NOT_CONNECTED',prepared
        assert admission('prepare',{'input':prepared_input,'idempotencyKey':prepared_input['reservationId']})==prepared
        dispatched=admission('dispatch');assert dispatched[0]==200 and dispatched[1]['delivery']['status']=='ACKNOWLEDGED',dispatched
        shadow=prepared[1]['reservation']
        assert admission('read',{'reservationId':shadow['id']})[1]['reservation']['phase']=='PREPARING'
        for to in ['RESERVED','START_AUTHORIZED']:
            transition={'reservationId':shadow['id'],'expectedVersion':shadow['version'],'generation':shadow['generation'],'fencing':shadow['fencing'],'to':to,'reason':'isolated shadow validation'}
            if to=='RESERVED':transition.update({'target':'shadow-body','capacityReceipt':'isolated-capacity-not-a-real-grant'})
            result=admission('transition',{'input':transition,'idempotencyKey':'shadow-'+to})
            assert result[0]==200,result
            shadow=result[1]['reservation']
        dispatched_start=admission('dispatch');assert dispatched_start[0]==200 and dispatched_start[1]['delivery']['status']=='ACKNOWLEDGED',dispatched_start
        assert admission('read',{'reservationId':shadow['id']})[1]['reservation']['phase']=='START_AUTHORIZED'
        issued=admission('credential',{'reservationId':shadow['id']})
        assert issued[0]==200 and issued[1]['protocolMode']=='SHADOW',issued
        shadow_credential=issued[1]['credential']
        assert admission('verify',{'reservationId':shadow['id'],'credential':shadow_credential,'target':'shadow-body'})[0]==200
        assert admission('verify',{'reservationId':shadow['id'],'credential':shadow_credential,'target':'wrong-body'})[0]==409
        assert admission('transition',{'input':{'to':'RUNNING'},'idempotencyKey':'untrusted-running'})[0]==409
        lowered=call(b,'/v1/admin/tenants/shared/quota',{**quota,'expectedVersion':1,'resources':{**quota['resources'],'concurrentAttempts':0}},{**headers,'Idempotency-Key':'quota-lower'},method='PUT')
        assert lowered[0]==200 and lowered[1]['policy']['version']==2,lowered
        first_history=call(a,'/v1/admin/tenants/shared/quota?limit=1',headers=headers)
        assert first_history[0]==200 and first_history[1]['current']['version']==2 and first_history[1]['history'][0]['version']==1 and first_history[1]['nextCursor'],first_history
        from urllib.parse import quote
        second_history=call(b,'/v1/admin/tenants/shared/quota?limit=1&cursor='+quote(first_history[1]['nextCursor']),headers=headers)
        assert second_history[0]==200 and second_history[1]['history'][0]['version']==2,second_history
        operation_id = 'tenant:' + hashlib.sha256(json.dumps(['shared','shared-create'], separators=(',', ':')).encode()).hexdigest()
        operation = call(b, '/v1/admin/operations/' + operation_id, headers=headers)
        assert operation[0] == 200 and operation[1]['resultVersion'] == 1, operation
        hidden = call(a, '/v1/admin/operations/' + operation_id, headers={'Authorization':'Bearer '+token('SRE_ADMIN','other')})
        assert hidden[0] == 404, hidden
        audit = call(a, '/v1/admin/audit', headers=headers)
        assert audit[0] == 200 and len(audit[1]['items']) == 3, audit
        scoped = call(b, '/v1/admin/tenants', headers={'Authorization': 'Bearer ' + token('VIEWER', 'other')})
        assert scoped[0] == 200 and scoped[1]['items'] == [], scoped
        archive = call(a, '/v1/admin/archives', {'expectedVersion':3,'reason':'recovery checkpoint'}, {**headers,'Idempotency-Key':'archive-checkpoint'})
        assert archive[0] == 200 and archive[1]['receiptRetention'] == 'ALL_RETAINED', archive
        archive_id=archive[1]['id']
        exported=call(b,'/v1/admin/archives/'+archive_id,headers=headers)
        assert exported[0] == 200 and exported[1]['manifest']['id'] == archive_id, exported
        assert call(b,'/v1/admin/archives/'+archive_id,headers={'Authorization':'Bearer '+token('SRE_ADMIN','other')})[0] == 403
        authority = PREFIX + '-authority'
        cmd('docker', 'restart', authority)
        ready(authority)
        assert call(b, '/v1/admin/audit', headers=headers) == audit
        assert call(a,'/v1/admin/archives/'+archive_id,headers=headers) == exported
        assert call(a, '/v1/admin/operations/' + operation_id, headers=headers) == operation
        recovered_shadow=admission('read',{'reservationId':shadow['id']})
        assert recovered_shadow[0]==200 and recovered_shadow[1]['reservation']==shadow,recovered_shadow
        assert admission('verify',{'reservationId':shadow['id'],'credential':shadow_credential,'target':'shadow-body'})[0]==200
        uncertain=admission('transition',{'input':{'reservationId':shadow['id'],'expectedVersion':shadow['version'],'generation':shadow['generation'],'fencing':shadow['fencing'],'to':'UNCERTAIN','reason':'isolated disconnect'},'idempotencyKey':'shadow-uncertain'})
        assert uncertain[0]==200,uncertain
        assert admission('verify',{'reservationId':shadow['id'],'credential':shadow_credential,'target':'shadow-body'})[0]==409
        pending_messages=admission('outbox');assert pending_messages[0]==200 and pending_messages[1]['messages'][0]['message']['kind']=='QUERY_EXECUTION',pending_messages
        restored_quota=call(b,'/v1/admin/tenants/shared/quota',headers=headers)
        assert restored_quota[0]==200 and restored_quota[1]['current']==lowered[1]['policy'],restored_quota
        cmd('docker', 'stop', authority)
        assert call(a, '/v1/admin/tenants', headers=headers)[0] == 503
        assert call(b, '/v1/admin/tenants', {**body, 'id': 'offline-write'}, {**headers, 'Idempotency-Key': 'offline-write'})[0] == 503
        cmd('docker', 'start', authority)
        ready(authority)
        assert admission('read',{'reservationId':shadow['id']})==uncertain
        assert admission('outbox')==pending_messages
        query_delivery=admission('dispatch');assert query_delivery[0]==200 and query_delivery[1]['delivery']['status']=='ACKNOWLEDGED',query_delivery
        assert admission('read',{'reservationId':shadow['id']})==uncertain
        restored = call(a, '/v1/admin/tenants', headers=headers)
        assert restored[0] == 200 and [item['id'] for item in restored[1]['items']] == ['shared'], restored
        assert call(b, '/v1/admin/audit', headers=headers) == audit
        auto_tenant={**body,'id':'automatic','name':'Automatic shadow alarm validation'}
        assert call(a,'/v1/admin/tenants',auto_tenant,{**headers,'Idempotency-Key':'automatic-tenant'})[0]==200
        auto_quota={**quota,'budget':{**quota['budget'],'limitCreditUnits':'100'}}
        assert call(a,'/v1/admin/tenants/automatic/quota',auto_quota,{**headers,'Idempotency-Key':'automatic-quota'},method='PUT')[0]==200
        def automatic(action,values=None):
            return call(authority,'/internal/v1/tenant-control/admission-'+action,{'actor':admission_actor,'tenantId':'automatic',**(values or {})},{'Authorization':'Bearer '+COORDINATOR})
        auto_input={'reservationId':'auto-r1','taskId':'auto-task-1','attemptId':'auto-attempt-1','fencing':7,'expectedPolicyVersion':1,'resources':{'concurrentAttempts':1,'cpuMilliCores':500,'memoryMiB':256,'queuedTasks':0},'estimatedCreditUnits':'1','expiresAt':int(time.time()*1000)+300000}
        auto_created=automatic('prepare',{'input':auto_input,'idempotencyKey':'automatic-prepare'});assert auto_created[0]==200,auto_created
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            auto_pending=automatic('outbox');assert auto_pending[0]==200,auto_pending
            if not auto_pending[1]['messages']:break
            time.sleep(.25)
        assert auto_pending[1]['messages']==[],auto_pending
        auto_status=automatic('delivery-status');assert auto_status[0]==200 and auto_status[1]['delivery']['runtime']['acknowledgedMessages']==1,auto_status
        assert automatic('read',{'reservationId':'auto-r1'})[1]['reservation']['phase']=='PREPARING'
        paused_auto=automatic('delivery-control',{'control':{'expectedVersion':1,'enabled':False,'reason':'pause before crash verification','idempotencyKey':'automatic-pause'}});assert paused_auto[0]==200,paused_auto
        cancelled_auto=automatic('transition',{'input':{'reservationId':'auto-r1','expectedVersion':1,'generation':1,'fencing':7,'to':'CANCELLED','reason':'cancel unstarted shadow fixture'},'idempotencyKey':'automatic-cancel'});assert cancelled_auto[0]==200,cancelled_auto
        time.sleep(1.2)
        paused_pending=automatic('outbox');assert paused_pending[0]==200 and paused_pending[1]['messages'][0]['message']['kind']=='RELEASE_CAPACITY',paused_pending
        resumed_auto=automatic('delivery-control',{'control':{'expectedVersion':2,'enabled':True,'reason':'resume across cold restart','idempotencyKey':'automatic-resume'}});assert resumed_auto[0]==200,resumed_auto
        cmd('docker','stop','-t','0',authority)
        stopped_at=int(time.time()*1000)
        time.sleep(1.2)
        cmd('docker','start',authority);ready(authority)
        # No tenant-control request during recovery: prove alarm work predates the first status read.
        time.sleep(2)
        first_read_at=int(time.time()*1000)
        recovered_auto=automatic('delivery-status')
        assert recovered_auto[0]==200 and recovered_auto[1]['delivery']['runtime']['acknowledgedMessages']==2,recovered_auto
        assert stopped_at<recovered_auto[1]['delivery']['runtime']['lastRunAt']<first_read_at,recovered_auto
        assert recovered_auto[1]['delivery']['runtime']['lastError'] is None,recovered_auto
        assert automatic('outbox')[1]['messages']==[]
        assert automatic('read',{'reservationId':'auto-r1'})==cancelled_auto
        print('PASS: automatic alarm delivery/pause/resume/cold-crash recovery and durable shadow cross-object protocol;  three native celld processes; shared authority, concurrent idempotency/CAS, tenant isolation, restart persistence and authority-offline fail-closed.')
    except Exception:
        for name in CONTAINERS:
            subprocess.run(['docker', 'logs', '--tail', '12', name])
        raise
    finally:
        for name in reversed(CONTAINERS):
            subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(['docker', 'network', 'rm', PREFIX], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
