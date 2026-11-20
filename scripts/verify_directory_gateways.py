#!/usr/bin/env python3
"""Three isolated celld processes exercise the real Gateway and Fleet entries."""
import base64
from datetime import datetime, timedelta, timezone
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
BODY_SECRET = 'f' * 64
SCHEDULER_AUTH = 'Gateway ' + hmac.new(BODY_SECRET.encode(), b'ansts-body-v1:gateway:control', hashlib.sha256).hexdigest()
TARGET_TOKEN = 'isolated-scheduler-target-token-no-production-value-12345'
CONFIG_TOKEN = 'isolated-config-service-token-no-production-value-12345'
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

def token(role='SRE_ADMIN', tenant='all', subject='github:1'):
    def enc(value):
        return base64.urlsafe_b64encode(json.dumps(value, separators=(',', ':')).encode()).rstrip(b'=').decode()
    now = int(time.time())
    text = enc({'alg': 'HS256', 'typ': 'JWT'}) + '.' + enc({'sub': subject, 'role': role,
        'scopeTenant': tenant, 'scopeRegion': 'all', 'iat': now, 'exp': now + 300})
    return text + '.' + base64.urlsafe_b64encode(hmac.new(USER.encode(), text.encode(), hashlib.sha256).digest()).rstrip(b'=').decode()

with tempfile.TemporaryDirectory(prefix=PREFIX) as scratch:
    cmd('docker', 'network', 'create', PREFIX)
    try:
        verify_rollout = os.environ.get('ANSTS_VERIFY_CONFIG_ROLLOUT')=='true'
        global_targets=['fleet','fleet-secondary'] if verify_rollout else ['fleet']
        processes=[('authority','worker.ts'),('gateway-a','gateway_worker.ts'),('gateway-b','gateway_worker.ts')]
        if verify_rollout: processes.insert(1,('target-2','worker.ts'))
        secondary=PREFIX+'-target-2'
        secondary_origin='http://'+secondary+':8080'
        for label, entry in processes:
            project = Path(scratch) / label
            project.mkdir()
            source = ROOT / 'src/cloudflare' / entry
            if label == 'authority':
                fixture = json.dumps(str(ROOT / 'scripts/period_native_fixture.ts'))
                worker = json.dumps(str(source))
                source = project / 'entry.ts'
                source.write_text(f"""
import worker from {worker};
export * from {worker};
export {{FixtureSettlementScheduler as AnstsSchedulerCell}} from {fixture};
export default {{fetch(request, env) {{
    if (new URL(request.url).pathname !== '/fixture/close-coverage') return worker.fetch(request, env);
    const ns = env.ANSTS_SCHEDULER_CELL;
    return ns.get(ns.idFromName('global-shard-0')).fetch(new Request('https://fixture/fixture-inventory-close-coverage', request));
}}}};
""")
            subprocess.run([str(ROOT / 'node_modules/.bin/esbuild'), str(source),
                '--bundle', '--format=esm', '--platform=neutral', '--external:node:*', '--outfile=' + str(project / 'worker.js')], check=True)
            config = {'name': PREFIX + '-' + label, 'main': 'worker.js', 'compatibility_date': '2025-08-15',
                'compatibility_flags': ['nodejs_compat'], 'vars': {'ANSTS_DIRECTORY_SERVICE_TOKEN': SERVICE}}
            if label == 'authority':
                config['vars'].update({'ANSTS_ADMISSION_SERVICE_TOKEN':COORDINATOR,'ANSTS_ADMISSION_SIGNING_KEY':ADMISSION_KEY,'ANSTS_CONFIG_SERVICE_TOKEN':CONFIG_TOKEN,'ANSTS_CONFIG_TARGET_TOKEN':TARGET_TOKEN,'ANSTS_BODY_AUTH_SECRET':BODY_SECRET,'ANSTS_CONFIG_SECRET_REFS':'provider/default','ANSTS_CONFIG_SCOPE_TARGETS_JSON':json.dumps({'global':global_targets}),'ANSTS_GITHUB_IDENTITIES_JSON':json.dumps({'1':{'role':'SRE_ADMIN','scopeTenant':'all'},'2':{'role':'SRE_ADMIN','scopeTenant':'all'}})})
                config['vars']['ANSTS_CONFIG_TARGET_ORIGINS_JSON']=json.dumps({'fleet':'http://'+PREFIX+'-authority:8080','fleet-secondary':secondary_origin})
                config.update({'durable_objects': {'bindings': [{'name': 'ANSTS_TENANT_DIRECTORY_CELL', 'class_name': 'TenantDirectoryCell'}, {'name':'ANSTS_TENANT_CONTROL_CELL','class_name':'TenantControlCell'},{'name':'ANSTS_ADMISSION_MAILBOX','class_name':'AdmissionShadowMailboxCell'},{'name':'ANSTS_SCHEDULER_CELL','class_name':'AnstsSchedulerCell'},{'name':'ANSTS_CONFIG_RELEASE_CELL','class_name':'ConfigReleaseCell'}]},
                    'migrations': [{'tag': 'v1', 'new_sqlite_classes': ['TenantDirectoryCell','TenantControlCell','AdmissionShadowMailboxCell','AnstsSchedulerCell','ConfigReleaseCell']}]})
            elif label=='target-2':
                config['vars'].update({'ANSTS_CONFIG_TARGET_TOKEN':TARGET_TOKEN,'ANSTS_BODY_AUTH_SECRET':BODY_SECRET})
                config.update({'durable_objects':{'bindings':[{'name':'ANSTS_SCHEDULER_CELL','class_name':'AnstsSchedulerCell'}]},
                    'migrations':[{'tag':'v1','new_sqlite_classes':['AnstsSchedulerCell']}]})
            else:
                config.update({'durable_objects': {'bindings': [{'name': 'ANSTS_GATEWAY_CELL', 'class_name': 'AnstsGatewayCell'}]}, 'migrations': [{'tag': 'v1', 'new_sqlite_classes': ['AnstsGatewayCell']}]})
                config['vars'].update({'ANSTS_USER_AUTH_SECRET': USER, 'ANSTS_DIRECTORY_URL': 'http://' + PREFIX + '-authority:8080'})
                origin = config['vars']['ANSTS_DIRECTORY_URL']
                config['vars'].update({'ANSTS_SCHEDULER_URL':origin,'ANSTS_SCHEDULER_SERVICE_TOKEN':SCHEDULER_AUTH.split(' ',1)[1]})
                config['vars'].update({'ANSTS_CONFIG_SERVICE_TOKEN': CONFIG_TOKEN,
                    'ANSTS_CONFIG_SCOPE_TARGETS_JSON': json.dumps({'global':global_targets if label=='gateway-a' else ['fleet-alt'], 'regional':['missing-fleet']}),
                    'ANSTS_SYSTEM_TOPOLOGY_JSON': json.dumps({'schemaVersion':1, 'edges':[], 'nodes':[
                        {'id':'fleet','kind':'fleet','plane':'control','label':'Native Fleet','description':'isolated preflight','url':origin},
                        {'id':'fleet-alt','kind':'fleet','plane':'control','label':'Alternate Fleet target','description':'isolated preflight','url':origin}
                    ]+([{'id':'fleet-secondary','kind':'fleet','plane':'control','label':'Secondary Scheduler','description':'isolated rollout','url':secondary_origin}] if verify_rollout else [])})})

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
        prior_month = (datetime.now(timezone.utc).replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
        period = '/v1/admin/tenants/automatic/settlements/' + prior_month
        close_headers = {**headers, 'Idempotency-Key': 'period-close'}
        preview_status, preview = call(a, period + '/preview', headers=headers)
        assert preview_status == 200 and not preview['complete'], preview
        close_body = {'reason':'Cross-process close acceptance', 'expectedRevision':preview['revision']}
        refused = call(b, period + '/close', close_body, close_headers)
        assert refused == (409, {'code':'PERIOD_INCOMPLETE'}), refused
        injection = call(authority, '/fixture/close-coverage', {}, {'x-fixture-key':'isolated-period-fixture-only'})
        assert injection[0] == 200, injection
        preview_status, preview = call(a, period + '/preview', headers=headers)
        assert preview_status == 200 and preview['complete'], preview
        close_body['expectedRevision'] = preview['revision']
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda gateway: call(gateway, period + '/close', close_body, close_headers), [a, b]))
        assert results[0] == results[1] and results[0][0] == 200 and results[0][1]['final'], results
        cmd('docker','restart',authority);ready(authority)
        assert call(b, period + '/close', close_body, close_headers) == results[0]
        config_actor = {'sub':'github:1','role':'SRE_ADMIN','scopeTenant':'all','scopeRegion':'all'}
        config_headers = {'Authorization':'Bearer ' + CONFIG_TOKEN}
        def config(scope, action, values, headers=config_headers):
            return call(authority, '/internal/v1/config-versions/' + scope + '/' + action,
                {'actor':config_actor,**values}, headers)
        payload = {'schemaVersion':1,'baseRevision':0,'content':{'defaultTaskTimeoutSeconds':90}}
        assert config('global','create',payload,{'Authorization':'Bearer ' + SERVICE})[0] == 401
        assert config('global','create',{**payload,'actor':{**config_actor,'role':'VIEWER'}})[0] == 403
        assert config('global','create',{**payload,'content':{'modelCredentialRef':'literal-secret'}})[0] == 422
        assert config('global','create',{**payload,'content':{'databaseUrl':'internal'}})[0] == 422
        with ThreadPoolExecutor(max_workers=2) as pool:
            writes = list(pool.map(lambda _: config('global','create',payload), range(2)))
        assert sorted(result[0] for result in writes) == [201,409], writes
        first = next(result[1] for result in writes if result[0]==201)
        assert config('other','create',payload)[1]['contentDigest'] == first['contentDigest']
        credential={**payload,'content':{**payload['content'],'modelCredentialRef':'provider/default'}}
        assert config('regional','create',credential)[1]['content']['modelCredentialRef']=='provider/default'
        second = config('global','create',{**payload,'baseRevision':1,'content':{**payload['content'],'unhealthyNodeThresholdBps':500}})
        assert second[0]==201 and second[1]['baseRevision']==1
        cmd('docker','restart',authority);ready(authority)
        assert config('global','read',{'revision':1}) == (200,first)
        assert config('global','read',{}) == (200,second[1])
        validate = '/v1/admin/config-bundles/global/validate'
        assert call(a,validate,{'revision':2})[0] == 401
        assert call(a,validate,{'revision':2},{'Authorization':'Bearer '+token('VIEWER')})[0] == 403
        assert call(a,validate,{'revision':2,'observations':[{'targetId':'fleet','healthy':True}]},headers)[0] == 422
        live = call(a,validate,{'revision':2},headers)
        assert live[0]==200 and live[1]['eligible'] and live[1]['contentDigest']==second[1]['contentDigest'], live
        assert live[1]['changes'][0]['targetIds']==global_targets and live[1]['changes'][0]['effect']=='HOT_RELOAD', live
        offline = call(b,validate.replace('/global/','/regional/'),{'revision':1},headers)
        assert offline[0]==200 and not offline[1]['eligible'], offline
        assert {item['code'] for item in offline[1]['blockers']}=={'CONFIG_RESTART_UNSUPPORTED','CONFIG_OBSERVATION_MISSING'}, offline
        submit = validate.replace('/validate','/submit')
        approvals = validate.replace('/validate','/approvals')
        approver_headers = {'Authorization':'Bearer '+token(subject='github:2')}
        submitted = call(a,submit,{'revision':2},headers)
        assert submitted[0]==200 and submitted[1]['requestedBy']=='github:1', submitted
        decision = {'revision':2,'sequence':submitted[1]['sequence'],'approved':True}
        assert call(a,approvals,decision,headers)==(403,{'code':'INDEPENDENT_APPROVER_REQUIRED'})
        assert call(a,approvals,{**decision,'approvedBy':'github:2'},approver_headers)[0]==422
        assert call(b,approvals,decision,approver_headers)==(409,{'code':'CONFIG_APPROVAL_STALE'})
        approved = call(a,approvals,decision,approver_headers)
        assert approved[0]==200 and approved[1]['status']=='APPROVED' and approved[1]['decidedBy']=='github:2', approved
        cmd('docker','restart',authority);ready(authority)
        assert call(a,approvals,decision,approver_headers)==approved
        public_draft=validate.replace('/validate','/draft')
        assert call(a,public_draft,headers=headers)==(200,second[1])
        assert call(a,approvals+'/status',headers=approver_headers)==approved
        assert call(a,public_draft,{'baseRevision':0,'content':{'defaultTaskTimeoutSeconds':91}},headers)==(409,{'code':'VERSION_CONFLICT'})
        assert call(a,public_draft.replace('/global/','/console-draft/'),{'baseRevision':0,'content':{'defaultTaskTimeoutSeconds':91}},headers)[0]==201
        if verify_rollout:
            release=validate.replace('/validate','/releases')
            heartbeat={'Authorization':SCHEDULER_AUTH}
            assert call(authority,'/v1/scheduler/register-node',{'node_id':'rollout-healthy','healthy':True},heartbeat)[0]==200
            started=call(a,release,{'revision':2,'releaseId':'native-rollout','batchSize':1,'healthWindowMs':1200},headers)
            assert started[0]==200 and started[1]['status']=='APPLYING' and 'targetOrigins' not in started[1]['plan'],started
            def wait_rollout(expected):
                deadline=time.monotonic()+30
                while time.monotonic()<deadline:
                    result=call(a,release+'/status',headers=headers)
                    assert result[0]==200,result
                    if result[1]['status']==expected:return result[1]
                    time.sleep(.1)
                raise AssertionError(result)
            wait_rollout('VERIFYING')
            cmd('docker','restart',authority);ready(authority)
            paused=wait_rollout('PAUSED')
            assert paused['batchIndex']==1 and len(paused['verifiedBatches'])==1,paused
            assert paused['acknowledgements'][0]['targetId']=='fleet-secondary',paused
            assert call(secondary,'/v1/scheduler/register-node',{'node_id':'rollout-recovered','healthy':True},heartbeat)[0]==200
            resumed=call(a,release+'/resume',{'releaseId':'native-rollout','generation':paused['generation']},headers)
            assert resumed[0]==200 and resumed[1]['status']=='VERIFYING',resumed
            done=wait_rollout('SUCCEEDED')
            assert [batch[0]['targetId'] for batch in done['verifiedBatches']]==global_targets,done
            print('PASS: authenticated Gateway starts an automatic two-target rollout; Fleet restart, second-batch pause and resume preserve real ACKs.')
        new_payload={**payload,'baseRevision':2,'content':{**payload['content'],'unhealthyNodeThresholdBps':600}}
        assert config('global','create',new_payload)[0]==201
        assert call(a,approvals,decision,approver_headers)==(409,{'code':'CONFIG_APPROVAL_STALE'})
        new_submission=call(a,submit,{'revision':3},headers)
        assert new_submission[0]==200 and new_submission[1]['status']=='SUBMITTED', new_submission
        if verify_rollout:
            next_decision={'revision':3,'sequence':new_submission[1]['sequence'],'approved':True}
            assert call(a,approvals,next_decision,approver_headers)[0]==200
            forward=call(a,release,{'revision':3,'releaseId':'native-forward','batchSize':1,'healthWindowMs':100},headers)
            assert forward[0]==200,forward
            forward_done=wait_rollout('SUCCEEDED')
            previous_generation=forward_done['generation']
            cmd('docker','stop','-t','0',secondary)
            rollback_url=validate.replace('/validate','/rollback')
            rollback_input={'sourceGeneration':done['generation'],'replacesGeneration':previous_generation,'baseRevision':3}
            rollback=call(a,rollback_url,rollback_input,headers)
            assert rollback[0]==201 and rollback[1]['revision']==4 and rollback[1]['contentDigest']==second[1]['contentDigest'],rollback
            assert call(a,rollback_url,rollback_input,headers)==rollback
            rollback_plan={'revision':4,'releaseId':'native-rollback','batchSize':1,'healthWindowMs':100}
            assert call(a,release,rollback_plan,headers)==(409,{'code':'CONFIG_APPROVAL_REQUIRED'})
            rollback_submission=call(a,submit,{'revision':4},headers)
            assert rollback_submission[0]==200,rollback_submission
            rollback_decision={'revision':4,'sequence':rollback_submission[1]['sequence'],'approved':True}
            assert call(a,approvals,rollback_decision,headers)[0]==403
            assert call(a,approvals,rollback_decision,approver_headers)[0]==200
            assert call(a,release,rollback_plan,headers)[0]==200
            partial=wait_rollout('PAUSED')
            assert partial['pendingTargetIds']==['fleet-secondary'] and len(partial['verifiedBatches'])==1,partial
            assert call(a,release+'/status?generation='+str(previous_generation),headers=headers)==(200,forward_done)
            cmd('docker','start',secondary);ready(secondary)
            for container,node in [(authority,'rollout-healthy'),(secondary,'rollout-recovered')]:
                assert call(container,'/v1/scheduler/register-node',{'node_id':node,'healthy':True},heartbeat)[0]==200
            assert call(a,release+'/resume',{'releaseId':'native-rollback','generation':partial['generation']},headers)[0]==200
            restored=wait_rollout('SUCCEEDED')
            assert restored['pendingTargetIds']==[] and restored['plan']['rollback']=={'sourceGeneration':done['generation'],'replacesGeneration':previous_generation},restored
            assert call(a,release+'/status?generation='+str(previous_generation),headers=headers)==(200,forward_done)
            print('PASS: forward rollback creates a new revision and approval; unavailable target remains pending until its verified recovery, preserving past evidence.')
        print('PASS: native dual control verifies two numeric GitHub identities, rejects spoofed decisions and stale bases, and persists the decision across restart.')
        print('PASS: real Gateway preflight loads immutable drafts and probes Fleet health; missing targets and restart requirements block approval input.')
        if os.environ.get('ANSTS_VERIFY_CONFIG_ROLLOUT')!='true':
            target_path='/internal/v1/config-target/scheduler/'
            target_headers={'Authorization':'Bearer '+TARGET_TOKEN}
            def target(action, values=None, auth=target_headers):
                return call(authority,target_path+action,values,auth)
            runtime_content={'defaultTaskTimeoutSeconds':90}
            encoded=json.dumps({'schemaVersion':1,'content':runtime_content},separators=(',', ':')).encode()
            command={'releaseId':'native-apply','generation':2,'content':runtime_content,'contentDigest':hashlib.sha256(encoded).hexdigest()}
            assert target('apply',command,config_headers)[0]==401
            def submit_runtime_task(task_id, **fields):
                request={'task_id':task_id,'tenant_id':'shared','prompt':task_id,**fields}
                result=call(authority,'/v1/scheduler/submit',request,{'Authorization':SCHEDULER_AUTH})
                assert result[0]==202,result
            submit_runtime_task('config-before')
            dry=target('dry-run',command)
            assert dry[0]==200 and dry[1]['actualDigest'] is None,dry
            applied=target('apply',command)
            assert applied==(200,{'releaseId':'native-apply','generation':2,'actualDigest':command['contentDigest']}),applied
            public_task=call(a,'/v1/tasks',{'task_id':'config-default','tenant_id':'shared','prompt':'config-default'},headers)
            assert public_task[0] in (200,202),public_task
            submit_runtime_task('config-explicit',timeout_seconds=120)
            cmd('docker','restart',authority);ready(authority)
            assert target('apply',command)==applied
            assert target('apply',{**command,'generation':1})==(409,{'code':'STALE_CONFIG_GENERATION'})
            assert target('readback')==(200,{'generation':2,'actualDigest':command['contentDigest'],'verified':True})
            submit_runtime_task('config-after-restart')
            saved=call(authority,'/v1/scheduler/tasks',headers={'Authorization':SCHEDULER_AUTH})[1]['tasks']
            effective={row['prompt']:row['timeout_seconds'] for row in saved if row['prompt'].startswith('config-')}
            assert effective=={'config-before':1800,'config-default':90,'config-explicit':120,'config-after-restart':90},effective
            heartbeat_headers={'Authorization':SCHEDULER_AUTH}
            assert call(authority,'/v1/scheduler/register-node',{'node_id':'config-unhealthy','healthy':False},heartbeat_headers)[0]==200
            def threshold_command(limit, generation):
                content={**runtime_content,'unhealthyNodeThresholdBps':limit}
                encoded=json.dumps({'schemaVersion':1,'content':content},separators=(',', ':')).encode()
                return {'releaseId':'threshold-'+str(generation),'generation':generation,
                    'content':content,'contentDigest':hashlib.sha256(encoded).hexdigest()}
            strict=threshold_command(0,3)
            assert target('dry-run',strict)[0]==200 and call(authority,'/readyz')[0]==200
            assert target('apply',strict)[1]['actualDigest']==strict['contentDigest']
            not_ready=call(authority,'/readyz')
            assert not_ready[0]==503 and not_ready[1]['schedulerReadiness']['unhealthyNodeCount']==1,not_ready
            assert call(authority,'/healthz')[0]==200
            relaxed=threshold_command(10000,4)
            assert target('apply',relaxed)[1]['actualDigest']==relaxed['contentDigest']
            assert call(authority,'/readyz')[0]==200
            cmd('docker','restart',authority);ready(authority)
            assert target('readback')==(200,{'generation':4,'actualDigest':relaxed['contentDigest'],'verified':True})
            actual_ready=call(authority,'/readyz')
            assert actual_ready[0]==200 and actual_ready[1]['schedulerReadiness']['unhealthyNodeThresholdBps']==10000, actual_ready
            print('PASS: native Scheduler ACKs real readback and applies the default only to new tasks; generation and behavior survive restart.')
        print('PASS: native ConfigReleaseCell rejects unauthorized writes and unsafe configuration, isolates scopes, and preserves immutable revisions after restart.')
        print('PASS: two distinct Gateway processes share one Fleet close receipt, including replay after Fleet restart.')
        print('PASS: automatic alarm delivery/pause/resume/cold-crash recovery and durable shadow cross-object protocol;  three native celld processes; shared authority, concurrent idempotency/CAS, tenant isolation, restart persistence and authority-offline fail-closed.')
    except Exception:
        for name in CONTAINERS:
            subprocess.run(['docker', 'logs', '--tail', '12', name])
        raise
    finally:
        for name in reversed(CONTAINERS):
            subprocess.run(['docker', 'rm', '-f', name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(['docker', 'network', 'rm', PREFIX], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
