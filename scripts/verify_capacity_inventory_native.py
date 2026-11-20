#!/usr/bin/env python3
"""Isolated real celld capacity authority, two Gateway objects, native signed reporters and recovery."""
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
ROOT=Path(__file__).resolve().parents[1]
NAME='ansts-capacity-'+uuid.uuid4().hex[:8]
KEY='d'*64
BODY_ROOT='f'*64
HTTP=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def cmd(*args):return subprocess.check_output(args,text=True,stderr=subprocess.PIPE).strip()
def address():return 'http://127.0.0.1:'+cmd('docker','port',NAME,'8080').split(':')[-1]
def call(path,body=None,auth='Bearer '+KEY,headers=None,method=None,origin=None):
    request=urllib.request.Request((origin or address())+path,data=None if body is None else json.dumps(body).encode(),headers={'content-type':'application/json','authorization':auth,**(headers or {})},method=method)
    try:
        with HTTP.open(request,timeout=15) as response:
            raw=response.read().decode();return response.status,json.loads(raw) if raw.startswith('{') else raw
    except urllib.error.HTTPError as error:
        raw=error.read().decode();return error.code,json.loads(raw) if raw.startswith('{') else raw
def ready():
    for _ in range(100):
        try:
            if call('/ping')[0]==200:return
        except (OSError,ValueError):pass
        time.sleep(.1)
    raise RuntimeError('isolated celld did not become ready')
def encoded(value):return base64.urlsafe_b64encode(json.dumps(value,separators=(',',':')).encode()).rstrip(b'=').decode()
actor={'sub':'github:1','role':'SRE_ADMIN','scopeTenant':'all','scopeRegion':'all'}
now=int(time.time());jwt=encoded({'alg':'HS256','typ':'JWT'})+'.'+encoded({**actor,'iat':now,'exp':now+900});jwt+='.'+base64.urlsafe_b64encode(hmac.new(KEY.encode(),jwt.encode(),hashlib.sha256).digest()).rstrip(b'=').decode()
limit={'cpuMilliCores':1000,'memoryMiB':1024,'slots':2}
with tempfile.TemporaryDirectory(prefix=NAME) as scratch:
    p=Path(scratch)
    p.joinpath('entry.ts').write_text('import {TenantControlCell} from '+json.dumps(str(ROOT/'src/cloudflare/tenant_control_cell.ts'))+';\nimport {CapacityInventoryCell} from '+json.dumps(str(ROOT/'src/cloudflare/capacity_inventory_cell.ts'))+';\nimport {handleCapacityIngress} from '+json.dumps(str(ROOT/'src/control/capacity_ingress.ts'))+';\nimport {handleUnifiedGatewayRequest} from '+json.dumps(str(ROOT/'src/cloudflare/gateway_cell.ts'))+';\nexport {CapacityInventoryCell};\nexport class TestDirectory {async fetch(request){const {tenantId}=await request.json();return Response.json({id:tenantId,name:tenantId,region:"dev",costCenter:"fixture",version:1,status:"ACTIVE"});}}\nexport class TestTenant extends TenantControlCell {constructor(ctx,env){super(ctx,env);this.fixtureEnv=env;}async fetch(request){const {tenantId,fixtureOffline}=await request.clone().json();const route=new URL(request.url).pathname;if(route==="/fixture-toggle"&&request.headers.get("authorization")==="Bearer "+this.fixtureEnv.ANSTS_ADMISSION_SERVICE_TOKEN){this.offline=fixtureOffline;return Response.json({ok:true});}if(this.offline&&route==="/admission-prepare-estimated")return Response.json({code:"TENANT_CONTROL_UNAVAILABLE"},{status:503});const headers={authorization:"Bearer "+this.fixtureEnv.ANSTS_DIRECTORY_SERVICE_TOKEN,"content-type":"application/json"};const fixture={actor:{sub:"fixture-admin",role:"SRE_ADMIN",scopeTenant:"all",scopeRegion:"all"},tenantId,input:{expectedVersion:0,resources:{cpuMilliCores:1000,memoryMiB:1024,concurrentAttempts:tenantId==="other"?0:1,queuedTasks:10},budget:{period:"UTC_DAY",limitCreditUnits:tenantId==="shadow-budget"?"4":"100",softThresholdBps:8000},reason:"isolated capacity fixture",idempotencyKey:"policy",requestId:"policy"}};const configured=await super.fetch(new Request("https://tenant.internal/policy-write",{method:"POST",headers,body:JSON.stringify(fixture)}));if(!configured.ok)return configured;const lostStart=tenantId==="pilot"&&route==="/admission-transition"&&(await request.clone().json()).input?.to==="START_AUTHORIZED";const result=await super.fetch(request);if(lostStart&&result.ok&&!this.lostStart){this.lostStart=true;await result.body?.cancel();return Response.json({code:"FIXTURE_LOST_START_ACK"},{status:503});}return result;}}\nexport class TestGateway {constructor(ctx,env){this.env=env;}async fetch(request){return await handleUnifiedGatewayRequest(request,this.env)??new Response("missing",{status:404});}}\nexport default {async fetch(request,env){const url=new URL(request.url);if(url.pathname==="/ping")return new Response("ok");const match=url.pathname.match(/^\\/(gateway-a|gateway-b)(\\/.*)$/);if(match){url.pathname=match[2];return env.INGRESS.get(env.INGRESS.idFromName(match[1])).fetch(new Request(url,request));}if(url.pathname.startsWith("/fixture/tenant/")&&request.headers.get("authorization")==="Bearer "+env.ANSTS_ADMISSION_SERVICE_TOKEN){return env.ANSTS_TENANT_CONTROL_CELL.get(env.ANSTS_TENANT_CONTROL_CELL.idFromName("tenant-control-v1:"+(await request.clone().json()).tenantId)).fetch(new Request("https://tenant.internal/"+url.pathname.split("/").pop(),request));}return handleCapacityIngress(request,env);}};\n')
    entry=p/'entry.ts'
    entry.write_text(entry.read_text().replace('export {CapacityInventoryCell};',
        'export class TestCapacity extends CapacityInventoryCell { async fetch(request){const pilotBind=new URL(request.url).pathname==="/grant-bind"&&(await request.clone().json()).reservation.tenantId==="pilot";const result=await super.fetch(request);if(pilotBind&&result.ok&&!this.lost){this.lost=true;await result.body?.cancel();return Response.json({code:"FIXTURE_LOST_BIND_ACK"},{status:503});}return result;}}' +
        '\nimport {AnstsSchedulerCell} from '+json.dumps(str(ROOT/'src/cloudflare/scheduler_cell.ts'))+';' +
        '\nimport schedulerWorker from '+json.dumps(str(ROOT/'src/cloudflare/worker.ts'))+';' +
        '\nexport class TestScheduler extends AnstsSchedulerCell {async fetch(request){const path=new URL(request.url).pathname;if(path==="/fixture-shard-toggle"){const input=await request.json();if("offline" in input)this.offline=input.offline;return Response.json({ok:true,failedExpiryProbes:this.failedExpiryProbes??0});}if((this.offline===true&&path==="/pilot-occupancy")||(this.offline==="expiry"&&path==="/pilot-expiry-status"&&!request.headers.get("x-fixture-inspect"))){if(path==="/pilot-expiry-status")this.failedExpiryProbes=(this.failedExpiryProbes??0)+1;return Response.json({}, {status:503});}return super.fetch(request);}}' +
        '\nexport {AnstsGatewayCell} from '+json.dumps(str(ROOT/'src/cloudflare/gateway_cell.ts'))+';'
    ).replace('return handleCapacityIngress(request,env);',
        'if(url.pathname==="/fixture/scheduler-object-id"&&request.headers.get("authorization")==="Bearer "+env.ANSTS_ADMISSION_SERVICE_TOKEN){const ns=env.ANSTS_SCHEDULER_CELL;const id=ns.idFromName("global-shard-0");if(typeof ns.idFromString!=="function")return Response.json({supported:false});const reconstructed=ns.idFromString(id.toString());const target=new URL("https://scheduler.internal/pilot-occupancy?tenantId=pilot");const read=await ns.get(reconstructed).fetch(new Request(target,{headers:{authorization:"Bearer "+env.ANSTS_DIRECTORY_SERVICE_TOKEN}}));return Response.json({supported:true,sameId:reconstructed.toString()===id.toString(),status:read.status,legacyId:ns.idFromName("legacy-shard").toString()});}if(url.pathname==="/fixture/shard-toggle"&&request.headers.get("authorization")==="Bearer "+env.ANSTS_ADMISSION_SERVICE_TOKEN)return env.ANSTS_SCHEDULER_CELL.get(env.ANSTS_SCHEDULER_CELL.idFromName(url.searchParams.get("target")==="main"?"global-shard-0":"legacy-shard")).fetch(new Request("https://cell/fixture-shard-toggle",request));if(url.pathname==="/fixture/expiry-status"&&request.headers.get("authorization")==="Bearer "+env.ANSTS_ADMISSION_SERVICE_TOKEN)return env.ANSTS_SCHEDULER_CELL.get(env.ANSTS_SCHEDULER_CELL.idFromName("global-shard-0")).fetch(new Request("https://cell/pilot-expiry-status",{method:request.method,headers:{authorization:request.headers.get("authorization"),"x-fixture-inspect":"true"},body:await request.text()}));if(url.pathname.startsWith("/v1/scheduler/")) return schedulerWorker.fetch(request,env);return handleCapacityIngress(request,env);'))
    if os.environ.get('ANSTS_VERIFY_PILOT_QUEUE_RACE')=='1':
        assert os.environ.get('ANSTS_VERIFY_SEPARATE_GATEWAYS')=='1'
        entry=p/'entry.ts'
        entry.write_text(entry.read_text().replace('queuedTasks:10','queuedTasks:tenantId==="pilot"?1:10'))
    drop_prepare=os.environ.get('ANSTS_VERIFY_LOST_PREPARE_ACK')=='1'
    if drop_prepare:
        entry=p/'entry.ts'
        source=entry.read_text()
        anchor='const result=await super.fetch(request);if(lostStart'
        assert source.count(anchor)==1
        entry.write_text(source.replace(anchor,'''const result=await super.fetch(request);
        if(tenantId==="pilot"&&route==="/admission-prepare-estimated"&&result.ok&&!this.lostPrepare){
            this.lostPrepare=true;await result.body?.cancel();return Response.json({code:"FIXTURE_LOST_PREPARE_ACK"},{status:503});
        }if(lostStart'''))
    drop_acceptance=os.environ.get('ANSTS_VERIFY_LOST_ACCEPTANCE_ACK')=='1'
    if drop_acceptance:
        entry=p/'entry.ts'
        source=entry.read_text()
        anchor='return super.fetch(request);}}'
        index=source.index(anchor,source.index('class TestScheduler'))
        source=source[:index]+'''const result=await super.fetch(request);
        if(path.endsWith("/submit")&&result.status===202&&!this.lostAcceptance){
          const submitted=await result.clone().json();
          if(submitted.tenant_id==="pilot"&&submitted.task_id==="ansts:v1:WyJwaWxvdCIsInBpbG90LWEiXQ"){this.lostAcceptance=true;await result.body?.cancel();
            return Response.json({code:"FIXTURE_LOST_ACCEPTANCE_ACK"},{status:503});}
        }
        return result;}}'''+source[index+len(anchor):]
        entry.write_text(source)
    subprocess.run([str(ROOT/'node_modules/.bin/esbuild'),str(p/'entry.ts'),'--bundle','--format=esm','--platform=neutral','--external:node:*','--outfile='+str(p/'worker.js')],check=True)
    p.joinpath('wrangler.json').write_text(json.dumps({'name':NAME,'main':'worker.js','compatibility_date':'2025-08-15','compatibility_flags':['nodejs_compat'],'vars':{'ANSTS_DIRECTORY_SERVICE_TOKEN':KEY,'ANSTS_USER_AUTH_SECRET':KEY,'ANSTS_BODY_AUTH_SECRET':BODY_ROOT,'ANSTS_ADMISSION_SERVICE_TOKEN':'e'*64},'durable_objects':{'bindings':[{'name':'ANSTS_CAPACITY_INVENTORY_CELL','class_name':'TestCapacity'},{'name':'INGRESS','class_name':'TestGateway'},{'name':'ANSTS_TENANT_CONTROL_CELL','class_name':'TestTenant'},{'name':'ANSTS_TENANT_DIRECTORY_CELL','class_name':'TestDirectory'}]},'migrations':[{'tag':'v1','new_sqlite_classes':['TestCapacity','TestGateway','TestTenant','TestDirectory']}]}))
    config_path=p/'wrangler.json'
    config=json.loads(config_path.read_text())
    config['vars'].update({
        'ANSTS_ADMISSION_MODE':'observe','ANSTS_ADMISSION_PILOT_TENANTS':'pilot',
        'ANSTS_ADMISSION_SCHEDULER_SHARDS':'global-shard-0,legacy-shard',
        'ANSTS_ADMISSION_PILOT_POOL':'pilot','ANSTS_ADMISSION_ESTIMATED_CREDIT_UNITS':'5',
        'ANSTS_SCHEDULER_URL':'http://127.0.0.1:8080','ANSTS_SANDBOX_IMAGE':'node@sha256:'+'a'*64,
        'ANSTS_SCHEDULER_SERVICE_TOKEN':hmac.new(BODY_ROOT.encode(),b'ansts-body-v1:gateway:control',hashlib.sha256).hexdigest(),
    })
    for binding,kind in [('ANSTS_SCHEDULER_CELL','TestScheduler'),('ANSTS_GATEWAY_CELL','AnstsGatewayCell')]:
        config['durable_objects']['bindings'].append({'name':binding,'class_name':kind})
        config['migrations'][0]['new_sqlite_classes'].append(kind)
    config_path.write_text(json.dumps(config))
    gateway_names=[]
    split_gateways=os.environ.get('ANSTS_VERIFY_SEPARATE_GATEWAYS')=='1'
    queue_race=os.environ.get('ANSTS_VERIFY_PILOT_QUEUE_RACE')=='1'

    network_args=['--network',NAME] if split_gateways else []
    if split_gateways:
        cmd('docker','network','create',NAME)
    try:
        cmd('docker','run','-d','--user',str(os.getuid())+':'+str(os.getgid()),'--name',NAME,*network_args,'-p','127.0.0.1::8080','-e','CELLD_ESBUILD=/tools/esbuild','-v',str(ROOT/'node_modules/@esbuild/linux-x64/bin/esbuild')+':/tools/esbuild:ro','-v',scratch+':/probe','ghcr.io/denoland/celld:0.6.3','dev','/probe','--host','0.0.0.0','--port','8080','--no-watch')
        ready()
        if os.environ.get('ANSTS_VERIFY_SCHEDULER_OBJECT_ID')=='1':
            result=call('/fixture/scheduler-object-id',auth='Bearer '+'e'*64)
            print('Scheduler object ID reconstruction:',result)
            assert result[0]==200 and result[1].get('sameId') is True and result[1].get('status')==200,result
            raise SystemExit(0)
        scheduler_auth='Gateway '+config['vars']['ANSTS_SCHEDULER_SERVICE_TOKEN']
        for route in ('pilot-expiry-status','pilot-occupancy'):
            assert call('/v1/scheduler/'+route,auth=scheduler_auth)==(403,{'code':'INTERNAL_ROUTE'})
        print('PASS: pilot reconciliation routes are internal only.')
        status,legacy_task=call('/v1/scheduler/submit?shard=legacy-shard',
                                {'task_id':'legacy','tenant_id':'pilot','prompt':'Existing queued work'},scheduler_auth)
        assert status==202,legacy_task
        ids=call('/fixture/scheduler-object-id',auth='Bearer '+'e'*64)
        assert ids[0]==200 and ids[1].get('sameId') is True,ids
        config['vars']['ANSTS_ADMISSION_SCHEDULER_SHARDS']='global-shard-0,id:'+ids[1]['legacyId']
        config['vars']['ANSTS_ADMISSION_MODE']='enforce'
        config_path.write_text(json.dumps(config))
        cmd('docker','restart',NAME)
        ready()
        pool={'id':'pool','name':'Pool','region':'dev','status':'ACTIVE','limit':limit,'expectedVersion':0,'reason':'isolated capacity acceptance'}
        def create(gateway):return call('/'+gateway+'/v1/admin/capacity/pools',pool,'Bearer '+jwt,{'idempotency-key':'pool'})
        with ThreadPoolExecutor(max_workers=2) as workers:responses=list(workers.map(create,['gateway-a','gateway-b']))
        assert responses[0][0]==200 and responses[0]==responses[1],responses
        def role_token(role):
            claims={**actor,'sub':'capacity-role-'+role,'role':role,'scopeTenant':'team',
                    'iat':int(time.time()),'exp':int(time.time())+300}
            payload=encoded({'alg':'HS256','typ':'JWT'})+'.'+encoded(claims)
            signature=base64.urlsafe_b64encode(hmac.new(KEY.encode(),payload.encode(),hashlib.sha256).digest()).rstrip(b'=').decode()
            return 'Bearer '+payload+'.'+signature
        allocation='/gateway-a/v1/admin/capacity/allocation-control?poolId=pool'
        for role in ('VIEWER','OPERATOR'):
            auth=role_token(role)
            assert call(allocation,{'enabled':False,'expectedVersion':0},auth,method='PUT')[0]==403
            assert call('/gateway-a/v1/admin/capacity/pools',pool,auth,{'idempotency-key':'role-pool'})[0]==403
            assert call('/gateway-a/v1/admin/capacity/catalog?tenantId=team',auth=auth)[0]==200
        status,state=call(allocation,auth='Bearer '+jwt)
        assert status==200 and state['enabled'],state
        print('PASS: signed VIEWER and OPERATOR can view their catalog but cannot manage pool or allocation state.')
        host={'id':'physical-1','poolId':'pool','daemonIds':['daemon-1','daemon-2'],'status':'ACTIVE','limit':limit,'expectedVersion':0,'reason':'Two reporters share one physical Host'}
        assert call('/gateway-a/v1/admin/capacity/hosts',host,'Bearer '+jwt,{'idempotency-key':'host'})[0]==200
        def race(gateway):return call('/'+gateway+'/v1/admin/capacity/pools',{**pool,'expectedVersion':1,'name':gateway},'Bearer '+jwt,{'idempotency-key':gateway},method='PUT')
        with ThreadPoolExecutor(max_workers=2) as workers:responses=list(workers.map(race,['gateway-a','gateway-b']))
        assert sorted(r[0] for r in responses)==[200,409],responses
        proofs=[]
        for daemon in ['daemon-1','daemon-2']:
            report={'schemaVersion':1,'purpose':'ansts-capacity-report-v1','hostId':'physical-1','daemonId':daemon,'epoch':1,'sequence':1,'observedAt':int(time.time()*1000),'total':limit,'reserved':{'cpuMilliCores':100,'memoryMiB':128,'slots':1},'draining':False,'recoveryRequired':False}
            key=hmac.new(BODY_ROOT.encode(),('ansts-body-v1:'+daemon).encode(),hashlib.sha256).hexdigest()
            payload=encoded(report);proof=payload+'.'+base64.urlsafe_b64encode(hmac.new(key.encode(),('ansts-capacity-report-v1.'+payload).encode(),hashlib.sha256).digest()).rstrip(b'=').decode();proofs.append((proof,key,daemon))
            assert call('/internal/v1/capacity/report',{'proof':proof},'Body '+key,{'x-ansts-body-id':daemon})[0]==200
        status,snapshot=call('/gateway-b/v1/admin/capacity/hosts',auth='Bearer '+jwt)
        assert status==200 and snapshot['pools'][0]['capacity']==limit and snapshot['hosts'][0]['retainedReserved']['slots']==2,snapshot
        tenant_query='/v1/admin/capacity/requests?poolId=pool&tenantId=team'
        request_body={'resources':{'cpuMilliCores':200,'memoryMiB':128,'slots':1}}
        def submit(gateway):
            return call('/'+gateway+tenant_query,request_body,'Bearer '+jwt,{'idempotency-key':'borrow'})
        with ThreadPoolExecutor(max_workers=2) as workers:
            requests=list(workers.map(submit,['gateway-a','gateway-b']))
        assert requests[0]==requests[1] and requests[0][0]==200,requests
        request_id=requests[0][1]['id']
        before=snapshot['hosts'][0]['reports']
        cmd('docker','restart',NAME);ready()
        status,recovered=call('/gateway-a/v1/admin/capacity/hosts',auth='Bearer '+jwt)
        for _ in range(20):
            if status==200 and recovered.get('hosts') and recovered['hosts'][0]['reports']==before:
                break
            time.sleep(.15)
            status,recovered=call('/gateway-a/v1/admin/capacity/hosts',auth='Bearer '+jwt)
        else:
            raise AssertionError((status,recovered))
        status,queue=call('/gateway-b'+tenant_query,auth='Bearer '+jwt)
        assert status==200 and [item['id'] for item in queue['requests']]==[request_id],queue
        status,other=call('/gateway-b'+tenant_query.replace('tenantId=team','tenantId=other'),auth='Bearer '+jwt)
        assert status==200 and other['requests']==[],other
        for gateway in ['gateway-a','gateway-b']:
            status,cancelled=call('/'+gateway+tenant_query+'&id='+request_id,auth='Bearer '+jwt,method='DELETE')
            assert status==200 and cancelled['state']=='CANCELLED',cancelled
        assert submit('gateway-b')[1]['state']=='CANCELLED'
        proof,key,daemon=proofs[0];assert call('/internal/v1/capacity/report',{'proof':proof},'Body '+key,{'x-ansts-body-id':daemon})[1]['outcome']=='REPLAY'
        deadline=time.monotonic()+22
        while time.monotonic()<deadline:
            status,stale=call('/gateway-b/v1/admin/capacity/hosts',auth='Bearer '+jwt)
            if status==200 and stale['hosts'][0]['health']=='STALE':
                break
            time.sleep(.25)
        assert status==200 and stale['hosts'][0]['health']=='STALE' and stale['hosts'][0]['available']['slots']==0 and stale['hosts'][0]['retainedReserved']['slots']==2,stale
        assert stale['hosts'][0]['reports']==before
        # Persist a higher reporting epoch, then reject a delayed signed report
        # from the previous daemon incarnation even with a higher sequence.
        def send_epoch(epoch, sequence, reserved):
            current={**report,'daemonId':daemon,'epoch':epoch,'sequence':sequence,
                     'observedAt':int(time.time()*1000),'reserved':reserved}
            payload=encoded(current)
            signature=hmac.new(key.encode(),('ansts-capacity-report-v1.'+payload).encode(),hashlib.sha256).digest()
            signed=payload+'.'+base64.urlsafe_b64encode(signature).rstrip(b'=').decode()
            return call('/internal/v1/capacity/report',{'proof':signed},'Body '+key,{'x-ansts-body-id':daemon})
        fresh_epoch=send_epoch(2,1,report['reserved'])
        assert fresh_epoch[0]==200,fresh_epoch
        cmd('docker','restart',NAME);ready()
        rejected=send_epoch(1,999,{'cpuMilliCores':0,'memoryMiB':0,'slots':0})
        assert rejected==(409,{'code':'STALE_CAPACITY_REPORT'}),rejected
        for gateway in ['gateway-a','gateway-b']:
            status,view=call('/'+gateway+'/v1/admin/capacity/hosts',auth='Bearer '+jwt)
            assert status==200 and view['hosts'][0]['reports'][0]['epoch']==2,view
            assert view['hosts'][0]['retainedReserved']['slots']==2,view
        loan_pool={**pool,'id':'loan','name':'Loan','expectedVersion':0}
        assert call('/gateway-a/v1/admin/capacity/pools',loan_pool,'Bearer '+jwt,{'idempotency-key':'loan-pool'})[0]==200
        loan_host={**host,'id':'loan-host','poolId':'loan','daemonIds':['loan-daemon']}
        assert call('/gateway-a/v1/admin/capacity/hosts',loan_host,'Bearer '+jwt,{'idempotency-key':'loan-host'})[0]==200
        report={**report,'hostId':'loan-host','daemonId':'loan-daemon','observedAt':int(time.time()*1000),
                'reserved':{'cpuMilliCores':0,'memoryMiB':0,'slots':0}}
        key=hmac.new(BODY_ROOT.encode(),b'ansts-body-v1:loan-daemon',hashlib.sha256).hexdigest()
        payload=encoded(report)
        proof=payload+'.'+base64.urlsafe_b64encode(hmac.new(key.encode(),('ansts-capacity-report-v1.'+payload).encode(),hashlib.sha256).digest()).rstrip(b'=').decode()
        assert call('/internal/v1/capacity/report',{'proof':proof},'Body '+key,{'x-ansts-body-id':'loan-daemon'})[0]==200
        policy_path='/gateway-a/v1/admin/capacity/queue-policy?poolId=loan'
        status,policy=call(policy_path,{'expectedVersion':0,'maxWaitMs':60000,'weights':{},
                                       'priorities':{'team':10}},'Bearer '+jwt,method='PUT')
        assert status==200 and policy['version']==1 and policy['priorities']=={'team':10},policy
        for tenant in ['other','competitor','team']:
            path='/gateway-a/v1/admin/capacity/requests?poolId=loan&tenantId='+tenant
            assert call(path,request_body,'Bearer '+jwt,{'idempotency-key':'loan-request'})[0]==200
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            status,queue=call(path,auth='Bearer '+jwt)
            if status==200 and queue['requests'][0]['state']=='GRANTED':
                break
            time.sleep(.2)
        else:
            raise AssertionError(queue)
        other_path=path.replace('tenantId=team','tenantId=other')
        assert call(other_path,auth='Bearer '+jwt)[1]['requests'][0]['state']=='QUEUED'
        assert call(path.replace('tenantId=team','tenantId=competitor'),auth='Bearer '+jwt)[1]['requests'][0]['state']=='QUEUED'
        def tenant_call(action,data):
            return call('/fixture/tenant/'+action,{'actor':actor,'tenantId':'team',**data},'Bearer '+'e'*64)
        receipt='capacity-grant:'+queue['requests'][0]['id']
        prep={'reservationId':'borrow-reservation','taskId':'borrow-task','attemptId':'borrow-attempt',
              'fencing':1,'expectedPolicyVersion':1,'estimatedCreditUnits':'1','expiresAt':int(time.time()*1000)+30000,
              'resources':{'concurrentAttempts':1,'cpuMilliCores':200,'memoryMiB':128,'queuedTasks':0}}
        def prepare_candidate(suffix):
            candidate={**prep,'reservationId':prep['reservationId']+suffix,'taskId':prep['taskId']+suffix,
                       'attemptId':prep['attemptId']+suffix}
            return tenant_call('admission-prepare',{'idempotencyKey':'prepare'+suffix,'input':candidate})
        with ThreadPoolExecutor(max_workers=2) as workers:
            preparations=list(workers.map(prepare_candidate,['-a','-b']))
        assert sorted(result[0] for result in preparations)==[200,409],preparations
        prepared=next(data for status,data in preparations if status==200)
        rejected=next(data for status,data in preparations if status==409)
        assert rejected['code']=='RESOURCE_QUOTA_EXCEEDED',rejected
        def transition(reservation,phase,**fields):
            return tenant_call('admission-transition',{'idempotencyKey':phase,'input':{
                'reservationId':reservation['id'],'expectedVersion':reservation['version'],
                'generation':reservation['generation'],'fencing':reservation['fencing'],'to':phase,
                'reason':'isolated capacity recovery',**fields}})
        status,reserved=transition(prepared['reservation'],'RESERVED',target='loan-daemon',capacityReceipt=receipt)
        assert status==200,reserved
        status,cancelled=transition(reserved['reservation'],'CANCELLED')
        assert status==200,cancelled
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            status,view=call(path,auth='Bearer '+jwt)
            if status==200 and view['requests'][0]['state']=='RELEASED':
                break
            time.sleep(.2)
        else:
            raise AssertionError(view)
        expiry_actor={**actor,'scopeTenant':'expiry'}
        status,data=call('/fixture/tenant/admission-prepare-estimated',{'actor':expiry_actor,'tenantId':'expiry',
            'idempotencyKey':'expiry-prepare','input':{'reservationId':'expiry-r','taskId':'expiry-task',
             'attemptId':'expiry-attempt','fencing':1,'executionInputDigest':'a'*64,
             'resources':{'concurrentAttempts':1,'cpuMilliCores':200,'memoryMiB':128,'queuedTasks':0}}},'Bearer '+'e'*64)
        assert status==200 and data['reservation']['phase']=='PREPARING',data
        one_slot={**limit,'slots':1}
        assert call('/gateway-a/v1/admin/capacity/pools',{**pool,'id':'pilot','limit':one_slot},
                    'Bearer '+jwt,{'idempotency-key':'pilot-pool'})[0]==200
        pilot_host={**host,'id':'pilot-host','poolId':'pilot','daemonIds':['pilot-daemon'],'limit':one_slot}
        assert call('/gateway-a/v1/admin/capacity/hosts',pilot_host,'Bearer '+jwt,{'idempotency-key':'pilot-host'})[0]==200
        report={**report,'hostId':'pilot-host','daemonId':'pilot-daemon','sequence':1,
                'observedAt':int(time.time()*1000),'total':one_slot,'reserved':dict.fromkeys(one_slot,0)}
        key=hmac.new(BODY_ROOT.encode(),b'ansts-body-v1:pilot-daemon',hashlib.sha256).hexdigest()
        payload=encoded(report)
        proof=payload+'.'+base64.urlsafe_b64encode(hmac.new(key.encode(),('ansts-capacity-report-v1.'+payload).encode(),hashlib.sha256).digest()).rstrip(b'=').decode()
        assert call('/internal/v1/capacity/report',{'proof':proof},'Body '+key,{'x-ansts-body-id':'pilot-daemon'})[0]==200
        gateway_origins={}
        if split_gateways:
            project=p/'gateway'
            project.mkdir()
            subprocess.run([str(ROOT/'node_modules/.bin/esbuild'),str(ROOT/'src/cloudflare/gateway_worker.ts'),
                            '--bundle','--format=esm','--platform=neutral','--external:node:*',
                            '--outfile='+str(project/'worker.js')],check=True)
            config_gateway={'name':NAME+'-edge','main':'worker.js','compatibility_date':'2025-08-15',
                'compatibility_flags':['nodejs_compat'],'vars':{key:config['vars'][key] for key in
                    ('ANSTS_USER_AUTH_SECRET','ANSTS_DIRECTORY_SERVICE_TOKEN','ANSTS_SCHEDULER_SERVICE_TOKEN','ANSTS_SANDBOX_IMAGE')},
                'durable_objects':{'bindings':[{'name':'ANSTS_GATEWAY_CELL','class_name':'AnstsGatewayCell'}]},
                'migrations':[{'tag':'v1','new_sqlite_classes':['AnstsGatewayCell']}]}
            config_gateway['vars']['ANSTS_SCHEDULER_URL']='http://'+NAME+':8080'
            config_gateway['vars']['ANSTS_DIRECTORY_URL']='http://'+NAME+':8080'
            (project/'wrangler.json').write_text(json.dumps(config_gateway))
            for suffix in ('a','b'):
                edge=NAME+'-edge-'+suffix
                edge_project=p/('gateway-'+suffix)
                edge_project.mkdir()
                for source in ('worker.js','wrangler.json'):
                    (edge_project/source).write_bytes((project/source).read_bytes())

                gateway_names.append(edge)
                cmd('docker','run','-d','--user',str(os.getuid())+':'+str(os.getgid()),'--name',edge,
                    '--network',NAME,'-p','127.0.0.1::8080','-e','CELLD_ESBUILD=/tools/esbuild',
                    '-v',str(ROOT/'node_modules/@esbuild/linux-x64/bin/esbuild')+':/tools/esbuild:ro',
                    '-v',str(edge_project)+':/probe','ghcr.io/denoland/celld:0.6.3','dev','/probe',
                    '--host','0.0.0.0','--port','8080','--no-watch')
                gateway_origins[suffix]='http://127.0.0.1:'+cmd('docker','port',edge,'8080').split(':')[-1]
                for _ in range(100):
                    try:
                        if call('/healthz',origin=gateway_origins[suffix])[0]==200:
                            break
                    except OSError:
                        pass
                    time.sleep(.1)
                else:
                    raise RuntimeError('independent Gateway failed readiness')
        if queue_race:
            path='/v1/admin/capacity/requests?poolId=pilot&tenantId=pilot'
            allocation='/gateway-a/v1/admin/capacity/allocation-control?poolId=pilot'
            assert call(allocation,{'expectedVersion':0,'enabled':False},'Bearer '+jwt,
                        {'idempotency-key':'queue-race-pause'},method='PUT')[0]==200
            def queue_submit(suffix):
                return call(path,request_body,'Bearer '+jwt,{'idempotency-key':'queue-race-'+suffix},
                            origin=gateway_origins[suffix])
            with ThreadPoolExecutor(max_workers=2) as workers:
                results=list(workers.map(queue_submit,['a','b']))
            assert sorted(status for status,_ in results)==[200,409],results
            winner=next(index for index,(status,_) in enumerate(results) if status==200)
            assert results[1-winner]==(409,{'code':'QUEUE_QUOTA_EXCEEDED'}),results
            accepted=results[winner][1]
            assert accepted['state']=='QUEUED',accepted
            assert queue_submit('ab'[winner])==results[winner]
            for origin in gateway_origins.values():
                status,view=call(path,auth='Bearer '+jwt,origin=origin)
                assert status==200 and [row['id'] for row in view['requests'] if row['state']=='QUEUED']==[accepted['id']],view
            assert call(path+'&id='+accepted['id'],auth='Bearer '+jwt,method='DELETE',
                        origin=gateway_origins['ab'[1-winner]])[0]==200
            assert queue_submit('ab'[1-winner])[0]==200
            print('PASS: separate Gateways enforce one tenant queue position, preserve replay, and free it on cancellation.')
            raise SystemExit(0)
        def submit_pilot(suffix):
            return call('/v1/tasks' if split_gateways else '/gateway-'+suffix+'/v1/tasks',{'task_id':'pilot-'+suffix,'tenant_id':'pilot',
                        'prompt':'Pilot workload','execution':{'kind':'container','image':'node@sha256:'+'a'*64,'argv':['node','-v'],
                                                               'cpu':0.2,'memoryMb':128,'timeoutMs':10000}},'Bearer '+jwt,origin=gateway_origins.get(suffix))
        assert submit_pilot('a')==(409,{'code':'ADMISSION_LEGACY_OCCUPANCY_PRESENT'})
        wrong_shard=call('/v1/scheduler/submit?shard=legacy-shard',
                        {'task_id':'alternate','tenant_id':'pilot','prompt':'Wrong shard probe'},scheduler_auth)
        assert wrong_shard==(409,{'code':'ADMISSION_PILOT_WRONG_SHARD'}),wrong_shard

        assert call('/v1/scheduler/cancel?shard=legacy-shard',{'task_id':legacy_task['task_id'],
                    'tenant_id':'pilot','actor':'fixture','reason':'Drain old work'},scheduler_auth)[0]==200
        def toggle_shard(offline):
            return call('/fixture/shard-toggle',{'offline':offline},'Bearer '+'e'*64)
        assert toggle_shard(True)[0]==200
        assert submit_pilot('a')==(503,{'code':'ADMISSION_FLEET_INVENTORY_UNAVAILABLE'})
        assert toggle_shard(False)[0]==200
        assert not call('/v1/scheduler/tasks',auth=scheduler_auth)[1]['tasks']
        print('PASS: remote legacy occupancy and remote shard outage reject ordinary Gateway submissions.')
        def toggle_control(offline):
            return call('/fixture/tenant/fixture-toggle',{'tenantId':'pilot','fixtureOffline':offline},'Bearer '+'e'*64)
        assert toggle_control(True)[0]==200
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            attempt=submit_pilot('a')
            if attempt[0]==503:
                assert attempt[1]['code']=='TENANT_CONTROL_UNAVAILABLE',attempt
                break
            assert attempt==(409,{'code':'CAPACITY_PENDING'}),attempt
            time.sleep(.3)
        else:
            raise AssertionError(attempt)
        assert not call('/v1/scheduler/tasks',auth=scheduler_auth)[1]['tasks']
        assert toggle_control(False)[0]==200
        if drop_prepare:
            missed=submit_pilot('a')
            assert missed==(503,{'code':'FIXTURE_LOST_PREPARE_ACK'}),missed
            assert not call('/v1/scheduler/tasks',auth=scheduler_auth)[1]['tasks']
            storage_id='ansts:v1:'+encoded(['pilot','pilot-a'])
            task_key=hashlib.sha256(json.dumps(['pilot',storage_id],separators=(',',':')).encode()).hexdigest()
            reservation_id=hashlib.sha256(json.dumps(['pilot','pilot',task_key],separators=(',',':')).encode()).hexdigest()
            state=call('/fixture/tenant/admission-read',{'actor':actor,'tenantId':'pilot','reservationId':reservation_id},'Bearer '+'e'*64)
            assert state[0]==200 and state[1]['reservation']['phase']=='PREPARING' and state[1]['reservation']['estimatedCreditUnits']=='5',state
            print('PASS: lost prepare acknowledgement leaves exactly the prepared reservation and no Scheduler task.')
        assert submit_pilot('a')==(503,{'code':'CAPACITY_BIND_REJECTED'})
        assert not call('/v1/scheduler/tasks',auth=scheduler_auth)[1]['tasks']
        assert submit_pilot('a')==(503,{'code':'FIXTURE_LOST_START_ACK'})
        assert not call('/v1/scheduler/tasks',auth=scheduler_auth)[1]['tasks']
        if os.environ.get('ANSTS_VERIFY_UNACCEPTED_PILOT_EXPIRY')=='1':
            def digest_json(parts):
                return hashlib.sha256(json.dumps(parts,separators=(',',':')).encode()).hexdigest()
            storage_id='ansts:v1:'+encoded(['pilot','pilot-a'])
            reservation_id=digest_json(['pilot','pilot',digest_json(['pilot',storage_id])])
            status,data=call('/fixture/tenant/admission-read',{'actor':actor,'tenantId':'pilot',
                             'reservationId':reservation_id},'Bearer '+'e'*64)
            assert status==200 and data['reservation']['phase']=='START_AUTHORIZED',data
            subject={'reservationId':reservation_id,'taskId':storage_id,
                     'inputDigest':data['reservation']['executionInputDigest']}
            def expiry_status(body):
                return call('/fixture/expiry-status',body,'Bearer '+'e'*64)
            assert expiry_status(subject)==(200,{'expiredBeforeAcceptance':False})
            cmd('docker','kill','--signal','KILL',NAME)
            cmd('docker','start',NAME)
            ready()
            assert call('/fixture/shard-toggle?target=main',{'offline':'expiry'},'Bearer '+'e'*64)[0]==200
            deadline=time.monotonic()+75
            while time.monotonic()<deadline:
                if expiry_status(subject)==(200,{'expiredBeforeAcceptance':True}):
                    break
                time.sleep(.5)
            else:
                raise AssertionError(expiry_status(subject))
            def replay_start():
                return call('/v1/scheduler/submit',{'task_id':storage_id,'tenant_id':'pilot',
                            'prompt':'Pilot workload','capacity_reservation_id':reservation_id,
                            'execution':{'kind':'container','image':'node@sha256:'+'a'*64,
                                         'argv':['node','-v'],'cpu':0.2,'memoryMb':128,'timeoutMs':10000}},
                            scheduler_auth)
            assert replay_start()==(409,{'code':'CAPACITY_RESERVATION_MISMATCH'})
            assert expiry_status({**subject,'inputDigest':'0'*64})==(200,{'expiredBeforeAcceptance':False})
            assert not call('/v1/scheduler/tasks',auth=scheduler_auth)[1]['tasks']
            deadline=time.monotonic()+15
            while time.monotonic()<deadline:
                injected=call('/fixture/shard-toggle?target=main',{},'Bearer '+'e'*64)
                if injected[0]==200 and injected[1]['failedExpiryProbes']>=1:
                    break
                time.sleep(.25)
            else:
                raise AssertionError(injected)
            before_retry=call('/fixture/tenant/admission-read',{'actor':actor,'tenantId':'pilot',
                              'reservationId':reservation_id},'Bearer '+'e'*64)
            assert before_retry[0]==200 and before_retry[1]['reservation']['phase']=='START_AUTHORIZED',before_retry
            assert call('/fixture/shard-toggle?target=main',{'offline':False},'Bearer '+'e'*64)[0]==200
            deadline=time.monotonic()+20
            while time.monotonic()<deadline:
                recovered=call('/fixture/tenant/admission-read',{'actor':actor,'tenantId':'pilot',
                               'reservationId':reservation_id},'Bearer '+'e'*64)
                listing=call('/gateway-a/v1/admin/capacity/requests?poolId=pilot&tenantId=pilot',auth='Bearer '+jwt)
                if (recovered[0]==200 and recovered[1]['reservation']['phase']=='CANCELLED' and listing[0]==200 and
                    any(row['id']==reservation_id and row['state'] in ('CANCELLED','RELEASED') for row in listing[1]['requests'])):
                    break
                time.sleep(.5)
            else:
                raise AssertionError((recovered,listing))
            assert replay_start()==(409,{'code':'CAPACITY_RESERVATION_MISMATCH'})
            assert not call('/v1/scheduler/tasks',auth=scheduler_auth)[1]['tasks']
            assert recovered[1]['reservation']['generation']==2
            assert recovered[1]['reservation']['terminalEvidence'].startswith('scheduler-expiry:')
            print('PASS: abandoned authorized reservation and capacity reclaimed after Scheduler confirmation.')
            print('PASS: an expired authorized intent never accepted by Scheduler is proven after hard restart.')
            raise SystemExit(0)

        cmd('docker','kill','--signal','KILL',NAME)
        cmd('docker','start',NAME)
        ready()


        if drop_acceptance:
            missed=submit_pilot('a')
            if drop_prepare and missed==(503,{'code':'FIXTURE_LOST_PREPARE_ACK'}):
                missed=submit_pilot('a')
            assert missed==(503,{'code':'FIXTURE_LOST_ACCEPTANCE_ACK'}),missed
            initial=call('/v1/scheduler/tasks',auth=scheduler_auth)[1]['tasks']
            assert len(initial)==1,initial
            cmd('docker','kill','--signal','KILL',NAME)
            cmd('docker','start',NAME);ready()
            replay=submit_pilot('a')
            assert replay[0] in (200,201,202) and replay[1]['executionTaskId']==initial[0]['task_id'],replay
            assert len(call('/v1/scheduler/tasks',auth=scheduler_auth)[1]['tasks'])==1
            print('PASS: accepted task survives lost Gateway response and hard restart without a second task.')
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            with ThreadPoolExecutor(max_workers=2) as workers:
                responses=list(workers.map(submit_pilot,['a','b']))
            assert all(status in (200,201,202) or (status==409 and data.get('code')=='CAPACITY_PENDING')
                       for status,data in responses),responses
            if sum(status in (200,201,202) for status,_ in responses)==1:
                break
            time.sleep(.3)
        else:
            raise AssertionError(responses)
        winner='a' if responses[0][0] in (200,201,202) else 'b'
        assert submit_pilot(winner)[0] in (200,201,202)
        task_id=responses[0 if winner=='a' else 1][1]['executionTaskId']
        task_key=hashlib.sha256(json.dumps(['pilot',task_id],separators=(',',':')).encode()).hexdigest()
        reservation_id=hashlib.sha256(json.dumps(['pilot','pilot',task_key],separators=(',',':')).encode()).hexdigest()
        status,data=call('/fixture/tenant/admission-read',{'actor':actor,'tenantId':'pilot','reservationId':reservation_id},'Bearer '+'e'*64)
        assert status==200 and data['reservation']['phase']=='START_AUTHORIZED' and data['reservation']['estimatedCreditUnits']=='5',data
        preview_input={'resources':{'concurrentAttempts':1,'cpuMilliCores':200,'memoryMiB':128,'queuedTasks':0}}
        def preview(tenant):
            return call('/fixture/tenant/admission-shadow-preview',{'actor':actor,'tenantId':tenant,
                        'input':preview_input},'Bearer '+'e'*64)
        assert preview('pilot')==(409,{'code':'RESOURCE_QUOTA_EXCEEDED'})
        assert preview('shadow-budget')==(409,{'code':'BUDGET_QUOTA_EXCEEDED'})
        assert preview('shadow-empty')==(200,{'code':'ADMISSION_QUOTA_AVAILABLE'})
        assert preview('shadow-empty')==(200,{'code':'ADMISSION_QUOTA_AVAILABLE'})
        print('PASS: shadow preview uses current quota and does not reserve a slot.')

        print('PASS: ordinary dual Gateway submissions competed for one pilot slot; tenant control outage admitted no task; lost bind and start authorization acknowledgements recovered on replay across a hard process stop.')
        split_limits=[{'cpuMilliCores':800,'memoryMiB':256,'slots':1},
                      {'cpuMilliCores':200,'memoryMiB':768,'slots':1}]
        assert call('/gateway-a/v1/admin/capacity/pools',{**pool,'id':'split'},
                    'Bearer '+jwt,{'idempotency-key':'split-pool'})[0]==200
        for index,shape in enumerate(split_limits):
            host_id=f'split-host-{index}'
            daemon=f'split-daemon-{index}'
            assert call('/gateway-a/v1/admin/capacity/hosts',
                        {**host,'id':host_id,'poolId':'split','daemonIds':[daemon],'limit':shape},
                        'Bearer '+jwt,{'idempotency-key':host_id})[0]==200
            report={**report,'hostId':host_id,'daemonId':daemon,'sequence':1,'observedAt':int(time.time()*1000),
                    'total':shape,'reserved':dict.fromkeys(shape,0)}
            key=hmac.new(BODY_ROOT.encode(),('ansts-body-v1:'+daemon).encode(),hashlib.sha256).hexdigest()
            payload=encoded(report)
            proof=payload+'.'+base64.urlsafe_b64encode(hmac.new(key.encode(),('ansts-capacity-report-v1.'+payload).encode(),hashlib.sha256).digest()).rstrip(b'=').decode()
            assert call('/internal/v1/capacity/report',{'proof':proof},'Body '+key,{'x-ansts-body-id':daemon})[0]==200
        split='/gateway-b/v1/admin/capacity/requests?poolId=split&tenantId='
        for tenant,shape in [('split-blocked',{'cpuMilliCores':500,'memoryMiB':512,'slots':1}),
                             ('split-fits',{'cpuMilliCores':400,'memoryMiB':128,'slots':1})]:
            assert call(split+tenant,{'resources':shape},'Bearer '+jwt,{'idempotency-key':tenant})[0]==200
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            fit=call(split+'split-fits',auth='Bearer '+jwt)[1]['requests'][0]
            if fit['state']=='GRANTED':
                break
            time.sleep(.2)
        else:
            raise AssertionError(fit)
        blocked=call(split+'split-blocked',auth='Bearer '+jwt)[1]['requests'][0]
        assert blocked['state']=='QUEUED' and blocked['queueReason']=='HOST_RESOURCES_UNAVAILABLE',blocked
        print('PASS: split CPU and memory across hosts cannot satisfy one request; another tenant obtains a fitting slot.')
        deadline=time.monotonic()+70
        while time.monotonic()<deadline:
            status,data=call('/fixture/tenant/admission-read',{'actor':expiry_actor,'tenantId':'expiry',
                              'reservationId':'expiry-r'},'Bearer '+'e'*64)
            if status==200 and data['reservation']['phase']=='CANCELLED':
                assert data['reservation']['generation']==2 and not data['reservation']['startAuthorized'],data
                break
            time.sleep(1)
        else:
            raise AssertionError(data)
        print('PASS: abandoned estimated reservation expired via the tenant alarm.')
        expected_reservation=call('/fixture/tenant/admission-read',{'actor':actor,'tenantId':'pilot',
                                  'reservationId':reservation_id},'Bearer '+'e'*64)
        headroom=lambda:call('/fixture/tenant/admission-capacity-headroom',{'actor':actor,'tenantId':'pilot'},
                             'Bearer '+'e'*64)
        before=headroom()
        assert before[0]==200,before
        config['vars'].update({'ANSTS_ADMISSION_MODE':'observe','ANSTS_ADMISSION_PILOT_TENANTS':''})
        config_path.write_text(json.dumps(config))
        cmd('docker','restart',NAME)
        ready()
        assert call('/fixture/tenant/admission-read',{'actor':actor,'tenantId':'pilot',
                    'reservationId':reservation_id},'Bearer '+'e'*64)==expected_reservation
        assert headroom()==before
        assert submit_pilot('b')[0]==202
        assert headroom()==before
        print('PASS: disabling the pilot preserves the existing reservation and occupied quota.')
        accepted=expected_reservation[1]['reservation']
        wait_for_expiry=accepted['expiresAt']/1000-time.time()
        if wait_for_expiry>0:
            time.sleep(wait_for_expiry+.1)
        live_tasks=call('/v1/scheduler/tasks',auth=scheduler_auth)
        assert live_tasks[0]==200 and any(t['task_id']==task_id for t in live_tasks[1]['tasks']),live_tasks
        expired_start=call('/v1/scheduler/lease-renew',{
            'task_id':task_id,'tenant_id':'pilot','node_id':accepted['target'],
            'attempt_id':accepted['attemptId'],'fencing_token':accepted['fencing'],
            'input_digest':accepted['executionInputDigest'],'start_check':True,
        },scheduler_auth)
        assert expired_start==(409,{'renewed':False,'reason':'CAPACITY_START_UNAVAILABLE'}),expired_start
        assert headroom()==before
        print('PASS: accepted task rejects expired Body start check without releasing occupied quota.')
        cancelled=call('/v1/scheduler/cancel',{'tenant_id':'pilot','task_id':task_id,
                       'actor':'fixture','reason':'Verify durable acceptance after terminal cleanup'},scheduler_auth)
        assert cancelled[0]==200,cancelled
        purge=call('/v1/scheduler/purge',{'states':['DEAD_LETTER']},scheduler_auth)
        assert purge[0]==200 and task_id in purge[1]['task_ids'],purge
        cmd('docker','restart',NAME)
        ready()
        assert all(t['task_id']!=task_id for t in call('/v1/scheduler/tasks',auth=scheduler_auth)[1]['tasks'])
        expiry_status=call('/fixture/expiry-status',{'reservationId':reservation_id,'taskId':task_id,
                           'inputDigest':accepted['executionInputDigest']},'Bearer '+'e'*64)
        assert expiry_status==(200,{'expiredBeforeAcceptance':False}),expiry_status
        print('PASS: Scheduler acceptance still blocks expiry recovery after task purge and restart.')
        browser_session=os.environ.get('ANSTS_CAPACITY_BROWSER_SESSION')
        if browser_session:
            browser_probe=p/'capacity-browser.js'
            credentials={}
            for role in ['VIEWER','OPERATOR','SRE_ADMIN']:
                claims={**actor,'sub':'browser-fixture','role':role,'scopeTenant':'all' if role=='SRE_ADMIN' else 'team',
                        'iat':int(time.time()),'exp':int(time.time())+600}
                payload=encoded({'alg':'HS256','typ':'JWT'})+'.'+encoded(claims)
                signature=base64.urlsafe_b64encode(hmac.new(KEY.encode(),payload.encode(),hashlib.sha256).digest()).rstrip(b'=').decode()
                credentials[role]='Bearer '+payload+'.'+signature
            browser_probe.write_text((ROOT/'scripts/verify_capacity_console_browser.js').read_text().replace(
                'const gateway = null;', 'const gateway = '+json.dumps(address())+';',1).replace(
                'const credentials = {};', 'const credentials = '+json.dumps(credentials)+';',1))
            result=subprocess.run(['npx','--yes','--package','@playwright/cli','playwright-cli',
                                   '-s='+browser_session,'run-code','--filename',str(browser_probe)],
                                  cwd='/tmp',capture_output=True,text=True)
            if result.returncode or '### Result\n"VIEWER,OPERATOR,SRE_ADMIN"' not in result.stdout:
                raise RuntimeError(result.stdout.split('### Ran Playwright code')[0]+result.stderr)
            print('PASS: real Chromium role actions through the signed isolated capacity Gateway.')
        print('PASS: real celld capacity persistence, dual-Gateway concurrent replay/CAS, two signed Daemons counted as one Host, report restart/replay, stale occupancy retention, durable request replay/cancellation, alarm issuance using versioned priority and tenant quota policy, and durable cancellation release.')
    finally:
        subprocess.run(['docker','rm','-f',*gateway_names,NAME],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        if split_gateways:
            subprocess.run(['docker','network','rm',NAME],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)









