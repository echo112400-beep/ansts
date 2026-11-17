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
def call(path,body=None,auth='Bearer '+KEY,headers=None,method=None):
    request=urllib.request.Request(address()+path,data=None if body is None else json.dumps(body).encode(),headers={'content-type':'application/json','authorization':auth,**(headers or {})},method=method)
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
    p.joinpath('entry.ts').write_text('import {CapacityInventoryCell} from '+json.dumps(str(ROOT/'src/cloudflare/capacity_inventory_cell.ts'))+';\nimport {handleCapacityIngress} from '+json.dumps(str(ROOT/'src/control/capacity_ingress.ts'))+';\nimport {handleUnifiedGatewayRequest} from '+json.dumps(str(ROOT/'src/cloudflare/gateway_cell.ts'))+';\nexport {CapacityInventoryCell};\nexport class TestGateway {constructor(ctx,env){this.env=env;}async fetch(request){return await handleUnifiedGatewayRequest(request,this.env)??new Response("missing",{status:404});}}\nexport default {fetch(request,env){const url=new URL(request.url);if(url.pathname==="/ping")return new Response("ok");const match=url.pathname.match(/^\\/(gateway-a|gateway-b)(\\/.*)$/);if(match){url.pathname=match[2];return env.INGRESS.get(env.INGRESS.idFromName(match[1])).fetch(new Request(url,request));}return handleCapacityIngress(request,env);}};\n')
    subprocess.run([str(ROOT/'node_modules/.bin/esbuild'),str(p/'entry.ts'),'--bundle','--format=esm','--platform=neutral','--external:node:*','--outfile='+str(p/'worker.js')],check=True)
    p.joinpath('wrangler.json').write_text(json.dumps({'name':NAME,'main':'worker.js','compatibility_date':'2025-08-15','compatibility_flags':['nodejs_compat'],'vars':{'ANSTS_DIRECTORY_SERVICE_TOKEN':KEY,'ANSTS_USER_AUTH_SECRET':KEY,'ANSTS_BODY_AUTH_SECRET':BODY_ROOT},'durable_objects':{'bindings':[{'name':'ANSTS_CAPACITY_INVENTORY_CELL','class_name':'CapacityInventoryCell'},{'name':'INGRESS','class_name':'TestGateway'}]},'migrations':[{'tag':'v1','new_sqlite_classes':['CapacityInventoryCell','TestGateway']}]}))
    try:
        cmd('docker','run','-d','--user',str(os.getuid())+':'+str(os.getgid()),'--name',NAME,'-p','127.0.0.1::8080','-e','CELLD_ESBUILD=/tools/esbuild','-v',str(ROOT/'node_modules/@esbuild/linux-x64/bin/esbuild')+':/tools/esbuild:ro','-v',scratch+':/probe','ghcr.io/denoland/celld:0.6.3','dev','/probe','--host','0.0.0.0','--port','8080','--no-watch')
        ready()
        pool={'id':'pool','name':'Pool','region':'dev','status':'ACTIVE','limit':limit,'expectedVersion':0,'reason':'isolated capacity acceptance'}
        def create(gateway):return call('/'+gateway+'/v1/admin/capacity/pools',pool,'Bearer '+jwt,{'idempotency-key':'pool'})
        with ThreadPoolExecutor(max_workers=2) as workers:responses=list(workers.map(create,['gateway-a','gateway-b']))
        assert responses[0][0]==200 and responses[0]==responses[1],responses
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
        before=snapshot['hosts'][0]['reports']
        cmd('docker','restart',NAME);ready()
        status,recovered=call('/gateway-a/v1/admin/capacity/hosts',auth='Bearer '+jwt)
        assert status==200 and recovered['hosts'][0]['reports']==before
        proof,key,daemon=proofs[0];assert call('/internal/v1/capacity/report',{'proof':proof},'Body '+key,{'x-ansts-body-id':daemon})[1]['outcome']=='REPLAY'
        remaining=max(0,15.1-(time.time()-min(r['observedAt'] for r in before)/1000))
        time.sleep(remaining)
        status,stale=call('/gateway-b/v1/admin/capacity/hosts',auth='Bearer '+jwt)
        assert status==200 and stale['hosts'][0]['health']=='STALE' and stale['hosts'][0]['available']['slots']==0 and stale['hosts'][0]['retainedReserved']['slots']==2,stale
        assert stale['hosts'][0]['reports']==before
        print('PASS: real celld capacity persistence, dual-Gateway concurrent replay/CAS, two signed Daemons counted as one Host, report restart/replay and stale occupancy retention.')
    finally:subprocess.run(['docker','rm','-f',NAME],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
