#!/usr/bin/env python3
"""Own isolated native celld state and real Engine fixtures; never use development tasks or secrets."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.request
import uuid
ROOT=Path(__file__).resolve().parents[1]
name='ansts-body-evidence-'+uuid.uuid4().hex[:8]
http=urllib.request.build_opener(urllib.request.ProxyHandler({}))
def command(*args):return subprocess.check_output(args,text=True,stderr=subprocess.PIPE).strip()
with tempfile.TemporaryDirectory(prefix=name) as scratch:
    project=Path(scratch)
    classes=[('ANSTS_TENANT_DIRECTORY_CELL','TenantDirectoryCell'),('ANSTS_TENANT_CONTROL_CELL','TenantControlCell'),('ANSTS_ADMISSION_MAILBOX','AdmissionShadowMailboxCell'),('ANSTS_SCHEDULER_CELL','AnstsSchedulerCell'),('ANSTS_BODY_CELL','AnstsBodyCell')]
    entry=project/'entry.ts'
    entry.write_text('import worker from '+json.dumps(str(ROOT/'src/cloudflare/worker.ts'))+'; export * from '+json.dumps(str(ROOT/'src/cloudflare/worker.ts'))+'; let dropped=false; export default {async fetch(request,env){const retired=new URL(request.url).pathname==="/internal/v1/body-usage"?{...env,ANSTS_BODY_CELL:{idFromName:n=>env.ANSTS_BODY_CELL.idFromName(n),get:()=>({fetch:async()=>Response.json({code:"FIXTURE_RETIRED_BODY_PROOF"},{status:503})})}}:env;const response=await worker.fetch(request,retired);if(new URL(request.url).pathname==="/internal/v1/body-usage"&&response.status===200&&!dropped){dropped=true;await response.body?.cancel();return Response.json({code:"FIXTURE_LOST_USAGE_RECEIPT"},{status:503});}return response;}};')
    subprocess.run([str(ROOT/'node_modules/.bin/esbuild'),str(entry),'--bundle','--format=esm','--platform=neutral','--external:node:*','--outfile='+str(project/'worker.js')],check=True)
    subprocess.run([str(ROOT/'node_modules/.bin/esbuild'),str(ROOT/'scripts/body_evidence_native_probe.ts'),'--bundle','--format=esm','--platform=node','--outfile='+str(project/'probe.mjs')],check=True)
    (project/'wrangler.json').write_text(json.dumps({'name':name,'main':'worker.js','compatibility_date':'2025-08-15','compatibility_flags':['nodejs_compat'],'vars':{'ANSTS_BODY_AUTH_SECRET':'f'*64,'ANSTS_DIRECTORY_SERVICE_TOKEN':'d'*64,'ANSTS_ADMISSION_SERVICE_TOKEN':'e'*64,'ANSTS_ADMISSION_SIGNING_KEY':'c'*64},'durable_objects':{'bindings':[{'name':binding,'class_name':kind} for binding,kind in classes]},'migrations':[{'tag':'v1','new_sqlite_classes':[kind for _,kind in classes]}]}))
    try:
        command('docker','run','-d','--user',str(os.getuid())+':'+str(os.getgid()),'--name',name,'-p','127.0.0.1::8080','-e','CELLD_ESBUILD=/tools/esbuild','-v',str(ROOT/'node_modules/@esbuild/linux-x64/bin/esbuild')+':/tools/esbuild:ro','-v',scratch+':/probe','ghcr.io/denoland/celld:0.6.3','dev','/probe','--host','0.0.0.0','--port','8080','--no-watch')
        port=command('docker','port',name,'8080').split(':')[-1];base='http://127.0.0.1:'+port
        for _ in range(120):
            try:
                with http.open(base+'/healthz',timeout=2) as response:
                    if response.status==200:break
            except Exception:time.sleep(.1)
        subprocess.run(['node',str(project/'probe.mjs')],cwd=ROOT,env={**os.environ,'ANSTS_EVIDENCE_NATIVE_URL':base,'ANSTS_EVIDENCE_NATIVE_CONTAINER':name},check=True)
    finally:subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
