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
    classes=[('ANSTS_TENANT_DIRECTORY_CELL','TenantDirectoryCell'),('ANSTS_TENANT_CONTROL_CELL','TenantControlCell'),('ANSTS_ADMISSION_MAILBOX','AdmissionShadowMailboxCell'),('ANSTS_SCHEDULER_CELL','AnstsSchedulerCell'),('ANSTS_BODY_CELL','AnstsBodyCell'),('ANSTS_CAPACITY_INVENTORY_CELL','CapacityInventoryCell')]
    if os.environ.get('ANSTS_VERIFY_COMPOSE_ACCEPTANCE')=='1':
        production=json.loads((ROOT/'deploy/celld/wrangler.celld.json').read_text())
        upgrades={'ANSTS_REGION_CATALOG_CELL','ANSTS_CONFIG_RELEASE_CELL'}
        deployed={(row['name'],row['class_name']) for row in production['durable_objects']['bindings']}
        assert set(classes)<=deployed
        classes+=sorted(pair for pair in deployed-set(classes) if pair[0] not in upgrades)

    entry=project/'entry.ts'
    entry.write_text("""
import worker from __WORKER__;
import gateway from __GATEWAY__;
import {isFinopsPath} from __FINOPS__;
export * from __WORKER__;
export {FixtureTenantControlCell as TenantControlCell, FixtureSettlementScheduler as AnstsSchedulerCell} from __FIXTURE__;
let dropped = false;
export default {
    async fetch(request, env, context) {
        const path = new URL(request.url).pathname;
        if (isFinopsPath(path) || path.startsWith("/v1/admin/capacity/") || path === "/v1/admin/regions" || path === "/v1beta1/gateway/state") return gateway.fetch(request, env, context);
        if (path === "/fixture/body-restored-result") {
            const url = new URL(request.url);
            url.pathname = "/v1/body/result";
            return worker.fetch(new Request(url, request), {
                ...env, ANSTS_BODY_NAMESPACE_PREFIX: "restore-fixture:",
            });
        }
        if (path.startsWith("/fixture/body-backup/")) {
            if (request.headers.get("authorization") !== "Bearer " + env.ANSTS_ADMISSION_SERVICE_TOKEN)
                return Response.json({}, {status:401});
            const task = new URL(request.url).searchParams.get("taskId");
            const action = path.split("/").pop();
            if (!task || !["export", "import", "execution-inventory"].includes(action)) return Response.json({}, {status:404});
            const ns = env.ANSTS_BODY_CELL;
            const name = (action === "export" && !new URL(request.url).searchParams.has("restored")
                ? "execution:" : "restore-fixture:execution:") + task;
            const headers = new Headers(request.headers);
            headers.set("x-ansts-verified-operator", "fixture-restore");
            const target = new URL("https://cell/v1/body/" + action);
            if (action === "execution-inventory") {
                headers.set("x-ansts-verified-inventory", "true");
                target.searchParams.set("tenantId", "evidence");
            }
            return ns.get(ns.idFromName(name)).fetch(new Request(
                new Request(target, request), {headers}));
        }
        if (path.startsWith("/fixture/period/")) {
            const namespace = env.ANSTS_TENANT_CONTROL_CELL;
            const tenant = path === "/fixture/period/backdate-uncertain" ? "close-probe" : "evidence";
            return namespace.get(namespace.idFromName("tenant-control-v1:" + tenant)).fetch(
                new Request("https://fixture/fixture-period-" + path.split("/").pop(), request));
        }
        if (path.startsWith("/fixture/inventory/")) {
            const namespace = env.ANSTS_SCHEDULER_CELL;
            return namespace.get(namespace.idFromName("global-shard-0")).fetch(
                new Request("https://fixture/fixture-inventory-" + path.split("/").pop(), request));
        }
        const usage = path === "/internal/v1/body-usage";
        const retired = usage ? {...env, ANSTS_BODY_CELL: {
            idFromName: name => env.ANSTS_BODY_CELL.idFromName(name),
            get: () => ({fetch: async () => Response.json({code: "FIXTURE_RETIRED_BODY_PROOF"}, {status: 503})}),
        }} : env;
        const response = await worker.fetch(request, retired);
        if (usage && response.status === 200 && !dropped) {
            dropped = true;
            await response.body?.cancel();
            return Response.json({code: "FIXTURE_LOST_USAGE_RECEIPT"}, {status: 503});
        }
        return response;
    },
};
""".replace('__WORKER__', json.dumps(str(ROOT/'src/cloudflare/worker.ts')))
        .replace('__GATEWAY__', json.dumps(str(ROOT/'src/cloudflare/gateway_worker.ts')))
        .replace('__FINOPS__', json.dumps(str(ROOT/'src/control/admin_finops_api.ts')))
        .replace('__FIXTURE__', json.dumps(str(ROOT/'scripts/period_native_fixture.ts'))))
    subprocess.run([str(ROOT/'node_modules/.bin/esbuild'),str(entry),'--bundle','--format=esm','--platform=neutral','--external:node:*','--outfile='+str(project/'worker.js')],check=True)
    subprocess.run([str(ROOT/'node_modules/.bin/esbuild'),str(ROOT/'scripts/body_evidence_native_probe.ts'),'--bundle','--format=esm','--platform=node','--outfile='+str(project/'probe.mjs')],check=True)
    (project/'wrangler.json').write_text(json.dumps({'name':name,'main':'worker.js','compatibility_date':'2025-08-15','compatibility_flags':['nodejs_compat'],'vars':{'ANSTS_ADMISSION_MODE':'shadow','ANSTS_ADMISSION_PILOT_TENANTS':'evidence','ANSTS_BODY_AUTH_SECRET':'f'*64,'ANSTS_USER_AUTH_SECRET':'a'*64,'ANSTS_DIRECTORY_SERVICE_TOKEN':'d'*64,'ANSTS_ADMISSION_SERVICE_TOKEN':'e'*64,'ANSTS_ADMISSION_SIGNING_KEY':'c'*64},'durable_objects':{'bindings':[{'name':binding,'class_name':kind} for binding,kind in classes]},'migrations':[{'tag':'v1','new_sqlite_classes':[kind for _,kind in classes]}]}))
    publish='127.0.0.1::8080'
    if os.environ.get('ANSTS_EVIDENCE_RESTART_WHILE_RUNNING')=='true' or os.environ.get('ANSTS_VERIFY_COMPOSE_ACCEPTANCE')=='1':
        import socket
        with socket.socket() as listener:
            listener.bind(('127.0.0.1',0))
            publish=f'127.0.0.1:{listener.getsockname()[1]}:8080'
    with_compose=os.environ.get('ANSTS_VERIFY_COMPOSE_ACCEPTANCE')=='1'
    compose=['docker','compose','-p',name,'-f',str(ROOT/'deploy/dev-cluster/docker-compose.acceptance.yml')]
    compose_env={**os.environ,'ANSTS_ACCEPTANCE_APP':scratch,'ANSTS_ACCEPTANCE_PORT_MAPPING':publish,'ANSTS_ACCEPTANCE_UID':str(os.getuid()),
                 'ANSTS_ACCEPTANCE_GID':str(os.getgid()),
                 'ANSTS_ACCEPTANCE_ESBUILD':str(ROOT/'node_modules/@esbuild/linux-x64/bin/esbuild')}
    try:
        if with_compose:
            subprocess.run([*compose,'up','-d','--wait','--wait-timeout','45'],env=compose_env,check=True)
            name=name+'-control-1'
        else:
            command('docker','run','-d','--user',str(os.getuid())+':'+str(os.getgid()),'--name',name,'-p',publish,'-e','CELLD_ESBUILD=/tools/esbuild','-v',str(ROOT/'node_modules/@esbuild/linux-x64/bin/esbuild')+':/tools/esbuild:ro','-v',scratch+':/probe','ghcr.io/denoland/celld:0.6.3','dev','/probe','--host','0.0.0.0','--port','8080','--no-watch')
        port=command('docker','port',name,'8080').split(':')[-1];base='http://127.0.0.1:'+port
        for _ in range(120):
            try:
                with http.open(base+'/healthz',timeout=2) as response:
                    if response.status==200:break
            except Exception:time.sleep(.1)
        subprocess.run(['node',str(project/'probe.mjs')],cwd=ROOT,env={**os.environ,'ANSTS_EVIDENCE_NATIVE_URL':base,'ANSTS_EVIDENCE_NATIVE_CONTAINER':name,'ANSTS_EVIDENCE_NATIVE_CONFIG':str(project/'wrangler.json')},check=True)
    finally:
        if with_compose:subprocess.run([*compose,'down'],env=compose_env,check=True)
        else:subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
