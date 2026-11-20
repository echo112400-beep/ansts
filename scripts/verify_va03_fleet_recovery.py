#!/usr/bin/env python3
"""Verify production Worker directory and tenant state across isolated Fleet stop and rejoin."""
from urllib.error import HTTPError
import json,re,secrets,subprocess,tempfile,time,urllib.request,uuid
from pathlib import Path
name="ansts-va03-fleet-"+uuid.uuid4().hex[:8]
key=secrets.token_hex(20)
auth={"AWS_ACCESS_KEY_ID":"va03fixture","AWS_SECRET_ACCESS_KEY":key,"AWS_DEFAULT_REGION":"us-east-1"}
http=urllib.request.build_opener(urllib.request.ProxyHandler({}))
nodes=["brain-1","brain-2","brain-3"]
services={
"storage":{"image":"rustfs/rustfs:1.0.4","environment":{"RUSTFS_ACCESS_KEY":"va03fixture","RUSTFS_SECRET_KEY":key},"volumes":["storage:/data"]},
"bucket":{"image":"amazon/aws-cli:2.39.3","environment":auth,"command":["--endpoint-url","http://storage:9000","s3api","create-bucket","--bucket","va03-fixture"]}}
for node in nodes:
    services[node]={"image":"ghcr.io/denoland/celld:0.6.3","environment":{**auth,"CELLD_NODE":node,"CELLD_DURABILITY":"fleet","CELLD_TTL_MS":"10000","CELLD_WATCH":"/var/lib/celld"},"command":["--bucket","s3://va03-fixture","--endpoint","http://storage:9000","--region","us-east-1","--listen","0.0.0.0:8081","--internal-listen","0.0.0.0:8082","--advertise",node+":8082"],"ports":["127.0.0.1::8081"],"volumes":[node+":/var/lib/celld"]}
with tempfile.TemporaryDirectory(prefix=name) as tmp:
    config=Path(tmp)/"compose.json"
    repo=Path(__file__).resolve().parents[1]
    release=Path(tmp)/"release"
    release.mkdir()
    bundle=repo/"node_modules/@esbuild/linux-x64/bin/esbuild"
    subprocess.run([str(bundle),str(repo/"src/cloudflare/worker.ts"),"--bundle","--format=esm","--platform=node","--outfile="+str(release/"worker.js")],check=True)
    wrangler=json.loads((repo/"deploy/celld/wrangler.celld.json").read_text())
    wrangler.update(main="worker.js",vars={"ANSTS_DIRECTORY_SERVICE_TOKEN":"d"*64,"ANSTS_ADMISSION_SERVICE_TOKEN":"e"*64,"ANSTS_ADMISSION_SIGNING_KEY":"c"*64,"ANSTS_BODY_AUTH_SECRET":"f"*64})
    (release/"wrangler.json").write_text(json.dumps(wrangler))
    services["release"]={"image":"ghcr.io/denoland/celld:0.6.3","environment":{**auth,"CELLD_ESBUILD":"/tools/esbuild"},"volumes":[str(release)+":/release",str(bundle)+":/tools/esbuild:ro"],"command":["deploy","/release","--bucket","s3://va03-fixture","--endpoint","http://storage:9000","--region","us-east-1"]}
    config.write_text(json.dumps({"services":services,"volumes":{k:{} for k in ["storage",*nodes]}}))
    def compose(*args,**kw):
        return subprocess.run(["docker","compose","-p",name,"-f",str(config),*args],check=True,**kw)
    def base(node):
        return "http://"+compose("port",node,"8081",capture_output=True,text=True).stdout.strip()

    def healthy(members):
        endpoints=[base(node) for node in members]
        until=time.monotonic()+45
        while time.monotonic()<until:
            try:
                for endpoint in endpoints:
                    with http.open(endpoint+"/.well-known/celld/health",timeout=2) as resp:
                        assert resp.status==200
                return
            except OSError:
                time.sleep(1)
        raise RuntimeError("Fleet health timeout: "+str(members))
    def admin(node, action, payload, token="d"*64, service="directory", scope="all"):
        req=urllib.request.Request(base(node)+"/internal/v1/"+service+"/"+action,
            data=json.dumps({"actor":{"sub":"va03-fixture","role":"SRE_ADMIN","scopeTenant":scope,"scopeRegion":"all"},**payload}).encode(),
            headers={"Authorization":"Bearer "+token,"Content-Type":"application/json"})
        with http.open(req,timeout=10) as resp:
            return json.load(resp)
    def tenant(node, action, payload=None, scope="all"):
        token=("e" if action.startswith("admission-") else "d")*64
        return admin(node,action,{"tenantId":"evidence",**(payload or {})},token,"tenant-control",scope)

    def tenant_snapshot(node):
        try:
            tenant(node,"cost-summary",scope="unrelated")
        except HTTPError as error:
            assert error.code==404, error
        else:
            raise AssertionError("An unrelated tenant scope read the cost summary")
        return {
            "policy":tenant(node,"policy-read"),
            "reservation":tenant(node,"admission-read",{"reservationId":"fleet-reservation"}),
            "summary":tenant(node,"cost-summary"),
            "entries":tenant(node,"cost-read"),
            "archive":tenant(node,"finops-archive-read",{"archiveId":archive_id}),
        }

    try:
        compose("up","-d","storage")
        compose("run","--rm","-e","AWS_MAX_ATTEMPTS=8","bucket")
        compose("run","--rm","release")
        compose("up","-d",*nodes)
        healthy(nodes)
        mutation={"input":{"id":"eng","name":"VA-03 fixture","status":"ACTIVE","expectedVersion":0,
            "reason":"Fleet recovery fixture","idempotencyKey":"va03-center","requestId":"va03-center"}}
        original=admin(nodes[0],"cost-center-mutate",mutation)
        admin(nodes[0],"mutate",{"input":{**mutation["input"],"id":"evidence",
            "costCenter":"eng","region":"dev","idempotencyKey":"fleet-tenant","requestId":"fleet-tenant"}})
        resources={"concurrentAttempts":1,"cpuMilliCores":1000,"memoryMiB":512,"queuedTasks":10}
        tenant(nodes[0],"policy-write",{"input":{"expectedVersion":0,"resources":resources,
            "budget":{"period":"UTC_DAY","limitCreditUnits":"100","softThresholdBps":8000},
            "reason":"Isolated Fleet policy","idempotencyKey":"fleet-policy","requestId":"fleet-policy"}})
        reservation={"reservationId":"fleet-reservation","taskId":"fleet-task","attemptId":"fleet-attempt",
            "fencing":1,"expectedPolicyVersion":1,"resources":{**resources,"queuedTasks":0},
            "estimatedCreditUnits":"10","expiresAt":int(time.time()*1000)+240000,"executionInputDigest":"a"*64}
        tenant(nodes[0],"admission-prepare",{"input":reservation,"idempotencyKey":"fleet-reservation"})
        rate={"schemaVersion":1,"unit":"credit-subunit","unitVersion":1,"rounding":"CEIL",
              "scope":{"region":None,"poolId":None},"resources":{
                  "cpuNanoCoreMilliseconds":{"numerator":"1","denominator":"1000000000000"},
                  "memoryMiBMilliseconds":{"numerator":"0","denominator":"1"},
                  "slotMilliseconds":{"numerator":"0","denominator":"1"}},"models":[]}
        admin(nodes[0],"rate-create",{"input":{"expectedVersion":0,"definition":rate,
            "reason":"Isolated Fleet rate","idempotencyKey":"fleet-rate","requestId":"fleet-rate"}})
        tenant(nodes[0],"rate-activate",{"input":{"expectedVersion":0,"rateCardVersion":1,
            "reason":"Isolated Fleet activation","idempotencyKey":"fleet-rate","requestId":"fleet-rate"}})
        usage_probe=Path(tmp)/"usage.ts"
        fixture=repo/"scripts/legacy_usage_native_fixture.ts"
        usage_probe.write_text(f"import {{runLegacyUsageFixture}} from {json.dumps(str(fixture))};\n"
            "const [root, directory] = process.argv.slice(2);\n"
            "await runLegacyUsageFixture(root, directory, "
            "'node@sha256:fe4f6d74e20370acb04ad889077d14d0f64a9c8ea6a42bc60cddf1e844699914', "
            "'f'.repeat(64), 'va03-host', true);\n")
        usage_bundle=Path(tmp)/"usage.mjs"
        subprocess.run([str(bundle),str(usage_probe),"--bundle","--platform=node","--format=esm",
                        "--outfile="+str(usage_bundle)],check=True)
        subprocess.run(["node",str(usage_bundle),base(nodes[0]),tmp],cwd=repo,check=True)
        status=tenant(nodes[0],"finops-archive-status")
        manifest=tenant(nodes[0],"finops-archive-create",{"input":{"expectedRevision":status["sourceRevision"],
            "reason":"Isolated Fleet archive","idempotencyKey":"fleet-archive","requestId":"fleet-archive"}})
        archive_id=manifest["id"]
        snapshot=tenant_snapshot(nodes[0])
        audit=admin(nodes[0],"audit",{"page":{"domain":"all"}})
        assert audit["items"], audit
        assert int(snapshot["summary"]["knownSubtotalCreditUnits"])>0, snapshot["summary"]
        assert snapshot["entries"]["items"], "Expected persisted cost ledger entries"
        assert snapshot["reservation"]["reservation"]["id"]=="fleet-reservation"
        assert snapshot["archive"]["snapshot"]["tables"], "Expected a persisted tenant archive"
        for stopped in nodes:
            survivors=[node for node in nodes if node!=stopped]
            compose("stop",stopped)
            time.sleep(12)
            healthy(survivors)
            assert admin(survivors[0],"cost-center-mutate",mutation)==original
            for survivor in survivors:
                assert admin(survivor,"audit",{"page":{"domain":"all"}})==audit
                assert tenant_snapshot(survivor)==snapshot
            compose("start",stopped)
            healthy(nodes)
            assert admin(stopped,"audit",{"page":{"domain":"all"}})==audit
            assert tenant_snapshot(stopped)==snapshot
            print(f"PASS: audit, policy, reservation, tenant authorization and financial archive survived {stopped} stop and rejoin.")
        logs=compose("logs","--no-color",*nodes,capture_output=True,text=True).stdout.splitlines()
        for class_name in ("TenantDirectoryCell","TenantControlCell"):
            handoffs=[line for line in logs if 'event="cell_handoff_accepted"' in line
                      and f"cell={class_name}:" in line]
            assert handoffs, f"No verified owner handoff for {class_name}"
            print(f"PASS: Fleet logged a successor taking ownership of {class_name}.")
        dated_logs=compose("logs","--no-color","--timestamps",*nodes,
                           capture_output=True,text=True).stdout.splitlines()
        owners=[]
        for line in dated_logs:
            if 'event="cell_runtime_publication"' not in line or "scope=TenantControlCell:" not in line:
                continue
            owner=re.search(r"node=(brain-[123]) ",line)
            epoch=re.search(r" epoch=(\d+)",line)
            if owner and epoch:
                owners.append((int(epoch[1]),owner[1]))
        assert owners, "No TenantControlCell owner in Fleet logs"
        epoch, owner=max(owners)
        survivors=[node for node in nodes if node!=owner]
        compose("kill","--signal","SIGKILL",owner)
        time.sleep(12)
        healthy(survivors)
        for survivor in survivors:
            assert tenant_snapshot(survivor)==snapshot
            assert admin(survivor,"audit",{"page":{"domain":"all"}})==audit
        recovered=compose("logs","--no-color",*survivors,capture_output=True,text=True).stdout
        assert any(int(match)>epoch for match in re.findall(
            r'event="cell_runtime_publication".*scope=TenantControlCell:.* epoch=(\d+)',recovered))
        compose("start",owner)
        healthy(nodes)
        assert tenant_snapshot(owner)==snapshot
        print("PASS: TenantControlCell recovered on a survivor at a newer epoch after its owner was killed.")
    finally:
        compose("down","--volumes")
