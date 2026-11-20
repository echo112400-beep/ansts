"""Verify an isolated celld to OpenOrange Scheduler round trip."""
import json
import os
import secrets
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request
from pathlib import Path
from uuid import uuid4
root=Path(__file__).resolve().parents[1]
http=urllib.request.build_opener(urllib.request.ProxyHandler({}))
env={**os.environ,"NO_PROXY":"localhost,127.0.0.1","no_proxy":"localhost,127.0.0.1"}
engine="ansts-orange-"+uuid4().hex[:8]
engine_key=secrets.token_hex(24)
conf=root/"deploy/open-orange/rivet.json"
subprocess.run(["docker","run","-d","--name",engine,"-p","127.0.0.1::6420",
 "-e","RIVET__AUTH__ADMIN_TOKEN","-v",str(conf)+":/etc/rivet/config.json:ro","-v","/var/lib/rivet/log","-v","/var/lib/rivet/objects","rivetdev/engine:3.1.1"],env={**env,"RIVET__AUTH__ADMIN_TOKEN":engine_key},check=True,stdout=subprocess.DEVNULL)
source_name=engine+"-source"
reverse_name=engine+"-reverse"
try:
 address=subprocess.check_output(["docker","port",engine,"6420/tcp"],text=True).strip()
 env.update(RIVET_ENDPOINT="http://"+address,RIVET_TOKEN=engine_key,RIVET_NAMESPACE="default")
 pool_url=env["RIVET_ENDPOINT"]+"/pools/default?namespace=default"
 for attempt in range(30):
  try:
   req=urllib.request.Request(pool_url,data=b"{}",method="PUT",headers={"Authorization":"Bearer "+engine_key,"Content-Type":"application/json"})
   with http.open(req,timeout=3):pass
   break
  except OSError:
   if attempt==29:
    subprocess.run(["docker","logs","--tail","30",engine])
    raise
   time.sleep(1)
 with tempfile.TemporaryDirectory(prefix="ansts-orange-scheduler-transfer-") as work:
    folder=Path(work)
    src=folder/"celld"
    target=folder/"orange"
    src.mkdir()
    target.mkdir()
    entry=root/"scripts/open_orange_scheduler_fixture.ts"
    esbuild=root/"node_modules/@esbuild/linux-x64/bin/esbuild"
    subprocess.run([str(esbuild),str(entry),"--bundle","--format=esm","--platform=node","--outfile="+str(src/"worker.mjs")],check=True)
    (target/"worker.mjs").write_bytes((src/"worker.mjs").read_bytes())
    auth=secrets.token_hex(20)
    for directory,main in [(src,"worker.mjs"),(target,"worker.mjs")]:
      config={"name":"ansts-scheduler-transfer","main":main,"compatibility_date":"2025-08-15","compatibility_flags":["nodejs_compat"],
        "durable_objects":{"bindings":[{"name":"SCHEDULER","class_name":"SchedulerProbe"}]},
        "migrations":[{"tag":"v1","new_sqlite_classes":["SchedulerProbe"]}],"vars":{"FIXTURE_TOKEN":auth}}
      (directory/"wrangler.json").write_text(json.dumps(config))
    subprocess.run(["docker","run","-d","--name",source_name,"--user",str(os.getuid())+":"+str(os.getgid()),"-p","127.0.0.1::8080","-e","CELLD_ESBUILD=/tools/esbuild","-v",str(esbuild)+":/tools/esbuild:ro","-v",str(src)+":/probe","ghcr.io/denoland/celld:0.6.3","dev","/probe","--host","0.0.0.0","--port","8080","--no-watch"],check=True,stdout=subprocess.DEVNULL)
    origin="http://"+subprocess.check_output(["docker","port",source_name,"8080"],text=True).strip()
    with socket.socket() as sock:
      sock.bind(("127.0.0.1",0))
      port=sock.getsockname()[1]
    endpoint=f"http://127.0.0.1:{port}"
    def call(base,action,payload=None,authenticated=True):
      headers={"content-type":"application/json"}
      if authenticated:headers["authorization"]="Bearer "+auth
      req=urllib.request.Request(base+"/"+action,data=None if payload is None else json.dumps(payload).encode(),headers=headers)
      try:
        with http.open(req,timeout=8) as res:return res.status,json.load(res)
      except urllib.error.HTTPError as err:
        return err.code,err.read().decode()[:1000]
    with (target/"worker.log").open("w") as log:
      worker=subprocess.Popen(["orange","serve","--port",str(port),"--ip","127.0.0.1"],cwd=target,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    try:
      for _ in range(25):
        try:
          if all(call(base,"view",authenticated=False)[0]==401 for base in (origin,endpoint)):break
        except OSError:pass
        if worker.poll() is not None:raise RuntimeError((target/"worker.log").read_text()[-1800:])
        time.sleep(1)
      else:raise RuntimeError("fixture did not start")
      status,backup=call(origin,"capture",{})
      assert status==200,(status,backup)
      tampered=json.loads(json.dumps(backup))
      row=tampered["rows"]["execution_inventory_events"][0]
      event=json.loads(row["payload_json"])
      event["observation"]["state"]="CORRUPT"
      row["payload_json"]=json.dumps(event)
      assert call(endpoint,"restore",tampered)[0]==500
      status,restored=call(endpoint,"restore",backup)
      assert status==200,(status,restored)
      for key in ("task","lease","node"):assert restored[key]==backup["source"][key], key
      assert restored["lease"]["expires_at"]>time.time()
      assert restored["task"]["state"]=="LEASED" and restored["node"]["slots"]==1
      assert restored["history"]["sourceId"]!=backup["source"]["history"]["sourceId"]
      assert restored["history"]["entries"]==backup["source"]["history"]["entries"]
      assert restored["history"]["coverageSince"]>=backup["source"]["history"]["coverageSince"]
      assert restored["history"]["watermark"]!=backup["source"]["history"]["watermark"]
      assert call(endpoint,"restore",backup)[0]==500
      assert call(endpoint,"alarm-state")[1]=={"at":backup["alarmAt"],"fired":0,"firstFiredAt":None}
      assert call(origin,"target-write")== (200,{"id":None,"state":None})
      status,checkpoint=call(endpoint,"checkpoint",{})
      if status!=200: print((target/"worker.log").read_text()[-5000:])
      assert status==200 and len(checkpoint["rows"]["ansts_tasks"])==2,(status,checkpoint)
      assert call(endpoint,"target-write")[1]["id"]=="orange-target-write"
      reverse_dir=folder/"rollback"
      reverse_dir.mkdir()
      for filename in ("worker.mjs","wrangler.json"):(reverse_dir/filename).write_bytes((src/filename).read_bytes())
      subprocess.run(["docker","run","-d","--name",reverse_name,"--user",str(os.getuid())+":"+str(os.getgid()),"-p","127.0.0.1::8080","-e","CELLD_ESBUILD=/tools/esbuild","-v",str(esbuild)+":/tools/esbuild:ro","-v",str(reverse_dir)+":/probe","ghcr.io/denoland/celld:0.6.3","dev","/probe","--host","0.0.0.0","--port","8080","--no-watch"],check=True,stdout=subprocess.DEVNULL)
      rollback="http://"+subprocess.check_output(["docker","port",reverse_name,"8080"],text=True).strip()
      for _ in range(30):
        try:
          if call(rollback,"view",authenticated=False)[0]==401:break
        except OSError:time.sleep(.3)
      else:raise RuntimeError("rollback runtime did not start")
      assert call(rollback,"restore",checkpoint,authenticated=False)[0]==401
      status,roundtrip=call(rollback,"restore",checkpoint)
      assert status==200,(status,roundtrip)
      for key in ("task","lease","node"):assert roundtrip[key]==checkpoint["source"][key],key
      assert roundtrip["history"]["entries"]==checkpoint["source"]["history"]["entries"]
      assert roundtrip["history"]["headDigest"]==checkpoint["source"]["history"]["headDigest"]
      assert call(rollback,"target-write")==call(endpoint,"target-write")
      assert call(origin,"target-write")== (200,{"id":None,"state":None})
      assert call(rollback,"restore",checkpoint)[0]==500
      print("PASS: original Scheduler lease and target-only write round-tripped into clean celld with identical history entries and digest")
      for target,archive in ((endpoint,checkpoint),(rollback,checkpoint)):
        assert call(target,"alarm-state")[1]["at"]==archive["alarmAt"]
        assert call(target,"claim",{"task_id":"orange-lease-fixture","holder_id":"other"})[0]==409
        assert call(target,"claim",{"task_id":"orange-lease-fixture","holder_id":"fixture-owner"})[0]==200
        for reservation,task,digest,expected in [("expired","expired-task","expired-digest",True),("accepted","purged-task","accepted-digest",False)]:
          assert call(target,"expiry-status",{"reservationId":reservation,"taskId":task,"inputDigest":digest})==(200,{"expiredBeforeAcceptance":expected})
      assert call(origin,"view")[1]["task"]==backup["source"]["task"]
      status,after=call(endpoint,"expire",{})
      assert status==200 and after["early"]==[] and after["expired"]==["orange-lease-fixture"],(status,after)
      assert after["state"]["task"]["state"]=="QUEUED" and after["state"]["node"]["slots"]==0
      assert after["state"]["node"]["cpu"]==8 and after["state"]["node"]["memory"]==8192
      for target in (endpoint,rollback):
        deadline=time.monotonic()+14
        while time.monotonic()<deadline:
          status,alarm=call(target,"alarm-state")
          if status==200 and alarm["fired"]:
            assert alarm["firstFiredAt"]>=checkpoint["alarmAt"],alarm
            break
          time.sleep(.2)
        else:raise AssertionError("restored Scheduler alarm did not fire")
      print("PASS: both runtime alarms fired at or after the restored timestamp; claim and admission checks agreed")
      print("PASS: celld to Rivet Scheduler lease and node occupancy match; verified inventory chain, new source watermark, expiry recovery and source preserved")
    finally:
      if worker.poll() is None:
        os.killpg(worker.pid,signal.SIGTERM)
        try: worker.wait(timeout=10)
        except subprocess.TimeoutExpired: os.killpg(worker.pid,signal.SIGKILL);worker.wait()
finally:
 subprocess.run(["docker","rm","-f",reverse_name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 subprocess.run(["docker","rm","-f",source_name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 subprocess.run(["docker","rm","-f","-v",engine],check=True,stdout=subprocess.DEVNULL)
