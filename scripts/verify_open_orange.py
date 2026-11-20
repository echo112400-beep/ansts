"""Smoke-test the repository Worker launchers on an isolated Rivet Engine."""
import hashlib
import hmac
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
def request(base, route):
 with http.open(base+route,timeout=7) as response:
  return response.status,json.load(response)
engine="ansts-orange-"+uuid4().hex[:8]
engine_key=secrets.token_hex(24)
conf=root/"deploy/open-orange/rivet.json"
subprocess.run(["docker","run","-d","--name",engine,"-p","127.0.0.1::6420",
 "-e","RIVET__AUTH__ADMIN_TOKEN","-v",str(conf)+":/etc/rivet/config.json:ro","-v","/var/lib/rivet/log","-v","/var/lib/rivet/objects","rivetdev/engine:3.1.1"],env={**env,"RIVET__AUTH__ADMIN_TOKEN":engine_key},check=True,stdout=subprocess.DEVNULL)
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
 with tempfile.TemporaryDirectory(prefix="ansts-orange-roles-") as d:
  processes=[]
  endpoints={}
  fixture_key=secrets.token_hex(32)
  try:
   for role in ("scheduler","gateway","body"):
    p=Path(d)/role
    p.mkdir()
    with socket.socket() as sock:
     sock.bind(("127.0.0.1",0))
     port=sock.getsockname()[1]
    endpoints[role]=f"http://127.0.0.1:{port}"
    role_env={**env,"ORANGE_WORKER_VARS":"","ORANGE_WORKER_SECRETS":"","RIVET_POOL":"default"}
    if role!="scheduler":role_env.update(ANSTS_SCHEDULER_URL=endpoints["scheduler"],ORANGE_WORKER_VARS="ANSTS_SCHEDULER_URL")
    if role!="gateway":role_env.update(ANSTS_BODY_AUTH_SECRET=fixture_key,ORANGE_WORKER_SECRETS="ANSTS_BODY_AUTH_SECRET")
    with (p/"worker.log").open("w") as log:
     proc=subprocess.Popen(["node","scripts/check_open_orange.mjs","--serve",role,str(port)],cwd=root,env=role_env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    processes.append(proc)
    for _ in range(30):
     try:
      for route in ("/healthz","/readyz"):
       status,response=request(endpoints[role],route)
       if status != 200 or response.get("healthy") is not True:
        raise OSError(f"{role} {route} is not ready")
       print(role,route,status,flush=True)
      break
     except OSError:
      pass
     if proc.poll() is not None:
      raise RuntimeError(f"{role} exited with status {proc.returncode}")
     time.sleep(1)
    else: raise RuntimeError(role+" timeout")
   token=hmac.new(fixture_key.encode(),b"ansts-body-v1:gateway:control",hashlib.sha256).hexdigest()
   headers={"Content-Type":"application/json","Authorization":"Gateway "+token}
   submit=urllib.request.Request(endpoints["scheduler"]+"/v1/scheduler/submit",
        data=json.dumps({"task_id":"orange-smoke","tenant_id":"isolated","prompt":"runtime verification"}).encode(),headers=headers)
   with http.open(submit,timeout=8) as response:
    assert response.status==202
    task_id=json.load(response)["task_id"]
   with http.open(urllib.request.Request(endpoints["scheduler"]+"/v1/scheduler/tasks",headers=headers),timeout=8) as response:
    assert any(task["task_id"]==task_id for task in json.load(response)["tasks"])
   print("scheduler authenticated submit/read passed",flush=True)

  finally:
   for proc in reversed(processes):
    if proc.poll() is None:
     os.killpg(proc.pid,signal.SIGTERM)
     try:proc.wait(timeout=8)
     except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()

finally:
 subprocess.run(["docker","rm","-f","-v",engine],check=True,stdout=subprocess.DEVNULL)
