/** Own isolated CJS runtime, needed to exercise the real forked legacy worker rather than a bundle stub. */
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {mkdirSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {spawn} from 'node:child_process';
import {bodyServiceToken} from '../src/cloudflare/body_auth.js';
import {NodeDaemon} from '../src/node_daemon.js';
import {taskStorageId} from '../src/cloudflare/task_identity.js';
import {taskInputDigest} from '../src/cloudflare/execution_attempt.js';
import type {UsageIdentity} from '../src/control/usage_event.js';
export async function runLegacyUsageFixture(root:string,directory:string,image:string,secret:string,previousHost:string):Promise<UsageIdentity>{
 const host=previousHost+'-legacy',token=bodyServiceToken(secret,host),storage=join(directory,'legacy-runtime');mkdirSync(storage,{recursive:true});const tokenFile=join(storage,'fixture.token');writeFileSync(tokenFile,token,{mode:0o600});
 const headers={authorization:'Body '+token,'x-ansts-body-id':host,'content-type':'application/json'};
 const rpc=async(path:string,body?:unknown,extra:Record<string,string>=headers,base=root)=>{const response=await fetch(base+path,{method:body===undefined?'GET':'POST',headers:extra,body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(10000),redirect:'error'});assert.ok(response.status>=200&&response.status<300,path+': '+response.status);return response.json();};
 await rpc('/v1/scheduler/register-node',{...new NodeDaemon(previousHost,1,512,0).emitHeartbeat(),healthy:false,execution_slots:1,warm_pool_ready:0},{'content-type':'application/json',authorization:'Body '+bodyServiceToken(secret,previousHost),'x-ansts-body-id':previousHost});
 const prompt='Isolated legacy slot metering',taskId=taskStorageId('evidence','legacy-meter');
 await rpc('/v1/scheduler/submit',{tenant_id:'evidence',task_id:'legacy-meter',prompt,timeout_seconds:60,resource_profile:{think_cpu_cores:.1,act_cpu_cores:.1,act_memory_mb:64,disk_mb:32}},{'content-type':'application/json',authorization:'Gateway '+bodyServiceToken(secret,'gateway:control')});
 const portServer=createServer();await new Promise<void>(resolve=>portServer.listen(0,'127.0.0.1',resolve));const port=(portServer.address() as {port:number}).port;await new Promise<void>(resolve=>portServer.close(()=>resolve()));
 const child=spawn(process.execPath,[join(process.cwd(),'scripts/start_body_daemon.mjs')],{cwd:process.cwd(),env:{...process.env,ANSTS_NODE_ID:host,ANSTS_STORAGE_DIR:storage,ANSTS_BODY_TOKEN_FILE:tokenFile,ANSTS_BODY_CELL_URL:root,ANSTS_BODY_CELL_URLS:root,ANSTS_SCHEDULER_URLS:root,ANSTS_SCHEDULER_URL:root,ANSTS_MONITORING_TOKEN_FILE:'',ANSTS_BODY_EXECUTION_SLOTS:'1',ANSTS_HOST:'127.0.0.1',ANSTS_LISTEN_PORT:String(port),ANSTS_BODY_CPU_CORES:'1',ANSTS_BODY_MEMORY_MB:'512',ANSTS_SANDBOX_IMAGE:image,ANSTS_BODY_EXECUTION_DELAY_MS:'10',ANSTS_USAGE_METERING:'shadow',ANSTS_USAGE_TENANTS:'evidence',ANSTS_PHYSICAL_HOST_ID_FILE:'',ANSTS_DEEPSEEK_API_KEY_FILE:'',S3_ENDPOINT:''},stdio:['ignore','pipe','pipe']});
 let errors='';child.stdout.on('data',()=>{});child.stderr.on('data',chunk=>{errors=(errors+String(chunk)).slice(-2048);});const exited=new Promise<void>(resolve=>child.once('exit',()=>resolve()));
 try{
  let complete=false;
  for(let retry=0;retry<400;retry++){
   if(child.exitCode!==null)throw new Error('Legacy native runtime exited: '+errors);
   const {tasks}=await rpc('/v1/scheduler/tasks');
   if(tasks.some((task:{task_id:string;state:string})=>task.task_id===taskId&&task.state==='COMPLETED')){const status=await rpc('/v1/daemon/usage/status',{},headers,'http://127.0.0.1:'+port);if(status.pendingEvents===0){complete=true;break;}}
   await new Promise(resolve=>setTimeout(resolve,100));
  }
  assert.ok(complete,'legacy CJS process must finish and automatically deliver slot-only quantities');
  const result=await rpc('/v1/body/result?taskId='+encodeURIComponent(taskId)+'&tenantId=evidence',undefined,{authorization:'Gateway '+bodyServiceToken(secret,'gateway:control')});assert.equal(result.nodeId,host);assert.equal(result.phase,'COMPLETED');
  return {producerId:host,tenantId:'evidence',taskId,attemptId:result.attemptId,fencing:result.fencingToken,executionInputDigest:taskInputDigest('evidence',taskId,prompt)};
 }finally{child.kill('SIGTERM');const timer=setTimeout(()=>child.kill('SIGKILL'),10000);try{await exited;}finally{clearTimeout(timer);}}
}
