import {runLegacyUsageFixture} from './legacy_usage_native_fixture.js';
/** Isolated real celld Scheduler/Body authority + native Daemon + actual local Engine acceptance. */
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {DeepSeekModel} from '../src/runtime/deepseek_model.js';
import {UsageInbox} from '../src/finops/usage_inbox.js';
import {verifyUsageEvent,type UsageIdentity,type UsageEvent} from '../src/control/usage_event.js';
import type {DirectoryStorage} from '../src/control/tenant_directory.js';
import {randomUUID} from 'node:crypto';
import {execFileSync,spawn} from 'node:child_process';
import {BodyDaemonServer} from '../src/runtime/body_daemon_server.js';
import {ContainerSandbox} from '../src/runtime/container_sandbox.js';
import {NodeDaemon} from '../src/node_daemon.js';
import {bodyServiceToken} from '../src/cloudflare/body_auth.js';
import {taskStorageId} from '../src/cloudflare/task_identity.js';
import {taskInputDigest} from '../src/cloudflare/execution_attempt.js';
import {lookupBodyObservationBinding} from '../src/runtime/body_observation_binding.js';
import {deliverBodyObservations} from '../src/runtime/body_observation_delivery.js';
import {verifyBodyEvidence} from '../src/control/body_execution_evidence.js';
let root=process.env.ANSTS_EVIDENCE_NATIVE_URL!;const container=process.env.ANSTS_EVIDENCE_NATIVE_CONTAINER!;
assert.ok(new URL(root).hostname==='127.0.0.1');assert.match(container,/^ansts-body-evidence-[a-z0-9]+$/);
const secret='f'.repeat(64),directoryKey='d'.repeat(64),coordinatorKey='e'.repeat(64),hostId='evidence-'+randomUUID().slice(0,8),token=bodyServiceToken(secret,hostId);
const image='node@sha256:fe4f6d74e20370acb04ad889077d14d0f64a9c8ea6a42bc60cddf1e844699914',directory=mkdtempSync(join(tmpdir(),'ansts-real-evidence-'));
const actor={sub:'native-evidence-verifier',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},bodyHeaders={authorization:'Body '+token,'x-ansts-body-id':hostId};
async function rpc(path:string,body?:unknown,headers:Record<string,string>=bodyHeaders,origin=root){const response=await fetch(origin+path,{method:body===undefined?'GET':'POST',headers:{'content-type':'application/json',...headers},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(20000),redirect:'error'});const data=await response.json();return {status:response.status,data};}
async function good(path:string,body?:unknown,headers=bodyHeaders){const result=await rpc(path,body,headers);if(result.status<200||result.status>=300)throw new Error('RPC '+path+' rejected: '+result.status+' '+(result.data.code??result.data.error??''));return result.data;}
const coord=(action:string,values:Record<string,unknown>={})=>rpc('/internal/v1/tenant-control/'+action,{actor,tenantId:'evidence',...values},{authorization:'Bearer '+(action.startsWith('admission-')?coordinatorKey:directoryKey)});
let agentTurn=0;
const providerServer=createServer(async(request,response)=>{for await(const _ of request){}assert.equal(request.headers.authorization,'Bearer fixture-key');agentTurn++;response.writeHead(200,{'content-type':'application/json'});response.end(JSON.stringify({id:'fixture-response-'+agentTurn,model:'fixture-reported-model',usage:{prompt_tokens:2,completion_tokens:3,total_tokens:5},choices:[{message:{content:JSON.stringify(agentTurn===1?{tool:'test',argv:['node','-e',"console.log('native-agent-tool')"]}:{tool:'finish'})},finish_reason:'stop'}]}));});
await new Promise<void>(resolve=>providerServer.listen(0,'127.0.0.1',resolve));
const providerUrl='http://127.0.0.1:'+(providerServer.address() as {port:number}).port;
const agentModel=new DeepSeekModel('fixture-key','fixture-requested-model',async(url,init)=>{assert.equal(String(url),'https://api.deepseek.com/chat/completions');return fetch(providerUrl,init);});
let daemon=new BodyDaemonServer({usageMetering:true,nodeId:hostId,token,storageDir:directory,bodyWorkerUrl:root,host:'127.0.0.1',port:0,sandboxImage:image,resources:{cpu:1,memoryMb:512},agentModel});
try{
 const metadata={actor,input:{id:'eng',name:'Evidence fixture',status:'ACTIVE',expectedVersion:0,reason:'Isolated real Engine verification',idempotencyKey:'center',requestId:'center'}};
 await good('/internal/v1/directory/cost-center-mutate',metadata,{authorization:'Bearer '+directoryKey});
 await good('/internal/v1/directory/mutate',{actor,input:{id:'evidence',name:'Evidence tenant',costCenter:'eng',region:'dev',status:'ACTIVE',expectedVersion:0,reason:'Isolated verification',idempotencyKey:'tenant',requestId:'tenant'}},{authorization:'Bearer '+directoryKey});
 assert.equal((await coord('policy-write',{input:{expectedVersion:0,resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'Fixture-only budget',idempotencyKey:'policy',requestId:'policy'}})).status,200);
 let address=await daemon.start(),base='http://127.0.0.1:'+address.port;
 const execution={kind:'container',image,argv:['node','-e',"setTimeout(()=>console.log('real-evidence-execution'),1500)"],cpu:.25,memoryMb:128,timeoutMs:10000};
 await good('/v1/scheduler/register-node',{...new NodeDaemon(hostId,1,512,0).emitHeartbeat(),healthy:true,execution_slots:1,warm_pool_ready:0});
 await good('/v1/scheduler/submit',{tenant_id:'evidence',task_id:'container',prompt:'evidence work',timeout_seconds:30,resource_profile:{think_cpu_cores:.25,act_cpu_cores:.25,act_memory_mb:128,disk_mb:32},execution},{authorization:'Gateway '+bodyServiceToken(secret,'gateway:control')});
 await good('/v1/scheduler/schedule-tick',{});
 const taskId=taskStorageId('evidence','container'),task=(await good('/v1/scheduler/tasks')).tasks.find((task:{task_id:string})=>task.task_id===taskId);assert.equal(task.assigned_node_id,hostId);
 const lease={tenant_id:'evidence',task_id:taskId,node_id:hostId,attempt_id:taskId+':'+task.lease_fencing_token,fencing_token:task.lease_fencing_token,input_digest:taskInputDigest('evidence',taskId,'evidence work',execution)};
 const input={reservationId:'real-r1',taskId,attemptId:lease.attempt_id,fencing:lease.fencing_token,expectedPolicyVersion:1,resources:{concurrentAttempts:1,cpuMilliCores:250,memoryMiB:128,queuedTasks:0},estimatedCreditUnits:'10',expiresAt:Date.now()+300000,executionInputDigest:lease.input_digest};
 let prepared=await coord('admission-prepare',{input,idempotencyKey:'prepare'});assert.equal(prepared.status,200);let reservation=prepared.data.reservation;
 for(const to of ['RESERVED','START_AUTHORIZED']){const changed=await coord('admission-transition',{input:{reservationId:reservation.id,expectedVersion:reservation.version,generation:reservation.generation,fencing:reservation.fencing,to,reason:'Legacy execution observation binding',...(to==='RESERVED'?{target:hostId,capacityReceipt:'legacy-body-observation-not-a-capacity-grant'}:{})},idempotencyKey:to});assert.equal(changed.status,200);reservation=changed.data.reservation;}
 const admitted=await good('/v1/body/acquire?taskId='+encodeURIComponent(taskId),lease);
 const started=await good('/v1/body/start?taskId='+encodeURIComponent(taskId),{...lease,expected_version:admitted.execution.version});
 const binding=await lookupBodyObservationBinding({tenantId:'evidence',taskId,attemptId:lease.attempt_id,fencing:lease.fencing_token,executionInputDigest:lease.input_digest,resources:input.resources},hostId,async lookup=>good('/internal/v1/body-observation-binding',lookup));
 assert.equal(binding?.reservationId,reservation.id);
 const identity={tenantId:'evidence',taskId,attemptId:lease.attempt_id};
 assert.equal((await rpc('/v1/daemon/execution-evidence',identity,{},base)).status,401);
 assert.equal((await rpc('/v1/daemon/execution-evidence',identity,bodyHeaders,base)).status,404);
 const executing=rpc('/v1/sandboxes/execute',{...identity,fencingToken:lease.fencing_token,promptText:'evidence work',execution,executionToken:started.execution_token,tenantAdmissionObservation:binding},bodyHeaders,base);
 let runningProof='';for(let retry=0;retry<100;retry++){const observed=await rpc('/v1/daemon/execution-evidence',identity,bodyHeaders,base);if(observed.status===200&&verifyBodyEvidence(observed.data.proof,token).phase==='RUNNING'){runningProof=observed.data.proof;break;}await new Promise(resolve=>setTimeout(resolve,25));}
 assert.ok(runningProof);
 const localRpc=async(route:string,body:unknown)=>{const result=await rpc(route,body,bodyHeaders,base);assert.equal(result.status,200);return result.data;};
 const publish=async(proof:string)=>{const result=await rpc('/internal/v1/body-execution-evidence',{proof});assert.equal(result.status,200);return result.data;};
 assert.equal((await rpc('/v1/daemon/execution-evidence/pending',{}, {},base)).status,401);
 assert.equal(await deliverBodyObservations(localRpc,publish,token),1);
 const running=await coord('admission-read',{reservationId:reservation.id});assert.equal(running.status,200);assert.equal(running.data.reservation.phase,'RUNNING');
 const result=await executing;assert.equal(result.status,200);assert.equal(result.data.exitCode,0);assert.ok(result.data.logs.includes('real-evidence-execution'));
 const final=await rpc('/v1/daemon/execution-evidence',identity,bodyHeaders,base);assert.equal(final.status,200);assert.equal(verifyBodyEvidence(final.data.proof,token).phase,'TERMINATED');
 await assert.rejects(()=>deliverBodyObservations(localRpc,async proof=>{await publish(proof);throw new Error('lost terminal response');},token),/lost terminal response/);
 const settled=await coord('admission-read',{reservationId:reservation.id});assert.equal(settled.status,200);assert.equal(settled.data.reservation.phase,'SETTLING');assert.equal(settled.data.reservation.actualCreditUnits,null);
 const nextInput={...input,reservationId:'real-r2',taskId:'next',attemptId:'next-attempt',estimatedCreditUnits:'91'};const over=await coord('admission-prepare',{input:nextInput,idempotencyKey:'budget-exceeded'});assert.equal(over.status,409);assert.equal(over.data.code,'BUDGET_QUOTA_EXCEEDED');
 const next=await coord('admission-prepare',{input:{...nextInput,estimatedCreditUnits:'1'},idempotencyKey:'resource-released'});assert.equal(next.status,200);
 assert.equal((await coord('admission-transition',{input:{reservationId:'real-r2',expectedVersion:1,generation:1,fencing:lease.fencing_token,to:'CANCELLED',reason:'Fixture complete'},idempotencyKey:'cancel-next'})).status,200);
 const finished=await good('/v1/body/finish?taskId='+encodeURIComponent(taskId),{...lease,expected_version:started.execution.version,result:result.data});await good('/v1/body/receipt?hostId='+hostId,{...lease,result:result.data});await good('/v1/scheduler/complete',{...lease,execution_outcome:'SUCCEEDED'});await good('/v1/body/acknowledge?taskId='+encodeURIComponent(taskId),{...lease,expected_version:finished.execution.version});
 await daemon.stop();daemon=new BodyDaemonServer({usageMetering:true,nodeId:hostId,token,storageDir:directory,bodyWorkerUrl:root,host:'127.0.0.1',port:0,sandboxImage:image,resources:{cpu:1,memoryMb:512},agentModel});address=await daemon.start();base='http://127.0.0.1:'+address.port;
 assert.equal((await rpc('/v1/daemon/execution-evidence',identity,bodyHeaders,base)).data.proof,final.data.proof);
 execFileSync('docker',['restart',container],{stdio:'ignore'});root='http://127.0.0.1:'+execFileSync('docker',['port',container,'8080'],{encoding:'utf8'}).trim().split(':').at(-1);for(let retry=0;retry<100;retry++){try{if((await rpc('/healthz')).status===200)break;}catch{}await new Promise(resolve=>setTimeout(resolve,100));}
 for(let retry=0;retry<50;retry++){const pending=await rpc('/v1/daemon/execution-evidence/pending',{},bodyHeaders,base);if(pending.data.observations.length)break;await new Promise(resolve=>setTimeout(resolve,100));}
 assert.equal(await deliverBodyObservations(localRpc,publish,token),1);
 assert.equal((await rpc('/v1/daemon/execution-evidence/pending',{},bodyHeaders,base)).data.observations.length,0);
 const replay=await coord('admission-observe-execution',{reservationId:reservation.id,proof:final.data.proof});assert.equal(replay.status,200);assert.deepEqual(replay.data.reservation,settled.data.reservation);
 // Exercise the actual production BodyAgent script: it discovers and passes the binding,
 // executes the Agent loop, delivers observations and acknowledges them without a fixture relay.
 await daemon.stop();daemon=new BodyDaemonServer({usageMetering:true,nodeId:hostId,token,storageDir:directory,bodyWorkerUrl:root,host:'127.0.0.1',port:0,sandboxImage:image,resources:{cpu:1,memoryMb:512},agentModel});address=await daemon.start();base='http://127.0.0.1:'+address.port;
 await good('/v1/scheduler/register-node',{...new NodeDaemon(hostId,1,512,0).emitHeartbeat(),healthy:true,execution_slots:1,warm_pool_ready:0});
 const agentPublicId='agent-'+ 'x'.repeat(120),agentTaskId=taskStorageId('evidence',agentPublicId),agentExecution={kind:'agent',image,cpu:.25,memoryMb:128,timeoutMs:30000,maxTurns:4,maxTokens:32768,files:{}};
 await good('/v1/scheduler/submit',{tenant_id:'evidence',task_id:agentPublicId,prompt:'Run isolated local tool',timeout_seconds:60,resource_profile:{think_cpu_cores:.25,act_cpu_cores:.25,act_memory_mb:128,disk_mb:32},execution:agentExecution},{authorization:'Gateway '+bodyServiceToken(secret,'gateway:control')});
 await good('/v1/scheduler/schedule-tick',{});
 const agentTask=(await good('/v1/scheduler/tasks')).tasks.find((item:{task_id:string})=>item.task_id===agentTaskId);assert.equal(agentTask.assigned_node_id,hostId);
 const agentInput={...input,reservationId:'agent-r1',taskId:agentTaskId,attemptId:agentTaskId+':'+agentTask.lease_fencing_token,fencing:agentTask.lease_fencing_token,executionInputDigest:taskInputDigest('evidence',agentTaskId,'Run isolated local tool',agentExecution),expiresAt:Date.now()+300000};
 const agentPrepared=await coord('admission-prepare',{input:agentInput,idempotencyKey:'agent-prepare'});assert.equal(agentPrepared.status,200);let agentReservation=agentPrepared.data.reservation;
 for(const to of ['RESERVED','START_AUTHORIZED']){const changed=await coord('admission-transition',{input:{reservationId:agentReservation.id,expectedVersion:agentReservation.version,generation:agentReservation.generation,fencing:agentReservation.fencing,to,reason:'Isolated Agent observation binding',...(to==='RESERVED'?{target:hostId,capacityReceipt:'legacy-body-observation-not-a-capacity-grant'}:{})},idempotencyKey:'agent-'+to});assert.equal(changed.status,200);agentReservation=changed.data.reservation;}
 const tokenFile=join(directory,'fixture.token');writeFileSync(tokenFile,token,{mode:0o600});
 const child=spawn(process.execPath,[join(process.cwd(),'scripts/body_cell_agent.mjs')],{cwd:process.cwd(),env:{...process.env,ANSTS_USAGE_METERING:'shadow',ANSTS_NODE_ID:hostId,ANSTS_STORAGE_DIR:directory,ANSTS_BODY_TOKEN_FILE:tokenFile,ANSTS_BODY_CELL_URL:root,ANSTS_BODY_CELL_URLS:root,ANSTS_SCHEDULER_URLS:root,ANSTS_SCHEDULER_URL:root,ANSTS_LISTEN_PORT:String(address.port),ANSTS_BODY_CPU_CORES:'1',ANSTS_BODY_MEMORY_MB:'512'},stdio:['ignore','pipe','pipe']});
 let output='';child.stdout.on('data',chunk=>{output=(output+String(chunk)).slice(-32768);});child.stderr.on('data',chunk=>{output=(output+String(chunk)).slice(-32768);});
 const exited=new Promise<void>(resolve=>child.once('exit',()=>resolve()));
 try {
  let completed=false;
  for(let retry=0;retry<300;retry++){
   if(child.exitCode!==null)throw new Error('BodyAgent exited before acceptance');
   const current=await coord('admission-read',{reservationId:'agent-r1'}),listed=await good('/v1/scheduler/tasks'),pending=await rpc('/v1/daemon/execution-evidence/pending',{},bodyHeaders,base);
   if(current.data.reservation?.phase==='SETTLING'&&listed.tasks.some((item:{task_id:string;state:string})=>item.task_id===agentTaskId&&item.state==='COMPLETED')&&pending.data.observations.length===0&&(await rpc('/v1/daemon/usage/status',{},bodyHeaders,base)).data.pendingEvents===0){completed=true;assert.equal(current.data.reservation.actualCreditUnits,null);break;}
   await new Promise(resolve=>setTimeout(resolve,100));
  }
  assert.ok(completed,'actual BodyAgent must bind, execute, deliver and acknowledge');assert.equal(agentTurn,2);
  const agentFact=await rpc('/v1/daemon/execution-evidence',{tenantId:'evidence',taskId:agentTaskId,attemptId:agentInput.attemptId},bodyHeaders,base);assert.equal(verifyBodyEvidence(agentFact.data.proof,token).phase,'TERMINATED');
  assert.ok(!output.includes('body-observation-unbound')&&!output.includes('body-observation-binding-deferred'));
 }finally{child.kill('SIGTERM');const timer=setTimeout(()=>child.kill('SIGKILL'),10000);try{await exited;}finally{clearTimeout(timer);}}
 const legacyIdentity=await runLegacyUsageFixture(root,directory,image,secret,hostId);
 const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');
 const usageFile=join(directory,'usage-receiver.sqlite');let usageDb=new DatabaseSync(usageFile);
 const usageStorage:DirectoryStorage={sql:{exec<T>(query:string,...values:unknown[]){const rows=usageDb.prepare(query).all(...values) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){usageDb.exec('BEGIN IMMEDIATE');try{const result=fn();usageDb.exec('COMMIT');return result;}catch(error){usageDb.exec('ROLLBACK');throw error;}}};
 let usageSink=new UsageInbox(usageStorage);const usageEvents:UsageEvent[]=[],usageIdentities=new Map<string,UsageIdentity>();
 try{
  let usageCursor:string|null=null;
  do{
   const page=await coord('usage-read',{page:{limit:1,...(usageCursor?{cursor:usageCursor}:{})}});assert.equal(page.status,200);assert.equal(page.data.pricing,'NOT_CONNECTED');usageCursor=page.data.nextCursor;
   for(const item of page.data.events){const event=item.event as UsageEvent;assert.equal(event.tenantId,'evidence');assert.ok([taskId,agentTaskId,legacyIdentity.taskId].includes(event.taskId));
    const identity:UsageIdentity=event.taskId===legacyIdentity.taskId?legacyIdentity:event.taskId===taskId?{producerId:hostId,tenantId:'evidence',taskId,attemptId:lease.attempt_id,fencing:lease.fencing_token,executionInputDigest:lease.input_digest}:{producerId:hostId,tenantId:'evidence',taskId:agentTaskId,attemptId:agentInput.attemptId,fencing:agentInput.fencing,executionInputDigest:agentInput.executionInputDigest};
    usageIdentities.set(event.taskId,identity);assert.equal(item.context.tenantVersion,1);assert.equal(item.context.rateCardVersion,null);const receipt=usageSink.receive(event,identity);assert.equal(receipt.outcome,'RECORDED');usageEvents.push(event);
   }
  }while(usageCursor);
  assert.equal(usageIdentities.size,3);assert.ok(usageEvents.some(e=>e.kind==='RESOURCE_SEGMENT'));
  for(const event of usageEvents.filter(e=>e.kind==='RESOURCE_SEGMENT')){if(event.taskId===legacyIdentity.taskId){assert.equal(event.allocation!.cpuNanoCoreMilliseconds,null);assert.equal(event.allocation!.memoryMiBMilliseconds,null);assert.ok(BigInt(event.allocation!.slotMilliseconds)>0n);}else{assert.equal(BigInt(event.allocation!.cpuNanoCoreMilliseconds!),BigInt(event.allocation!.slotMilliseconds)*250000000n);assert.equal(BigInt(event.allocation!.memoryMiBMilliseconds!),BigInt(event.allocation!.slotMilliseconds)*128n);}}
  const providerEvents=usageEvents.filter(e=>e.kind==='PROVIDER_USAGE');assert.equal(providerEvents.length,2);assert.ok(providerEvents.every(e=>e.provider!.totalTokens==='5'&&e.provider!.inputTokens==='2'&&e.provider!.outputTokens==='3'&&e.provider!.completeness==='COMPLETE'&&e.provider!.reportedModel==='fixture-reported-model'));
  usageDb.close();usageDb=new DatabaseSync(usageFile);usageSink=new UsageInbox(usageStorage);
  for(const identity of usageIdentities.values())assert.equal(usageSink.inspect(identity)!.pending,identity.taskId===legacyIdentity.taskId);
  assert.equal(usageSink.receive(usageEvents[0],usageIdentities.get(usageEvents[0].taskId)!).outcome,'RECORDED');
 }finally{usageDb.close();}
 console.log('PASS: real allocation segments and DeepSeek adapter HTTP-response token evidence persisted in native usage outboxes, automatically signed/ordered/acknowledged into the Fleet tenant authority and paged with frozen context, then replayed into an independent SQLite inbox and replayed after receiver reconstruction; no cost values or paid request. Real forked legacy runtime records slot time with unknown CPU/memory. No manual usage relay; lost remote receipt is retried by the production BodyAgent.');
 console.log('PASS: actual production BodyAgent automatically resolved a long-ID Agent binding, executed a local tool on real Docker Engine, delivered signed terminal evidence and acknowledged its durable outbox; model adapter was deterministic, no paid provider call.');
 console.log('PASS: real native Scheduler lease/Body grant/Daemon/Engine facts authenticated into tenant RUNNING/SETTLING; resource quota freed, unpriced budget retained, host/control restart, lost-response outbox retry and digest acknowledgement verified.');
}finally{providerServer.closeAllConnections();await new Promise<void>(resolve=>providerServer.close(()=>resolve()));await daemon.stop();await new ContainerSandbox(image,'/var/run/docker.sock',hostId).recoverOrphans();rmSync(directory,{recursive:true,force:true});}
