import {signCapacityReport} from '../src/control/capacity_report.js';
import {runLegacyUsageFixture} from './legacy_usage_native_fixture.js';
/** Isolated real celld Scheduler/Body authority + native Daemon + actual local Engine acceptance. */
import assert from 'node:assert/strict';
import {cpSync,mkdtempSync,readFileSync,rmSync,writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import {DeepSeekModel} from '../src/runtime/deepseek_model.js';
import {UsageInbox} from '../src/finops/usage_inbox.js';
import {signUsageEvent,usageStreamId,verifyUsageEvent,type UsageIdentity,type UsageEvent} from '../src/control/usage_event.js';
import {BodyUsageJournal} from '../src/runtime/body_usage_journal.js';
import {PeriodSettlement,settlementMonth} from '../src/finops/period_settlement.js';
import {AdmissionReservationStore} from '../src/control/admission_reservations.js';
import type {DirectoryStorage} from '../src/control/tenant_directory.js';
import {createHmac,randomUUID} from 'node:crypto';
import {execFileSync,spawn} from 'node:child_process';
import {BodyDaemonServer} from '../src/runtime/body_daemon_server.js';
import {ContainerSandbox} from '../src/runtime/container_sandbox.js';
import {NodeDaemon} from '../src/node_daemon.js';
import {bodyServiceToken} from '../src/cloudflare/body_auth.js';
import {taskStorageId} from '../src/cloudflare/task_identity.js';
import {taskInputDigest} from '../src/cloudflare/execution_attempt.js';
import {lookupBodyObservationBinding} from '../src/runtime/body_observation_binding.js';
import {deliverBodyObservations} from '../src/runtime/body_observation_delivery.js';
import {signBodyEvidence,verifyBodyEvidence} from '../src/control/body_execution_evidence.js';
let root=process.env.ANSTS_EVIDENCE_NATIVE_URL!;const container=process.env.ANSTS_EVIDENCE_NATIVE_CONTAINER!;
assert.ok(new URL(root).hostname==='127.0.0.1');assert.match(container,/^ansts-body-evidence-[a-f0-9]{8}(?:-control-1)?$/);
const secret='f'.repeat(64),directoryKey='d'.repeat(64),coordinatorKey='e'.repeat(64),hostId='evidence-'+randomUUID().slice(0,8),token=bodyServiceToken(secret,hostId);
const cancelFixture=process.env.ANSTS_EVIDENCE_TERMINAL==='cancel';
const image='node@sha256:fe4f6d74e20370acb04ad889077d14d0f64a9c8ea6a42bc60cddf1e844699914',directory=mkdtempSync(join(tmpdir(),'ansts-real-evidence-'));
const actor={sub:'native-evidence-verifier',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},bodyHeaders={authorization:'Body '+token,'x-ansts-body-id':hostId};
async function rpc(path:string,body?:unknown,headers:Record<string,string>=bodyHeaders,origin=root,method=body===undefined?'GET':'POST'){const response=await fetch(origin+path,{method,headers:{'content-type':'application/json',...headers},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(20000),redirect:'error'});const data=await response.json();return {status:response.status,data};}
async function good(path:string,body?:unknown,headers=bodyHeaders){const result=await rpc(path,body,headers);if(result.status<200||result.status>=300)throw new Error('RPC '+path+' rejected: '+result.status+' '+(result.data.code??result.data.error??''));return result.data;}
const coord=(action:string,values:Record<string,unknown>={})=>rpc('/internal/v1/tenant-control/'+action,{actor,tenantId:'evidence',...values},{authorization:'Bearer '+(action.startsWith('admission-')?coordinatorKey:directoryKey)});
function gatewayHeaders(scopeTenant='evidence',scopeRegion='all',role='VIEWER'){
 const encode=(value:unknown)=>Buffer.from(JSON.stringify(value)).toString('base64url');
 const iat=Math.floor(Date.now()/1000);
 const claims={...actor,role,scopeTenant,scopeRegion,iat,exp:iat+300};
 const unsigned=encode({alg:'HS256',typ:'JWT'})+'.'+encode(claims);
 return {authorization:'Bearer '+unsigned+'.'+createHmac('sha256','a'.repeat(64)).update(unsigned).digest('base64url')};
}
let agentTurn=0;
let releaseFirstModelResponse:()=>void=()=>{};
const firstModelResponse=new Promise<void>(resolve=>{releaseFirstModelResponse=resolve;});
const providerServer=createServer(async(request,response)=>{for await(const _ of request){}assert.equal(request.headers.authorization,'Bearer fixture-key');agentTurn++;if(agentTurn===1)await firstModelResponse;response.writeHead(200,{'content-type':'application/json'});response.end(JSON.stringify({id:'fixture-response-'+agentTurn,model:'fixture-reported-model',usage:{prompt_tokens:2,completion_tokens:3,total_tokens:5},choices:[{message:{content:JSON.stringify(agentTurn===1?{tool:'test',argv:['node','-e',"console.log('native-agent-tool')"]}:{tool:'finish'})},finish_reason:'stop'}]}));});
await new Promise<void>(resolve=>providerServer.listen(0,'127.0.0.1',resolve));
const providerUrl='http://127.0.0.1:'+(providerServer.address() as {port:number}).port;
const agentModel=new DeepSeekModel('fixture-key','fixture-requested-model',async(url,init)=>{assert.equal(String(url),'https://api.deepseek.com/chat/completions');return fetch(providerUrl,init);});
let daemon=new BodyDaemonServer({usageMetering:true,nodeId:hostId,token,storageDir:directory,bodyWorkerUrl:root,host:'127.0.0.1',port:0,sandboxImage:image,resources:{cpu:1,memoryMb:512},agentModel});
try{
 const metadata={actor,input:{id:'eng',name:'Evidence fixture',status:'ACTIVE',expectedVersion:0,reason:'Isolated real Engine verification',idempotencyKey:'center',requestId:'center'}};
 await good('/internal/v1/directory/cost-center-mutate',metadata,{authorization:'Bearer '+directoryKey});
 await good('/internal/v1/directory/mutate',{actor,input:{id:'evidence',name:'Evidence tenant',costCenter:'eng',region:'dev',status:'ACTIVE',expectedVersion:0,reason:'Isolated verification',idempotencyKey:'tenant',requestId:'tenant'}},{authorization:'Bearer '+directoryKey});
 const fixtureHeaders={'x-fixture-key':'isolated-period-fixture-only'};
 const legacyStatus=await good('/internal/v1/tenant-control/finops-archive-status',{actor,tenantId:'evidence'},{authorization:'Bearer '+directoryKey});
 const legacyManifest=await good('/internal/v1/tenant-control/finops-archive-create',{actor,tenantId:'evidence',input:{expectedRevision:legacyStatus.sourceRevision,reason:'Before period schema fixture',idempotencyKey:'legacy-v1',requestId:'legacy-v1'}},{authorization:'Bearer '+directoryKey});assert.equal(legacyManifest.schemaVersion,1);assert.ok(!('tenant_period_closes' in legacyManifest.tableCounts));
 const legacyArtifact=await good('/internal/v1/tenant-control/finops-archive-read',{actor,tenantId:'evidence',archiveId:legacyManifest.id},{authorization:'Bearer '+directoryKey});
 const seeded=await good('/fixture/period/seed',{},fixtureHeaders);
 assert.equal((await coord('policy-write',{input:{expectedVersion:0,resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'Fixture-only budget',idempotencyKey:'policy',requestId:'policy'}})).status,200);
 let address=await daemon.start(),base='http://127.0.0.1:'+address.port;
 const priceTerminal=process.env.ANSTS_EVIDENCE_PRICE_TERMINAL==='true';
 const fixtureRate={schemaVersion:1,unit:'credit-subunit',unitVersion:1,rounding:'CEIL',scope:{region:null,poolId:null},resources:{cpuNanoCoreMilliseconds:{numerator:'1',denominator:'1000000000000'},memoryMiBMilliseconds:{numerator:'0',denominator:'1'},slotMilliseconds:{numerator:'0',denominator:'1'}},models:[{provider:'deepseek',requestedModel:'fixture-requested-model',reportedModel:'fixture-reported-model',input:{mode:'ALL',rate:{numerator:'1',denominator:'1'}},output:{numerator:'0',denominator:'1'}}]};
 async function activateFixtureRate(){
 await good('/internal/v1/directory/rate-create',{actor,input:{expectedVersion:0,definition:fixtureRate,reason:'Isolated fixture rate, never production policy',idempotencyKey:'native-rate',requestId:'native-rate'}},{authorization:'Bearer '+directoryKey});
 assert.equal((await coord('rate-activate',{input:{expectedVersion:0,rateCardVersion:1,reason:'Isolated activation',idempotencyKey:'native-activate',requestId:'native-activate'}})).status,200);
 }
 if(priceTerminal)await activateFixtureRate();
 const execution={kind:'container',image,argv:['node','-e',"console.log('real-evidence-execution');setTimeout(()=>process.exit(7),"+(cancelFixture?5000:1500)+")"],cpu:.25,memoryMb:128,timeoutMs:10000};
 await good('/v1/scheduler/register-node',{...new NodeDaemon(hostId,1,512,0).emitHeartbeat(),healthy:true,execution_slots:1,warm_pool_ready:0});
 await good('/v1/scheduler/submit',{tenant_id:'evidence',task_id:'container',prompt:'evidence work',timeout_seconds:30,resource_profile:{think_cpu_cores:.25,act_cpu_cores:.25,act_memory_mb:128,disk_mb:32},execution},{authorization:'Gateway '+bodyServiceToken(secret,'gateway:control')});
 await good('/v1/scheduler/schedule-tick',{});
 const taskId=taskStorageId('evidence','container'),task=(await good('/v1/scheduler/tasks')).tasks.find((task:{task_id:string})=>task.task_id===taskId);assert.equal(task.assigned_node_id,hostId);
 const lease={tenant_id:'evidence',task_id:taskId,node_id:hostId,attempt_id:taskId+':'+task.lease_fencing_token,fencing_token:task.lease_fencing_token,input_digest:taskInputDigest('evidence',taskId,'evidence work',execution)};
 const input={reservationId:'real-r1',taskId,attemptId:lease.attempt_id,fencing:lease.fencing_token,expectedPolicyVersion:1,resources:{concurrentAttempts:1,cpuMilliCores:250,memoryMiB:128,queuedTasks:0},estimatedCreditUnits:'10',expiresAt:Date.now()+240000,executionInputDigest:lease.input_digest};
 let prepared=await coord('admission-prepare',{input,idempotencyKey:'prepare'});assert.equal(prepared.status,200,prepared.data.code);let reservation=prepared.data.reservation;
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
 if(cancelFixture){
  const stopped=await rpc('/v1/sandboxes/stop',{...identity,fencingToken:lease.fencing_token},bodyHeaders,base);
  assert.equal(stopped.status,200);assert.equal(stopped.data.stopped,true);
 }
 const result=await executing;assert.equal(result.status,200);
 if(cancelFixture)assert.equal(result.data.executionOutcome,'CANCELLED');
 else{assert.equal(result.data.exitCode,7);assert.ok(result.data.logs.includes('real-evidence-execution'));}
 const final=await rpc('/v1/daemon/execution-evidence',identity,bodyHeaders,base);assert.equal(final.status,200);assert.equal(verifyBodyEvidence(final.data.proof,token).phase,'TERMINATED');
 await assert.rejects(()=>deliverBodyObservations(localRpc,async proof=>{await publish(proof);throw new Error('lost terminal response');},token),/lost terminal response/);
 const settled=await coord('admission-read',{reservationId:reservation.id});assert.equal(settled.status,200);assert.equal(settled.data.reservation.phase,'SETTLING');assert.equal(settled.data.reservation.actualCreditUnits,null);
 const nextInput={...input,reservationId:'real-r2',taskId:'next',attemptId:'next-attempt',estimatedCreditUnits:'91'};const over=await coord('admission-prepare',{input:nextInput,idempotencyKey:'budget-exceeded'});assert.equal(over.status,409);assert.equal(over.data.code,'BUDGET_QUOTA_EXCEEDED');
 const next=await coord('admission-prepare',{input:{...nextInput,estimatedCreditUnits:'1'},idempotencyKey:'resource-released'});assert.equal(next.status,200);
 assert.equal((await coord('admission-transition',{input:{reservationId:'real-r2',expectedVersion:1,generation:1,fencing:lease.fencing_token,to:'CANCELLED',reason:'Fixture complete'},idempotencyKey:'cancel-next'})).status,200);
 const finished=await good('/v1/body/finish?taskId='+encodeURIComponent(taskId),{...lease,expected_version:started.execution.version,result:result.data});await good('/v1/body/receipt?hostId='+hostId,{...lease,result:result.data});await good('/v1/scheduler/complete',{...lease,execution_outcome:cancelFixture?'CANCELLED':'FAILED'});await good('/v1/body/acknowledge?taskId='+encodeURIComponent(taskId),{...lease,expected_version:finished.execution.version});
 const archiveQuery='?taskId='+encodeURIComponent(taskId);
 const archiveAuth={authorization:'Bearer '+'e'.repeat(64)};
 const verifyBodyRestore=process.env.ANSTS_EVIDENCE_EXPIRE_BEFORE_START==='true';
 let archived:Awaited<ReturnType<typeof good>>;
 if(verifyBodyRestore){
  archived=await good('/fixture/body-backup/export'+archiveQuery,undefined,archiveAuth);
  assert.ok(archived.rows.some((row:{id:string})=>row.id==='execution'));
  assert.ok(archived.rows.some((row:{id:string})=>row.id.startsWith('grant:')));
  const restoredCell=await good('/fixture/body-backup/import'+archiveQuery,archived,archiveAuth);
  assert.equal(restoredCell.imported,true);
  const restoredArchive=await good('/fixture/body-backup/export'+archiveQuery+'&restored',undefined,archiveAuth);
  assert.deepEqual(restoredArchive,archived);
  const repeatRestore=await good('/fixture/body-backup/import'+archiveQuery,archived,archiveAuth);
  assert.equal(repeatRestore.already_initialized,true);
  console.log('PASS: independent Body Cell restore preserves execution and consumed grant state without overwriting it on replay.');
 }
 await daemon.stop();
 const restoredHost=mkdtempSync(join(tmpdir(),'ansts-body-restored-'));
 try{
  cpSync(directory,restoredHost,{recursive:true});
  const restored=new BodyDaemonServer({usageMetering:true,nodeId:hostId,token,storageDir:restoredHost,
   bodyWorkerUrl:root,host:'127.0.0.1',port:0,sandboxImage:image,resources:{cpu:1,memoryMb:512},agentModel});
  try{
   const restoredAddress=await restored.start(),restoredBase='http://127.0.0.1:'+restoredAddress.port;
   const recovered=await rpc('/v1/daemon/execution-evidence',identity,bodyHeaders,restoredBase);
   assert.equal(recovered.status,200);
   assert.equal(recovered.data.proof,final.data.proof);
   let pending;
   for(let retry=0;retry<50;retry++){
    pending=await rpc('/v1/daemon/execution-evidence/pending',{},bodyHeaders,restoredBase);
    assert.equal(pending.status,200);
    if(pending.data.observations.length)break;
    await new Promise(resolve=>setTimeout(resolve,100));
   }
   assert.deepEqual(pending?.data.observations.map((item:{proof:string})=>item.proof),[final.data.proof]);
   console.log('PASS: independent stopped Host copy retains the signed terminal proof and pending acknowledgement.');
  }finally{await restored.stop();}
 }finally{rmSync(restoredHost,{recursive:true,force:true});}
 daemon=new BodyDaemonServer({usageMetering:true,nodeId:hostId,token,storageDir:directory,bodyWorkerUrl:root,host:'127.0.0.1',port:0,sandboxImage:image,resources:{cpu:1,memoryMb:512},agentModel});address=await daemon.start();base='http://127.0.0.1:'+address.port;
 assert.equal((await rpc('/v1/daemon/execution-evidence',identity,bodyHeaders,base)).data.proof,final.data.proof);
 execFileSync('docker',['restart',container],{stdio:'ignore'});root='http://127.0.0.1:'+execFileSync('docker',['port',container,'8080'],{encoding:'utf8'}).trim().split(':').at(-1);for(let retry=0;retry<100;retry++){try{if((await rpc('/healthz')).status===200)break;}catch{}await new Promise(resolve=>setTimeout(resolve,100));}
 if(verifyBodyRestore){
 assert.deepEqual(await good('/fixture/body-backup/export'+archiveQuery+'&restored',undefined,archiveAuth),archived);
 const resultHeaders={authorization:'Gateway '+bodyServiceToken(secret,'gateway:control')};
 const query=archiveQuery+'&tenantId=evidence';
 const canonical=await good('/v1/body/result'+query,undefined,resultHeaders);
 assert.equal(canonical.phase,'COMPLETED');
 assert.deepEqual(await good('/fixture/body-restored-result'+query,undefined,resultHeaders),canonical);
 assert.equal((await rpc('/fixture/body-restored-result'+query,undefined,{})).status,401);
 assert.equal((await rpc('/fixture/body-restored-result'+archiveQuery+'&tenantId=other',undefined,resultHeaders)).status,404);
 const restoredInventory=await good('/fixture/body-backup/execution-inventory'+archiveQuery,undefined,archiveAuth);
 assert.equal(restoredInventory.entries.length,1);
 assert.equal(restoredInventory.entries[0].baseline,true);
 assert.equal(restoredInventory.entries[0].observation.state,'COMPLETED');
 assert.ok(restoredInventory.coverageSince>restoredInventory.entries[0].observation.sourceAt);
 console.log('PASS: restored Body Cell execution and grant state survive celld restart.');
 }
 for(let retry=0;retry<50;retry++){const pending=await rpc('/v1/daemon/execution-evidence/pending',{},bodyHeaders,base);if(pending.data.observations.length)break;await new Promise(resolve=>setTimeout(resolve,100));}
 assert.equal(await deliverBodyObservations(localRpc,publish,token),1);
 assert.equal((await rpc('/v1/daemon/execution-evidence/pending',{},bodyHeaders,base)).data.observations.length,0);
 const replay=await coord('admission-observe-execution',{reservationId:reservation.id,proof:final.data.proof});assert.equal(replay.status,200);assert.deepEqual(replay.data.reservation,settled.data.reservation);
 const legacyIdentity=await runLegacyUsageFixture(root,directory,image,secret,hostId);
 if(!priceTerminal)await activateFixtureRate();
 const legacyHost=legacyIdentity.producerId;
 const legacyHeaders={authorization:'Body '+bodyServiceToken(secret,legacyHost),'x-ansts-body-id':legacyHost};
 await good('/v1/scheduler/register-node',{...new NodeDaemon(legacyHost,1,512,0).emitHeartbeat(),healthy:false,execution_slots:1,warm_pool_ready:0},legacyHeaders);
 const fixtureConfig=process.env.ANSTS_EVIDENCE_NATIVE_CONFIG;
 assert.ok(fixtureConfig,'isolated deployment config is required');
 const pilotConfig=JSON.parse(readFileSync(fixtureConfig,'utf8'));
 assert.equal(pilotConfig.vars.ANSTS_ADMISSION_MODE,'shadow');
 assert.equal(pilotConfig.vars.ANSTS_ADMISSION_PILOT_TENANTS,'evidence');
 pilotConfig.vars.ANSTS_ADMISSION_MODE='enforce';
 const composeUpgrade=process.env.ANSTS_VERIFY_COMPOSE_ACCEPTANCE==='1';
 let upgradedCatalog:unknown,upgradedDraft:unknown;
 const gatewayState=async()=>good('/v1beta1/gateway/state',undefined,gatewayHeaders('all','all','SRE_ADMIN'));
 const originalGateway=composeUpgrade?await gatewayState():null;

 const configAuth={authorization:'Bearer '+'9'.repeat(64)};
 const configPath='/internal/v1/config-versions/global/';
 if(composeUpgrade){
  pilotConfig.durable_objects.bindings.push({name:'ANSTS_REGION_CATALOG_CELL',class_name:'RegionCatalogCell'});
  pilotConfig.migrations.push({tag:'v2-region-catalog',new_sqlite_classes:['RegionCatalogCell']});
  pilotConfig.durable_objects.bindings.push({name:'ANSTS_CONFIG_RELEASE_CELL',class_name:'ConfigReleaseCell'});
  pilotConfig.migrations.push({tag:'v3-config-release',new_sqlite_classes:['ConfigReleaseCell']});
  pilotConfig.vars.ANSTS_CONFIG_SERVICE_TOKEN='9'.repeat(64);

 }

 pilotConfig.vars.ANSTS_ADMISSION_SCHEDULER_SHARDS='global-shard-0';
 writeFileSync(fixtureConfig,JSON.stringify(pilotConfig));
 execFileSync('docker',['restart',container],{stdio:'ignore'});
 root='http://127.0.0.1:'+execFileSync('docker',['port',container,'8080'],{encoding:'utf8'}).trim().split(':').at(-1);
 for(let retry=0;retry<100;retry++){try{if((await rpc('/healthz')).status===200)break;}catch{}await new Promise(resolve=>setTimeout(resolve,100));}
 if(composeUpgrade){
  const recovered=await coord('admission-read',{reservationId:reservation.id});
  assert.equal(recovered.status,200);
  assert.deepEqual(recovered.data.reservation,settled.data.reservation);
  const headers=gatewayHeaders('all','all','SRE_ADMIN');
  const catalog=await good('/v1/admin/regions',undefined,headers);
  assert.deepEqual(catalog.regions,[]);
  const created=await rpc('/v1/admin/regions',{expectedVersion:0,input:{id:'compose-dev',jurisdiction:null,failureDomain:null,
   identityRef:null,adapterRef:null,models:[],dataTypes:[],environment:'development',evidence:null}},headers,root,'PUT');
  assert.equal(created.status,200,JSON.stringify(created.data));
  assert.equal(created.data.region.trust,'DEVELOPMENT_ONLY');
  upgradedCatalog=await good('/v1/admin/regions',undefined,headers);
  assert.deepEqual(upgradedCatalog,{regions:[created.data.region]});
  const draft=await rpc(configPath+'create',{actor,schemaVersion:1,baseRevision:0,content:{defaultTaskTimeoutSeconds:90}},configAuth);
  assert.equal(draft.status,201,JSON.stringify(draft.data));
  upgradedDraft=draft.data;
  execFileSync('docker',['restart',container],{stdio:'ignore'});
  root='http://127.0.0.1:'+execFileSync('docker',['port',container,'8080'],{encoding:'utf8'}).trim().split(':').at(-1);
  for(let retry=0;retry<100;retry++){
   try{if((await rpc('/healthz')).status===200)break;}catch{}
   await new Promise(resolve=>setTimeout(resolve,100));
  }
  assert.deepEqual(await good(configPath+'read',{actor},configAuth),upgradedDraft);
  console.log('PASS: upgraded config release binding preserves its draft across restart.');


  console.log('PASS: isolated Compose binding upgrade adds region catalog and retains the old tenant reservation.');
 }
 const gateProbe=await rpc('/v1/scheduler/submit',{tenant_id:'evidence',task_id:'missing-reservation',prompt:'Gate probe'},
  {authorization:'Gateway '+bodyServiceToken(secret,'gateway:control')});
 assert.equal(gateProbe.data.code,'ADMISSION_RESERVATION_REQUIRED');
 // Exercise the actual production BodyAgent script: it discovers and passes the binding,
 // executes the Agent loop, delivers observations and acknowledges them without a fixture relay.
 await daemon.stop();daemon=new BodyDaemonServer({usageMetering:true,nodeId:hostId,token,storageDir:directory,bodyWorkerUrl:root,host:'127.0.0.1',port:0,sandboxImage:image,resources:{cpu:1,memoryMb:512},agentModel});address=await daemon.start();base='http://127.0.0.1:'+address.port;
 await good('/v1/scheduler/register-node',{...new NodeDaemon(hostId,1,512,0).emitHeartbeat(),healthy:true,execution_slots:1,warm_pool_ready:0});
 const agentPublicId='agent-'+ 'x'.repeat(120),agentTaskId=taskStorageId('evidence',agentPublicId),agentExecution={kind:'agent',image,cpu:.25,memoryMb:128,timeoutMs:30000,maxTurns:4,maxTokens:32768,files:{}};
 const capacityHeaders={...gatewayHeaders('all','all','SRE_ADMIN'),'idempotency-key':'agent-capacity-fixture'};
 const capacityLimit={cpuMilliCores:1000,memoryMiB:512,slots:1};
 const initial={status:'ACTIVE',limit:capacityLimit,expectedVersion:0,reason:'native agent capacity fixture'};
 await good('/v1/admin/capacity/pools',{...initial,id:'agent-pool',name:'Agent pool',region:'dev'},capacityHeaders);
 await good('/v1/admin/capacity/hosts',{...initial,id:'agent-host',poolId:'agent-pool',daemonIds:[hostId]},{...capacityHeaders,'idempotency-key':'agent-capacity-host'});
 const reportCapacity=async(sequence:number)=>{
  const proof=signCapacityReport({schemaVersion:1,purpose:'ansts-capacity-report-v1',hostId:'agent-host',daemonId:hostId,
   epoch:1,sequence,observedAt:Date.now(),total:capacityLimit,reserved:{cpuMilliCores:0,memoryMiB:0,slots:0},
   draining:false,recoveryRequired:false},token);
  await good('/internal/v1/capacity/report',{proof},bodyHeaders);
 };
 await reportCapacity(1);
 const capacityPath='/v1/admin/capacity/requests?poolId=agent-pool&tenantId=evidence';
 const capacityRequest=await good(capacityPath,{resources:{cpuMilliCores:250,memoryMiB:128,slots:1}},capacityHeaders);
 let capacityState='';
 for(let retry=0;retry<30&&capacityState!=='GRANTED';retry++){
  await new Promise(resolve=>setTimeout(resolve,200));
  const view=await good(capacityPath,undefined,capacityHeaders);
  capacityState=view.requests.find((item:{id:string})=>item.id===capacityRequest.id)?.state;
 }
 assert.equal(capacityState,'GRANTED');
 const expireBeforeStart=process.env.ANSTS_EVIDENCE_EXPIRE_BEFORE_START==='true';
 const revokeBeforeStart=process.env.ANSTS_EVIDENCE_REVOKE_BEFORE_START==='true';
 const agentInput={...input,reservationId:'agent-r1',taskId:agentTaskId,attemptId:agentTaskId+':1',fencing:1,
  executionInputDigest:taskInputDigest('evidence',agentTaskId,'Run isolated local tool',agentExecution),expiresAt:Date.now()+(expireBeforeStart?8000:240000)};
 const agentPrepared=await coord('admission-prepare',{input:agentInput,idempotencyKey:'agent-prepare'});assert.equal(agentPrepared.status,200);let agentReservation=agentPrepared.data.reservation;
 for(const to of ['RESERVED','START_AUTHORIZED']){const changed=await coord('admission-transition',{input:{reservationId:agentReservation.id,expectedVersion:agentReservation.version,generation:agentReservation.generation,fencing:agentReservation.fencing,to,reason:'Isolated Agent observation binding',...(to==='RESERVED'?{target:hostId,capacityReceipt:'capacity-grant:'+capacityRequest.id}:{})},idempotencyKey:'agent-'+to});assert.equal(changed.status,200);agentReservation=changed.data.reservation;}
 await good('/v1/scheduler/submit',{tenant_id:'evidence',task_id:agentPublicId,capacity_reservation_id:agentReservation.id,prompt:'Run isolated local tool',timeout_seconds:60,resource_profile:{think_cpu_cores:.25,act_cpu_cores:.25,act_memory_mb:128,disk_mb:32},execution:agentExecution},{authorization:'Gateway '+bodyServiceToken(secret,'gateway:control')});
 await good('/v1/scheduler/schedule-tick',{});
 if(expireBeforeStart||revokeBeforeStart){
  const scheduled=(await good('/v1/scheduler/tasks')).tasks.find((item:{task_id:string})=>item.task_id===agentTaskId);
  assert.equal(scheduled.assigned_node_id,hostId);
  const lease={tenant_id:'evidence',task_id:agentTaskId,node_id:hostId,attempt_id:agentInput.attemptId,
   fencing_token:scheduled.lease_fencing_token,input_digest:agentInput.executionInputDigest};
  const query='?taskId='+encodeURIComponent(agentTaskId);
  const acquired=await good('/v1/body/acquire'+query,lease);
  const started=await good('/v1/body/start'+query,{...lease,expected_version:acquired.execution.version});
  const identity={tenantId:'evidence',taskId:agentTaskId,attemptId:agentInput.attemptId};
  const binding=await lookupBodyObservationBinding({...identity,fencing:lease.fencing_token,
   executionInputDigest:lease.input_digest,resources:agentInput.resources},hostId,async lookup=>good('/internal/v1/body-observation-binding',lookup));
  assert.ok(binding);
  if(revokeBeforeStart){
   const proof=signBodyEvidence({...binding,...identity,schemaVersion:1,purpose:'ansts-body-execution-observation-v1',
    hostId,fencing:lease.fencing_token,resources:agentInput.resources,sequence:1,phase:'NOT_STARTED',
    executionObserved:false,observedAt:Date.now()},token);
   const revoked=await coord('admission-observe-execution',{reservationId:agentReservation.id,proof});
   assert.equal(revoked.status,200,JSON.stringify(revoked.data));
   assert.equal(revoked.data.reservation.phase,'CANCELLED');
   assert.equal(revoked.data.reservation.generation,binding.generation+1);
   assert.ok(Date.now()<agentReservation.expiresAt);
  }else{
  await new Promise(resolve=>setTimeout(resolve,Math.max(0,agentReservation.expiresAt-Date.now())+100));
  }
  const result=await rpc('/v1/sandboxes/execute',{...identity,fencingToken:lease.fencing_token,promptText:'Run isolated local tool',
   execution:agentExecution,executionToken:started.execution_token,tenantAdmissionObservation:binding},bodyHeaders,base);
  assert.equal(result.status,409,JSON.stringify(result.data));
  assert.equal(result.data.error,'STALE_EXECUTION_LEASE');
  assert.equal(agentTurn,0);
  assert.equal((await rpc('/v1/daemon/execution-evidence',identity,bodyHeaders,base)).status,404);
  console.log('PASS: native Body rejects '+(revokeBeforeStart?'revoked':'expired')+' authorization before execution evidence or model calls.');
 }else{
 const allocationPath='/v1/admin/capacity/allocation-control?poolId=agent-pool';
 const allocationChange=(expectedVersion:number,enabled:boolean)=>rpc(allocationPath,
  {expectedVersion,enabled},capacityHeaders,root,'PUT');
 const pause=await allocationChange(0,false);
 assert.equal(pause.status,200,JSON.stringify(pause.data));
 const pending=await good(capacityPath,{resources:{cpuMilliCores:250,memoryMiB:128,slots:1}},
  {...capacityHeaders,'idempotency-key':'agent-capacity-pending'});
 const tokenFile=join(directory,'fixture.token');writeFileSync(tokenFile,token,{mode:0o600});
 const child=spawn(process.execPath,[join(process.cwd(),'scripts/body_cell_agent.mjs')],{cwd:process.cwd(),env:{...process.env,ANSTS_USAGE_METERING:'shadow',ANSTS_NODE_ID:hostId,ANSTS_STORAGE_DIR:directory,ANSTS_BODY_TOKEN_FILE:tokenFile,ANSTS_BODY_CELL_URL:root,ANSTS_BODY_CELL_URLS:root,ANSTS_SCHEDULER_URLS:root,ANSTS_SCHEDULER_URL:root,ANSTS_LISTEN_PORT:String(address.port),ANSTS_BODY_CPU_CORES:'1',ANSTS_BODY_MEMORY_MB:'512'},stdio:['ignore','pipe','pipe']});
 let output='';child.stdout.on('data',chunk=>{output=(output+String(chunk)).slice(-32768);});child.stderr.on('data',chunk=>{output=(output+String(chunk)).slice(-32768);});
 const exited=new Promise<void>(resolve=>child.once('exit',()=>resolve()));
 try {
  for(let retry=0;retry<200&&agentTurn===0;retry++)await new Promise(resolve=>setTimeout(resolve,50));
  assert.equal(agentTurn,1,'real Agent must reach its first model call');
  if(process.env.ANSTS_EVIDENCE_RESTART_WHILE_RUNNING==='true'){
   let beforeRestart=await coord('admission-read',{reservationId:agentReservation.id});
   for(let retry=0;retry<30&&beforeRestart.data.reservation?.phase!=='RUNNING';retry++){
    await new Promise(resolve=>setTimeout(resolve,100));
    beforeRestart=await coord('admission-read',{reservationId:agentReservation.id});
   }
   assert.equal(beforeRestart.data.reservation?.phase,'RUNNING',JSON.stringify(beforeRestart.data));
   execFileSync('docker',['kill','--signal','KILL',container],{stdio:'ignore'});
   execFileSync('docker',['start',container],{stdio:'ignore'});
   const recoveredRoot='http://127.0.0.1:'+execFileSync('docker',['port',container,'8080'],{encoding:'utf8'}).trim().split(':').at(-1);
   assert.equal(recoveredRoot,root,'running BodyAgent must keep the same control endpoint');
   for(let retry=0;retry<50;retry++){
    try{if((await rpc('/healthz')).status===200)break;}catch{}
    await new Promise(resolve=>setTimeout(resolve,100));
   }
   const recovered=await coord('admission-read',{reservationId:agentReservation.id});
   assert.equal(recovered.status,200);
   assert.deepEqual(recovered.data.reservation,beforeRestart.data.reservation);
   console.log('PASS: hard control-plane restart preserves the in-flight RUNNING reservation.');
  }
  const updateQuota=(expectedVersion:number,concurrentAttempts:number)=>coord('policy-write',{input:{expectedVersion,
   resources:{concurrentAttempts,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10},
   budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'Native quota cutover',
   idempotencyKey:'cutover-quota-'+expectedVersion,requestId:'cutover-quota-'+expectedVersion}});
  assert.equal((await updateQuota(1,0)).status,200);
  const newRate={...fixtureRate,models:fixtureRate.models.map(model=>({...model,input:{...model.input,rate:{numerator:'2',denominator:'1'}}}))};
  await good('/internal/v1/directory/rate-create',{actor,input:{expectedVersion:1,definition:newRate,
   reason:'Native rate cutover',idempotencyKey:'cutover-rate',requestId:'cutover-rate'}},{authorization:'Bearer '+directoryKey});
  const setRate=(expectedVersion:number,rateCardVersion:number)=>coord('rate-activate',{input:{expectedVersion,rateCardVersion,
   reason:'Native rate cutover',idempotencyKey:'cutover-activation-'+expectedVersion,requestId:'cutover-activation-'+expectedVersion}});
  assert.equal((await setRate(1,2)).status,200);
  const inFlight=await coord('admission-read',{reservationId:agentReservation.id});
  assert.equal(inFlight.data.reservation.policy.version,1);
  const liveQuota=await coord('policy-read');
  assert.equal(liveQuota.data.current.version,2);
  assert.equal(liveQuota.data.current.resources.concurrentAttempts,0);
  const denied=await coord('admission-prepare',{input:{...agentInput,reservationId:'cutover-new',
   attemptId:'cutover-new',expectedPolicyVersion:2,estimatedCreditUnits:'1'},idempotencyKey:'cutover-new'});
  assert.equal(denied.data.code,'RESOURCE_QUOTA_EXCEEDED');
  releaseFirstModelResponse();
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
  assert.equal((await updateQuota(2,1)).status,200);
  assert.equal((await setRate(2,1)).status,200);
 }finally{releaseFirstModelResponse();child.kill('SIGTERM');const timer=setTimeout(()=>child.kill('SIGKILL'),10000);try{await exited;}finally{clearTimeout(timer);}}
 const capacityAfterExit=await good(capacityPath,undefined,capacityHeaders);
 assert.equal(capacityAfterExit.requests.find((item:{id:string})=>item.id===capacityRequest.id)?.state,'RELEASED');
 assert.equal(capacityAfterExit.requests.find((item:{id:string})=>item.id===pending.id)?.state,'QUEUED');
 const pricedReservation=await coord('admission-cost-preview',{reservationId:agentReservation.id});
 assert.equal(pricedReservation.status,200);
 assert.equal(pricedReservation.data.phase,'SETTLING');
 assert.equal(pricedReservation.data.complete,true,JSON.stringify(pricedReservation.data.reasons));
 assert.ok(pricedReservation.data.buckets.some(bucket=>bucket.descriptor.dimension==='inputTokens'));
 const costing={reservationId:agentReservation.id,expectedVersion:pricedReservation.data.version,expectedRevision:pricedReservation.data.revision};
 const settleCost=()=>coord('admission-settle-cost',{input:costing,idempotencyKey:'settle-priced-agent'});
 const additional={...nextInput,expectedPolicyVersion:3,reservationId:'after-agent-settlement',attemptId:'after-agent-settlement',estimatedCreditUnits:'81'};
 assert.equal((await coord('admission-prepare',{input:additional,idempotencyKey:'before-agent-settlement'})).data.code,'BUDGET_QUOTA_EXCEEDED');
 assert.equal((await coord('admission-settle-cost',{input:{...costing,actualCreditUnits:'0'},idempotencyKey:'forged-cost'})).status,422);
 const posted=await Promise.all([settleCost(),settleCost()]);
 assert.equal(posted[0].status,200,posted[0].data.code);assert.deepEqual(posted[0],posted[1]);
 const settledAgent=posted[0].data.reservation;
 const charge=BigInt(settledAgent.actualCreditUnits);
 assert.equal(settledAgent.phase,'SETTLED');assert.ok(charge>0n&&charge<10n);
 assert.equal(settledAgent.budgetSettlement.revision,costing.expectedRevision);
 assert.equal(settledAgent.budgetSettlement.chargesByDay.reduce((sum,row)=>sum+BigInt(row.creditUnits),0n),charge);
 const seen=new Set<string>(), recomputed=new Map<string,[bigint,bigint]>();
 let ledgerCursor:string|undefined;
 do{
  const page=await coord('cost-read',{page:{limit:25,...(ledgerCursor?{cursor:ledgerCursor}:{})}});
  assert.equal(page.status,200);
  for(const {entry} of page.data.items){
   if(entry.source.attemptId!==agentReservation.attemptId)continue;
   assert.ok(!seen.has(entry.eventId));seen.add(entry.eventId);
   for(const line of entry.lines){
    assert.ok(line.exactCredit);
    assert.equal(entry.context.activation.rateCard.rounding,'CEIL');
    const key=JSON.stringify([entry.periodId,line.dimension]);
    const [n,d]=recomputed.get(key)??[0n,1n],x=BigInt(line.exactCredit.numerator),y=BigInt(line.exactCredit.denominator);
    recomputed.set(key,[n*y+x*d,d*y]);
   }
  }
  ledgerCursor=page.data.nextCursor;
 }while(ledgerCursor);
 assert.ok(seen.size>0&&recomputed.size>0);
 const ledgerCharge=[...recomputed.values()].reduce((sum,[n,d])=>sum+(n+d-1n)/d,0n);
 assert.equal(ledgerCharge,charge);
 console.log('PASS: independent ledger recomputation: '+seen.size+' unique events, '+recomputed.size+' buckets, '+charge+' credit subunits.');
 const exactAvailable=90n-charge;
 assert.equal((await coord('admission-prepare',{input:{...additional,estimatedCreditUnits:(exactAvailable+1n).toString()},idempotencyKey:'one-over-after-settlement'})).data.code,'BUDGET_QUOTA_EXCEEDED');
 const funded=await coord('admission-prepare',{input:{...additional,estimatedCreditUnits:exactAvailable.toString()},idempotencyKey:'after-agent-settlement'});
 assert.equal(funded.status,200,funded.data.code);
 assert.equal((await coord('admission-transition',{input:{reservationId:additional.reservationId,expectedVersion:1,generation:1,fencing:additional.fencing,to:'CANCELLED',reason:'Fixture complete'},idempotencyKey:'cancel-after-agent-settlement'})).status,200);
 const enforcedCosts=await coord('cost-summary');
 assert.equal(enforcedCosts.status,200);
 assert.ok(BigInt(enforcedCosts.data.knownSubtotalCreditUnits)>0n);
 assert.equal(JSON.parse(readFileSync(fixtureConfig,'utf8')).vars.ANSTS_ADMISSION_MODE,'enforce');
 pilotConfig.vars.ANSTS_ADMISSION_MODE='observe';
 pilotConfig.vars.ANSTS_ADMISSION_PILOT_TENANTS='';
 writeFileSync(fixtureConfig,JSON.stringify(pilotConfig));
 execFileSync('docker',['restart',container],{stdio:'ignore'});
 root='http://127.0.0.1:'+execFileSync('docker',['port',container,'8080'],{encoding:'utf8'}).trim().split(':').at(-1);
 let observedCosts:Awaited<ReturnType<typeof coord>>|undefined;
 for(let retry=0;retry<150;retry++){try{observedCosts=await coord('cost-summary');if(observedCosts.status===200)break;}catch{}await new Promise(resolve=>setTimeout(resolve,100));}
 assert.deepEqual(observedCosts,enforcedCosts);
 assert.deepEqual(await settleCost(),posted[0]);
 assert.deepEqual((await coord('admission-read',{reservationId:agentReservation.id})).data.reservation,settledAgent);
 console.log('PASS: leaving the enforced pilot preserved the settled cost summary and settlement receipt across restart.');
 const pricedIdentity=await runLegacyUsageFixture(root,directory,image,secret,hostId,true);
 const costEntries=[];let costCursor:string|null=null;
 do{const page=await coord('cost-read',{page:{limit:1,...(costCursor?{cursor:costCursor}:{})}});assert.equal(page.status,200);costCursor=page.data.nextCursor;assert.equal(page.data.summary.totalCreditUnits,null);for(const item of page.data.items)if(item.entry.source.taskId===pricedIdentity.taskId)costEntries.push(item.entry);}while(costCursor);
 assert.ok(costEntries.some(e=>e.source.kind==='RESOURCE_SEGMENT'));for(const entry of costEntries){assert.equal(entry.context.activation.rateCard.version,1);assert.equal(entry.context.costCenterVersion,1);assert.deepEqual(entry.pending,[]);assert.ok(entry.lines.every(line=>line.pending.length===0));}
 let terminalPosted;
 if(priceTerminal){
  const terminalCost=await coord('admission-cost-preview',{reservationId:reservation.id});
  assert.equal(terminalCost.status,200);
  assert.equal(terminalCost.data.complete,true,JSON.stringify(terminalCost.data.reasons));
  terminalPosted=await coord('admission-settle-cost',{input:{reservationId:reservation.id,expectedVersion:terminalCost.data.version,
   expectedRevision:terminalCost.data.revision},idempotencyKey:'settle-terminal-container'});
  assert.equal(terminalPosted.status,200,terminalPosted.data.code);
  assert.equal(terminalPosted.data.reservation.phase,'SETTLED');
  assert.ok(terminalPosted.data.reservation.budgetSettlement.chargesByDay.length>0);
 }
 const lateIdentity={producerId:hostId,tenantId:'evidence',taskId:'isolated-late-source',attemptId:'isolated-late-attempt',fencing:1,executionInputDigest:'b'.repeat(64)};
 const lateResources={cpuNanoCores:1,cpuMilliCores:1,memoryMiB:1,slots:1};
 assert.equal((await coord('admission-usage-bind',{identity:lateIdentity,resources:lateResources})).status,200);
 const lateJournal=new BodyUsageJournal(join(directory,'isolated-late'));
 const lateStart=settlementMonth(seeded.periodId).startAt+1000;
 lateJournal.begin(lateIdentity,lateResources,lateStart);lateJournal.finish(lateIdentity,lateStart+2);
 for(const event of [...lateJournal.events(lateIdentity),lateJournal.events(lateIdentity)[1]]){
  const received=await coord('admission-usage-ingest',{identity:lateIdentity,proof:signUsageEvent(event,token)});
  assert.equal(received.status,200,received.data.code);
 }
 const routed=await good('/fixture/period/reconcile',{},fixtureHeaders);assert.equal(routed.added,0);
 let savedExport:{path:string;csv:string}|undefined;
 if(process.env.ANSTS_VERIFY_COMPOSE_ACCEPTANCE==='1'){
  const path='/v1/admin/tenants/evidence/cost-exports';
  const headers={...gatewayHeaders('evidence','all','SRE_ADMIN'),'idempotency-key':'compose-cost-export'};
  const created=await rpc(path,{},headers);
  assert.equal(created.status,202);
  assert.equal(created.data.status,'PENDING');
  const job=path+'/'+created.data.id;
  let status=await good(job,undefined,headers);
  for(let retry=0;retry<20&&status.status==='PENDING';retry++){
   await new Promise(resolve=>setTimeout(resolve,100));
   status=await good(job,undefined,headers);
  }
  assert.equal(status.status,'READY','celld Gateway must finish export after returning the creation receipt');
  const file=await fetch(root+job+'/download',{headers,signal:AbortSignal.timeout(3000)});
  assert.equal(file.status,200);
  const csv=await file.text();
  assert.match(csv,/^"event_id","watermark"/);
  assert.ok(csv.split('\r\n').length>2,'the native export must contain data');
  savedExport={path:job+'/download',csv};
  console.log('PASS: native celld Gateway finishes background export and serves authorized CSV.');
 }
 const beforeCosts=await coord('cost-summary');assert.equal(beforeCosts.status,200);assert.ok(BigInt(beforeCosts.data.knownSubtotalCreditUnits)>0n);
 const archiveKeyFile=join(directory,'archive-verifier.key');writeFileSync(archiveKeyFile,directoryKey,{mode:0o600});
 const captured=[];
 for(const kind of ['DIRECTORY','TENANT']){
  const endpoint=kind==='DIRECTORY'?'/internal/v1/directory/':'/internal/v1/tenant-control/';
  const envelope=kind==='DIRECTORY'?{actor}:{actor,tenantId:'evidence'};
  const status=await good(endpoint+'finops-archive-status',envelope,{authorization:'Bearer '+directoryKey});
  const manifest=await good(endpoint+'finops-archive-create',{...envelope,input:{expectedRevision:status.sourceRevision,reason:'Independent native recovery',idempotencyKey:'native-backup',requestId:'native-backup'}},{authorization:'Bearer '+directoryKey});
  assert.equal(manifest.schemaVersion,kind==='TENANT'?2:1);if(kind==='TENANT'){assert.equal(manifest.tableCounts.tenant_period_closes,1);assert.ok(manifest.tableCounts.tenant_period_adjustments>0);}
  const artifact=await good(endpoint+'finops-archive-read',{...envelope,archiveId:manifest.id},{authorization:'Bearer '+directoryKey});captured.push({endpoint,envelope,artifact});
  if(kind==='TENANT'){
   const state=JSON.parse(artifact.snapshot.tables.find(table=>table.name==='tenant_admission_state').rows[0][1]);
   assert.deepEqual(state.reservations.find(row=>row.id===settledAgent.id),settledAgent);
   assert.ok(Object.keys(state.settledCostBuckets).length>0);
  }
  const artifactFile=join(directory,kind+'-backup.json'),databaseFile=join(directory,kind+'-offline.sqlite');writeFileSync(artifactFile,JSON.stringify(artifact),{mode:0o600});
  const output=execFileSync(process.execPath,[join(process.cwd(),'scripts/restore_finops_archive.mjs'),artifactFile,archiveKeyFile,databaseFile,kind,...(kind==='TENANT'?['evidence']:[])],{encoding:'utf8',stdio:['ignore','pipe','pipe']});
  const restored=JSON.parse(output);assert.equal(restored.sourceRevision,manifest.sourceRevision);assert.equal(restored.operationalResume,'DISABLED');if(kind==='TENANT')assert.deepEqual(restored.summary,beforeCosts.data);
  assert.throws(()=>execFileSync(process.execPath,[join(process.cwd(),'scripts/restore_finops_archive.mjs'),artifactFile,archiveKeyFile,databaseFile,kind,...(kind==='TENANT'?['evidence']:[])],{stdio:'pipe'}));
 }
 const legacyFile=join(directory,'legacy-v1.json');writeFileSync(legacyFile,JSON.stringify(legacyArtifact),{mode:0o600});
 const legacyRestored=JSON.parse(execFileSync(process.execPath,[join(process.cwd(),'scripts/restore_finops_archive.mjs'),legacyFile,archiveKeyFile,join(directory,'legacy-v1.sqlite'),'TENANT','evidence'],{encoding:'utf8',stdio:['ignore','pipe','pipe']}));assert.equal(legacyRestored.sourceRevision,legacyArtifact.manifest.sourceRevision);
 captured.push({endpoint:'/internal/v1/tenant-control/',envelope:{actor,tenantId:'evidence'},artifact:legacyArtifact});
 const unified=await good('/internal/v1/directory/audit',{actor,page:{domain:'all'}},{authorization:'Bearer '+directoryKey});assert.ok(unified.items.some(item=>item.domain==='rate-card'));
 const activationAudit=await coord('rate-activation-audit');
 assert.equal(activationAudit.status,200);
 assert.equal(activationAudit.data.items[0].domain,'rate-activation');

 const inventoryRead=(source:string,taskId?:string)=>good('/internal/v1/execution-inventory',{actor,tenantId:'evidence',source,...taskId?{taskId}:{}},{authorization:'Bearer '+directoryKey});
 const sourceBefore=await inventoryRead('SCHEDULER'),bodyBefore=await inventoryRead('BODY',taskId);
 assert.equal(sourceBefore.complete,true);assert.ok(sourceBefore.entries.some(entry=>entry.observation.taskId===taskId));assert.equal(bodyBefore.complete,true);assert.ok(bodyBefore.entries.some(entry=>entry.observation.state==='RUNNING'));
 const coverageRead=await good('/internal/v1/settlement-coverage',{actor,tenantId:'evidence',periodId:new Date().toISOString().slice(0,7)},{authorization:'Bearer '+directoryKey});
 assert.equal(coverageRead.coverage.complete,false);assert.ok(coverageRead.reasons.includes('HISTORICAL_COVERAGE_UNAVAILABLE'));assert.equal(coverageRead.final,false);
 assert.equal((await rpc('/v1/scheduler/execution-inventory?tenantId=evidence',undefined,{'x-ansts-verified-inventory':'true',...bodyHeaders})).status,403);
 execFileSync('docker',['restart',container],{stdio:'ignore'});root='http://127.0.0.1:'+execFileSync('docker',['port',container,'8080'],{encoding:'utf8'}).trim().split(':').at(-1);let costRecovered=false;
 for(let retry=0;retry<150;retry++){let value;try{value=await coord('cost-summary');}catch{}if(value?.status===200){assert.deepEqual(value.data,beforeCosts.data);costRecovered=true;break;}await new Promise(resolve=>setTimeout(resolve,100));}
 assert.ok(costRecovered,'actual native Fleet restart must preserve immutable rates, pins and cost entries');
 const recoveredAudit=await coord('rate-activation-audit');
 assert.equal(recoveredAudit.status,200);
 assert.deepEqual(recoveredAudit.data.items,activationAudit.data.items);
 assert.equal(recoveredAudit.data.nextCursor,activationAudit.data.nextCursor);
 console.log('PASS: rate-activation audit rows are unchanged across the control restart.');

 if(savedExport){
  const download=await fetch(root+savedExport.path,{headers:gatewayHeaders('evidence','all','SRE_ADMIN')});
  assert.equal(download.status,200);
  assert.equal(await download.text(),savedExport.csv);
  assert.equal((await fetch(root+savedExport.path,{headers:gatewayHeaders('other','all','SRE_ADMIN')})).status,404);
  console.log('PASS: native export bytes survive Compose restart and remain tenant scoped.');
 }

 if(composeUpgrade){
  assert.deepEqual(await gatewayState(),originalGateway);
  console.log('PASS: production Gateway binding retains its state across the isolated upgrade.');
  assert.deepEqual(await good('/v1/admin/regions',undefined,gatewayHeaders('all','all','SRE_ADMIN')),upgradedCatalog);
  assert.deepEqual(await good(configPath+'read',{actor},configAuth),upgradedDraft);
  console.log('PASS: upgraded config release binding preserves its draft across restart.');
  console.log('PASS: upgraded region catalog survives the same restart as the immutable cost ledger.');
 }

 assert.deepEqual(await settleCost(),posted[0]);

 assert.deepEqual((await coord('admission-read',{reservationId:agentReservation.id})).data.reservation,settledAgent);
 await reportCapacity(2);
 const resume=await allocationChange(1,true);
 assert.equal(resume.status,200,JSON.stringify(resume.data));
 const recoveredCapacity=await good(capacityPath,undefined,capacityHeaders);
 assert.equal(recoveredCapacity.requests.find((item:{id:string})=>item.id===capacityRequest.id)?.state,'RELEASED');
 let pendingState='';
 for(let retry=0;retry<40&&pendingState!=='GRANTED';retry++){
  const view=await good(capacityPath,undefined,capacityHeaders);
  assert.equal(view.requests.find((item:{id:string})=>item.id===capacityRequest.id)?.state,'RELEASED');
  pendingState=view.requests.find((item:{id:string})=>item.id===pending.id)?.state;
  if(pendingState!=='GRANTED')await new Promise(resolve=>setTimeout(resolve,250));
 }
 assert.equal(pendingState,'GRANTED');
 console.log('PASS: real reservation-bound BodyAgent costs atomically replace the estimate, return the same receipt after Fleet restart and survive offline archive restore.');
 if(priceTerminal){
  const recovered=await coord('admission-read',{reservationId:reservation.id});
  assert.deepEqual(recovered.data.reservation,terminalPosted.data.reservation);
  console.log('PASS: priced '+(cancelFixture?'cancelled':'failed')+' execution passed trusted settlement and survived Fleet restart.');
 }else{
 const costPreview=await coord('admission-cost-preview',{reservationId:reservation.id,actualCreditUnits:'0'});
 assert.equal(costPreview.status,200);
 assert.equal(costPreview.data.phase,'SETTLING');
 assert.equal(costPreview.data.complete,false);
 assert.ok(costPreview.data.reasons.includes('PRICE_SNAPSHOT_MISSING'));
 assert.equal(costPreview.data.revision,null);
 assert.equal((await rpc('/internal/v1/tenant-control/admission-cost-preview',{actor,tenantId:'evidence',reservationId:reservation.id},{authorization:'Bearer '+directoryKey})).status,401);
 const heldBudget=await coord('admission-prepare',{input:{...nextInput,expectedPolicyVersion:3,reservationId:'after-cost-preview',attemptId:'after-cost-preview-attempt'},idempotencyKey:'budget-exceeded-after-cost-preview'});
 assert.equal(heldBudget.status,409);assert.equal(heldBudget.data.code,'BUDGET_QUOTA_EXCEEDED');
 console.log('PASS: native reservation cost preview keeps unpriced terminal usage pending and does not release the budget.');
 }
 const sourceAfter=await inventoryRead('SCHEDULER'),bodyAfter=await inventoryRead('BODY',taskId);
 assert.deepEqual(sourceAfter.entries,sourceBefore.entries);assert.equal(sourceAfter.watermark,sourceBefore.watermark);assert.deepEqual(bodyAfter.entries,bodyBefore.entries);assert.equal(bodyAfter.watermark,bodyBefore.watermark);
 for(const {endpoint,envelope,artifact} of captured)assert.deepEqual(await good(endpoint+'finops-archive-read',{...envelope,archiveId:artifact.manifest.id},{authorization:'Bearer '+directoryKey}),artifact);

 const now=new Date();
 const openPeriod=new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth()+1,1)).toISOString().slice(0,7);
 const previewPath='/v1/admin/tenants/evidence/settlements/'+openPeriod+'/preview';
 const fixtureState=()=>good('/fixture/period/state',{},fixtureHeaders);
 const preview=(path=previewPath)=>good(path,undefined,gatewayHeaders());
 const inventoryFixture=(action:string)=>good('/fixture/inventory/'+action,{},fixtureHeaders);
 const beforePreview=await fixtureState();
 assert.ok(beforePreview.admission.length&&beforePreview.closes.length&&beforePreview.adjustments.length);
 assert.equal((await rpc(previewPath,undefined,{})).status,401,'anonymous preview');
 assert.equal((await rpc(previewPath,undefined,gatewayHeaders('other'))).status,404,'cross-tenant preview');
 const regional=await rpc(previewPath,undefined,gatewayHeaders('evidence','dev'));
 assert.equal(regional.status,403);assert.equal(regional.data.code,'COMPLETE_TENANT_SCOPE_REQUIRED');
 assert.equal((await rpc(previewPath,{coverage:{complete:true}},gatewayHeaders('all','all','SRE_ADMIN'))).status,404,'admin cannot POST coverage');
 const closed=await rpc(previewPath.replace(openPeriod,seeded.periodId),undefined,gatewayHeaders());
 assert.equal(closed.status,409);assert.equal(closed.data.code,'PERIOD_ALREADY_CLOSED');
 await inventoryFixture('page');
 const firstPage=await good('/internal/v1/execution-inventory',{actor,tenantId:'evidence',source:'SCHEDULER',after:0},{authorization:'Bearer '+directoryKey});
 assert.equal(firstPage.nextAfter,512);
 const first=await preview();
 assert.equal(first.totalCreditUnits,null);assert.equal(first.complete,false);assert.equal(first.final,false);
 const trusted=await good('/internal/v1/settlement-coverage',{actor,tenantId:'evidence',periodId:openPeriod},{authorization:'Bearer '+directoryKey});
 assert.ok(trusted.sources.some(source=>source.kind==='BODY'&&source.taskId===taskId&&source.watermark===bodyAfter.watermark));
 assert.equal(first.coverage.inventoryWatermark,trusted.coverage.inventoryWatermark);
 assert.ok(first.coverageDiagnostics.includes('INVENTORY_WATERMARK_BEFORE_PERIOD_END'));
 const oldPeriod=new Date(Date.UTC(now.getUTCFullYear(),now.getUTCMonth()-2,1)).toISOString().slice(0,7);
 const historical=await preview(previewPath.replace(openPeriod,oldPeriod));
 assert.ok(historical.coverageDiagnostics.includes('HISTORICAL_COVERAGE_UNAVAILABLE'));
 assert.equal(historical.totalCreditUnits,null);
 assert.ok(!('basis' in first)&&!('attempts' in first.coverage));
 const override=await preview(previewPath+'?coverage='+encodeURIComponent(JSON.stringify({complete:true,attempts:[]})));
 assert.equal(override.revision,first.revision);assert.equal(override.totalCreditUnits,null);
 await inventoryFixture('advance');
 const changed=await preview();
 assert.ok(changed.coverageDiagnostics.includes('SCHEDULER_INVENTORY_CHANGED'));
 assert.equal(changed.totalCreditUnits,null);assert.notEqual(changed.revision,first.revision);
 await inventoryFixture('recheck');
 const rechecked=await preview();
 assert.ok(rechecked.coverageDiagnostics.includes('SCHEDULER_INVENTORY_CHANGED'));
 assert.equal(rechecked.totalCreditUnits,null);
 assert.notEqual(rechecked.coverage.inventoryWatermark,changed.coverage.inventoryWatermark);
 const beforeScan=await preview();
 await good('/fixture/period/scan-limit',{},fixtureHeaders);
 try{
  const scan=await rpc(previewPath,undefined,gatewayHeaders());
  assert.equal(scan.status,503);
  assert.deepEqual(scan.data,{code:'SETTLEMENT_SCAN_LIMIT'});
 }finally{
  await good('/fixture/period/clear-scan',{},fixtureHeaders);
 }
 assert.equal((await preview()).revision,beforeScan.revision);
 console.log('PASS: native Gateway preview authorization, multi-page Scheduler and Body coverage, revision stability, watermark changes, Body discovery and settlement scan limits, with unchanged admission and period records.');

 await good('/internal/v1/directory/mutate',{actor,input:{id:'close-probe',name:'Isolated close probe',costCenter:'eng',
  region:'dev',status:'ACTIVE',expectedVersion:0,reason:'Verify Gateway close',idempotencyKey:'close-probe',requestId:'close-probe'}},{authorization:'Bearer '+directoryKey});
 const closeBase='/v1/admin/tenants/close-probe/settlements/'+seeded.periodId;
 const closeHeaders={...gatewayHeaders('close-probe','all','SRE_ADMIN'),'idempotency-key':'native-period-close'};
 const incomplete=await good(closeBase+'/preview',undefined,closeHeaders);
 const closeInput={expectedRevision:incomplete.revision,reason:'Isolated month close'};
 assert.equal((await rpc(closeBase+'/close',closeInput,{})).status,401);
 assert.equal((await rpc(closeBase+'/close',closeInput,gatewayHeaders('close-probe'))).status,403);
 assert.equal((await rpc(closeBase+'/close',closeInput,gatewayHeaders('evidence','all','SRE_ADMIN'))).status,404);
 assert.equal((await rpc(closeBase+'/close',{...closeInput,coverage:{complete:true}},closeHeaders)).status,422);
 const refused=await rpc(closeBase+'/close',closeInput,closeHeaders);
 assert.equal(refused.status,409);assert.equal(refused.data.code,'PERIOD_INCOMPLETE');
 // Only the isolated empty tenant is used for a synthetic historical coverage cutover.
 await inventoryFixture('close-coverage');
 const currentPeriod=now.toISOString().slice(0,7);
 const liveCoverage=await good('/internal/v1/settlement-coverage',{actor,tenantId:'evidence',periodId:currentPeriod},{authorization:'Bearer '+directoryKey});
 assert.ok(liveCoverage.coverage.attempts.some(attempt=>attempt.taskId===taskId&&attempt.attemptId===lease.attempt_id));
 assert.ok(liveCoverage.coverage.attempts.some(attempt=>attempt.taskId===agentTaskId&&attempt.attemptId===agentInput.attemptId));
 if(liveCoverage.reasons.includes('BODY_ATTEMPT_SEQUENCE_UNPROVEN')){
  for(const id of [taskId,agentTaskId,legacyIdentity.taskId]){
   const journal=await inventoryRead('BODY',id);
   console.error('Body sequence diagnostic',id,JSON.stringify(journal.entries.map(({observation:o,baseline}:any)=>[o.state,o.version,o.sourceAt,baseline])));
  }
 }
 assert.deepEqual(liveCoverage.reasons,['INVENTORY_WATERMARK_BEFORE_PERIOD_END','PERIOD_NOT_ENDED']);
 const livePreview=await preview(previewPath.replace(openPeriod,currentPeriod));
 assert.deepEqual(livePreview.coverageDiagnostics,liveCoverage.reasons);
 assert.equal(livePreview.totalCreditUnits,null);
 console.log('PASS: native non-empty Scheduler and Body inventories prove the execution attempts born during the month.');
 await inventoryFixture('limit');
 const limited=await preview();
 assert.ok(limited.coverageDiagnostics.includes('BODY_DISCOVERY_LIMIT'));
 assert.equal(limited.totalCreditUnits,null);
 assert.deepEqual(await fixtureState(),beforePreview);
 const closeCoord=(action:string,values:object)=>coord(action,{tenantId:'close-probe',...values});
 const quotaInput={expectedVersion:0,resources:{concurrentAttempts:1,cpuMilliCores:10,memoryMiB:10,queuedTasks:1},
  budget:{period:'UTC_DAY',limitCreditUnits:'10',softThresholdBps:8000},reason:'Isolated close probe',requestId:'close-policy',idempotencyKey:'close-policy'};
 assert.equal((await closeCoord('policy-write',{input:quotaInput})).status,200);
 const preparedClose=await closeCoord('admission-prepare',{input:{reservationId:'close-uncertain',taskId:'close-task',
  attemptId:'close-attempt',fencing:1,expectedPolicyVersion:1,resources:{...quotaInput.resources,queuedTasks:0},
  estimatedCreditUnits:'1',expiresAt:Date.now()+60000},idempotencyKey:'prepare-close'});
 assert.equal(preparedClose.status,200);
 const transitionClose=(to:string,expectedVersion:number)=>closeCoord('admission-transition',{
  input:{reservationId:'close-uncertain',expectedVersion,generation:1,fencing:1,to,reason:'Isolated unstarted reservation'},
  idempotencyKey:'close-'+to});
 assert.equal((await transitionClose('UNCERTAIN',1)).status,200);
 await good('/fixture/period/backdate-uncertain',{periodId:seeded.periodId},fixtureHeaders);
 const blocked=await good(closeBase+'/preview',undefined,closeHeaders);
 assert.equal(blocked.coverage.complete,true);
 assert.deepEqual(blocked.reasons,['UNSETTLED_ADMISSION_RESERVATIONS']);
 const blockedClose=await rpc(closeBase+'/close',{...closeInput,expectedRevision:blocked.revision},closeHeaders);
 assert.equal(blockedClose.status,409);assert.equal(blockedClose.data.code,'PERIOD_INCOMPLETE');
 assert.equal((await transitionClose('CANCELLED',2)).status,200);
 const complete=await good(closeBase+'/preview',undefined,closeHeaders);
 assert.equal(complete.complete,true);
 const stale=await rpc(closeBase+'/close',closeInput,closeHeaders);
 assert.equal(stale.status,409);assert.equal(stale.data.code,'VERSION_CONFLICT');
 const closing={...closeInput,expectedRevision:complete.revision};
 const closedResults=await Promise.all([rpc(closeBase+'/close',closing,closeHeaders),rpc(closeBase+'/close',closing,closeHeaders)]);
 assert.equal(closedResults[0].status,200,closedResults[0].data.code);assert.deepEqual(closedResults[0],closedResults[1]);
 assert.equal(closedResults[0].data.final,true);assert.equal(closedResults[0].data.totalCreditUnits,'0');
 assert.ok(!('basis' in closedResults[0].data)&&!('streamIds' in closedResults[0].data));
 const reused=await rpc(closeBase+'/close',{...closing,reason:'Different close reason'},closeHeaders);
 assert.equal(reused.status,409);assert.equal(reused.data.code,'IDEMPOTENCY_CONFLICT');
 const correction={creditUnits:'-2',reason:'Isolated credit',expectedCloseDigest:closedResults[0].data.digest};
 const correctionHeaders={...closeHeaders,'idempotency-key':'native-credit'};
 const corrections=await Promise.all([rpc(closeBase+'/adjustments',correction,correctionHeaders),rpc(closeBase+'/adjustments',correction,correctionHeaders)]);
 assert.equal(corrections[0].status,200,corrections[0].data.code);
 assert.deepEqual(corrections[0],corrections[1]);
 const correctionPreview=await good(closeBase.replace(seeded.periodId,corrections[0].data.targetPeriodId)+'/preview',undefined,closeHeaders);
 assert.equal(correctionPreview.manualAdjustmentCreditUnits,'-2');
 assert.deepEqual(await rpc(closeBase+'/close',closing,closeHeaders),closedResults[0]);
 console.log('PASS: authorized Gateway close checks trusted coverage and revision, rejects override and reused keys, and commits one immutable receipt for concurrent requests.');

 const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');
 const usageFile=join(directory,'usage-receiver.sqlite');let usageDb=new DatabaseSync(usageFile);
 const usageStorage:DirectoryStorage={sql:{exec<T>(query:string,...values:unknown[]){const rows=usageDb.prepare(query).all(...values) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){usageDb.exec('BEGIN IMMEDIATE');try{const result=fn();usageDb.exec('COMMIT');return result;}catch(error){usageDb.exec('ROLLBACK');throw error;}}};
 let usageSink=new UsageInbox(usageStorage);const usageEvents:UsageEvent[]=[],usageIdentities=new Map<string,UsageIdentity>();
 try{
  let usageCursor:string|null=null;
  do{
   const page=await coord('usage-read',{page:{limit:1,...(usageCursor?{cursor:usageCursor}:{})}});assert.equal(page.status,200);assert.equal(page.data.pricing,'NOT_CONNECTED');usageCursor=page.data.nextCursor;
   for(const item of page.data.events){const event=item.event as UsageEvent;assert.equal(event.tenantId,'evidence');
    if(event.taskId===lateIdentity.taskId){assert.equal(item.context.rateCardVersion,null);continue;}
    assert.ok([taskId,agentTaskId,legacyIdentity.taskId,pricedIdentity.taskId].includes(event.taskId));
    const identity:UsageIdentity=event.taskId===pricedIdentity.taskId?pricedIdentity:event.taskId===legacyIdentity.taskId?legacyIdentity:event.taskId===taskId?{producerId:hostId,tenantId:'evidence',taskId,attemptId:lease.attempt_id,fencing:lease.fencing_token,executionInputDigest:lease.input_digest}:{producerId:hostId,tenantId:'evidence',taskId:agentTaskId,attemptId:agentInput.attemptId,fencing:agentInput.fencing,executionInputDigest:agentInput.executionInputDigest};
    usageIdentities.set(event.taskId,identity);assert.equal(item.context.tenantVersion,1);assert.equal(item.context.rateCardVersion,(event.taskId===pricedIdentity.taskId||event.taskId===agentTaskId||priceTerminal)?1:null);const receipt=usageSink.receive(event,identity);assert.equal(receipt.outcome,'RECORDED');usageEvents.push(event);
   }
  }while(usageCursor);
  assert.ok(usageEvents.some(event=>event.taskId===taskId&&event.kind==='RESOURCE_SEGMENT'));
  assert.ok(usageEvents.some(event=>event.taskId===taskId&&event.kind==='RESOURCE_CLOSE'));
  console.log('PASS: actual '+(cancelFixture?'cancelled':'failed')+' container retained resource usage through Fleet restart.');
  assert.equal(usageIdentities.size,4);assert.ok(usageEvents.some(e=>e.kind==='RESOURCE_SEGMENT'));
  for(const event of usageEvents.filter(e=>e.kind==='RESOURCE_SEGMENT')){if(event.taskId===legacyIdentity.taskId){assert.equal(event.allocation!.cpuNanoCoreMilliseconds,null);assert.equal(event.allocation!.memoryMiBMilliseconds,null);assert.ok(BigInt(event.allocation!.slotMilliseconds)>0n);}else if(event.taskId===pricedIdentity.taskId){assert.equal(BigInt(event.allocation!.cpuNanoCoreMilliseconds!),BigInt(event.allocation!.slotMilliseconds)*100000000n);assert.equal(BigInt(event.allocation!.memoryMiBMilliseconds!),BigInt(event.allocation!.slotMilliseconds)*64n);}else{assert.equal(BigInt(event.allocation!.cpuNanoCoreMilliseconds!),BigInt(event.allocation!.slotMilliseconds)*250000000n);assert.equal(BigInt(event.allocation!.memoryMiBMilliseconds!),BigInt(event.allocation!.slotMilliseconds)*128n);}}
  const providerEvents=usageEvents.filter(e=>e.kind==='PROVIDER_USAGE');assert.equal(providerEvents.length,2);assert.ok(providerEvents.every(e=>e.provider!.totalTokens==='5'&&e.provider!.inputTokens==='2'&&e.provider!.outputTokens==='3'&&e.provider!.completeness==='COMPLETE'&&e.provider!.reportedModel==='fixture-reported-model'));
  usageDb.close();usageDb=new DatabaseSync(usageFile);usageSink=new UsageInbox(usageStorage);
  for(const identity of usageIdentities.values())assert.equal(usageSink.inspect(identity)!.pending,identity.taskId===legacyIdentity.taskId);
  assert.equal(usageSink.receive(usageEvents[0],usageIdentities.get(usageEvents[0].taskId)!).outcome,'RECORDED');
  if(priceTerminal){
   usageDb.close();usageDb=new DatabaseSync(join(directory,'terminal-close.sqlite'));
   new UsageInbox(usageStorage);
   new AdmissionReservationStore(usageStorage);
   const periods=new PeriodSettlement(usageStorage);
   const {snapshot}=captured.find(item=>item.artifact.manifest.schemaVersion===2)!.artifact;
   const rows=(table:string)=>snapshot.tables.find(item=>item.name===table).rows;
   const identity=usageIdentities.get(taskId)!;
   const streamId=usageStreamId(identity),events=usageEvents.filter(event=>event.taskId===taskId);
   for(const table of ['tenant_usage_price_bindings','tenant_usage_streams']){
    const row=rows(table).find(item=>item[0]===streamId);
    assert.ok(row);
    usageStorage.sql.exec('INSERT INTO '+table+' (id, payload_json) VALUES (?, ?)',...row);
   }
   for(const [index,event] of events.entries()){
    usageStorage.sql.exec('INSERT INTO tenant_usage_events (id, event_json, digest) VALUES (?, ?, ?)',...rows('tenant_usage_events').find(item=>item[0]===event.eventId));
    usageStorage.sql.exec('INSERT INTO tenant_usage_cost_entries (id, payload_json, digest) VALUES (?, ?, ?)',...rows('tenant_usage_cost_entries').find(item=>item[0]===event.eventId));
    usageStorage.sql.exec('INSERT INTO tenant_usage_event_order (id, ordinal, tenant_id, region) VALUES (?, ?, ?, ?)',event.eventId,index+1,'evidence','dev');
   }
   const settled=terminalPosted.data.reservation;
   usageStorage.sql.exec('INSERT INTO tenant_admission_state (id, payload_json) VALUES (?, ?)',
    'admission',JSON.stringify({schemaVersion:1,tenantId:'evidence',reservations:[settled],receipts:{},audit:[]}));
   const month=new Date(events.find(event=>event.kind==='RESOURCE_SEGMENT')!.allocation!.startAt).toISOString().slice(0,7);
   const asOf=Math.max(settlementMonth(month).endAt,...events.map(event=>event.at))+1000;
   const coverage={schemaVersion:1 as const,purpose:'ansts-settlement-coverage-v1' as const,tenantId:'evidence',periodId:month,
    inventoryWatermark:bodyAfter.watermark,observedAt:asOf,complete:true,attempts:[identity]};
   const preview=periods.preview(actor as import('../src/control/contracts.js').ControlActor,snapshot.tenant,month,coverage,asOf);
   assert.deepEqual(preview.reasons,[]);
   const admin=actor as import('../src/control/contracts.js').ControlActor;
   const input={expectedRevision:preview.revision,reason:'Replay of native terminal evidence',requestId:'terminal',idempotencyKey:'terminal'};
   const statement=periods.close(admin,snapshot.tenant,month,input,coverage,asOf);
   assert.ok(BigInt(statement.totalCreditUnits)>0n);
   assert.ok(statement.entryDigests.some(entry=>events.some(event=>event.kind==='RESOURCE_SEGMENT'&&event.eventId===entry.eventId)));
   assert.deepEqual(periods.close(admin,snapshot.tenant,month,input,null,asOf),statement);
   console.log('PASS: non-empty month close preserved original priced terminal usage and replayed one immutable statement.');
  }
 }finally{usageDb.close();}
 console.log('PASS: native Scheduler and Body execution inventories retain exact journal bytes and watermarks after Fleet restart; private coverage diagnostics reject pre-cutover history and the public spoofed-header route is forbidden.');
 console.log('PASS: native v2 archive retains isolated closing/late-adjustment state; pre-upgrade v1 remains readable and independently restorable after v2 data and actual Fleet restart. Simulated period coverage is harness-only.');
 console.log('PASS: native directory/tenant archives exported complete allowlisted accounting authority, authenticated and restored into new offline databases with exact source revision and cost summary; existing destination rejected, unified template/activation audit and archived bytes survived real Fleet restart.');
 console.log('PASS: isolated directory rates, explicit activation, production forked Daemon/BodyAgent real priced container, automatic immutable cost projection and actual Fleet restart preserved rate/pin/entry bytes; prior unpriced history remains pending, no paid provider request.');
 console.log('PASS: real allocation segments and DeepSeek adapter HTTP-response token evidence persisted in native usage outboxes, automatically signed/ordered/acknowledged into the Fleet tenant authority and paged with frozen context, then replayed into an independent SQLite inbox and replayed after receiver reconstruction; original unpriced streams stay pending and no paid request. Real forked legacy runtime records slot time with unknown CPU/memory. No manual usage relay; lost remote receipt is retried by the production BodyAgent.');
 console.log('PASS: actual production BodyAgent automatically resolved a long-ID Agent binding, executed a local tool on real Docker Engine, delivered signed terminal evidence and acknowledged its durable outbox; model adapter was deterministic, no paid provider call.');
 console.log('PASS: real native Scheduler lease/Body grant/Daemon/Engine facts authenticated into tenant RUNNING/SETTLING; resource quota freed, unpriced budget retained, host/control restart, lost-response outbox retry and digest acknowledgement verified.');
 }
}finally{providerServer.closeAllConnections();await new Promise<void>(resolve=>providerServer.close(()=>resolve()));await daemon.stop();await new ContainerSandbox(image,'/var/run/docker.sock',hostId).recoverOrphans();rmSync(directory,{recursive:true,force:true});}
