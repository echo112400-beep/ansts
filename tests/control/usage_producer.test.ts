import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,readFileSync,mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {BodyUsageJournal} from '../../src/runtime/body_usage_journal.js';
import {DeepSeekModel,type ModelUsageResponse} from '../../src/runtime/deepseek_model.js';
import {AgentToolLoop} from '../../src/runtime/agent_tool_loop.js';
import {UsageInbox} from '../../src/finops/usage_inbox.js';
import {signUsageEvent,verifyUsageEvent,usageEventDigest,usageEventId,usageStreamId,type UsageIdentity} from '../../src/control/usage_event.js';
import type {DirectoryStorage} from '../../src/control/tenant_directory.js';
const identity:UsageIdentity={producerId:'body-1',tenantId:'tenant',taskId:'task',attemptId:'attempt',fencing:7,executionInputDigest:'a'.repeat(64)},resources={cpuNanoCores:250000000,cpuMilliCores:250,memoryMiB:128,slots:1};
function journal(){const dir=mkdtempSync(join(tmpdir(),'ansts-usage-'));return {dir,source:new BodyUsageJournal(dir),close(){rmSync(dir,{recursive:true,force:true});}};}
function inbox(file=':memory:'){
 const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite') as {DatabaseSync:new(path:string)=>{exec(q:string):void;prepare(q:string):{all(...args:unknown[]):unknown[]};close():void}};const db=new DatabaseSync(file);
 const storage:DirectoryStorage={sql:{exec<T>(query:string,...values:unknown[]){const rows=db.prepare(query).all(...values) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const value=fn();db.exec('COMMIT');return value;}catch(error){db.exec('ROLLBACK');throw error;}}};return {db,storage,sink:new UsageInbox(storage)};
}
const providerResponse={reportedModel:'actual-model',responseId:'response-1',inputTokens:'12',outputTokens:'8',totalTokens:'20',cacheHitTokens:'4',cacheMissTokens:'8',evidenceDigest:'d'.repeat(64),source:'PROVIDER_RESPONSE' as const,completeness:'COMPLETE' as const};
test('allocated resource quantities split at UTC midnight using exact integers; restart preserves sequence',()=>{
 const j=journal();try{const midnight=Date.UTC(2026,10,17);j.source.begin(identity,resources,midnight-10);j.source.checkpoint(identity,midnight+10);let source=new BodyUsageJournal(j.dir);source.finish(identity,midnight+20);
  const events=source.events(identity),segments=events.filter(e=>e.kind==='RESOURCE_SEGMENT');assert.deepEqual(events.map(e=>e.sequence),[1,2,3,4,5]);assert.deepEqual(segments.map(e=>e.allocation!.slotMilliseconds),['10','10','10']);assert.equal(segments.reduce((sum,e)=>sum+BigInt(e.allocation!.cpuNanoCoreMilliseconds!),0n),7500000000n);
  const proof=signUsageEvent(events[1],'b'.repeat(64));assert.deepEqual(verifyUsageEvent(proof,'b'.repeat(64)),events[1]);assert.throws(()=>verifyUsageEvent(proof,'c'.repeat(64)),{code:'USAGE_AUTH_REQUIRED'});
  source.finish(identity,midnight+30);assert.equal(source.events(identity).length,5);assert.throws(()=>source.begin({...identity,executionInputDigest:'f'.repeat(64)},resources,midnight),/USAGE_STREAM_CORRUPT/);
 }finally{j.close();}
});
test('immutable outbox replays in order, keeps acknowledged events and persists bounded retry/quarantine',()=>{
 const j=journal();try{j.source.begin(identity,resources,1000);j.source.finish(identity,1010);const events=j.source.events(identity);
  assert.equal(j.source.acknowledge(identity,2,usageEventDigest(events[1])),false);assert.deepEqual(j.source.pending(),[events[0]]);
  assert.equal(j.source.defer(identity,1,usageEventDigest(events[0]),false,2000),true);assert.deepEqual(new BodyUsageJournal(j.dir).pending(4,3000),[]);assert.deepEqual(j.source.pending(4,4000),[events[0]]);
  assert.equal(j.source.acknowledge(identity,1,usageEventDigest(events[0])),true);assert.deepEqual(j.source.pending(),[events[1]]);
  assert.equal(j.source.defer(identity,2,usageEventDigest(events[1]),true),true);assert.equal(j.source.acknowledge(identity,2,usageEventDigest(events[1])),false);assert.deepEqual(new BodyUsageJournal(j.dir).pending(4,Number.MAX_SAFE_INTEGER),[]);assert.equal(j.source.events(identity).length,3);
 }finally{j.close();}
});
test('provider intent survives process loss; resource recovery does not invent missing provider tokens',()=>{
 const j=journal();try{j.source.begin(identity,resources,1000);j.source.providerStarted(identity,1,{provider:'deepseek',model:'requested'},1001);assert.equal(j.source.stats().unresolvedProviderCalls,0);const source=new BodyUsageJournal(j.dir);source.recoverReleased(1010);assert.equal(source.stats().unresolvedProviderCalls,1);
  const events=source.events(identity);assert.equal(events.at(-1)!.kind,'RESOURCE_CLOSE');assert.equal(events.filter(e=>e.kind==='PROVIDER_USAGE').length,0);
  const request=events.find(e=>e.kind==='PROVIDER_REQUEST')!.provider!;assert.equal(request.inputTokens,null);assert.equal(request.totalTokens,null);assert.equal(request.completeness,'UNKNOWN');
 }finally{j.close();}
});
test('source model responses never store prompt text, response text or credentials; invalid actions keep actual tokens',async()=>{
 const response={id:'response-1',model:'actual-model',usage:{prompt_tokens:12,completion_tokens:8,total_tokens:20,prompt_cache_hit_tokens:4,prompt_cache_miss_tokens:8},choices:[{message:{content:'not-json-private-response'},finish_reason:'stop'}]};
 const model=new DeepSeekModel('private-test-key','requested',async()=>Response.json(response));let observed:ModelUsageResponse|undefined;
 await assert.rejects(()=>model.next([{role:'user',content:'private-prompt'}],100,new AbortController().signal,usage=>{observed=usage;}),/MODEL_ACTION_JSON_INVALID/);
 assert.equal(observed!.inputTokens,'12');assert.equal(observed!.totalTokens,'20');assert.equal(observed!.completeness,'COMPLETE');assert.equal(observed!.reportedModel,'actual-model');assert.ok(!JSON.stringify(observed).includes('private'));
 const missing=new DeepSeekModel('test-key','requested',async()=>Response.json({...response,usage:{total_tokens:20},choices:[{message:{content:'{"tool":"finish"}'},finish_reason:'stop'}]}));
 await missing.next([{role:'user',content:'work'}],100,new AbortController().signal,usage=>{observed=usage;});assert.equal(observed!.inputTokens,null);assert.equal(observed!.outputTokens,null);assert.equal(observed!.completeness,'MISSING');
});
test('agent model retry records a distinct request and never bills its conservative runtime budget as tokens',async()=>{
 const j=journal();try{j.source.begin(identity,resources);let count=0;const model={usageSource:{provider:'test-provider',model:'configured'},next:async()=>{if(++count===1)throw new Error('MODEL_TRANSPORT_FAILED');return {action:{tool:'finish'},tokens:123};}};
  const spec={kind:'agent' as const,image:'node@sha256:'+'a'.repeat(64),timeoutMs:10000,cpu:.25,memoryMb:128,maxTurns:3,maxTokens:32768,files:{}};
  const result=await new AgentToolLoop(join(j.dir,'turns'),model).run('identity',7,'work',spec,{execute:async()=>{throw new Error('no tool');}},new AbortController().signal,undefined,false,undefined,{started:(call,source)=>j.source.providerStarted(identity,call,source),response:(call,usage)=>j.source.providerFinished(identity,call,usage),uncertain:call=>j.source.providerUncertain(identity,call)});
  assert.equal(result.executionOutcome,'FAILED');assert.equal(count,2);const usages=j.source.events(identity).filter(e=>e.kind==='PROVIDER_USAGE');assert.equal(usages.length,2);assert.ok(usages.every(e=>e.provider!.totalTokens===null&&e.provider!.completeness==='UNKNOWN'));
 }finally{j.close();}
});
test('native receiver deduplicates immutable events and rejects gaps and window overlap',()=>{
 const j=journal(),s=inbox();try{j.source.begin(identity,resources,1000);j.source.finish(identity,1010);const events=j.source.events(identity);
  assert.throws(()=>s.sink.receive(events[1],identity),{code:'USAGE_SEQUENCE_GAP'});const receipt=s.sink.receive(events[0],identity);assert.deepEqual(s.sink.receive(events[0],identity),receipt);
  assert.throws(()=>s.sink.receive(events[1],{...identity,producerId:'other'}),{code:'USAGE_BINDING_FORBIDDEN'});
  s.sink.receive(events[1],identity);s.sink.receive(events[2],identity);assert.equal(s.sink.inspect(identity)!.pending,false);
 }finally{s.db.close();j.close();}
});
test('same event identity with different payload persists isolation and attention alert across native restart',()=>{
 const j=journal();let s=inbox(join(j.dir,'receiver.sqlite'));try{j.source.begin(identity,resources,1000);const event=j.source.events(identity)[0];s.sink.receive(event,identity);
  const changed={...event,at:1001};assert.equal(s.sink.receive(changed,identity).outcome,'CONFLICT');s.db.close();s=inbox(join(j.dir,'receiver.sqlite'));assert.equal(s.sink.inspect(identity)!.quarantined,true);assert.equal(s.sink.alerts()[0].requiresAttention,true);assert.equal(s.sink.alerts()[0].code,'USAGE_EVENT_CONFLICT');
  j.source.finish(identity,1010);assert.equal(s.sink.receive(j.source.events(identity)[1],identity).outcome,'QUARANTINED');
 }finally{s.db.close();j.close();}
});
test('usage event and stream watermark roll back together if durable commit fails',()=>{
 const j=journal(),s=inbox();try{j.source.begin(identity,resources,1000);const event=j.source.events(identity)[0],original=s.storage.sql.exec.bind(s.storage.sql);
  s.storage.sql.exec=(query,...values)=>{if(query.startsWith('INSERT OR REPLACE INTO tenant_usage_streams'))throw new Error('disk failure');return original(query,...values);};
  assert.throws(()=>s.sink.receive(event,identity),/disk failure/);s.storage.sql.exec=original;assert.equal(s.sink.inspect(identity),null);assert.equal(s.sink.receive(event,identity).outcome,'RECORDED');
 }finally{s.db.close();j.close();}
});
test('provider response evidence is immutable and complete response counts survive replay and closure',()=>{
 const j=journal(),s=inbox();try{j.source.begin(identity,resources,1000);j.source.providerStarted(identity,1,{provider:'deepseek',model:'requested'},1001);j.source.providerFinished(identity,1,providerResponse,1002);j.source.providerFinished(identity,1,providerResponse,1003);j.source.finish(identity,1010);
  for(const e of j.source.events(identity))s.sink.receive(e,identity);const projection=s.sink.inspect(identity)!;assert.equal(projection.pending,false);assert.equal(projection.calls['1'].response!.inputTokens,'12');assert.equal(j.source.events(identity).filter(e=>e.kind==='PROVIDER_USAGE').length,1);
  assert.ok(!readFileSync(join(j.dir,usageStreamId(identity)+'.json'),'utf8').includes('private-test-key'));
 }finally{s.db.close();j.close();}
});

test('source fsync-file failure cannot consume sequence or partially publish a resource close',()=>{
 const j=journal();try{j.source.begin(identity,resources,1000);const temporary=join(j.dir,usageStreamId(identity)+'.json.tmp');mkdirSync(temporary);
  assert.throws(()=>j.source.finish(identity,1020));assert.equal(new BodyUsageJournal(j.dir).events(identity).length,1);
  rmSync(temporary,{recursive:true});j.source.finish(identity,1030);const events=j.source.events(identity);assert.deepEqual(events.map(e=>e.sequence),[1,2,3]);assert.equal(events[1].allocation!.slotMilliseconds,'30');
 }finally{j.close();}
});

test('unconfirmed Engine stop leaves source allocation open until positive Host startup recovery',async()=>{
 const {createServer}=await import('node:http'),{BodyDaemonServer}=await import('../../src/runtime/body_daemon_server.js');
 const server=createServer(async(request,response)=>{for await(const _ of request){}response.writeHead(200,{'content-type':'application/json'});response.end(JSON.stringify(request.url==='/internal/v1/body-usage-binding'?{bound:true}:{lease_expires_at:Date.now()/1000+30}));});
 await new Promise<void>(resolve=>server.listen(0,'127.0.0.1',resolve));const j=journal(),key='b'.repeat(64),image='node@sha256:'+'a'.repeat(64);
 const options={usageMetering:true,nodeId:identity.producerId,token:key,storageDir:j.dir,bodyWorkerUrl:'http://127.0.0.1:'+(server.address() as {port:number}).port,host:'127.0.0.1',port:0,sandboxImage:image,containerRuntime:{recoverOrphans:async()=>0,execute:async()=>{throw new Error('BODY_STOP_UNCONFIRMED');}}};
 let daemon=new BodyDaemonServer(options);
 try{
  const {port}=await daemon.start(),execution={kind:'container',image,argv:['true'],cpu:.25,memoryMb:128,timeoutMs:10000};
  const response=await fetch('http://127.0.0.1:'+port+'/v1/sandboxes/execute',{method:'POST',headers:{authorization:'Body '+key,'x-ansts-body-id':identity.producerId,'content-type':'application/json'},body:JSON.stringify({tenantId:identity.tenantId,taskId:identity.taskId,attemptId:identity.attemptId,fencingToken:identity.fencing,promptText:'work',execution})});assert.equal(response.status,503);
  const {taskInputDigest}=await import('../../src/cloudflare/execution_attempt.js'),bound={...identity,executionInputDigest:taskInputDigest(identity.tenantId,identity.taskId,'work',execution)},source=new BodyUsageJournal(join(j.dir,'usage-outbox'));
  assert.ok(!source.events(bound).some(e=>e.kind==='RESOURCE_CLOSE'));
  await daemon.stop();daemon=new BodyDaemonServer(options);await daemon.start();assert.equal(source.events(bound).at(-1)!.kind,'RESOURCE_CLOSE');
 }finally{await daemon.stop();server.closeAllConnections();await new Promise<void>(resolve=>server.close(()=>resolve()));j.close();}
});

test('metered execution does not report success before resource closure is durable',async()=>{
 const {createServer}=await import('node:http'),{BodyDaemonServer}=await import('../../src/runtime/body_daemon_server.js');
 const server=createServer(async(request,response)=>{for await(const _ of request){}response.writeHead(200,{'content-type':'application/json'});response.end(JSON.stringify(request.url==='/internal/v1/body-usage-binding'?{bound:true}:{lease_expires_at:Date.now()/1000+30}));});
 await new Promise<void>(resolve=>server.listen(0,'127.0.0.1',resolve));const j=journal(),key='b'.repeat(64),image='node@sha256:'+'a'.repeat(64);
 const execution={kind:'container',image,argv:['true'],cpu:.25,memoryMb:128,timeoutMs:10000};
 const {taskInputDigest}=await import('../../src/cloudflare/execution_attempt.js');
 const bound={...identity,executionInputDigest:taskInputDigest(identity.tenantId,identity.taskId,'work',execution)};
 const temporary=join(j.dir,'usage-outbox',usageStreamId(bound)+'.json.tmp');
 let executions=0;
 const options={usageMetering:true,nodeId:identity.producerId,token:key,storageDir:j.dir,bodyWorkerUrl:'http://127.0.0.1:'+(server.address() as {port:number}).port,host:'127.0.0.1',port:0,sandboxImage:image,containerRuntime:{recoverOrphans:async()=>0,execute:async()=>{
  executions++;
  mkdirSync(temporary);
  return {executionOutcome:'SUCCEEDED' as const,exitCode:0,logs:'',logsTruncated:false,artifacts:[],artifactsTruncated:false};
 }}};
 let daemon=new BodyDaemonServer(options);
 try{
  const {port}=await daemon.start();
  const run=(port:number)=>fetch('http://127.0.0.1:'+port+'/v1/sandboxes/execute',{method:'POST',headers:{authorization:'Body '+key,'x-ansts-body-id':identity.producerId,'content-type':'application/json'},body:JSON.stringify({tenantId:identity.tenantId,taskId:identity.taskId,attemptId:identity.attemptId,fencingToken:identity.fencing,promptText:'work',execution})});
  const response=await run(port);assert.equal(response.status,503);
  const source=new BodyUsageJournal(join(j.dir,'usage-outbox'));
  assert.ok(!source.events(bound).some(e=>e.kind==='RESOURCE_CLOSE'));
  rmSync(temporary,{recursive:true});await daemon.stop();daemon=new BodyDaemonServer(options);
  const recovered=await daemon.start();assert.equal(source.events(bound).at(-1)!.kind,'RESOURCE_CLOSE');
  const replay=await run(recovered.port);assert.equal(replay.status,200);
  assert.equal((await replay.json()).executionOutcome,'SUCCEEDED');assert.equal(executions,1);
 }finally{await daemon.stop();server.closeAllConnections();await new Promise<void>(resolve=>server.close(()=>resolve()));j.close();}
});

test('sub-millicore Engine allocation is preserved in nano-core milliseconds without rounded-up billing',()=>{
 const j=journal();try{const precise={...resources,cpuNanoCores:333333334,cpuMilliCores:334};j.source.begin(identity,precise,1000);j.source.finish(identity,1010);const segment=j.source.events(identity).find(e=>e.kind==='RESOURCE_SEGMENT')!;
  assert.equal(segment.allocation!.cpuNanoCoreMilliseconds,'3333333340');assert.equal(segment.resources.cpuMilliCores,334);
 }finally{j.close();}
});

test('durable execution binding precedes events and preserves original ownership context',()=>{
 const j=journal(),s=inbox();try{const context={tenantVersion:1,region:'dev',costCenter:'eng',rateCardVersion:null};s.sink.bind(identity,resources,context,1000);assert.equal(s.sink.binding(identity)!.context!.costCenter,'eng');
  s.sink.bind(identity,resources,{...context,tenantVersion:2,costCenter:'changed'},1001);assert.equal(s.sink.binding(identity)!.context!.costCenter,'eng');
  assert.throws(()=>s.sink.bind({...identity,executionInputDigest:'b'.repeat(64)},resources,context),{code:'USAGE_BINDING_CONFLICT'});
  j.source.begin(identity,resources,1000);j.source.finish(identity,1010);for(const e of j.source.events(identity))s.sink.receive(e,identity);assert.equal(s.sink.inspect(identity)!.pending,false);
  assert.throws(()=>s.sink.bind({...identity,tenantId:'other'},resources,context),{code:'USAGE_TENANT_IDENTITY_CONFLICT'});
 }finally{s.db.close();j.close();}
});

test('usage pages freeze append watermark, authenticate cursors, preserve context and enforce tenant scope',()=>{
 const j=journal(),s=inbox(),key='b'.repeat(64),actor={sub:'github:1',role:'VIEWER' as const,scopeTenant:'tenant',scopeRegion:'dev'},tenant={id:'tenant',name:'Tenant',region:'dev',costCenter:'eng',status:'ACTIVE' as const,version:1};
 try{
  s.sink.bind(identity,resources,{tenantVersion:1,region:'dev',costCenter:'original',rateCardVersion:null});j.source.begin(identity,resources,1000);j.source.finish(identity,1010);for(const e of j.source.events(identity))s.sink.receive(e,identity);
  const first=s.sink.page(actor,tenant,{limit:1},key);assert.equal(first.events[0].context!.costCenter,'original');assert.ok(first.nextCursor);
  const extra={...identity,taskId:'task2',attemptId:'attempt2'};s.sink.bind(extra,resources,{tenantVersion:1,region:'dev',costCenter:'new',rateCardVersion:null});j.source.begin(extra,resources,2000);s.sink.receive(j.source.events(extra)[0],extra);
  let cursor:string|null=first.nextCursor,total=1;do{const page=s.sink.page(actor,tenant,{limit:1,cursor:cursor!},key);total+=page.events.length;cursor=page.nextCursor;}while(cursor);assert.equal(total,3);
  assert.equal(s.sink.page(actor,tenant,{limit:100},key).events.length,4);
  assert.throws(()=>s.sink.page(actor,tenant,{cursor:first.nextCursor+'.extra'},key),{code:'INVALID_CURSOR'});
  assert.throws(()=>s.sink.page({...actor,sub:'other'},tenant,{cursor:first.nextCursor!},key),{code:'CURSOR_STALE'});
  assert.throws(()=>s.sink.page({...actor,scopeTenant:'other'},tenant,{},key),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>s.sink.page(actor,{...tenant,version:2},{cursor:first.nextCursor!},key),{code:'CURSOR_STALE'});
 }finally{s.db.close();j.close();}
});

test('automatic usage relay replays a lost committed receipt and quarantines a conflict without starving siblings',async()=>{
 const {deliverBodyUsage}=await import('../../src/runtime/body_usage_delivery.js');
 const j=journal(),s=inbox(),key='b'.repeat(64);let now=2000,source=j.source;const sibling={...identity,taskId:'sibling',attemptId:'sibling'};
 const local=async(path:string,value:unknown)=>{const body=value as UsageIdentity&{sequence:number;digest:string;quarantine:boolean;limit:number};
  if(path.endsWith('/pending'))return {events:source.pending(body.limit,now).map(e=>({proof:signUsageEvent(e,key),digest:usageEventDigest(e)}))};
  if(path.endsWith('/ack'))return {acknowledged:source.acknowledge(body,body.sequence,body.digest)};
  return {deferred:source.defer(body,body.sequence,body.digest,body.quarantine,now)};
 };
 try{
  source.begin(identity,resources,1000);let attempts=0;
  const publish=async(proof:string)=>{const e=verifyUsageEvent(proof,key),receipt=s.sink.receive(e,identity);if(++attempts===1)throw new Error('lost receipt');return {sourceVerified:true,receipt};};
  assert.equal((await deliverBodyUsage(local,publish,key)).deferred,1);source=new BodyUsageJournal(j.dir);now=4000;assert.equal((await deliverBodyUsage(local,publish,key)).delivered,1);assert.equal(attempts,2);
  source.checkpoint(identity,4010);source.begin(sibling,resources,4010);now=5000;
  const result=await deliverBodyUsage(local,async proof=>{const e=verifyUsageEvent(proof,key);return {sourceVerified:true,receipt:{eventId:e.eventId,digest:usageEventDigest(e),outcome:e.taskId==='task'?'CONFLICT':'RECORDED'}};},key);
  assert.equal(result.delivered,1);assert.equal(result.quarantined,1);assert.equal(new BodyUsageJournal(j.dir).stats(now).quarantinedStreams,1);
 }finally{s.db.close();j.close();}
});

test('unstructured legacy allocation records known slot time and preserves unknown CPU/memory',()=>{
 const j=journal(),s=inbox();try{j.source.begin(identity,{cpuNanoCores:null,cpuMilliCores:null,memoryMiB:null,slots:1},1000);j.source.finish(identity,1020);const events=j.source.events(identity),segment=events.find(e=>e.kind==='RESOURCE_SEGMENT')!;
  assert.equal(segment.allocation!.cpuNanoCoreMilliseconds,null);assert.equal(segment.allocation!.memoryMiBMilliseconds,null);assert.equal(segment.allocation!.slotMilliseconds,'20');for(const e of events)s.sink.receive(e,identity);assert.equal(s.sink.inspect(identity)!.pending,true);
 }finally{s.db.close();j.close();}
});
test('provider callbacks flush crossed UTC boundaries atomically without reordering persisted signed events',()=>{
 const j=journal(),s=inbox();try{
  const midnight=Date.UTC(2026,10,18);j.source.begin(identity,resources,midnight-10);j.source.providerStarted(identity,1,{provider:'deepseek',model:'requested'},midnight+1);
  const prefix=j.source.events(identity);assert.deepEqual(prefix.map(e=>e.kind),['RESOURCE_OPEN','RESOURCE_SEGMENT','PROVIDER_REQUEST']);assert.equal(prefix[1].at,midnight);
  const source=new BodyUsageJournal(j.dir),temporary=join(j.dir,usageStreamId(identity)+'.json.tmp');mkdirSync(temporary);
  assert.throws(()=>source.providerFinished(identity,1,providerResponse,midnight+2));assert.deepEqual(source.events(identity),prefix);rmSync(temporary,{recursive:true});
  source.providerFinished(identity,1,providerResponse,midnight+2);source.finish(identity,midnight+10);const events=source.events(identity);assert.deepEqual(events.slice(0,prefix.length),prefix);for(const event of events)assert.equal(s.sink.receive(event,identity).outcome,'RECORDED');
  assert.equal(s.sink.inspect(identity)!.pending,false);assert.equal(events.filter(e=>e.kind==='RESOURCE_SEGMENT').reduce((sum,e)=>sum+BigInt(e.allocation!.slotMilliseconds),0n),20n);
 }finally{s.db.close();j.close();}
});
test('unknown provider response spanning midnight remains unknown while exact resource segments still close',()=>{
 const j=journal(),s=inbox();try{const midnight=Date.UTC(2026,10,18);j.source.begin(identity,resources,midnight-10);j.source.providerStarted(identity,1,{provider:'deepseek',model:'requested'},midnight-5);j.source.providerUncertain(identity,1,midnight+5);new BodyUsageJournal(j.dir).finish(identity,midnight+10);const events=j.source.events(identity);for(const event of events)s.sink.receive(event,identity);assert.ok(s.sink.inspect(identity)!.closed);assert.ok(s.sink.inspect(identity)!.pending);assert.equal(events.find(e=>e.kind==='PROVIDER_USAGE')!.provider!.totalTokens,null);
 }finally{s.db.close();j.close();}
});
