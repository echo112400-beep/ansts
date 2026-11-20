/** Isolated harness only. Never imported by a production Worker or deployment configuration. */
import {TenantControlCell} from '../src/cloudflare/tenant_control_cell.js';
import {AnstsSchedulerCell} from '../src/cloudflare/scheduler_cell.js';
import {ExecutionInventory,INVENTORY_PAGE_SIZE} from '../src/control/execution_inventory.js';
import type {DurableObjectStateLike} from '../src/cloudflare/env.js';
import {PeriodSettlement,settlementMonth} from '../src/finops/period_settlement.js';
import type {DirectoryStorage} from '../src/control/tenant_directory.js';
export class FixtureTenantControlCell extends TenantControlCell {
 constructor(private readonly fixtureContext:any,env:any){super(fixtureContext,env);}
 async fetch(request:Request):Promise<Response>{
  const path=new URL(request.url).pathname;if(!path.startsWith('/fixture-period-'))return super.fetch(request);
  if(request.method!=='POST'||request.headers.get('x-fixture-key')!=='isolated-period-fixture-only')return Response.json({code:'FIXTURE_AUTH_REQUIRED'},{status:401});
  const storage:DirectoryStorage={sql:this.fixtureContext.storage.sql,transactionSync:this.fixtureContext.storage.transactionSync.bind(this.fixtureContext.storage)};
  if(path==='/fixture-period-backdate-uncertain'){
   const {startAt}=settlementMonth((await request.json() as {periodId:string}).periodId);
   const row=storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_admission_state WHERE id = ?', 'admission').toArray()[0];
   const state=JSON.parse(row.payload_json);
   const reservation=state.reservations.find(item=>item.id==='close-uncertain'&&item.phase==='UNCERTAIN');
   if(state.tenantId!=='close-probe'||!reservation)return Response.json({code:'FIXTURE_RESERVATION_REQUIRED'},{status:409});
   reservation.createdAt=startAt;
   storage.sql.exec('UPDATE tenant_admission_state SET payload_json = ? WHERE id = ?',JSON.stringify(state),'admission');
   return Response.json({ok:true});
  }
  if(path==='/fixture-period-state'){
   const admission=storage.sql.exec('SELECT payload_json FROM tenant_admission_state ORDER BY id').toArray();
   const closes=storage.sql.exec('SELECT payload_json FROM tenant_period_closes ORDER BY id').toArray();
   const adjustments=storage.sql.exec('SELECT payload_json FROM tenant_period_adjustments ORDER BY id').toArray();
   return Response.json({admission,closes,adjustments});
  }
  if(path==='/fixture-period-scan-limit'){
   storage.sql.exec(`
    WITH RECURSIVE fixture_rows(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM fixture_rows WHERE n<10001)
    INSERT INTO tenant_usage_event_order (id, ordinal, tenant_id, region)
    SELECT 'fixture-scan-'||n, n+head.ordinal, 'evidence', 'dev'
    FROM fixture_rows CROSS JOIN (SELECT COALESCE(MAX(ordinal),0) AS ordinal FROM tenant_usage_event_order) head
   `);
   return Response.json({ok:true});
  }
  if(path==='/fixture-period-clear-scan'){
   storage.sql.exec("DELETE FROM tenant_usage_event_order WHERE id LIKE 'fixture-scan-%'");
   return Response.json({ok:true});
  }
  const periods=new PeriodSettlement(storage),tenant={id:'evidence',name:'Evidence tenant',costCenter:'eng',region:'dev',status:'ACTIVE' as const,version:1},actor={sub:'isolated-period-fixture',role:'SRE_ADMIN' as const,scopeTenant:'all',scopeRegion:'all'};
  const current=new Date();
  const periodId=new Date(Date.UTC(current.getUTCFullYear(),current.getUTCMonth()-1,1)).toISOString().slice(0,7);
  const now=settlementMonth(periodId).endAt;
  if(path==='/fixture-period-seed'){
   const coverage={schemaVersion:1 as const,purpose:'ansts-settlement-coverage-v1' as const,tenantId:tenant.id,periodId,inventoryWatermark:'isolated-empty-before-execution',observedAt:now,complete:true,attempts:[]};
   const preview=periods.preview(actor,tenant,periodId,coverage,now);
   return Response.json(periods.close(actor,tenant,periodId,{expectedRevision:preview.revision,reason:'Isolated simulated month end before fixture executions',requestId:'seed',idempotencyKey:'seed'},coverage,now));
  }
  if(path==='/fixture-period-reconcile')return Response.json({added:periods.reconcileLate(actor,tenant)});
  return Response.json({code:'NOT_FOUND'},{status:404});
 }
}

export class FixtureSettlementScheduler extends AnstsSchedulerCell {
 private advanceOnNextPage=false;
 private firstPagesUntilAdvance=0;

 constructor(private readonly fixtureContext:DurableObjectStateLike,env:{ANSTS_CONFIG_TARGET_TOKEN?:string}={}){super(fixtureContext,env);}

 private appendFixtures(prefix:string,count:number,fencing:number){
  const storage=this.fixtureContext.storage as DirectoryStorage;
  const inventory=new ExecutionInventory(storage,'SCHEDULER',this.fixtureContext.id.toString(),()=>[]);
  for(let index=0;index<count;index++){
   inventory.record({tenantId:'evidence',taskId:prefix+index,inputDigest:'fixture',state:'QUEUED',
    fencing,nodeId:null,attemptId:null,version:null,sourceAt:0},()=>{});
  }
 }

 async fetch(request:Request):Promise<Response>{
  const url=new URL(request.url);
  if(url.pathname.startsWith('/fixture-inventory-')){
   if(request.method!=='POST'||request.headers.get('x-fixture-key')!=='isolated-period-fixture-only'){
    return Response.json({code:'FIXTURE_AUTH_REQUIRED'},{status:401});
   }
   switch(url.pathname){
    case '/fixture-inventory-page': this.appendFixtures('page-',INVENTORY_PAGE_SIZE+1,0);break;
    case '/fixture-inventory-advance': this.advanceOnNextPage=true;break;
    case '/fixture-inventory-recheck': this.firstPagesUntilAdvance=2;break;
    case '/fixture-inventory-limit': this.appendFixtures('limit-',1001,1);break;
    case '/fixture-inventory-close-coverage':
     this.fixtureContext.storage.sql.exec("UPDATE execution_inventory_meta SET payload_json = json_set(payload_json, '$.coverageSince', 0) WHERE id = 'source'");
     break;
    default:return Response.json({code:'NOT_FOUND'},{status:404});
   }
   return Response.json({ok:true});
  }
  if(url.pathname==='/v1/scheduler/execution-inventory'){
   const after=url.searchParams.get('after');
   if(this.advanceOnNextPage&&Number(after)>0){
    this.advanceOnNextPage=false;
    this.appendFixtures('between-pages-',1,0);
   }
   if(after==='0'&&this.firstPagesUntilAdvance>0&&--this.firstPagesUntilAdvance===0){
    this.appendFixtures('before-recheck-',1,0);
   }
  }
  return super.fetch(request);
 }
}
