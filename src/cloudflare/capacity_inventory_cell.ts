import type {AdmissionReservation} from '../control/admission_reservations.js';
import type {DurableObjectNamespaceLike,DurableObjectStateLike} from './env.js';
import {equalBodyToken} from './body_auth.js';
import {CapacityInventory,type HostWrite,type PoolWrite} from '../cluster/capacity_inventory.js';
import {assertControlAccess,ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';
import type {CapacityReport} from '../control/capacity_report.js';
import {CapacityRequestStore} from '../cluster/capacity_request_store.js';
import type {CapacityResources} from '../control/capacity_report.js';
export class CapacityInventoryCell {
 private readonly storage:DirectoryStorage|null;
 private readonly inventory:CapacityInventory|null;
 constructor(private readonly ctx:DurableObjectStateLike&{storage:{transactionSync?:DirectoryStorage['transactionSync']}},private readonly env:{ANSTS_DIRECTORY_SERVICE_TOKEN?:string;ANSTS_ADMISSION_SERVICE_TOKEN?:string;ANSTS_ADMISSION_MODE?:string;ANSTS_ADMISSION_PILOT_TENANTS?:string;ANSTS_TENANT_CONTROL_CELL?:DurableObjectNamespaceLike}={}){
  this.storage=typeof ctx.storage.transactionSync==='function'
   ?{sql:ctx.storage.sql,transactionSync:ctx.storage.transactionSync.bind(ctx.storage)}
   :null;
  this.inventory=this.storage?new CapacityInventory(this.storage):null;
 }
 private async scheduleAllocation(at:number) {
  const storage=this.ctx.storage;
  if(typeof storage.getAlarm!=='function'||typeof storage.setAlarm!=='function')return;
  const current=await storage.getAlarm();
  if(current==null||current>at)await storage.setAlarm(at);
 }

 async alarm():Promise<void>{
  const token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
  if(!this.storage||!token||!this.env.ANSTS_TENANT_CONTROL_CELL)return;
  const pools=this.storage.sql.exec<{pool_id:string}>(
   `SELECT DISTINCT r.pool_id FROM capacity_requests r WHERE r.state='QUEUED'
    AND NOT EXISTS (SELECT 1 FROM capacity_request_grants g
     WHERE g.pool_id=r.pool_id AND g.request_id=r.id)`,
  ).toArray();
  if(!pools.length)return;
  await this.scheduleAllocation(Date.now()+15_000);
  const actor={sub:'system:capacity',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'};
  for(const {pool_id:id} of pools){
   const response=await this.fetch(new Request('https://capacity.internal/grant-issue',{
    method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},
    body:JSON.stringify({actor,input:{id}}),
   }));
   await response.body?.cancel();
  }
 }
 private async readHeadroom(tenantId:string,region:string){
  const ns=this.env.ANSTS_TENANT_CONTROL_CELL,token=this.env.ANSTS_ADMISSION_SERVICE_TOKEN;
  if(!ns||!token||token===this.env.ANSTS_DIRECTORY_SERVICE_TOKEN)throw new ControlError('CAPACITY_QUOTA_UNAVAILABLE',503);
  const actor={sub:'system:capacity',role:'OPERATOR',scopeTenant:tenantId,scopeRegion:region};
  const response=await ns.get(ns.idFromName('tenant-control-v1:'+tenantId)).fetch(new Request(
   'https://tenant.internal/admission-capacity-headroom',{
    method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},
    body:JSON.stringify({actor,tenantId}),
   },
  ));
  if(!response.ok){
   await response.body?.cancel();
   if([403,404,409].includes(response.status))return null;
   throw new ControlError('CAPACITY_QUOTA_UNAVAILABLE',503);
  }
  return await response.json() as {remaining:CapacityResources;used:CapacityResources;activeReservationIds?:string[];queueLimit?:number};
 }
 async fetch(request:Request):Promise<Response>{
  const reply=(value:unknown,status=200)=>Response.json(value,{status,headers:{'cache-control':'no-store'}}),token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
  if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token))return reply({code:'SERVICE_AUTH_REQUIRED'},401);
  if(!this.inventory)return reply({code:'DURABLE_TRANSACTION_REQUIRED'},503);
  if(request.method!=='POST')return reply({code:'METHOD_NOT_ALLOWED'},405);
  try{
   const raw=await request.text();if(raw.length>65536)return reply({code:'PAYLOAD_TOO_LARGE'},413);
   const body=JSON.parse(raw) as {actor:ControlActor;input:HostWrite&PoolWrite;report:CapacityReport;daemonId:string;requestInput:{poolId:string;tenantId:string;key:string;resources:CapacityResources;id:string;binding?:{reservationId:string;attemptId:string;generation:number;fencing:number}}};
   const path=new URL(request.url).pathname;
   if(path==='/request-list'||path==='/request-submit'||path==='/request-cancel'){
    const {actor,requestInput}=body;
    if(!actor||!requestInput)throw new ControlError('INVALID_CAPACITY_INPUT',422);
    const {poolId,tenantId,key,resources,id}=requestInput;
    const pool=this.inventory.getPool(poolId);
    assertControlAccess(actor,path==='/request-list'?'read':'request_capacity',tenantId,pool.region);
    const store=new CapacityRequestStore(this.storage!,pool.id);
    if(path==='/request-list'){
     const now=Date.now();
     const snapshot=this.inventory.snapshot({sub:'system:capacity',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},now);
     const {snapshot:effective,grants}=store.accountForGrants(snapshot);
     const available=store.poolAvailabilityFor(effective.pools.find(item=>item.id===pool.id)!.available,grants)(tenantId);
     const healthy=effective.hosts.filter(host=>host.poolId===pool.id&&host.health==='HEALTHY');
     const enabled=store.readAllocationControl().enabled;
     const requests=store.list(tenantId);
     const quota=enabled&&healthy.length&&requests.some(request=>request.state==='QUEUED')
      ?await this.readHeadroom(tenantId,pool.region).catch(()=>null):null;
     return reply({requests:requests.map(request=>{
      if(request.state!=='QUEUED')return request;
      const fits=healthy.some(host=>(['cpuMilliCores','memoryMiB','slots'] as const)
       .every(dimension=>request.resources[dimension]<=host.available[dimension]&&request.resources[dimension]<=available[dimension]));
      const queueReason=pool.status!=='ACTIVE'?'POOL_DISABLED':!enabled?'ALLOCATION_PAUSED':
       !healthy.length?'NO_HEALTHY_HOST':!fits?'HOST_RESOURCES_UNAVAILABLE':
       !quota?'TENANT_QUOTA_UNAVAILABLE':
       (['cpuMilliCores','memoryMiB','slots'] as const).some(dimension=>request.resources[dimension]>quota.remaining[dimension])
        ?'TENANT_QUOTA_EXCEEDED':'AWAITING_ALLOCATION';
      return {...request,queueReason};
     })});
    }
    if(path==='/request-cancel')return reply(store.cancel(tenantId,id));
    if(pool.status!=='ACTIVE')throw new ControlError('CAPACITY_POOL_DISABLED',409);
    if(resources?.slots!==1)throw new ControlError('CAPACITY_REQUEST_REQUIRES_ONE_SLOT',422);
    await this.scheduleAllocation(Date.now()+1000);
    const enforce=this.env.ANSTS_ADMISSION_MODE==='enforce'&&
     (this.env.ANSTS_ADMISSION_PILOT_TENANTS??'').split(',').some(id=>id.trim()===tenantId);
    const quota=enforce?await this.readHeadroom(tenantId,pool.region).catch(()=>null):null;
    return reply(store.submit(tenantId,key,resources,0,Date.now(),enforce?quota?.queueLimit??null:undefined));
   }
   if(path==='/queue-policy-read'||path==='/queue-policy-write'){
    const pool=this.inventory.getPool(body.input.id);
    assertControlAccess(body.actor,'administer','all',pool.region);
    const store=new CapacityRequestStore(this.storage!,body.input.id);
    const input=body.input as unknown as {expectedVersion:number;maxWaitMs:number;weights:Record<string,number>;guaranteedShares?:Record<string,CapacityResources>;priorities?:Record<string,number>};
    const policy=path==='/queue-policy-read'?store.readPolicy():store.writePolicy(input.expectedVersion,input.maxWaitMs,input.weights,input.guaranteedShares===undefined?undefined:{shares:input.guaranteedShares,limit:pool.limit},input.priorities);
    return reply({...policy,weights:Object.fromEntries(policy.weights),priorities:Object.fromEntries(policy.priorities),guaranteedShares:store.readGuaranteedShares()});
   }
   if(path==='/allocation-control-read'||path==='/allocation-control-write'){
    const pool=this.inventory.getPool(body.input.id);
    assertControlAccess(body.actor,'administer','all',pool.region);
    const store=new CapacityRequestStore(this.storage!,pool.id);
    const input=body.input as unknown as {expectedVersion:number;enabled:boolean};
    if(path==='/allocation-control-write'&&input.enabled===true)await this.scheduleAllocation(Date.now()+1000);
    return reply(path==='/allocation-control-read'?store.readAllocationControl():
     store.writeAllocationControl(input.expectedVersion,input.enabled));
   }
   if(path==='/grant-issue'){
    const {actor,input}=body;
    const pool=this.inventory.getPool(input.id);
    assertControlAccess(actor,'administer','all',pool.region);
    const candidates=this.storage!.sql.exec<{tenant_id:string}>(
     `SELECT DISTINCT r.tenant_id FROM capacity_requests r WHERE r.pool_id=? AND r.state='QUEUED'
      AND NOT EXISTS (SELECT 1 FROM capacity_request_grants g WHERE g.pool_id=r.pool_id AND g.request_id=r.id)`,
     pool.id,
    ).toArray();
    const remaining=new Map<string,CapacityResources>(),usage=new Map<string,CapacityResources>();
    const activeReservationIds=new Map<string,ReadonlySet<string>>();
    for(const {tenant_id:tenantId} of candidates){
     const headroom=await this.readHeadroom(tenantId,pool.region);
     if(!headroom)continue;
     remaining.set(tenantId,headroom.remaining);
     usage.set(tenantId,headroom.used);
     activeReservationIds.set(tenantId,new Set(headroom.activeReservationIds));
    }
    const now=Date.now();
    const inventory=this.inventory.snapshot({sub:'system:capacity',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},now);
    const store=new CapacityRequestStore(this.storage!,pool.id);
    const grant=store.issueGrant(inventory,{tenantRemaining:remaining,tenantUsage:usage,activeReservationIds},
     store.readPolicy(),now);
    if(!grant)return reply({grant:null});
    const host=inventory.hosts.find(candidate=>candidate.id===grant.hostId);
    const target=host?.daemonIds[0];
    if(!target)throw new ControlError('CAPACITY_TARGET_UNAVAILABLE',503);
    return reply({grant:{...grant,target,capacityReceipt:'capacity-grant:'+grant.requestId}});
   }
   if(path==='/grant-read'){
    const {actor,requestInput}=body;
    if(!actor||!requestInput)throw new ControlError('INVALID_CAPACITY_INPUT',422);
    const {tenantId,id}=requestInput;
    const poolId=requestInput.poolId??this.storage!.sql.exec<{pool_id:string}>(
     'SELECT pool_id FROM capacity_requests WHERE id=? AND tenant_id=?',id,tenantId,
    ).toArray()[0]?.pool_id;
    if(!poolId)return reply({grant:null});
    const pool=this.inventory.getPool(poolId);
    assertControlAccess(actor,'request_capacity',tenantId,pool.region);
    const now=Date.now();
    const row=this.storage!.sql.exec<{host_id:string;policy_version:number;expires_at:number|null;bound_id:string|null;daemon_id:string;epoch:number;reservation_id:string|null;attempt_id:string|null;generation:number|null;fencing:number|null}>(
     `SELECT g.host_id,g.policy_version,e.expires_at,b.request_id AS bound_id,f.daemon_id,f.epoch,b.reservation_id,b.attempt_id,b.generation,b.fencing
      FROM capacity_request_grants g
      JOIN capacity_requests r ON r.pool_id=g.pool_id AND r.id=g.request_id
      LEFT JOIN capacity_grant_expiry e ON e.pool_id=g.pool_id AND e.request_id=g.request_id
      LEFT JOIN capacity_grant_bindings b ON b.pool_id=g.pool_id AND b.request_id=g.request_id
      JOIN capacity_grant_epochs f ON f.pool_id=g.pool_id AND f.request_id=g.request_id
      WHERE g.pool_id=? AND g.request_id=? AND r.tenant_id=?`,poolId,id,tenantId,
    ).toArray()[0];
    if(!row||(!row.bound_id&&(row.expires_at===null||row.expires_at<=now)))return reply({grant:null});
    const binding=requestInput.binding;
    if(binding&&(!row.bound_id||row.reservation_id!==binding.reservationId||row.attempt_id!==binding.attemptId||
       row.generation!==binding.generation||row.fencing!==binding.fencing))return reply({grant:null});
    const snapshot=this.inventory.snapshot({sub:'system:capacity',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},now);
    const host=snapshot.hosts.find(item=>item.id===row.host_id&&item.poolId===poolId);
    if(!host||host.health!=='HEALTHY'||pool.status!=='ACTIVE'||!host.daemonIds.includes(row.daemon_id)||
       !host.reports.some(report=>report.daemonId===row.daemon_id&&report.epoch===row.epoch))return reply({grant:null});
    return reply({grant:{requestId:id,hostId:host.id,target:row.daemon_id,
     policyVersion:row.policy_version,capacityReceipt:'capacity-grant:'+id}});
   }
   if(path==='/grant-bind'){
    const {requestId,reservation,target} = body as unknown as {
     requestId:string;reservation:AdmissionReservation;target:string;
    };
    const grant=this.storage!.sql.exec<{pool_id:string;host_id:string;daemon_id:string;epoch:number}>(
     `SELECT g.pool_id,g.host_id,f.daemon_id,f.epoch FROM capacity_request_grants g
      JOIN capacity_requests r ON r.pool_id=g.pool_id AND r.id=g.request_id
      JOIN capacity_grant_epochs f ON f.pool_id=g.pool_id AND f.request_id=g.request_id
      WHERE g.request_id=? AND r.tenant_id=?`,requestId,reservation.tenantId,
    ).toArray()[0];
    if(!grant)throw new ControlError('CAPACITY_GRANT_NOT_FOUND',404);
    const now=Date.now();
    const snapshot=this.inventory.snapshot({sub:'system:capacity',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},now);
    const host=snapshot.hosts.find(candidate=>candidate.id===grant.host_id&&candidate.poolId===grant.pool_id);
    const pool=snapshot.pools.find(candidate=>candidate.id===grant.pool_id);
    if(!host||host.health!=='HEALTHY'||grant.daemon_id!==target||!host.daemonIds.includes(target)||
       pool?.status!=='ACTIVE'||pool.region!==reservation.policy.region||
       !host.reports.some(report=>report.daemonId===grant.daemon_id&&report.epoch===grant.epoch)) {
     throw new ControlError('CAPACITY_TARGET_UNAVAILABLE',409);
    }
    const capacityReceipt=new CapacityRequestStore(this.storage!,grant.pool_id).bindGrant(requestId,reservation,now);
    return reply({capacityReceipt});
   }
   if(path==='/grant-release'){
    const reservation=(body as unknown as {reservation:AdmissionReservation}).reservation;
    const match=/^capacity-grant:([a-f0-9]{64})$/.exec(reservation?.capacityReceipt??'');
    if(!match)throw new ControlError('INVALID_CAPACITY_INPUT',422);
    const binding=this.storage!.sql.exec<{pool_id:string}>(
     'SELECT pool_id FROM capacity_requests WHERE id = ? AND tenant_id = ?',match[1],reservation.tenantId,
    ).toArray()[0];
    if(!binding)throw new ControlError('CAPACITY_GRANT_NOT_FOUND',404);
    new CapacityRequestStore(this.storage!,binding.pool_id).releaseTerminatedGrant(match[1],reservation);
    return reply({released:true});
   }
   if(path==='/pool-catalog'){
    const {actor,requestInput}=body;
    if(!actor||!requestInput)throw new ControlError('INVALID_CAPACITY_INPUT',422);
    const now=Date.now();
    const catalog=this.inventory.listPools(actor,requestInput.tenantId,now);
    const inventory=this.inventory.snapshot({sub:'system:capacity',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},now);
    return reply({...catalog,pools:catalog.pools.map(pool=>{
     const store=new CapacityRequestStore(this.storage!,pool.id);
     const {snapshot,grants,borrowed}=store.accountForGrants(inventory);
     const available=store.poolAvailabilityFor(snapshot.pools.find(candidate=>candidate.id===pool.id)!.available,grants)(requestInput.tenantId);
     const shares=store.readGuaranteedShares();
     const guaranteedShare=Object.hasOwn(shares,requestInput.tenantId)?shares[requestInput.tenantId]:null;
     return {...pool,available,borrowed,guaranteedShare,activeGrants:grants.length,allocationEnabled:store.readAllocationControl().enabled};
    })});
   }
   if(path==='/report'){const result=this.inventory.report(body.daemonId,body.report,Date.now());return reply(result,result.outcome==='CONFLICT'?409:200);}
   if(path==='/snapshot')return reply(this.inventory.snapshot(body.actor,Date.now()));
   if(path==='/audit')return reply({audit:this.inventory.audit(body.actor)});
   if(!body.input||typeof body.input!=='object')return reply({code:'INVALID_CAPACITY_INPUT'},422);
   if(path==='/pool-write'){const result=this.inventory.writePool(body.actor,body.input,Date.now());const response=reply(result.record);response.headers.set('x-ansts-operation-id',result.operationId);return response;}
   if(path==='/host-write'){const result=this.inventory.writeHost(body.actor,body.input,Date.now());const response=reply(result.record);response.headers.set('x-ansts-operation-id',result.operationId);return response;}
   return reply({code:'NOT_FOUND'},404);
  }catch(error){return reply({code:error instanceof ControlError?error.code:error instanceof SyntaxError?'INVALID_JSON':'CAPACITY_UNAVAILABLE'},error instanceof ControlError?error.status:error instanceof SyntaxError?400:503);}
 }
}


























