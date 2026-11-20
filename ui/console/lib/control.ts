export interface CapacityResources {cpuMilliCores:number;memoryMiB:number;slots:number}
export interface CapacityPoolSummary {
  id:string;name:string;region:string;status:'ACTIVE'|'DISABLED';
  capacity:CapacityResources;available:CapacityResources;retainedReserved:CapacityResources;borrowed:CapacityResources;guaranteedShare:CapacityResources|null;
  hostCount:number;isolatedHostCount:number;activeGrants:number;complete:boolean;allocationEnabled?:boolean;
}
export interface CapacityRequestRow {
  id:string;tenantId:string;resources:CapacityResources;enqueuedAt:number;
  state:'QUEUED'|'CANCELLED'|'GRANTED'|'RELEASED';
  grantExpiresAt?:number;
  queueReason?:'POOL_DISABLED'|'ALLOCATION_PAUSED'|'NO_HEALTHY_HOST'|'HOST_RESOURCES_UNAVAILABLE'|'TENANT_QUOTA_EXCEEDED'|'TENANT_QUOTA_UNAVAILABLE'|'AWAITING_ALLOCATION';
}
export interface CapacityAllocationControl {version:number;enabled:boolean}
/** Shared management transport. Mutations never retry automatically. */
export interface DirectoryArchiveManifest {id:string;schemaVersion:1;sourceVersion:number;digest:string;actor:string;reason:string;requestId:string;createdAt:string;bytes:number;tenantAuditWatermark:number;costCenterAuditWatermark:number;receiptRetention:'ALL_RETAINED';safeReplayWatermark:null}
export interface DirectoryArchive {manifest:DirectoryArchiveManifest;snapshot:Record<string,unknown>}
export interface DirectoryArchiveStatus {currentVersion:number;items:DirectoryArchiveManifest[];receiptRetention:'ALL_RETAINED';safeReplayWatermark:null}
export async function verifyDirectoryArchive(archive:DirectoryArchive,expectedId:string):Promise<boolean> {
  try {
    const {manifest,snapshot}=archive;
    if(!manifest||manifest.id!==expectedId||!/^([a-f0-9]{64})$/.test(expectedId)||manifest.schemaVersion!==1||!snapshot||snapshot.schemaVersion!==1||!Array.isArray(snapshot.audit)||!/^([a-f0-9]{64})$/.test(manifest.digest))return false;
    const bytes=new TextEncoder().encode(JSON.stringify(snapshot));
    if(bytes.length>8*1024*1024||bytes.length!==manifest.bytes)return false;
    const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),value=>value.toString(16).padStart(2,'0')).join('');
    return digest===manifest.digest;
  } catch {return false;}
}
export interface TenantQuotaPolicy {schemaVersion:1;tenantId:string;tenantVersion:number;region:string;costCenter:string;version:number;resources:{concurrentAttempts:number;cpuMilliCores:number;memoryMiB:number;queuedTasks:number};budget:{period:'UTC_DAY'|'UTC_MONTH';limitCreditUnits:string;softThresholdBps:number};createdAt:string;actor:string;reason:string}
export interface TenantQuotaView {current:TenantQuotaPolicy|null;history:TenantQuotaPolicy[];nextCursor:string|null;historyTotal:number;enforcement:'NOT_CONNECTED';tenantVersion:number;tenantStatus:'ACTIVE'|'DISABLED'}
export type TenantQuotaEdit=Pick<TenantQuotaPolicy,'resources'|'budget'|'reason'>&{expectedVersion:number};
export interface ControlOperation {operationId:string;status:"SUCCEEDED";domain:string;targetId:string;resultVersion:number;requestId:string|null;timestamp:string|null}
export interface DirectoryTenant {
  operationId?:string;
  id: string;
  region: string;
  name: string;
  costCenter: string;
  status: 'ACTIVE' | 'DISABLED';
  version: number;
  costCenterHistory?: {costCenter:string;version:number;timestamp:string|null}[];
}
export interface ManagementAudit {id:string;domain:"tenant"|"cost-center"|"rate-card"|"rate-activation";targetId:string;actor:string;reason:string;requestId:string;timestamp:string;previousVersion:number;version:number;changes?:{field:string;before:string|null;after:string}[]}
export interface CostCenter {operationId?:string;id:string;name:string;status:"ACTIVE"|"DISABLED";version:number}
export type CostCenterEdit=Pick<TenantEdit,"name"|"status"|"expectedVersion"|"reason">;
export interface ControlPage<T> {items:T[];version:number;nextCursor:string|null}
export class ControlApiError extends Error {
  constructor(readonly code:string,readonly status:number,readonly requestId?:string) {super(code);}
  get conflict() {return this.status===409;}
  get unavailable() {return this.status===503;}
}
export interface TenantEdit {
  region:string;
  name:string;
  costCenter:string;
  status:'ACTIVE'|'DISABLED';
  expectedVersion:number;
  reason:string;
}
function capacityPath(section:string,query:Record<string,string>) {
  return '/v1/admin/capacity/'+section+'?'+new URLSearchParams(query);
}
export function controlClient(fetcher:typeof fetch=fetch) {
  async function request<T>(path:string,options:RequestInit={}):Promise<T> {
    let response:Response;
    try {response=await fetcher(path,{...options,credentials:'same-origin',cache:'no-store',redirect:'error'});}
    catch(error) {
      if(error instanceof Error && error.name==='AbortError') throw error;
      throw new ControlApiError('NETWORK_UNAVAILABLE',0);
    }
    let body:unknown;
    try {body=await response.json();} catch {throw new ControlApiError('INVALID_API_RESPONSE',response.status);}
    if(!response.ok) {
      const detail=body as {code?:unknown;error?:unknown;requestId?:unknown}|null;
      const code=typeof detail?.code==='string'?detail.code:typeof detail?.error==='string'?detail.error:'REQUEST_FAILED';
      throw new ControlApiError(code,response.status,typeof detail?.requestId==='string'?detail.requestId:undefined);
    }
    const operationId=response.headers.get("x-ansts-operation-id");
    if(operationId&&body&&typeof body==='object'&&!Array.isArray(body)) Object.assign(body,{operationId});
    return body as T;
  }
  return {
    capacityPools(tenantId:string,signal?:AbortSignal) {
      return request<{pools:CapacityPoolSummary[];asOf:number}>(capacityPath('catalog',{tenantId}),{signal});
    },
    capacityRequests(poolId:string,tenantId:string,signal?:AbortSignal) {
      return request<{requests:CapacityRequestRow[]}>(capacityPath('requests',{poolId,tenantId}),{signal});
    },
    submitCapacityRequest(poolId:string,tenantId:string,resources:CapacityResources,key:string) {
      return request<CapacityRequestRow>(capacityPath('requests',{poolId,tenantId}),{
        method:'POST',headers:{'content-type':'application/json','idempotency-key':key},
        body:JSON.stringify({resources}),
      });
    },
    cancelCapacityRequest(poolId:string,tenantId:string,id:string) {
      return request<CapacityRequestRow>(capacityPath('requests',{poolId,tenantId,id}),{method:'DELETE'});
    },
    capacityAllocationControl(poolId:string,signal?:AbortSignal) {
      return request<CapacityAllocationControl>(capacityPath('allocation-control',{poolId}),{signal});
    },
    setCapacityAllocationControl(poolId:string,expectedVersion:number,enabled:boolean) {
      return request<CapacityAllocationControl>(capacityPath('allocation-control',{poolId}),{
        method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify({expectedVersion,enabled}),
      });
    },
    reservationSummary(tenantId:string){
      return request<{policyVersion:number;used:{slots:number;cpuMilliCores:number;memoryMiB:number}}>(
        '/v1/admin/tenants/'+encodeURIComponent(tenantId)+'/reservation-summary');
    },
    quota(id:string,options:{cursor?:string;limit?:number;signal?:AbortSignal}={}) {
      const query=new URLSearchParams();if(options.cursor)query.set('cursor',options.cursor);if(options.limit!==undefined)query.set('limit',String(options.limit));
      return request<TenantQuotaView>('/v1/admin/tenants/'+encodeURIComponent(id)+'/quota'+(query.size?'?'+query:''),{signal:options.signal});
    },
    saveQuota(id:string,input:TenantQuotaEdit,idempotencyKey:string) {
      if(!id||!idempotencyKey)throw new ControlApiError('INVALID_MUTATION',422);
      return request<{policy:TenantQuotaPolicy;enforcement:'NOT_CONNECTED'}>('/v1/admin/tenants/'+encodeURIComponent(id)+'/quota',{method:'PUT',headers:{'content-type':'application/json','idempotency-key':idempotencyKey},body:JSON.stringify(input)});
    },
    archives(signal?:AbortSignal) {return request<DirectoryArchiveStatus>('/v1/admin/archives',{signal});},
    createArchive(expectedVersion:number,reason:string,idempotencyKey:string) {
      if(!idempotencyKey)throw new ControlApiError('INVALID_MUTATION',422);
      return request<DirectoryArchiveManifest>('/v1/admin/archives',{method:'POST',headers:{'content-type':'application/json','idempotency-key':idempotencyKey},body:JSON.stringify({expectedVersion,reason})});
    },
    archive(id:string,signal?:AbortSignal) {return request<DirectoryArchive>('/v1/admin/archives/'+encodeURIComponent(id),{signal});},
    operation(id:string,signal?:AbortSignal) {
      return request<ControlOperation>('/v1/admin/operations/'+encodeURIComponent(id),{signal});
    },
    audit(options:{domain?:string;tenantId?:string;cursor?:string;limit?:number;signal?:AbortSignal}={}) {
      const query=new URLSearchParams();
      if(options.domain)query.set('domain',options.domain);
      if(options.tenantId)query.set('tenantId',options.tenantId);
      if(options.cursor)query.set('cursor',options.cursor);
      if(options.limit!==undefined)query.set('limit',String(options.limit));
      return request<ControlPage<ManagementAudit>>('/v1/admin/audit'+(query.size?'?'+query:''),{signal:options.signal});
    },
    costCenters(options:{cursor?:string;limit?:number;signal?:AbortSignal}={}) {
      const query=new URLSearchParams();
      if(options.cursor)query.set('cursor',options.cursor);
      if(options.limit!==undefined)query.set('limit',String(options.limit));
      return request<ControlPage<CostCenter>>('/v1/admin/cost-centers'+(query.size?'?'+query:''),{signal:options.signal});
    },
    saveCostCenter(id:string,input:CostCenterEdit,idempotencyKey:string,create=false) {
      if(!id||!idempotencyKey)throw new ControlApiError('INVALID_MUTATION',422);
      return request<CostCenter>('/v1/admin/cost-centers'+(create?'':'/'+encodeURIComponent(id)),{method:create?'POST':'PUT',
        headers:{'content-type':'application/json','idempotency-key':idempotencyKey},body:JSON.stringify({id,...input})});
    },
    tenants(options:{cursor?:string;limit?:number;region?:string;tenantId?:string;signal?:AbortSignal}={}) {
      const query=new URLSearchParams();
      if(options.cursor) query.set('cursor',options.cursor);
      if(options.limit!==undefined) query.set('limit',String(options.limit));
      if(options.region) query.set('region',options.region);
      if(options.tenantId) query.set('tenantId',options.tenantId);
      return request<ControlPage<DirectoryTenant>&{regions:string[]}>('/v1/admin/tenants'+(query.size?'?'+query:''),{signal:options.signal});
    },
    createTenant(id:string,input:Omit<TenantEdit,'expectedVersion'>,idempotencyKey:string,signal?:AbortSignal) {
      if(!id || !idempotencyKey) throw new ControlApiError('INVALID_MUTATION',422);
      return request<DirectoryTenant>('/v1/admin/tenants',{method:'POST',signal,
        headers:{'content-type':'application/json','idempotency-key':idempotencyKey},body:JSON.stringify({id,...input})});
    },
    saveTenant(id:string,input:TenantEdit,idempotencyKey:string,signal?:AbortSignal) {
      if(!id || !idempotencyKey) throw new ControlApiError('INVALID_MUTATION',422);
      return request<DirectoryTenant>('/v1/admin/tenants/'+encodeURIComponent(id),{
        method:'PUT',signal,headers:{'content-type':'application/json','idempotency-key':idempotencyKey},body:JSON.stringify(input)
      });
    }
  };
}







