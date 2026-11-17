import type { DurableObjectNamespaceLike } from '../cloudflare/env.js';
import { ControlActor } from './contracts.js';
export interface DirectoryApiEnv {
  ANSTS_TENANT_CONTROL_CELL?: DurableObjectNamespaceLike;
  ANSTS_TENANT_DIRECTORY_CELL?: DurableObjectNamespaceLike;
  ANSTS_DIRECTORY_SERVICE_TOKEN?: string;
  /** Single authoritative service origin; no write failover or local fallback. */
  ANSTS_DIRECTORY_URL?: string;
}
/** Called only after Gateway authentication. Never accepts actor fields from browser JSON. */
export async function handleDirectoryApi(request: Request, env: DirectoryApiEnv, actor: ControlActor & {readOnly?:boolean}): Promise<Response> {
  const url=new URL(request.url);
  const quota=/^\/v1\/admin\/tenants\/[^/]+\/quota$/.test(url.pathname);
  const usage=/^\/v1\/admin\/tenants\/[^/]+\/usage$/.test(url.pathname),usageRead=usage&&request.method==='GET',tenantControl=quota||usage;
  const quotaRead=quota&&request.method==='GET',quotaWrite=quota&&request.method==='PUT';
  const archiveRead=request.method==='GET'&&/^\/v1\/admin\/archives\/[^/]+$/.test(url.pathname);
  const archiveCreate=request.method==='POST'&&url.pathname==='/v1/admin/archives';
  const archives=archiveRead||archiveCreate||(request.method==='GET'&&url.pathname==='/v1/admin/archives');
  const centers=url.pathname==='/v1/admin/cost-centers'||url.pathname.startsWith('/v1/admin/cost-centers/');
  const listing=request.method==='GET' && (url.pathname==='/v1/admin/tenants'||url.pathname==='/v1/admin/cost-centers');
  const operation=request.method==='GET'&&/^\/v1\/admin\/operations\/[^/]+$/.test(url.pathname);
  const audit=request.method==='GET' && url.pathname==='/v1/admin/audit';
  const updating=request.method==='PUT' && /^\/v1\/admin\/(tenants|cost-centers)\/[^/]+$/.test(url.pathname);
  const creating=request.method==='POST' && (url.pathname==='/v1/admin/tenants'||url.pathname==='/v1/admin/cost-centers');
  const result=(code:string,status:number)=>Response.json({code},{status,headers:{'cache-control':'no-store'}});
  let quotaTenantId='';
  if(tenantControl){try{quotaTenantId=decodeURIComponent(url.pathname.split('/')[4]);}catch{return result('INVALID_TENANT_ID',422);}if(quotaTenantId.length>128||!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]*$/.test(quotaTenantId))return result('INVALID_TENANT_ID',422);if(actor.scopeTenant!=='all'&&actor.scopeTenant!==quotaTenantId)return result('TENANT_NOT_FOUND',404);}
  if (!usageRead && !quotaRead && !quotaWrite && !archives && !listing && !audit && !operation && !updating && !creating) return result('NOT_FOUND',404);
  if ((quotaWrite || archives || audit || operation || updating || creating) && actor.role!=='SRE_ADMIN') return result('CONTROL_FORBIDDEN',403);
  if ((quotaWrite || archiveCreate || updating || creating) && actor.readOnly) return result('CONTROL_FORBIDDEN',403);
  if (((tenantControl?!env.ANSTS_TENANT_CONTROL_CELL:!env.ANSTS_TENANT_DIRECTORY_CELL) && !env.ANSTS_DIRECTORY_URL) || !env.ANSTS_DIRECTORY_SERVICE_TOKEN || env.ANSTS_DIRECTORY_SERVICE_TOKEN.length<32) return result('DIRECTORY_UNAVAILABLE',503);
  let input:unknown;
  if (quotaWrite || archiveCreate || updating || creating) {
    const raw=await request.text();
    if (raw.length>65536) return result('PAYLOAD_TOO_LARGE',413);
    try {
      const value=JSON.parse(raw);
      if (!value || typeof value!=='object' || Array.isArray(value)) return result('INVALID_JSON',400);
      // Explicit allowlist; body-supplied identity and audit fields are discarded.
      input=quotaWrite?{resources:value.resources,budget:value.budget,expectedVersion:value.expectedVersion,reason:value.reason,idempotencyKey:request.headers.get("idempotency-key"),requestId:crypto.randomUUID()}:{id:updating?decodeURIComponent(url.pathname.split('/').pop()!):value.id,
        region:value.region,name:value.name,costCenter:value.costCenter,status:value.status,
        expectedVersion:creating?0:value.expectedVersion,reason:value.reason,
        idempotencyKey:request.headers.get('idempotency-key'),requestId:crypto.randomUUID()};
    } catch {return result('INVALID_JSON',400);}
  }
  try {
    const action=usage?'usage-read':quota?(quotaWrite?'policy-write':'policy-read'):archives?(archiveCreate?'archive-create':archiveRead?'archive-read':'archive-list'):operation?'operation':centers?(listing?'cost-centers':'cost-center-mutate'):listing?'list':audit?'audit':'mutate';
    let destination='https://directory.internal/'+action;
    let dispatch:(request:Request)=>Promise<Response>;
    if(env.ANSTS_DIRECTORY_URL) {
      const base=new URL(env.ANSTS_DIRECTORY_URL);
      if(!['http:','https:'].includes(base.protocol)||base.username||base.password||base.search||base.hash||base.pathname!=='/') return result('INVALID_DIRECTORY_ORIGIN',503);
      destination=base.origin+(tenantControl?'/internal/v1/tenant-control/':'/internal/v1/directory/')+action;
      dispatch=request=>fetch(request,{redirect:'error',signal:AbortSignal.timeout(3000)});
    } else {
      const ns=tenantControl?env.ANSTS_TENANT_CONTROL_CELL:env.ANSTS_TENANT_DIRECTORY_CELL;
      if(!ns)return result("TENANT_CONTROL_UNAVAILABLE",503);
      const stub=ns.get(ns.idFromName(tenantControl?'tenant-control-v1:'+quotaTenantId:'tenant-directory-v1'));
      dispatch=request=>stub.fetch(request);
    }
    const response=await dispatch(new Request(destination,{
      method:'POST',headers:{'content-type':'application/json',authorization:'Bearer '+env.ANSTS_DIRECTORY_SERVICE_TOKEN},
      body:JSON.stringify({tenantId:tenantControl?quotaTenantId:undefined,archiveId:archiveRead?decodeURIComponent(url.pathname.split('/').pop()!):undefined,operationId:operation?decodeURIComponent(url.pathname.split('/').pop()!):undefined,page:{domain:url.searchParams.get("domain")??undefined,limit:url.searchParams.has("limit")?Number(url.searchParams.get("limit")):undefined,cursor:url.searchParams.get("cursor")??undefined},actor:{sub:actor.sub,role:actor.role,scopeTenant:actor.scopeTenant,scopeRegion:actor.scopeRegion},input})
    }));
    return new Response(response.body,{status:response.status,headers:{'content-type':'application/json','cache-control':'no-store',...(response.headers.get('x-ansts-operation-id')?{'x-ansts-operation-id':response.headers.get('x-ansts-operation-id')!}:{})}});
  } catch {return result('DIRECTORY_UNAVAILABLE',503);}
}
