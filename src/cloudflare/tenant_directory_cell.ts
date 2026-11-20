import {AuthorityArchiveStore,type ArchiveWrite} from '../finops/authority_archive.js';
import {RateCatalog,assertRateAdministrator,type RateWrite} from '../finops/rate_cards.js';
import { timingSafeEqual } from 'node:crypto';
import type { DurableObjectStateLike } from './env.js';
import { TenantDirectory, DirectoryStorage, TenantMutation, CostCenterMutation } from '../control/tenant_directory.js';
import { ControlActor, ControlError } from '../control/contracts.js';
/** Internal binding only; callers require the directory-specific service credential. */
export class TenantDirectoryCell {
  private readonly archives:AuthorityArchiveStore|null;
  private readonly rates:RateCatalog|null;
  private readonly directory: TenantDirectory | null;
  constructor(ctx: DurableObjectStateLike & {storage: {transactionSync?: DirectoryStorage['transactionSync']}}, private readonly env: {ANSTS_DIRECTORY_SERVICE_TOKEN?:string} = {}) {
    this.archives=typeof ctx.storage.transactionSync==='function'?new AuthorityArchiveStore({sql:ctx.storage.sql,transactionSync:ctx.storage.transactionSync.bind(ctx.storage)},'DIRECTORY'):null;
    this.rates = typeof ctx.storage.transactionSync==='function'?new RateCatalog({sql:ctx.storage.sql,transactionSync:ctx.storage.transactionSync.bind(ctx.storage)}):null;
    this.directory = typeof ctx.storage.transactionSync==='function' ? new TenantDirectory({sql:ctx.storage.sql,transactionSync:ctx.storage.transactionSync.bind(ctx.storage)}) : null;
  }
  async fetch(request: Request): Promise<Response> {
    const respond=(body:unknown,status:number)=>Response.json(body,{status,headers:{'cache-control':'no-store'}});
    const token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
    const supplied=request.headers.get('authorization')??'';
    const expected='Bearer '+(token??'');
    if (!token || token.length<32 || Buffer.byteLength(supplied)!==Buffer.byteLength(expected) || !timingSafeEqual(Buffer.from(supplied),Buffer.from(expected))) return respond({code:'SERVICE_AUTH_REQUIRED'},401);
    if (!this.directory) return respond({code:'DURABLE_TRANSACTION_REQUIRED'},503);
    if (request.method!=='POST') return respond({code:'NOT_FOUND'},404);
    try {
      const raw=await request.text();
      if(raw.length>65536) return respond({code:'PAYLOAD_TOO_LARGE'},413);
      const body=JSON.parse(raw) as {actor:ControlActor;input:unknown;page?:{limit?:number;cursor?:string;domain?:string};operationId?:string;archiveId?:string;tenantId?:string;rateCardVersion?:number};
      if(!body?.actor || !['VIEWER','OPERATOR','SRE_ADMIN'].includes(body.actor.role) || typeof body.actor.sub!=='string' || !body.actor.sub.trim()) return respond({code:'INVALID_ACTOR'},403);
      const path=new URL(request.url).pathname;
      if(path==='/finops-archive-status')return respond(this.archives!.status(body.actor,null),200);
      if(path==='/finops-archive-create')return respond(this.archives!.create(body.actor,null,body.input as ArchiveWrite,token),200);
      if(path==='/finops-archive-read')return respond(this.archives!.read(body.actor,null,body.archiveId??''),200);
      if(path==='/rate-list'||path==='/rate-audit')return respond(this.rates!.page(body.actor,body.page??{},token,path==='/rate-audit'),200);
      if(path==='/rate-create')return respond({rateCard:this.rates!.create(body.actor,body.input as RateWrite)},200);
      if(path==='/rate-resolve'){assertRateAdministrator(body.actor);return respond({rateCard:this.rates!.get(body.rateCardVersion!)},200);}
      if(path==='/accounting-context'){
        const tenant=this.directory.tenant(body.actor,body.tenantId??'');
        const center=this.directory.costCenters(body.actor).find(c=>c.id===tenant.costCenter);
        return respond({tenant,costCenterVersion:center?.version??null},200);
      }
      if(path==='/tenant') return respond(this.directory.tenant(body.actor,body.tenantId??''),200);
      if(path==='/archive-list') return respond(this.directory.archiveStatus(body.actor),200);
      if(path==='/archive-read') return respond(this.directory.readArchive(body.actor,body.archiveId??''),200);
      if(path==='/archive-create') return respond(this.directory.archive(body.actor,body.input as {expectedVersion:number;reason:string;idempotencyKey:string;requestId:string}),200);
      if(path==='/operation') return respond(this.directory.operation(body.actor,body.operationId??''),200);
      if(path==='/cost-centers') return respond(this.directory.costCentersPage(body.actor,body.page),200);
      if(path==='/cost-center-mutate') {
        const input=body.input as CostCenterMutation;const result=this.directory.mutateCostCenter(body.actor,input);
        const response=respond(result,200);response.headers.set('x-ansts-operation-id',this.directory.operationId('cost-center',input.id,input.idempotencyKey));return response;
      }
      if(path==='/list') return respond(this.directory.listPage(body.actor,body.page),200);
      if(path==='/audit') return respond(this.directory.unifiedAuditPage(body.actor,body.page),200);
      if(path==='/mutate') {
        const input=body.input as TenantMutation;const result=this.directory.mutate(body.actor,input);
        const response=respond(result,200);response.headers.set('x-ansts-operation-id',this.directory.operationId('tenant',input.id,input.idempotencyKey));return response;
      }
      return respond({code:'NOT_FOUND'},404);
    } catch(error) {
      if(error instanceof ControlError) return respond({code:error.code},error.status);
      return respond({code:error instanceof SyntaxError?'INVALID_JSON':'DIRECTORY_UNAVAILABLE'},error instanceof SyntaxError?400:503);
    }
  }
}
