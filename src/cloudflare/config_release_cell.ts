/** Private, per-scope authority for immutable configuration drafts. */
import {configScopeTargetIds} from'../config_release/config_preflight.js';
import {ConfigApprovals,type ConfigApprovalBasis} from '../config_release/config_approvals.js';
import {ConfigVersions,canonicalConfigContent} from '../config_release/config_versions.js';
import {ControlError,type ControlActor} from '../control/contracts.js';
import {ConfigRollouts} from '../config_release/config_rollout.js';
import {stepConfigRollout} from './config_rollout_targets.js';
import {equalBodyToken} from './body_auth.js';
import type {CloudflareCelldEnv,DurableObjectStateLike} from './env.js';

const route=/^\/internal\/v1\/config-versions\/([a-z0-9][a-z0-9_-]{0,79})\/(read|create|submit|approvals|approval-status|rollback-draft|rollout-start|rollout-status|rollout-step|rollout-pause|rollout-resume)$/;
const reply=(body:unknown,status=200)=>Response.json(body,{status,headers:{'cache-control':'no-store'}});

function authenticated(request:Request,env:CloudflareCelldEnv):boolean {
  const token=env.ANSTS_CONFIG_SERVICE_TOKEN;
  return !!token&&token.length>=32&&equalBodyToken(request.headers.get('authorization')??'','Bearer '+token);
}

export async function handleConfigVersionRequest(request:Request,env:CloudflareCelldEnv):Promise<Response> {
  if(!authenticated(request,env))return reply({code:'SERVICE_AUTH_REQUIRED'},401);
  const match=route.exec(new URL(request.url).pathname);
  if(!match||request.method!=='POST')return reply({code:'NOT_FOUND'},404);
  const namespace=env.ANSTS_CONFIG_RELEASE_CELL;
  if(!namespace)return reply({code:'CONFIG_UNAVAILABLE'},503);
  return namespace.get(namespace.idFromName('config-version-v1:'+match[1])).fetch(request);
}

export class ConfigReleaseCell {
  private readonly versions:ConfigVersions|null;
  private readonly rollouts:ConfigRollouts|null;
  private stepping=false;
  private readonly approvals:ConfigApprovals|null;
  constructor(private readonly ctx:DurableObjectStateLike,private readonly env:CloudflareCelldEnv){
    const storage=ctx.storage;
    const transactional=storage.transactionSync?{sql:storage.sql,transactionSync:storage.transactionSync.bind(storage)}:null;
    this.versions=transactional?new ConfigVersions(transactional):null;
    this.rollouts=transactional?new ConfigRollouts(transactional):null;
    this.approvals=transactional?new ConfigApprovals(transactional):null;
  }

  private configAdminRoster():string[] {
    const identities=JSON.parse(this.env.ANSTS_GITHUB_IDENTITIES_JSON??'{}') as Record<string,{role?:string;scopeTenant?:string}>;
    return Object.entries(identities).filter(([,entry])=>entry?.role==='SRE_ADMIN'&&entry.scopeTenant==='all').map(([id])=>'github:'+id);
  }

  private async armRollout(at:number):Promise<void> {
    const alarm=await this.ctx.storage.getAlarm();
    if(alarm===null||alarm>at)await this.ctx.storage.setAlarm(at);
  }

  private async advance(scope:string,current:NonNullable<ReturnType<ConfigRollouts['latest']>>){
    const actor:ControlActor={sub:'system:config-rollout',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'};
    this.stepping=true;
    try{
      const draft=this.versions!.get(actor,scope,current.plan.revision);
      if(!draft||draft.contentDigest!==current.plan.contentDigest||!this.env.ANSTS_CONFIG_TARGET_TOKEN){
        return this.rollouts!.pause(current.plan.releaseId,current.generation);
      }
      const updated=await stepConfigRollout(this.rollouts!,current,draft.content,this.env.ANSTS_CONFIG_TARGET_TOKEN);
      if(updated.status==='APPLYING'||updated.status==='VERIFYING'){
        await this.armRollout(Math.max(Date.now()+500,updated.verifyAfter??0));
      }
      return updated;
    }finally{this.stepping=false;}
  }

  async alarm():Promise<void> {
    const current=this.rollouts?.latest();
    if(!current?.plan.scope||!['APPLYING','VERIFYING'].includes(current.status))return;
    // Arm before network calls so a worker restart can continue the persisted batch.
    await this.ctx.storage.setAlarm(Date.now()+300000);
    if(this.stepping)return;
    const live=this.rollouts?.latest();
    if(!live||live.generation!==current.generation||!['APPLYING','VERIFYING'].includes(live.status))return;
    await this.advance(current.plan.scope,live);
  }

  async fetch(request:Request):Promise<Response> {
    if(!authenticated(request,this.env))return reply({code:'SERVICE_AUTH_REQUIRED'},401);
    const match=route.exec(new URL(request.url).pathname);
    if(!match||request.method!=='POST')return reply({code:'NOT_FOUND'},404);
    const [,scope,action]=match,namespace=this.env.ANSTS_CONFIG_RELEASE_CELL;
    if(!this.versions||!namespace)return reply({code:'CONFIG_UNAVAILABLE'},503);
    if(namespace.idFromName('config-version-v1:'+scope).toString()!==this.ctx.id.toString()){
      return reply({code:'CONFIG_SCOPE_CONFLICT'},409);
    }
    try{
      const raw=await request.text();
      if(raw.length>16384)return reply({code:'PAYLOAD_TOO_LARGE'},413);
      const body=JSON.parse(raw) as {actor:ControlActor;revision?:number;schemaVersion:number;baseRevision:number;content:unknown;basis:ConfigApprovalBasis;sequence:number;approved:boolean;releaseId:string;batchSize:number;healthWindowMs:number;generation:number;sourceGeneration:number;replacesGeneration:number};
      const allowed=action==='approval-status'?['actor']:action==='rollback-draft'?['actor','sourceGeneration','replacesGeneration','baseRevision']:action==='rollout-status'?['actor','generation']:action==='rollout-start'?['actor','revision','releaseId','batchSize','healthWindowMs']:
        ['rollout-pause','rollout-resume'].includes(action)?['actor','releaseId','generation']:action.startsWith('rollout-')?['actor']:action==='read'?['actor','revision']:action==='create'?['actor','schemaVersion','baseRevision','content']:
        action==='submit'?['actor','basis']:['actor','basis','sequence','approved'];
      if(!body||typeof body!=='object'||Array.isArray(body)||Object.keys(body).some(key=>!allowed.includes(key))){
        return reply({code:'INVALID_CONFIG_REQUEST'},422);
      }
      if(action==='approval-status'){
        if(body.actor?.role!=='SRE_ADMIN')return reply({code:'CONTROL_FORBIDDEN'},403);
        const version=this.versions.get(body.actor,scope);
        const approval=this.approvals!.latest();
        return reply(approval?.basis.revision===version?.revision?approval:null);
      }
      if(action==='rollback-draft'){
        if(body.actor?.role!=='SRE_ADMIN'||!this.configAdminRoster().includes(body.actor.sub)){
          return reply({code:'CONTROL_FORBIDDEN'},403);
        }
        if(!Number.isSafeInteger(body.sourceGeneration)||body.sourceGeneration<1||
          !Number.isSafeInteger(body.replacesGeneration)||body.sourceGeneration>=body.replacesGeneration){
          return reply({code:'INVALID_CONFIG_ROLLBACK'},422);
        }
        const active=this.rollouts!.latest(),source=this.rollouts!.get(body.sourceGeneration);
        const targetIds=configScopeTargetIds(this.env.ANSTS_CONFIG_SCOPE_TARGETS_JSON,scope);
        if(!source||source.status!=='SUCCEEDED'||!source.plan.revision||
          JSON.stringify([...source.plan.batches.flat()].sort())!==JSON.stringify(targetIds)){
          return reply({code:'CONFIG_ROLLBACK_SOURCE_UNVERIFIED'},409);
        }
        if(!active||active.generation!==body.replacesGeneration||!['PAUSED','SUCCEEDED'].includes(active.status)){
          return reply({code:'STALE_CONFIG_ROLLOUT'},409);
        }
        const previous=this.versions.get(body.actor,scope,source.plan.revision);
        if(!previous||previous.contentDigest!==source.plan.contentDigest)return reply({code:'CONFIG_ROLLBACK_SOURCE_UNVERIFIED'},409);
        const rollback={sourceGeneration:source.generation,replacesGeneration:active.generation};
        const latest=this.versions.get(body.actor,scope);
        if(latest?.baseRevision===body.baseRevision&&latest.contentDigest===previous.contentDigest&&
          latest.rollback?.sourceGeneration===rollback.sourceGeneration&&latest.rollback.replacesGeneration===rollback.replacesGeneration){
          return reply(latest,201);
        }
        return reply(this.versions.create(body.actor,scope,1,body.baseRevision,previous.content,Date.now(),rollback),201);
      }
      if(action.startsWith('rollout-')){
        if(body.actor?.role!=='SRE_ADMIN'||body.actor.scopeTenant!=='all'||body.actor.scopeRegion!=='all'){
          return reply({code:'CONTROL_FORBIDDEN'},403);
        }
        const rollouts=this.rollouts!;
        const current=rollouts.latest();
        if(action==='rollout-status'){
          if(body.generation!==undefined&&(!Number.isSafeInteger(body.generation)||body.generation<1))return reply({code:'INVALID_CONFIG_ROLLOUT'},422);
          const result=body.generation===undefined?current:rollouts.get(body.generation);
          if(!result)return reply({code:'CONFIG_ROLLOUT_NOT_FOUND'},404);
          const verified=new Set(result.verifiedBatches.flat().map(item=>item.targetId));
          return reply({...result,pendingTargetIds:result.plan.batches.flat().filter(id=>!verified.has(id))});
        }
        if(this.stepping)return reply({code:'CONFIG_ROLLOUT_BUSY'},409);
        if(action==='rollout-start'){
          const draft=this.versions.get(body.actor,scope);
          if(!draft||draft.revision!==body.revision)return reply({code:'CONFIG_APPROVAL_STALE'},409);
          if(!Number.isSafeInteger(body.batchSize)||body.batchSize<1||body.batchSize>24){
            return reply({code:'INVALID_CONFIG_ROLLOUT'},422);
          }
          const targetIds=configScopeTargetIds(this.env.ANSTS_CONFIG_SCOPE_TARGETS_JSON,scope);
          const roster=this.configAdminRoster();
          this.approvals!.requireApproved(body.actor,roster,{revision:draft.revision,contentDigest:draft.contentDigest,targetIds});
          const origins=JSON.parse(this.env.ANSTS_CONFIG_TARGET_ORIGINS_JSON??'{}') as Record<string,string>;
          const batches:string[][]=[];
          for(let offset=0;offset<targetIds.length;offset+=body.batchSize)batches.push(targetIds.slice(offset,offset+body.batchSize));
          if(targetIds.some(id=>!origins||!Object.hasOwn(origins,id)||typeof origins[id]!=='string')){
            return reply({code:'CONFIG_TARGET_UNAVAILABLE'},503);
          }
          await this.armRollout(Date.now()+500);
          const latest=this.versions.get(body.actor,scope);
          if(latest?.revision!==draft.revision)return reply({code:'CONFIG_APPROVAL_STALE'},409);
          this.approvals!.requireApproved(body.actor,roster,{revision:draft.revision,contentDigest:draft.contentDigest,targetIds});
          return reply(rollouts.start({scope,releaseId:body.releaseId,revision:draft.revision,contentDigest:draft.contentDigest,rollback:draft.rollback,
            batches,healthWindowMs:body.healthWindowMs,targetOrigins:Object.fromEntries(targetIds.map(id=>[id,origins[id]]))}));
        }
        if(!current)return reply({code:'CONFIG_ROLLOUT_NOT_FOUND'},404);
        if(action==='rollout-pause'||action==='rollout-resume'){
          if(body.releaseId!==current.plan.releaseId||body.generation!==current.generation){
            return reply({code:'STALE_CONFIG_ROLLOUT'},409);
          }
          if(action==='rollout-pause')return reply(rollouts.pause(body.releaseId,body.generation));
          await this.armRollout(Date.now()+500);
          return reply(rollouts.resume(body.releaseId,body.generation,Date.now()));
        }
        return reply(await this.advance(scope,current));

      }
      if(action==='read'){
        const version=this.versions.get(body.actor,scope,body.revision);
        return version?reply(version):reply({code:'CONFIG_VERSION_NOT_FOUND'},404);
      }
      if(action==='submit'||action==='approvals'){
        const latest=this.versions.get(body.actor,scope);
        if(!latest||!body.basis||latest.revision!==body.basis.revision||latest.contentDigest!==body.basis.contentDigest){
          return reply({code:'CONFIG_APPROVAL_STALE'},409);
        }
        const roster=this.configAdminRoster();
        const targetIds=configScopeTargetIds(this.env.ANSTS_CONFIG_SCOPE_TARGETS_JSON,scope);
        if(!Array.isArray(body.basis.targetIds)||JSON.stringify(body.basis.targetIds)!==JSON.stringify(targetIds)){
          return reply({code:'CONFIG_APPROVAL_STALE'},409);
        }
        const basis={revision:latest.revision,contentDigest:latest.contentDigest,targetIds};
        return reply(action==='submit'?this.approvals!.submit(body.actor,roster,basis):
          this.approvals!.decide(body.actor,roster,body.sequence,basis,body.approved));
      }
      const content=canonicalConfigContent(body.content);
      if(content.modelCredentialRef&&!this.env.ANSTS_CONFIG_SECRET_REFS?.split(',').includes(content.modelCredentialRef)){
        return reply({code:'CONFIG_SECRET_REF_UNKNOWN'},422);
      }
      return reply(this.versions.create(body.actor,scope,body.schemaVersion,body.baseRevision,content),201);
    }catch(error){
      if(error instanceof ControlError)return reply({code:error.code},error.status);
      if(error instanceof SyntaxError)return reply({code:'INVALID_JSON'},400);
      return reply({code:'CONFIG_UNAVAILABLE'},503);
    }
  }
}
