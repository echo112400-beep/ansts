/** Gateway preflight uses immutable drafts and server-owned target observations. */
import {assessConfigImpact,configScopeTargetIds,type ConfigTarget,type ConfigTargetObservation} from '../config_release/config_preflight.js';
import type {ConfigVersion} from '../config_release/config_versions.js';
import type {ControlActor} from '../control/contracts.js';
import {collectSystemTopology,type TopologyEnvironment} from './system_topology.js';

export interface ConfigPreflightEnv extends TopologyEnvironment {
  ANSTS_DIRECTORY_URL?:string;
  ANSTS_CONFIG_SERVICE_TOKEN?:string;
  ANSTS_CONFIG_SCOPE_TARGETS_JSON?:string;
}

const validationPath=/^\/v1\/admin\/config-bundles\/([a-z0-9][a-z0-9_-]{0,79})\/(draft|validate|submit|approvals(?:\/status)?|rollback|releases(?:\/(?:status|pause|resume))?)$/;
export function isConfigPreflightPath(path:string):boolean {return validationPath.test(path);}
const response=(body:unknown,status=200)=>Response.json(body,{status,headers:{'cache-control':'no-store'}});

export async function handleConfigPreflight(request:Request,env:ConfigPreflightEnv,actor:ControlActor&{readOnly?:boolean}):Promise<Response> {
  const match=validationPath.exec(new URL(request.url).pathname);
  if(!match)return response({code:'NOT_FOUND'},404);
  const operation=match[2],read=request.method==='GET';
  if(operation==='draft'?!(read||request.method==='POST'):
    request.method!==(operation.endsWith('/status')?'GET':'POST'))return response({code:'NOT_FOUND'},404);
  if(actor.role!=='SRE_ADMIN'||actor.readOnly||actor.scopeTenant!=='all'||actor.scopeRegion!=='all'){
    return response({code:'CONTROL_FORBIDDEN'},403);
  }
  const generationQuery=new URL(request.url).searchParams.get('generation');
  const raw=request.method==='GET'?'{}':await request.text();
  if(raw.length>4096)return response({code:'PAYLOAD_TOO_LARGE'},413);
  const [,scope,action]=match;
  let body:{revision:number;sequence:number;approved:boolean;releaseId:string;generation:number;batchSize:number;healthWindowMs:number;sourceGeneration:number;replacesGeneration:number;baseRevision:number;content:unknown};
  try{body=JSON.parse(raw);}catch{return response({code:'INVALID_JSON'},400);}
  const decision=action==='approvals';
  const controls=action==='releases/pause'||action==='releases/resume',status=action==='releases/status',start=action==='releases';
  const rollback=action==='rollback',draftRequest=action==='draft',approvalStatus=action==='approvals/status';
  const allowed=draftRequest?(read?[]:['baseRevision','content']):approvalStatus?[]:rollback?['sourceGeneration','replacesGeneration','baseRevision']:controls?['releaseId','generation']:status?[]:start?['revision','releaseId','batchSize','healthWindowMs']:
    decision?['revision','sequence','approved']:['revision'];
  if(!body||typeof body!=='object'||Array.isArray(body)||Object.keys(body).some(key=>!allowed.includes(key))||
    !draftRequest&&!approvalStatus&&!rollback&&!controls&&!status&&(!Number.isSafeInteger(body.revision)||body.revision<1)||
    controls&&(typeof body.releaseId!=='string'||!Number.isSafeInteger(body.generation)||body.generation<1)||
    decision&&(!Number.isSafeInteger(body.sequence)||body.sequence<1||typeof body.approved!=='boolean')){
    return response({code:'INVALID_CONFIG_PREFLIGHT'},422);
  }

  const token=env.ANSTS_CONFIG_SERVICE_TOKEN;
  if(!token||token.length<32||!env.ANSTS_DIRECTORY_URL)return response({code:'CONFIG_UNAVAILABLE'},503);
  try{
    const origin=new URL(env.ANSTS_DIRECTORY_URL);
    if(!['http:','https:'].includes(origin.protocol)||origin.username||origin.password||origin.search||origin.hash||origin.pathname!=='/'){
      return response({code:'CONFIG_UNAVAILABLE'},503);
    }
    async function dispatch(operation:string,values:object):Promise<Response>{
      return fetch(new URL('/internal/v1/config-versions/'+scope+'/'+operation,origin),{
        method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},
        body:JSON.stringify({actor:{sub:actor.sub,role:actor.role,scopeTenant:actor.scopeTenant,scopeRegion:actor.scopeRegion},...values}),
        signal:AbortSignal.timeout(4000),redirect:'error',
      });
    }
    async function rollout(operation:string,values:object):Promise<Response>{
      const result=await dispatch(operation,values);
      const data=await result.json();
      if(result.ok&&data?.plan)delete data.plan.targetOrigins;
      return response(data,result.status);
    }
    if(approvalStatus)return rollout('approval-status',{});
    if(draftRequest){
      if(read){
        const result=await dispatch('read',{});
        return result.status===404?response(null):response(await result.json(),result.status);
      }
      return rollout('create',{schemaVersion:1,baseRevision:body.baseRevision,content:body.content});
    }
    if(status){
      if(generationQuery!==null&&!/^[1-9][0-9]*$/.test(generationQuery))return response({code:'INVALID_CONFIG_ROLLOUT'},422);
      return rollout('rollout-status',generationQuery===null?{}:{generation:Number(generationQuery)});
    }
    if(rollback)return rollout('rollback-draft',body);
    if(controls)return rollout(action==='releases/pause'?'rollout-pause':'rollout-resume',{releaseId:body.releaseId,generation:body.generation});
    const result=await dispatch('read',{revision:body.revision});
    if(result.status===404)return response({code:'CONFIG_VERSION_NOT_FOUND'},404);
    if(!result.ok)return response({code:'CONFIG_UNAVAILABLE'},503);
    const draft=await result.json() as ConfigVersion;
    if(draft.scope!==scope||draft.revision!==body.revision||draft.schemaVersion!==1||draft.baseRevision!==draft.revision-1){
      return response({code:'CONFIG_UNAVAILABLE'},503);
    }
    let base:ConfigVersion|null=null;
    if(draft.baseRevision){
      const previous=await dispatch('read',{revision:draft.baseRevision});
      if(!previous.ok)return response({code:'CONFIG_BASE_UNAVAILABLE'},503);
      base=await previous.json() as ConfigVersion;
      if(!base||base.scope!==scope||base.revision!==draft.baseRevision)return response({code:'CONFIG_BASE_UNAVAILABLE'},503);
    }

    const ids=configScopeTargetIds(env.ANSTS_CONFIG_SCOPE_TARGETS_JSON,scope);
    const targets:ConfigTarget[]=ids.map(id=>({id,fields:['defaultTaskTimeoutSeconds','unhealthyNodeThresholdBps','modelCredentialRef']}));
    let observations:ConfigTargetObservation[]=[];
    if(targets.length&&env.ANSTS_SYSTEM_TOPOLOGY_JSON){
      // A configured target absent from the live topology remains in the impact and blocks preflight.
      const snapshot=await collectSystemTopology(env).catch(()=>null);
      observations=snapshot?.nodes.flatMap(node=>node.kind==='fleet'&&node.checkedAt!==null&&node.httpStatus!==null?
        [{targetId:node.id,observedAt:node.checkedAt,healthy:node.status==='healthy'}]:[])??[];
    }
    const impact=assessConfigImpact(base?.content??null,draft.content,targets,observations,Date.now(),30000,!!draft.rollback);
    const summary={scope,revision:draft.revision,baseRevision:draft.baseRevision,contentDigest:draft.contentDigest,...impact};
    if(action==='validate')return response(summary);
    const onlyUnavailable=draft.rollback&&impact.blockers.every(item=>
      ['CONFIG_OBSERVATION_MISSING','CONFIG_OBSERVATION_STALE','CONFIG_TARGET_UNHEALTHY'].includes(item.code));
    if(!impact.eligible&&!onlyUnavailable)return response({code:'CONFIG_PREFLIGHT_BLOCKED',...summary},409);
    if(start)return rollout('rollout-start',{revision:body.revision,releaseId:body.releaseId,batchSize:body.batchSize,healthWindowMs:body.healthWindowMs});
    const basis={revision:draft.revision,contentDigest:draft.contentDigest,
      targetIds:[...new Set(impact.changes.flatMap(change=>change.targetIds))].sort()};
    const resultApproval=await dispatch(action,{basis,...(decision?{sequence:body.sequence,approved:body.approved}:{})});
    return response(await resultApproval.json(),resultApproval.status);
  }catch{
    return response({code:'CONFIG_UNAVAILABLE'},503);
  }
}
