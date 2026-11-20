/** Pure preflight over configuration drafts and server-collected target observations. */
import {canonicalConfigContent,type ConfigContent} from './config_versions.js';
import {ControlError} from '../control/contracts.js';

const fields={
  defaultTaskTimeoutSeconds:{effect:'NEW_TASKS',risk:'Changes the timeout for tasks created after application.'},
  unhealthyNodeThresholdBps:{effect:'HOT_RELOAD',risk:'Changes the threshold used to report unhealthy nodes.'},
  modelCredentialRef:{effect:'RESTART_REQUIRED',risk:'The current release workflow cannot restart targets.'},
} as const;
type Field=keyof typeof fields;
type Effect=(typeof fields)[Field]['effect'];

export interface ConfigTarget {
  id:string;
  fields:readonly Field[];
}

export interface ConfigTargetObservation {
  targetId:string;
  observedAt:number;
  healthy:boolean;
}

export interface ConfigChange {
  field:Field;
  before:ConfigContent[Field]|null;
  after:ConfigContent[Field]|null;
  effect:Effect;
  risk:string;
  targetIds:string[];
}

export interface ConfigPreflight {
  eligible:boolean;
  changes:ConfigChange[];
  blockers:{code:string;targetId?:string;field?:Field}[];
}

export function configScopeTargetIds(source:string|undefined,scope:string):string[] {
  let registry:unknown;
  try{registry=JSON.parse(source??'{}');}catch{throw new ControlError('CONFIG_TARGET_REGISTRY_INVALID',503);}
  if(!registry||typeof registry!=='object'||Array.isArray(registry)){
    throw new ControlError('CONFIG_TARGET_REGISTRY_INVALID',503);
  }
  if(!Object.hasOwn(registry,scope))return [];
  const ids=(registry as Record<string,unknown>)[scope];
  if(!Array.isArray(ids)||ids.length>24||ids.some(id=>typeof id!=='string'||!/^[a-z0-9-]{1,64}$/.test(id))||
    new Set(ids).size!==ids.length)throw new ControlError('CONFIG_TARGET_REGISTRY_INVALID',503);
  return [...ids].sort();
}

export function assessConfigImpact(current:ConfigContent|null,proposed:unknown,targets:readonly ConfigTarget[],
  observations:readonly ConfigTargetObservation[],now:number,maxObservationAgeMs:number,allowRollbackRemoval=false):ConfigPreflight {
  if(!Number.isSafeInteger(now)||now<0||!Number.isSafeInteger(maxObservationAgeMs)||maxObservationAgeMs<1){
    throw new ControlError('INVALID_PREFLIGHT_WINDOW',422);
  }
  let before:ConfigContent,after:ConfigContent;
  try{
    before=current===null?{}:canonicalConfigContent(current);
    after=canonicalConfigContent(proposed);
  }catch(error){
    if(!(error instanceof ControlError))throw error;
    return {eligible:false,changes:[],blockers:[{code:'INVALID_CONFIG_CONTENT'}]};
  }

  const blockers:ConfigPreflight['blockers']=[],changes:ConfigChange[]=[];
  const targetIds=new Set<string>();
  for(const target of targets){
    if(!target.id||targetIds.has(target.id))throw new ControlError('INVALID_CONFIG_TARGETS',422);
    targetIds.add(target.id);
  }
  const byTarget=new Map<string,ConfigTargetObservation>();
  for(const observation of observations){
    if(byTarget.has(observation.targetId))throw new ControlError('DUPLICATE_CONFIG_OBSERVATION',422);
    byTarget.set(observation.targetId,observation);
  }
  const affected=new Set<string>();
  for(const field of Object.keys(fields) as Field[]){
    if(before[field]===after[field]){
      if(field==='modelCredentialRef'&&after[field]!==undefined)blockers.push({code:'CONFIG_RESTART_UNSUPPORTED',field});
      continue;
    }
    const ids=targets.filter(target=>target.fields.includes(field)).map(target=>target.id).sort();
    changes.push({field,before:before[field]??null,after:after[field]??null,...fields[field],targetIds:ids});
    if(!ids.length)blockers.push({code:'NO_APPLICABLE_CONFIG_TARGET',field});
    if(fields[field].effect==='RESTART_REQUIRED')blockers.push({code:'CONFIG_RESTART_UNSUPPORTED',field});
    if(after[field]===undefined&&!allowRollbackRemoval)blockers.push({code:'CONFIG_FIELD_REMOVAL_UNSUPPORTED',field});
    for(const id of ids)affected.add(id);
  }
  if(after.defaultTaskTimeoutSeconds!==undefined&&after.defaultTaskTimeoutSeconds<5){
    blockers.push({code:'CONFIG_TARGET_FIELD_UNSUPPORTED',field:'defaultTaskTimeoutSeconds'});
  }
  if(!changes.length)blockers.push({code:'NO_CONFIG_CHANGES'});
  for(const targetId of [...affected].sort()){
    const observation=byTarget.get(targetId);
    if(!observation)blockers.push({code:'CONFIG_OBSERVATION_MISSING',targetId});
    else if(!Number.isSafeInteger(observation.observedAt)||observation.observedAt<0||
      observation.observedAt>now||now-observation.observedAt>maxObservationAgeMs){
      blockers.push({code:'CONFIG_OBSERVATION_STALE',targetId});
    }else if(observation.healthy!==true)blockers.push({code:'CONFIG_TARGET_UNHEALTHY',targetId});
  }
  return {eligible:blockers.length===0,changes,blockers};
}
