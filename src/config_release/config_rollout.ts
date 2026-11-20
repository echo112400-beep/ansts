/** Durable rollout state for one configuration scope. */
import {ControlError} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';

export interface ConfigRolloutPlan {
  releaseId:string;
  revision?:number;
  scope?:string;
  rollback?:{sourceGeneration:number;replacesGeneration:number};
  targetOrigins?:Readonly<Record<string,string>>;
  contentDigest:string;
  batches:readonly (readonly string[])[];
  healthWindowMs:number;
}

export interface TargetConfigAck {
  releaseId:string;
  generation:number;
  targetId:string;
  actualDigest:string;
}

export interface TargetConfigHealth extends TargetConfigAck {
  observedAt:number;
  healthy:boolean;
}

export interface ConfigRollout {
  plan:ConfigRolloutPlan;
  generation:number;
  batchIndex:number;
  status:'APPLYING'|'VERIFYING'|'PAUSED'|'SUCCEEDED';
  acknowledgements:TargetConfigAck[];
  verifiedBatches:TargetConfigHealth[][];
  verifyAfter:number|null;
  pauseReason:string|null;
}

export class ConfigRollouts {
  constructor(private readonly storage:DirectoryStorage){
    storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS config_rollouts (generation INTEGER PRIMARY KEY, release_id TEXT NOT NULL UNIQUE, payload_json TEXT NOT NULL)'));
  }

  latest():ConfigRollout|null {
    const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM config_rollouts ORDER BY generation DESC LIMIT 1').toArray()[0];
    return row?JSON.parse(row.payload_json) as ConfigRollout:null;
  }

  get(generation:number):ConfigRollout|null {
    const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM config_rollouts WHERE generation = ?',generation).toArray()[0];
    return row?JSON.parse(row.payload_json) as ConfigRollout:null;
  }

  start(input:ConfigRolloutPlan):ConfigRollout {
    if(!input||typeof input.releaseId!=='string'||!/^[a-zA-Z0-9_.:-]{1,128}$/.test(input.releaseId)||
      typeof input.contentDigest!=='string'||!/^[a-f0-9]{64}$/.test(input.contentDigest)||
      !Array.isArray(input.batches)||!input.batches.length||input.batches.length>24||
      input.batches.some(batch=>!Array.isArray(batch)||!batch.length||batch.length>24)||
      !Number.isSafeInteger(input.healthWindowMs)||input.healthWindowMs<1||input.healthWindowMs>3600000){
      throw new ControlError('INVALID_CONFIG_ROLLOUT',422);
    }
    const ids=input.batches.flat();
    if(ids.length>24||ids.some(id=>typeof id!=='string'||!/^[a-z0-9-]{1,64}$/.test(id))||new Set(ids).size!==ids.length){
      throw new ControlError('INVALID_CONFIG_ROLLOUT_TARGETS',422);
    }
    if(input.revision!==undefined&&(!Number.isSafeInteger(input.revision)||input.revision<1))throw new ControlError('INVALID_CONFIG_ROLLOUT',422);
    const plan:ConfigRolloutPlan={releaseId:input.releaseId,...(input.revision===undefined?{}:{revision:input.revision}),...(input.scope?{scope:input.scope}:{}),
      ...(input.rollback?{rollback:{...input.rollback}}:{}),contentDigest:input.contentDigest,
      ...(input.targetOrigins?{targetOrigins:{...input.targetOrigins}}:{}),batches:input.batches.map(batch=>[...batch]),healthWindowMs:input.healthWindowMs};
    return this.storage.transactionSync(()=>{
      const existing=this.latest();
      if(existing?.plan.releaseId===plan.releaseId){
        if(JSON.stringify(existing.plan)!==JSON.stringify(plan))throw new ControlError('CONFIG_ROLLOUT_CONFLICT',409);
        return existing;
      }
      if(plan.rollback&&existing?.generation!==plan.rollback.replacesGeneration)throw new ControlError('STALE_CONFIG_ROLLOUT',409);
      if(existing&&existing.status!=='SUCCEEDED'&&!(existing.status==='PAUSED'&&plan.rollback)){
        throw new ControlError('CONFIG_ROLLOUT_ACTIVE',409);
      }
      if(this.storage.sql.exec('SELECT generation FROM config_rollouts WHERE release_id = ?',plan.releaseId).toArray().length){
        throw new ControlError('CONFIG_ROLLOUT_CONFLICT',409);
      }
      const record:ConfigRollout={plan,generation:(existing?.generation??0)+1,batchIndex:0,status:'APPLYING',
        acknowledgements:[],verifiedBatches:[],verifyAfter:null,pauseReason:null};
      this.storage.sql.exec('INSERT INTO config_rollouts (generation, release_id, payload_json) VALUES (?, ?, ?)',record.generation,plan.releaseId,JSON.stringify(record));
      return record;
    });
  }

  private change(releaseId:string,generation:number,update:(record:ConfigRollout)=>void):ConfigRollout {
    return this.storage.transactionSync(()=>{
      const record=this.latest();
      if(!record||record.generation!==generation||record.plan.releaseId!==releaseId){
        throw new ControlError('STALE_CONFIG_ROLLOUT',409);
      }
      update(record);
      this.storage.sql.exec('UPDATE config_rollouts SET payload_json = ? WHERE generation = ?',JSON.stringify(record),generation);
      return record;
    });
  }

  acknowledge(ack:TargetConfigAck,now:number):ConfigRollout {
    if(!Number.isSafeInteger(now)||now<0)throw new ControlError('INVALID_CONFIG_OBSERVATION_TIME',422);
    return this.change(ack.releaseId,ack.generation,record=>{
      if(!record.plan.batches[record.batchIndex].includes(ack.targetId)||ack.actualDigest!==record.plan.contentDigest){
        throw new ControlError('CONFIG_ACK_MISMATCH',409);
      }
      if(record.acknowledgements.some(entry=>entry.targetId===ack.targetId))return;
      if(record.status!=='APPLYING')throw new ControlError('CONFIG_ROLLOUT_NOT_APPLYING',409);
      record.acknowledgements.push({releaseId:ack.releaseId,generation:ack.generation,targetId:ack.targetId,actualDigest:ack.actualDigest});
      if(record.acknowledgements.length===record.plan.batches[record.batchIndex].length){
        record.status='VERIFYING';
        record.verifyAfter=now+record.plan.healthWindowMs;
      }
    });
  }

  verify(releaseId:string,generation:number,observations:readonly TargetConfigHealth[],now:number):ConfigRollout {
    if(!Number.isSafeInteger(now)||now<0)throw new ControlError('INVALID_CONFIG_OBSERVATION_TIME',422);
    return this.change(releaseId,generation,record=>{
      if(record.status!=='VERIFYING'||record.verifyAfter===null||now<record.verifyAfter){
        throw new ControlError('CONFIG_VERIFICATION_NOT_READY',409);
      }
      const selected:TargetConfigHealth[]=[];
      for(const targetId of record.plan.batches[record.batchIndex]){
        const matches=observations.filter(value=>value.targetId===targetId);
        const value=matches[0];
        if(matches.length!==1||!value||value.releaseId!==releaseId||value.generation!==generation||
          value.actualDigest!==record.plan.contentDigest||!Number.isSafeInteger(value.observedAt)||
          value.observedAt<record.verifyAfter||value.observedAt>now||now-value.observedAt>record.plan.healthWindowMs||value.healthy!==true){
          record.status='PAUSED';
          record.pauseReason='CONFIG_HEALTH_NOT_VERIFIED';
          return;
        }
        selected.push({releaseId,generation,targetId,actualDigest:value.actualDigest,observedAt:value.observedAt,healthy:true});
      }
      record.verifiedBatches.push(selected);
      if(record.batchIndex+1===record.plan.batches.length){record.status='SUCCEEDED';return;}
      record.batchIndex++;
      record.acknowledgements=[];
      record.verifyAfter=null;
      record.status='APPLYING';
    });
  }

  pause(releaseId:string,generation:number):ConfigRollout {
    return this.change(releaseId,generation,record=>{
      if(record.status==='SUCCEEDED')throw new ControlError('CONFIG_ROLLOUT_TERMINAL',409);
      record.status='PAUSED';
      record.pauseReason??='CONFIG_APPLY_INTERRUPTED';
    });
  }

  resume(releaseId:string,generation:number,now:number):ConfigRollout {
    if(!Number.isSafeInteger(now)||now<0)throw new ControlError('INVALID_CONFIG_OBSERVATION_TIME',422);
    return this.change(releaseId,generation,record=>{
      if(record.status!=='PAUSED')throw new ControlError('CONFIG_ROLLOUT_NOT_PAUSED',409);
      const hasAllAcks=record.acknowledgements.length===record.plan.batches[record.batchIndex].length;
      record.status=hasAllAcks?'VERIFYING':'APPLYING';
      record.verifyAfter=hasAllAcks?now+record.plan.healthWindowMs:null;
      record.pauseReason=null;
    });
  }
}
