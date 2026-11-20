/** Trusted HTTP client for rollout target operations. */
import {ControlError} from '../control/contracts.js';
import {ConfigRollouts,type ConfigRollout,type TargetConfigAck} from '../config_release/config_rollout.js';
import type {ConfigContent} from '../config_release/config_versions.js';

export async function stepConfigRollout(rollouts:ConfigRollouts,record:ConfigRollout,content:ConfigContent,
  token:string|undefined):Promise<ConfigRollout> {
  const plan=record.plan,generation=record.generation,releaseId=plan.releaseId;
  if(record.status==='VERIFYING'&&record.verifyAfter!==null&&Date.now()<record.verifyAfter)return record;
  if(record.status!=='APPLYING'&&record.status!=='VERIFYING')return record;
  if(!token||token.length<32)throw new ControlError('CONFIG_TARGET_UNAVAILABLE',503);
  const registry=record.plan.targetOrigins;
  async function request(targetId:string,path:string,command?:object):Promise<Record<string,unknown>> {
    if(!registry||!Object.hasOwn(registry,targetId)||typeof registry[targetId]!=='string')throw new Error('CONFIG_TARGET_UNAVAILABLE');
    const origin=new URL(registry[targetId]);
    if(!['http:','https:'].includes(origin.protocol)||origin.username||origin.password||origin.search||origin.hash||origin.pathname!=='/'){
      throw new Error('CONFIG_TARGET_UNAVAILABLE');
    }
    const result=await fetch(new URL(path,origin),{method:command?'POST':'GET',
      headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:command?JSON.stringify(command):undefined,
      redirect:'error',signal:AbortSignal.timeout(3000)});
    if(!result.ok)throw new Error('CONFIG_TARGET_UNAVAILABLE');
    return result.json();
  }
  const ids=plan.batches[record.batchIndex],endpoint='/internal/v1/config-target/scheduler/';
  const stillCurrent=()=>{const current=rollouts.latest();return current?.generation===generation&&
    current.status===record.status&&current.batchIndex===record.batchIndex;};
  if(record.status==='APPLYING'){
    for(const targetId of ids){
      if(!stillCurrent())return rollouts.latest()!;
      if(rollouts.latest()!.acknowledgements.some(ack=>ack.targetId===targetId))continue;
      const command={releaseId,generation,contentDigest:plan.contentDigest,content,...(plan.rollback?{rollback:true}:{})};
      let ack:TargetConfigAck;
      try{
        await request(targetId,endpoint+'dry-run',command);
        if(!stillCurrent())return rollouts.latest()!;
        const applied=await request(targetId,endpoint+'apply',command);
        const actual=await request(targetId,endpoint+'readback');
        if(applied.releaseId!==releaseId||applied.generation!==generation||applied.actualDigest!==plan.contentDigest||
          actual.verified!==true||actual.generation!==generation||actual.actualDigest!==plan.contentDigest)throw new Error('CONFIG_ACK_MISMATCH');
        ack={releaseId,generation,targetId,actualDigest:plan.contentDigest};
      }catch{
        return stillCurrent()?rollouts.pause(releaseId,generation):rollouts.latest()!;
      }
      if(!stillCurrent())return rollouts.latest()!;
      rollouts.acknowledge(ack,Date.now());
    }
    return rollouts.latest()!;
  }
  const observations=await Promise.all(ids.map(async targetId=>{
    try{
      const [report,actual]=await Promise.all([request(targetId,'/readyz'),request(targetId,endpoint+'readback')]);
      if(report.healthy===true&&actual.verified===true&&actual.generation===generation&&actual.actualDigest===plan.contentDigest){
        return [{releaseId,generation,targetId,actualDigest:plan.contentDigest,observedAt:Date.now(),healthy:true}];
      }
    }catch{/* Missing evidence is passed to the durable verification gate. */}
    return [];
  }));
  if(!stillCurrent())return rollouts.latest()!;
  return rollouts.verify(releaseId,generation,observations.flat(),Date.now());
}
