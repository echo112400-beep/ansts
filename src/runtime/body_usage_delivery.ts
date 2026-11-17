/** Strict immutable receipts, with per-stream persisted retry/quarantine and no sibling starvation. */
import {verifyUsageEvent,usageEventDigest,type UsageIdentity} from '../control/usage_event.js';
type Rpc=(path:string,body:unknown)=>Promise<unknown>;
export async function deliverBodyUsage(local:Rpc,publish:(proof:string)=>Promise<unknown>,key:string){
 const response=await local('/v1/daemon/usage/pending',{limit:4}) as {events?:{proof:string;digest:string}[]};if(!Array.isArray(response.events)||response.events.length>4)throw new Error('INVALID_USAGE_BATCH');
 const result={delivered:0,deferred:0,quarantined:0};
 for(const item of response.events){
  const event=verifyUsageEvent(item.proof,key),digest=usageEventDigest(event);if(item.digest!==digest)throw new Error('INVALID_USAGE_BATCH');
  const identity:UsageIdentity={producerId:event.producerId,tenantId:event.tenantId,taskId:event.taskId,attemptId:event.attemptId,fencing:event.fencing,executionInputDigest:event.executionInputDigest};let quarantine=false;
  try{
   const ack=await publish(item.proof) as {sourceVerified?:boolean;receipt?:{eventId:string;digest:string;outcome:string}};
   if(!ack||ack.sourceVerified!==true||ack.receipt?.eventId!==event.eventId||ack.receipt.digest!==digest)throw new Error('INVALID_USAGE_RECEIPT');
   quarantine=['CONFLICT','QUARANTINED'].includes(ack.receipt.outcome);if(ack.receipt.outcome!=='RECORDED')throw new Error('USAGE_NOT_RECORDED');
   const recorded=await local('/v1/daemon/usage/ack',{...identity,sequence:event.sequence,digest}) as {acknowledged?:boolean};if(recorded?.acknowledged!==true)throw new Error('USAGE_ACK_PENDING');result.delivered++;
  }catch{
   await local('/v1/daemon/usage/defer',{...identity,sequence:event.sequence,digest,quarantine});result.deferred++;if(quarantine)result.quarantined++;
  }
 }
 return result;
}
