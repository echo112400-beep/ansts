/** Bounded replay of durable state observations. A remote error never clears local evidence. */
import {bodyEvidenceDigest,verifyBodyEvidence} from '../control/body_execution_evidence.js';
type Rpc=(route:string,body:unknown)=>Promise<unknown>;
export async function deliverBodyObservations(local:Rpc,publish:(proof:string)=>Promise<unknown>,hostKey:string):Promise<number> {
 const pending=await local('/v1/daemon/execution-evidence/pending',{limit:4}) as {observations?:{proof:string;digest:string}[]};
 if(!Array.isArray(pending.observations)||pending.observations.length>4)throw new Error('INVALID_EVIDENCE_BATCH');
 let delivered=0;
 for(const item of pending.observations){
  const fact=verifyBodyEvidence(item.proof,hostKey),digest=bodyEvidenceDigest(fact);
  if(digest!==item.digest)throw new Error('INVALID_EVIDENCE_BATCH');
  try {
  const receipt=await publish(item.proof) as {sourceVerified?:boolean;digest?:string};
  if(receipt?.sourceVerified!==true||receipt.digest!==digest)throw new Error('INVALID_EVIDENCE_RECEIPT');
  const ack=await local('/v1/daemon/execution-evidence/ack',{tenantId:fact.tenantId,taskId:fact.taskId,attemptId:fact.attemptId,digest}) as {acknowledged?:boolean};
  if(ack?.acknowledged!==true)throw new Error('EVIDENCE_ACK_PENDING');
  delivered++;
  } catch(error) {
   await local('/v1/daemon/execution-evidence/defer',{tenantId:fact.tenantId,taskId:fact.taskId,attemptId:fact.attemptId,digest});
   throw error;
  }
 }
 return delivered;
}
