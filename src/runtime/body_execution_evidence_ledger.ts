/** Native host evidence, independent of HTTP completion and retained across daemon restart. */
import {createHash} from 'node:crypto';
import {mkdirSync,readFileSync,openSync,writeFileSync,fsyncSync,closeSync,renameSync,existsSync,readdirSync} from 'node:fs';
import {join} from 'node:path';
import {bodyEvidenceDigest,canonicalBodyEvidence,type BodyExecutionEvidence} from '../control/body_execution_evidence.js';
type Identity=Pick<BodyExecutionEvidence,'tenantId'|'taskId'|'attemptId'>;
interface StoredEvidence extends BodyExecutionEvidence { deliveryAcknowledgedDigest?:string; deliveryAttempts?:number; deliveryNextAttemptAt?:number; }
export class BodyExecutionEvidenceLedger {
 constructor(private readonly directory:string){mkdirSync(directory,{recursive:true});}
 private file(tenantId:string,taskId:string,attemptId:string){return join(this.directory,createHash('sha256').update(JSON.stringify([tenantId,taskId,attemptId])).digest('hex')+'.json');}
 read(tenantId:string,taskId:string,attemptId:string):BodyExecutionEvidence|null {
  const file=this.file(tenantId,taskId,attemptId);if(!existsSync(file))return null;
  const result=canonicalBodyEvidence(JSON.parse(readFileSync(file,'utf8')));if(result.tenantId!==tenantId||result.taskId!==taskId||result.attemptId!==attemptId)throw new Error('BODY_EVIDENCE_IDENTITY_CONFLICT');return result;
 }
 /** The latest fact is also its durable outbox intent. Intermediate observations may coalesce;
  * this is a state observation stream, never an ordered usage-event ledger. */
 pending(limit=25,now=Date.now()):BodyExecutionEvidence[] {
  if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new Error('INVALID_EVIDENCE_LIMIT');
  const pending:BodyExecutionEvidence[]=[];
  for(const name of readdirSync(this.directory).filter(name=>/^[a-f0-9]{64}\.json$/.test(name)).sort()){
   const stored=JSON.parse(readFileSync(join(this.directory,name),'utf8')) as StoredEvidence;
   const fact=canonicalBodyEvidence(stored);
   if(this.file(fact.tenantId,fact.taskId,fact.attemptId)!==join(this.directory,name))throw new Error('BODY_EVIDENCE_IDENTITY_CONFLICT');
   if(stored.deliveryAcknowledgedDigest!==bodyEvidenceDigest(fact)&&(stored.deliveryNextAttemptAt??0)<=now)pending.push(fact);
   if(pending.length===limit)break;
  }
  return pending;
 }
 /** An old network acknowledgement cannot clear a newer observation. Never delete the fact. */
 acknowledge(identity:Identity,digest:string):boolean {
  if(typeof digest!=='string'||!/^[a-f0-9]{64}$/.test(digest))throw new Error('INVALID_EVIDENCE_ACK');
  const fact=this.read(identity.tenantId,identity.taskId,identity.attemptId);
  if(!fact)throw new Error('BODY_EVIDENCE_NOT_FOUND');
  if(bodyEvidenceDigest(fact)!==digest)return false;
  this.save(this.file(fact.tenantId,fact.taskId,fact.attemptId),{...fact,deliveryAcknowledgedDigest:digest});
  return true;
 }
 defer(identity:Identity,digest:string,now=Date.now()):boolean {
  if(typeof digest!=='string'||!/^[a-f0-9]{64}$/.test(digest)||!Number.isSafeInteger(now)||now<0)throw new Error('INVALID_EVIDENCE_ACK');
  const fact=this.read(identity.tenantId,identity.taskId,identity.attemptId);
  if(!fact)throw new Error('BODY_EVIDENCE_NOT_FOUND');
  if(bodyEvidenceDigest(fact)!==digest)return false;
  const file=this.file(fact.tenantId,fact.taskId,fact.attemptId),stored=JSON.parse(readFileSync(file,'utf8')) as StoredEvidence;
  if(stored.deliveryAcknowledgedDigest===digest)return false;
  const attempts=Math.min(20,(stored.deliveryAttempts??0)+1);
  this.save(file,{...fact,deliveryAttempts:attempts,deliveryNextAttemptAt:now+Math.min(60000,1000*2**attempts)});
  return true;
 }
 private save(file:string,value:StoredEvidence,create=false){
  const target=create?file:file+'.tmp',fd=openSync(target,create?'wx':'w',0o600);try{writeFileSync(fd,JSON.stringify(value));fsyncSync(fd);}finally{closeSync(fd);}if(!create)renameSync(target,file);const directory=openSync(this.directory,'r');try{fsyncSync(directory);}finally{closeSync(directory);}
 }
 begin(value:BodyExecutionEvidence):void {
  const canonical=canonicalBodyEvidence(value);if(canonical.phase!=='UNCERTAIN'||canonical.sequence!==1)throw new Error('INVALID_BODY_EVIDENCE_BEGIN');
  const old=this.read(value.tenantId,value.taskId,value.attemptId);if(old){const normalized={...old,sequence:1,phase:'UNCERTAIN' as const,executionObserved:false,observedAt:canonical.observedAt};if(JSON.stringify(normalized)!==JSON.stringify(canonical))throw new Error('BODY_EVIDENCE_BINDING_CONFLICT');return;}
  this.save(this.file(value.tenantId,value.taskId,value.attemptId),canonical,true);
 }
 advance(identity:Pick<BodyExecutionEvidence,'tenantId'|'taskId'|'attemptId'>,phase:'UNCERTAIN'|'RUNNING'|'NOT_STARTED'|'TERMINATED',at=Date.now()):BodyExecutionEvidence {
  const old=this.read(identity.tenantId,identity.taskId,identity.attemptId);if(!old)throw new Error('BODY_EVIDENCE_NOT_FOUND');
  if(old.phase===phase)return old;
  if(['NOT_STARTED','TERMINATED'].includes(old.phase)||old.executionObserved&&phase==='NOT_STARTED')throw new Error('BODY_EVIDENCE_TERMINAL_CONFLICT');
  const result=canonicalBodyEvidence({...old,sequence:old.sequence+1,phase,executionObserved:old.executionObserved||phase==='RUNNING',observedAt:at});this.save(this.file(old.tenantId,old.taskId,old.attemptId),result);return result;
 }
}
