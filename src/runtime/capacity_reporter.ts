/** Persist the reporter epoch/sequence before signing. No process-local epoch after restart. */
import {mkdirSync,existsSync,readFileSync,openSync,writeFileSync,fsyncSync,closeSync,renameSync} from 'node:fs';
import {dirname} from 'node:path';
import {createHash} from 'node:crypto';
import {capacityId,canonicalCapacityReport,signCapacityReport,type CapacityReport,type CapacityResources} from '../control/capacity_report.js';
export function physicalHostIdFromMachineId(raw:string):string {
 const identity=raw.trim();if(!/^[a-f0-9]{32}$/.test(identity))throw new Error('INVALID_PHYSICAL_HOST_IDENTITY');
 return 'physical-'+createHash('sha256').update(identity).digest('hex').slice(0,32);
}
export class CapacityReporter {
 private epoch:number;private sequence=0;
 constructor(private readonly file:string,private readonly hostId:string,private readonly daemonId:string,private readonly key:string){
  capacityId(hostId);capacityId(daemonId);mkdirSync(dirname(file),{recursive:true});let epoch=0;
  if(existsSync(file)){const old=JSON.parse(readFileSync(file,'utf8'));if(old.schemaVersion!==1||old.hostId!==hostId||old.daemonId!==daemonId||!Number.isSafeInteger(old.epoch)||old.epoch<1||!Number.isSafeInteger(old.sequence)||old.sequence<0)throw new Error('CAPACITY_REPORTER_IDENTITY_CONFLICT');epoch=old.epoch;}
  this.epoch=epoch+1;this.persist();
 }
 private persist(){
  if(!Number.isSafeInteger(this.epoch)||!Number.isSafeInteger(this.sequence))throw new Error('CAPACITY_REPORTER_SEQUENCE_EXHAUSTED');
  const fd=openSync(this.file+'.tmp','w',0o600);try{writeFileSync(fd,JSON.stringify({schemaVersion:1,hostId:this.hostId,daemonId:this.daemonId,epoch:this.epoch,sequence:this.sequence}));fsyncSync(fd);}finally{closeSync(fd);}renameSync(this.file+'.tmp',this.file);const dir=openSync(dirname(this.file),'r');try{fsyncSync(dir);}finally{closeSync(dir);}
 }
 capture(input:{total:CapacityResources;reserved:CapacityResources;draining:boolean;recoveryRequired:boolean},now=Date.now()):{proof:string;report:CapacityReport} {
  const report=canonicalCapacityReport({...input,schemaVersion:1,purpose:'ansts-capacity-report-v1',hostId:this.hostId,daemonId:this.daemonId,epoch:this.epoch,sequence:this.sequence+1,observedAt:now});this.sequence=report.sequence;this.persist();return {report,proof:signCapacityReport(report,this.key)};
 }
}
