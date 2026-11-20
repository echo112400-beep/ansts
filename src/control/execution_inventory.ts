/** Private, append-only execution discovery. This is evidence, never a billing decision. */
import {createHash} from 'node:crypto';
import {ControlError} from './contracts.js';
import type {DirectoryStorage} from './tenant_directory.js';

export type InventoryKind = 'SCHEDULER' | 'BODY';
export interface InventoryObservation {
 tenantId:string; taskId:string; inputDigest:string; state:string; fencing:number;
 nodeId:string|null; attemptId:string|null; version:number|null; sourceAt:number|null;
}
export interface InventoryEntry {
 ordinal:number; observedAt:number; baseline:boolean; observation:InventoryObservation;
 previousDigest:string; digest:string;
}
export interface InventorySnapshot {
 schemaVersion:1; purpose:'ansts-execution-inventory-v1'; kind:InventoryKind; sourceId:string;
 tenantId:string; coverageSince:number; observedAt:number; count:number; headDigest:string;
 entries:InventoryEntry[]; complete:boolean; reasons:string[]; watermark:string;
}
export interface InventoryPage extends InventorySnapshot {after:number;nextAfter:number|null;}
interface Metadata {kind:InventoryKind;sourceId:string;coverageSince:number;}
interface Head {count:number;digest:string;}
const LIMIT=10000,MAX_BYTES=8*1024*1024;
export const INVENTORY_PAGE_SIZE=512;
export function inventoryDigest(value:unknown):string {return createHash('sha256').update(JSON.stringify(value)).digest('hex');}
function fail():never {throw new ControlError('EXECUTION_INVENTORY_CORRUPT',503);}
function bounded(value:unknown,max=512):value is string {return typeof value==='string'&&value.length>0&&value.length<=max&&!/[\u0000-\u001f]/.test(value);}
function observationValue(value:InventoryObservation):InventoryObservation {
 if(!value||!bounded(value.tenantId)||!bounded(value.taskId)||!bounded(value.state,64)||typeof value.inputDigest!=='string'||value.inputDigest.length>512||/[\u0000-\u001f]/.test(value.inputDigest)||!Number.isSafeInteger(value.fencing)||value.fencing<0)fail();
 if(value.nodeId!==null&&!bounded(value.nodeId))fail();if(value.attemptId!==null&&!bounded(value.attemptId))fail();
 if(value.version!==null&&(!Number.isSafeInteger(value.version)||value.version<1))fail();
 if(value.sourceAt!==null&&(!Number.isSafeInteger(value.sourceAt)||value.sourceAt<0))fail();
 return {tenantId:value.tenantId,taskId:value.taskId,inputDigest:value.inputDigest,state:value.state,fencing:value.fencing,nodeId:value.nodeId,attemptId:value.attemptId,version:value.version,sourceAt:value.sourceAt};
}
/** State updates and observations share this source object's real SQLite transaction. */
export class ExecutionInventory {
 constructor(private readonly storage:DirectoryStorage,kind:InventoryKind,sourceId:string,baseline:()=>InventoryObservation[],now=Date.now()) {
  storage.transactionSync(()=>{
   storage.sql.exec('CREATE TABLE IF NOT EXISTS execution_inventory_meta (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS execution_inventory_heads (tenant_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS execution_inventory_events (tenant_id TEXT NOT NULL, ordinal INTEGER NOT NULL, task_id TEXT NOT NULL, payload_json TEXT NOT NULL, PRIMARY KEY (tenant_id, ordinal))');
   storage.sql.exec('CREATE INDEX IF NOT EXISTS execution_inventory_task ON execution_inventory_events (tenant_id, task_id, ordinal)');
   const saved=this.metadata();
   if(saved){if(saved.kind!==kind||saved.sourceId!==sourceId)fail();return;}
   this.setMetadata({kind,sourceId,coverageSince:now});
   for(const observation of baseline())this.append(observation,true,now);
  });
 }
 private metadata():Metadata|null {const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM execution_inventory_meta WHERE id = ?', 'source').toArray()[0];return row?JSON.parse(row.payload_json):null;}
 private setMetadata(value:Metadata){this.storage.sql.exec('INSERT OR REPLACE INTO execution_inventory_meta (id, payload_json) VALUES (?, ?)', 'source',JSON.stringify(value));}
 private head(tenantId:string):Head {const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM execution_inventory_heads WHERE tenant_id = ?',tenantId).toArray()[0];return row?JSON.parse(row.payload_json):{count:0,digest:''};}
 private append(value:InventoryObservation,baseline:boolean,now:number){
  const observation=observationValue(value),head=this.head(observation.tenantId);
  const prior=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM execution_inventory_events WHERE tenant_id = ? AND task_id = ? ORDER BY ordinal DESC LIMIT 1',observation.tenantId,observation.taskId).toArray()[0];
  if(prior&&!baseline&&JSON.stringify(JSON.parse(prior.payload_json).observation)===JSON.stringify(observation))return;
  if(!Number.isSafeInteger(head.count)||head.count<0||head.count>=Number.MAX_SAFE_INTEGER)fail();
  const content={ordinal:head.count+1,observedAt:now,baseline,observation,previousDigest:head.digest},entry={...content,digest:inventoryDigest(content)};
  this.storage.sql.exec('INSERT INTO execution_inventory_events (tenant_id, ordinal, task_id, payload_json) VALUES (?, ?, ?, ?)',observation.tenantId,entry.ordinal,observation.taskId,JSON.stringify(entry));
  this.storage.sql.exec('INSERT OR REPLACE INTO execution_inventory_heads (tenant_id, payload_json) VALUES (?, ?)',observation.tenantId,JSON.stringify({count:entry.ordinal,digest:entry.digest}));
 }
 record<T>(observation:InventoryObservation,write:()=>T,now=Date.now()):T {
  return this.storage.transactionSync(()=>{const result=write();this.append(observation,false,now);return result;});
 }
 /** Imported current-state records cannot establish the history that preceded the import. */
 importBaseline(write:()=>void,baseline:()=>InventoryObservation[],now=Date.now()):void {
  this.storage.transactionSync(()=>{write();const meta=this.metadata();if(!meta)fail();this.setMetadata({...meta,coverageSince:Math.max(meta.coverageSince,now)});for(const observation of baseline())this.append(observation,true,now);});
 }
 private readEntry(row:{ordinal:number;payload_json:string},tenantId:string,ordinal:number,now:number):InventoryEntry {
  const value=JSON.parse(row.payload_json) as InventoryEntry;const observation=observationValue(value.observation);
  const content={ordinal:value.ordinal,observedAt:value.observedAt,baseline:value.baseline,observation,previousDigest:value.previousDigest};
  if(value.ordinal!==ordinal||value.ordinal!==row.ordinal||observation.tenantId!==tenantId||inventoryDigest(content)!==value.digest||typeof value.baseline!=='boolean'||!Number.isSafeInteger(value.observedAt)||value.observedAt<0||value.observedAt>now)fail();
  return value;
 }
 page(tenantId:string,after:number,now=Date.now()):InventoryPage {
  if(!bounded(tenantId,128)||!Number.isSafeInteger(after)||after<0)throw new ControlError('INVALID_INVENTORY_SCOPE',422);
  return this.storage.transactionSync(()=>{
   const meta=this.metadata();if(!meta||meta.kind!=='SCHEDULER')fail();
   const head=this.head(tenantId);
   if(!Number.isSafeInteger(head.count)||head.count<0)fail();
   if(after>head.count)throw new ControlError('SCHEDULER_INVENTORY_CHANGED',409);
   const rows=this.storage.sql.exec<{ordinal:number;payload_json:string}>('SELECT ordinal, payload_json FROM execution_inventory_events WHERE tenant_id = ? AND ordinal > ? ORDER BY ordinal LIMIT ?',tenantId,after,INVENTORY_PAGE_SIZE+1).toArray();
   if(rows.length!==Math.min(INVENTORY_PAGE_SIZE+1,head.count-after))fail();
   const entries=rows.slice(0,INVENTORY_PAGE_SIZE).map((row,index)=>this.readEntry(row,tenantId,after+index+1,now));
   const end=after+entries.length,nextAfter=end<head.count?end:null;
   return {schemaVersion:1,purpose:'ansts-execution-inventory-v1',...meta,tenantId,observedAt:now,count:head.count,headDigest:head.digest,entries,after,nextAfter,complete:nextAfter===null,reasons:[],watermark:inventoryDigest({meta,tenantId,head})};
  });
 }
 snapshot(tenantId:string,now=Date.now()):InventorySnapshot {
  if(!bounded(tenantId,128))throw new ControlError('INVALID_TENANT_ID',422);
  return this.storage.transactionSync(()=>{
   const meta=this.metadata();if(!meta)fail();const head=this.head(tenantId);
   const rows=this.storage.sql.exec<{ordinal:number;payload_json:string}>('SELECT ordinal, payload_json FROM execution_inventory_events WHERE tenant_id = ? ORDER BY ordinal LIMIT ?',tenantId,LIMIT+1).toArray();
   const reasons:string[]=[],entries:InventoryEntry[]=[];let bytes=0,previousDigest='';
   if(rows.length>LIMIT||head.count>LIMIT)reasons.push('INVENTORY_SCAN_LIMIT');
   for(const row of rows.slice(0,LIMIT)){
    bytes+=Buffer.byteLength(row.payload_json);if(bytes>MAX_BYTES){reasons.push('INVENTORY_BYTE_LIMIT');break;}
    const value=this.readEntry(row,tenantId,entries.length+1,now);
    if(value.previousDigest!==previousDigest)fail();
    previousDigest=value.digest;entries.push(value);
   }
   if(!reasons.length&&(head.count!==entries.length||head.digest!==previousDigest))fail();
   const content={schemaVersion:1 as const,purpose:'ansts-execution-inventory-v1' as const,...meta,tenantId,observedAt:now,count:head.count,headDigest:head.digest,entries,complete:reasons.length===0,reasons};
   // Observation time is separate from the stable revision for unchanged sources.
   return {...content,watermark:inventoryDigest({meta,tenantId,head})};
  });
 }
}
