import {PeriodSettlement} from './period_settlement.js';
/** Accounting-authority backup. Restore has no HTTP route and never attaches a live runtime/alarm. */
import {createHash,createHmac,timingSafeEqual} from 'node:crypto';
import {assertControlAccess,ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage,TenantRecord} from '../control/tenant_directory.js';
import {canonicalUsageEvent,usageEventDigest,usageStreamId,type UsageIdentity} from '../control/usage_event.js';
import {verifyRateCard,rateHash,validText} from './rate_cards.js';
import {joinedTransactions} from './finops_page.js';
import {UsageCostLedger} from './usage_cost_ledger.js';
const PURPOSE='finops-authority-backup-v1';
const MAX_BYTES=32*1024*1024,MAX_ROWS=100000;
const periodTables=new Set(['tenant_settlement_identity','tenant_period_closes','tenant_period_close_requests','tenant_period_adjustments']);
const schemas={
 tenant_settlement_identity:'id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL',
 tenant_period_closes:'id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, digest TEXT NOT NULL',
 tenant_period_close_requests:'id TEXT PRIMARY KEY, digest TEXT NOT NULL, period_id TEXT NOT NULL',
 tenant_period_adjustments:'id TEXT PRIMARY KEY, ordinal INTEGER NOT NULL UNIQUE, origin_period_id TEXT NOT NULL, target_period_id TEXT NOT NULL, payload_json TEXT NOT NULL',
 control_directory:'id TEXT PRIMARY KEY, payload_json TEXT NOT NULL',
 directory_rate_cards:'version INTEGER PRIMARY KEY, payload_json TEXT NOT NULL',
 directory_rate_requests:'id TEXT PRIMARY KEY, digest TEXT NOT NULL, version INTEGER NOT NULL',
 directory_rate_audit:'version INTEGER PRIMARY KEY, payload_json TEXT NOT NULL',
 tenant_rate_identity:'id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL',
 tenant_rate_activations:'version INTEGER PRIMARY KEY, payload_json TEXT NOT NULL',
 tenant_rate_requests:'id TEXT PRIMARY KEY, digest TEXT NOT NULL, version INTEGER NOT NULL',
 tenant_rate_audit:'version INTEGER PRIMARY KEY, payload_json TEXT NOT NULL',
 tenant_usage_price_bindings:'id TEXT PRIMARY KEY, payload_json TEXT NOT NULL',
 tenant_usage_metadata:'id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL',
 tenant_usage_streams:'id TEXT PRIMARY KEY, payload_json TEXT NOT NULL',
 tenant_usage_events:'id TEXT PRIMARY KEY, event_json TEXT NOT NULL, digest TEXT NOT NULL',
 tenant_usage_conflicts:'id TEXT PRIMARY KEY, payload_json TEXT NOT NULL',
 tenant_usage_event_order:'id TEXT PRIMARY KEY, ordinal INTEGER NOT NULL UNIQUE, tenant_id TEXT NOT NULL, region TEXT',
 tenant_usage_cost_entries:'id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, digest TEXT NOT NULL',
 tenant_policy_state:'id TEXT PRIMARY KEY, payload_json TEXT NOT NULL',
 tenant_admission_state:'id TEXT PRIMARY KEY, payload_json TEXT NOT NULL',
 admission_outbox:'id TEXT PRIMARY KEY, payload_json TEXT NOT NULL',
 admission_delivery_control:'id TEXT PRIMARY KEY, payload_json TEXT NOT NULL',
} as const;
type TableName=keyof typeof schemas;
export type ArchiveKind='DIRECTORY'|'TENANT';
const names=(kind:ArchiveKind,version=1):TableName[]=>Object.keys(schemas).filter(n=>version===2||!periodTables.has(n)).filter(n=>kind==='DIRECTORY'?n==='control_directory'||n.startsWith('directory_'):n!=='control_directory'&&!n.startsWith('directory_')) as TableName[];
const columns=(name:TableName)=>schemas[name].split(', ').map(item=>item.split(' ')[0]);
type Cell=string|number|null;
interface Table {name:TableName;rows:Cell[][]}
export interface AuthoritySnapshot {schemaVersion:1|2;purpose:typeof PURPOSE;kind:ArchiveKind;tenant:TenantRecord|null;createdAt:number;actor:string;reason:string;requestId:string;tables:Table[]}
export interface AuthorityManifest {id:string;schemaVersion:1|2;purpose:typeof PURPOSE;kind:ArchiveKind;tenantId:string|null;createdAt:number;digest:string;bytes:number;sourceRevision:string;tableCounts:Record<string,number>;operationalResume:'DISABLED';signature:string}
export interface AuthorityArchive {manifest:AuthorityManifest;snapshot:AuthoritySnapshot}
export interface ArchiveWrite {expectedRevision:string;reason:string;requestId:string;idempotencyKey:string}
function bad():never{throw new ControlError('FINOPS_ARCHIVE_INVALID',422);}
function secret(value:string){if(!value||value.length<32)throw new ControlError('FINOPS_ARCHIVE_KEY_UNAVAILABLE',503);}
function digest(value:string){return createHash('sha256').update(value).digest('hex');}
function authorize(actor:ControlActor,kind:ArchiveKind,tenant:TenantRecord|null){
 if(actor.role!=='SRE_ADMIN')throw new ControlError('CONTROL_FORBIDDEN',403);
 if(kind==='DIRECTORY'){if(actor.scopeTenant!=='all'||actor.scopeRegion!=='all'||!actor.sub)throw new ControlError('CONTROL_FORBIDDEN',403);}
 else{if(!tenant)bad();assertControlAccess(actor,'administer',tenant.id,tenant.region);if(actor.scopeRegion!=='all')throw new ControlError('COMPLETE_AUTHORITY_SCOPE_REQUIRED',403);}
}
function capture(storage:DirectoryStorage,kind:ArchiveKind,version?:number):Table[]{
 let bytes=0,count=0;
 if(version===undefined){version=1;if(kind==='TENANT')for(const name of periodTables){if(storage.sql.exec('SELECT name FROM sqlite_master WHERE type = ? AND name = ?','table',name).toArray().length&&storage.sql.exec(`SELECT 1 AS present FROM ${name} LIMIT 1`).toArray().length){version=2;break;}}}
 return names(kind,version).map(name=>{
  const exists=storage.sql.exec('SELECT name FROM sqlite_master WHERE type = ? AND name = ?','table',name).toArray().length;
  const rows=exists?storage.sql.exec<Record<string,Cell>>(`SELECT ${columns(name).join(', ')} FROM ${name} ORDER BY ${columns(name)[0]} LIMIT ?`,MAX_ROWS+1).toArray().map(row=>columns(name).map(c=>row[c])):[];
  count+=rows.length;bytes+=Buffer.byteLength(JSON.stringify(rows));if(count>MAX_ROWS||bytes>MAX_BYTES)throw new ControlError('FINOPS_ARCHIVE_CAPACITY_REACHED',503);return {name,rows};
 });
}
function checkShape(snapshot:AuthoritySnapshot){
 if(!snapshot||!([1,2].includes(snapshot.schemaVersion))||snapshot.purpose!==PURPOSE||!['DIRECTORY','TENANT'].includes(snapshot.kind)||!Number.isSafeInteger(snapshot.createdAt)||snapshot.createdAt<0||!validText(snapshot.actor,256)||!validText(snapshot.reason,1000)||!validText(snapshot.requestId,256)||!Array.isArray(snapshot.tables))bad();
 if(snapshot.kind==='DIRECTORY'?snapshot.tenant!==null:!snapshot.tenant||!validText(snapshot.tenant.id))bad();
 const expected=names(snapshot.kind,snapshot.schemaVersion);if(JSON.stringify(snapshot.tables.map(t=>t.name))!==JSON.stringify(expected))bad();
 let count=0;
 for(const table of snapshot.tables){if(!Array.isArray(table.rows))bad();count+=table.rows.length;if(count>MAX_ROWS)bad();for(const row of table.rows){if(!Array.isArray(row)||row.length!==columns(table.name).length||row.some(v=>v!==null&&typeof v!=='string'&&(!Number.isSafeInteger(v)||typeof v!=='number')))bad();}}
 if(Buffer.byteLength(JSON.stringify(snapshot))>MAX_BYTES)bad();
}
/** Additional semantic checks reject a checksum-valid but inconsistent authority snapshot. */
function validate(storage:DirectoryStorage,kind:ArchiveKind,tenant:TenantRecord|null,version=1){
 if(kind==='DIRECTORY'){
  const row=storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM control_directory WHERE id = ?','directory').toArray()[0];if(!row)bad();const state=JSON.parse(row.payload_json);if(state.schemaVersion!==1||!Array.isArray(state.tenants)||!state.receipts||!Array.isArray(state.audit))bad();
  const cards=storage.sql.exec<{version:number;payload_json:string}>('SELECT version, payload_json FROM directory_rate_cards ORDER BY version').toArray();
  cards.forEach((row,i)=>{const card=verifyRateCard(JSON.parse(row.payload_json));if(row.version!==i+1||card.version!==row.version)bad();});
  const versions=new Set(cards.map(row=>row.version));for(const row of storage.sql.exec<{version:number}>('SELECT version FROM directory_rate_requests').toArray())if(!versions.has(row.version))bad();
  const audit=storage.sql.exec<{version:number;payload_json:string}>('SELECT version, payload_json FROM directory_rate_audit ORDER BY version').toArray();if(audit.length!==cards.length)bad();audit.forEach((row,i)=>{if(row.version!==cards[i].version||JSON.parse(row.payload_json).digest!==JSON.parse(cards[i].payload_json).digest)bad();});return;
 }
 if(!tenant)bad();
 for(const table of ['tenant_rate_identity','tenant_usage_metadata'] as const)for(const row of storage.sql.exec<{tenant_id:string}>(`SELECT tenant_id FROM ${table}`).toArray())if(row.tenant_id!==tenant.id)bad();
 for(const table of ['tenant_policy_state','tenant_admission_state','admission_outbox'] as const)for(const row of storage.sql.exec<{payload_json:string}>(`SELECT payload_json FROM ${table}`).toArray()){const value=JSON.parse(row.payload_json);if(value.schemaVersion!==1||value.tenantId!==tenant.id)bad();}
 for(const row of storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM admission_delivery_control').toArray())if(JSON.parse(row.payload_json).configuration?.tenantId!==tenant.id)bad();
 const activations=storage.sql.exec<{version:number;payload_json:string}>('SELECT version, payload_json FROM tenant_rate_activations ORDER BY version').toArray();
 activations.forEach((row,i)=>{const value=JSON.parse(row.payload_json);if(row.version!==i+1||value.version!==row.version||value.tenantId!==tenant.id)bad();verifyRateCard(value.rateCard);});
 for(const row of storage.sql.exec<{version:number}>('SELECT version FROM tenant_rate_requests').toArray())if(!activations.some(a=>a.version===row.version))bad();
 const activationAudit=storage.sql.exec<{version:number;payload_json:string}>('SELECT version, payload_json FROM tenant_rate_audit ORDER BY version').toArray();if(activationAudit.length!==activations.length)bad();activationAudit.forEach((row,i)=>{if(row.version!==activations[i].version||JSON.parse(row.payload_json).rateDigest!==JSON.parse(activations[i].payload_json).rateCard.digest)bad();});
 const streams=new Map<string,{identity:UsageIdentity;sequence:number;context?:{tenantVersion:number;region:string;costCenter:string;rateCardVersion:number|null}}>();
 for(const row of storage.sql.exec<{id:string;payload_json:string}>('SELECT id, payload_json FROM tenant_usage_streams').toArray()){const value=JSON.parse(row.payload_json);if(value.identity?.tenantId!==tenant.id||!Number.isSafeInteger(value.sequence)||value.sequence<0||usageStreamId(value.identity)!==row.id)bad();streams.set(row.id,value);}
 const counts=new Map<string,number>();
 for(const row of storage.sql.exec<{id:string;event_json:string;digest:string}>('SELECT id, event_json, digest FROM tenant_usage_events').toArray()){
  const raw=JSON.parse(row.event_json),event=canonicalUsageEvent(raw.event??raw),stream=streams.get(usageStreamId(event));if(event.tenantId!==tenant.id||event.sequence>(stream?.sequence??-1)||event.eventId!==row.id||usageEventDigest(event)!==row.digest||!stream||JSON.stringify(stream.identity)!==JSON.stringify({producerId:event.producerId,tenantId:event.tenantId,taskId:event.taskId,attemptId:event.attemptId,fencing:event.fencing,executionInputDigest:event.executionInputDigest}))bad();counts.set(usageStreamId(event),(counts.get(usageStreamId(event))??0)+1);
 }
 for(const [id,stream] of streams)if(stream.sequence!==(counts.get(id)??0))bad();
 const orders=storage.sql.exec<{id:string;tenant_id:string;ordinal:number}>('SELECT id, tenant_id, ordinal FROM tenant_usage_event_order ORDER BY ordinal').toArray();
 const events=storage.sql.exec<{id:string}>('SELECT id FROM tenant_usage_events').toArray();if(orders.length!==events.length)bad();const ids=new Set(events.map(e=>e.id));orders.forEach((row,i)=>{if(row.ordinal!==i+1||row.tenant_id!==tenant.id||!ids.has(row.id))bad();});
 const ledger=new UsageCostLedger(storage);
 for(const row of storage.sql.exec<{id:string}>('SELECT id FROM tenant_usage_cost_entries').toArray()){const entry=ledger.project(row.id),binding=storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_usage_price_bindings WHERE id = ?',usageStreamId(entry.source)).toArray()[0];if(JSON.stringify(entry.context)!==JSON.stringify(binding?JSON.parse(binding.payload_json).context:null))bad();}
 for(const row of storage.sql.exec<{id:string;payload_json:string}>('SELECT id, payload_json FROM tenant_usage_price_bindings').toArray()){const value=JSON.parse(row.payload_json),stream=streams.get(usageStreamId(value.identity));if(row.id!==usageStreamId(value.identity)||value.context?.tenantId!==tenant.id||value.identity?.tenantId!==tenant.id||!stream?.context||stream.context.tenantVersion!==value.context.tenantVersion||stream.context.region!==value.context.region||stream.context.costCenter!==value.context.costCenter||stream.context.rateCardVersion!==(value.context.activation?.rateCard.version??null))bad();if(value.context.activation&&!activations.some(a=>JSON.stringify(JSON.parse(a.payload_json))===JSON.stringify(value.context.activation)))bad();}
 if(version===2){
  for(const row of storage.sql.exec<{tenant_id:string}>('SELECT tenant_id FROM tenant_settlement_identity').toArray())if(row.tenant_id!==tenant.id)bad();
  const periods=new PeriodSettlement(storage),actor:ControlActor={sub:'archive:validator',role:'SRE_ADMIN',scopeTenant:tenant.id,scopeRegion:'all'};
  const closed=storage.sql.exec<{id:string}>('SELECT id FROM tenant_period_closes').toArray();if(closed.length>1200)bad();
  const ids=new Set(closed.map(row=>row.id));for(const row of closed)periods.readClosed(actor,tenant,row.id);
  for(const row of storage.sql.exec<{period_id:string}>('SELECT period_id FROM tenant_period_close_requests').toArray())if(!ids.has(row.period_id))bad();
  periods.adjustments(actor,tenant,{limit:1},'archive-validation-cursor-key-000000000000');
 }

}
export function verifyAuthorityArchive(archive:AuthorityArchive,key:string):AuthorityArchive{
 secret(key);if(!archive?.manifest||!archive.snapshot)bad();checkShape(archive.snapshot);
 const {signature,...manifest}=archive.manifest,payload=JSON.stringify(archive.snapshot),expected=createHmac('sha256',key).update(PURPOSE+'.'+JSON.stringify(manifest)).digest();
 if(typeof signature!=='string'||!/^[A-Za-z0-9_-]{43}$/.test(signature))bad();const actual=Buffer.from(signature,'base64url');if(actual.length!==expected.length||!timingSafeEqual(actual,expected))throw new ControlError('FINOPS_ARCHIVE_AUTH_REQUIRED',401);
 if(manifest.schemaVersion!==archive.snapshot.schemaVersion||manifest.purpose!==PURPOSE||manifest.kind!==archive.snapshot.kind||manifest.tenantId!==(archive.snapshot.tenant?.id??null)||manifest.digest!==digest(payload)||manifest.id!==manifest.digest||manifest.bytes!==Buffer.byteLength(payload)||manifest.createdAt!==archive.snapshot.createdAt||manifest.sourceRevision!==rateHash(archive.snapshot.tables)||manifest.operationalResume!=='DISABLED'||JSON.stringify(manifest.tableCounts)!==JSON.stringify(Object.fromEntries(archive.snapshot.tables.map(t=>[t.name,t.rows.length]))))bad();return archive;
}
export class AuthorityArchiveStore {
 private readonly storage:DirectoryStorage;
 constructor(storage:DirectoryStorage,private readonly kind:ArchiveKind){
  this.storage=joinedTransactions(storage);this.storage.transactionSync(()=>{
   this.storage.sql.exec('CREATE TABLE IF NOT EXISTS finops_authority_archives (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, manifest_json TEXT NOT NULL)');
   this.storage.sql.exec('CREATE TABLE IF NOT EXISTS finops_authority_archive_requests (id TEXT PRIMARY KEY, digest TEXT NOT NULL, archive_id TEXT NOT NULL)');
  });
 }
 status(actor:ControlActor,tenant:TenantRecord|null){authorize(actor,this.kind,tenant);return this.storage.transactionSync(()=>({kind:this.kind,tenantId:tenant?.id??null,sourceRevision:rateHash(capture(this.storage,this.kind)),items:this.storage.sql.exec<{manifest_json:string}>('SELECT manifest_json FROM finops_authority_archives ORDER BY rowid').toArray().map(row=>JSON.parse(row.manifest_json)),operationalResume:'DISABLED'}));}
 read(actor:ControlActor,tenant:TenantRecord|null,id:string):AuthorityArchive{
  authorize(actor,this.kind,tenant);if(!/^[a-f0-9]{64}$/.test(id))throw new ControlError('FINOPS_ARCHIVE_NOT_FOUND',404);
  const row=this.storage.sql.exec<{payload_json:string;manifest_json:string}>('SELECT payload_json, manifest_json FROM finops_authority_archives WHERE id = ?',id).toArray()[0];if(!row)throw new ControlError('FINOPS_ARCHIVE_NOT_FOUND',404);
  const snapshot=JSON.parse(row.payload_json) as AuthoritySnapshot,manifest=JSON.parse(row.manifest_json) as AuthorityManifest;
  checkShape(snapshot);if(digest(row.payload_json)!==id||manifest.id!==id||manifest.digest!==id||manifest.kind!==this.kind||snapshot.tenant?.id!==(tenant?.id??undefined))throw new ControlError('FINOPS_ARCHIVE_CORRUPT',503);return {snapshot,manifest};
 }
 create(actor:ControlActor,tenant:TenantRecord|null,input:ArchiveWrite,key:string,now=Date.now()):AuthorityManifest{
  authorize(actor,this.kind,tenant);secret(key);if(!input||!validText(input.reason,1000)||!validText(input.requestId,256)||!validText(input.idempotencyKey)||!(/^[a-f0-9]{64}$/).test(input.expectedRevision)||!Number.isSafeInteger(now)||now<0)throw new ControlError('INVALID_FINOPS_ARCHIVE_WRITE',422);
  const requestKey=rateHash([actor.sub,input.idempotencyKey]),requestDigest=rateHash([this.kind,tenant?.id??null,input.expectedRevision,input.reason]);
  return this.storage.transactionSync(()=>{
   const old=this.storage.sql.exec<{digest:string;archive_id:string}>('SELECT digest, archive_id FROM finops_authority_archive_requests WHERE id = ?',requestKey).toArray()[0];if(old){if(old.digest!==requestDigest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);return this.read(actor,tenant,old.archive_id).manifest;}
   if(this.storage.sql.exec('SELECT id FROM finops_authority_archives LIMIT 100').toArray().length>=100)throw new ControlError('FINOPS_ARCHIVE_CAPACITY_REACHED',503);
   const tables=capture(this.storage,this.kind),revision=rateHash(tables);if(revision!==input.expectedRevision)throw new ControlError('VERSION_CONFLICT',409);
   validate(this.storage,this.kind,tenant,tables.some(t=>periodTables.has(t.name))?2:1);
   const snapshot:AuthoritySnapshot={schemaVersion:tables.some(t=>periodTables.has(t.name))?2:1,purpose:PURPOSE,kind:this.kind,tenant,createdAt:now,actor:actor.sub,reason:input.reason,requestId:input.requestId,tables};checkShape(snapshot);
   const payload=JSON.stringify(snapshot),hash=digest(payload),content:Omit<AuthorityManifest,'signature'>={id:hash,schemaVersion:snapshot.schemaVersion,purpose:PURPOSE,kind:this.kind,tenantId:tenant?.id??null,createdAt:now,digest:hash,bytes:Buffer.byteLength(payload),sourceRevision:revision,tableCounts:Object.fromEntries(tables.map(t=>[t.name,t.rows.length])),operationalResume:'DISABLED' as const};
   const manifest={...content,signature:createHmac('sha256',key).update(PURPOSE+'.'+JSON.stringify(content)).digest('base64url')};
   this.storage.sql.exec('INSERT INTO finops_authority_archives (id, payload_json, manifest_json) VALUES (?, ?, ?)',hash,payload,JSON.stringify(manifest));this.storage.sql.exec('INSERT INTO finops_authority_archive_requests (id, digest, archive_id) VALUES (?, ?, ?)',requestKey,requestDigest,hash);verifyAuthorityArchive(this.read(actor,tenant,hash),key);return manifest;
  });
 }
}
const celldReservedTables = new Set<string>([
 '_litestream_seq', '_litestream_lock', '_cf_METADATA', '_cf_KV', '_cf_ALARM', '_cf_FACETS', '_cf_WAKE',
]);

const celldAlarmTriggers = new Set(['_cf_WAKE_insert', '_cf_WAKE_update', '_cf_WAKE_delete']);

type RestoreScope = {kind:ArchiveKind;tenantId:string|null};

/** Offline-only primitive. The caller must provide an exclusive new file. */
export function restoreIntoEmptyOfflineDatabase(storage:DirectoryStorage,archive:AuthorityArchive,key:string,expected:RestoreScope){
 verifyRestoreScope(archive,key,expected);
 return restoreArchive(storage,archive,false);
}

/** Invoke only in a dedicated, fenced celld import object before serving application traffic. */
export async function restoreIntoFreshCelldObject(
 storage:DirectoryStorage & {getAlarm():Promise<number|null>;list(options:{limit:number}):Promise<ReadonlyMap<string,unknown>>},
 archive:AuthorityArchive,key:string,expected:RestoreScope,
){
 verifyRestoreScope(archive,key,expected);
 if(await storage.getAlarm()!==null||(await storage.list({limit:1})).size)
  throw new ControlError('FINOPS_RESTORE_REQUIRES_EMPTY_DATABASE',409);
 return restoreArchive(storage,archive,true);
}

function verifyRestoreScope(archive:AuthorityArchive,key:string,expected:RestoreScope){
 verifyAuthorityArchive(archive,key);
 if(archive.manifest.kind!==expected.kind||archive.manifest.tenantId!==expected.tenantId)
  throw new ControlError('FINOPS_RESTORE_SCOPE_CONFLICT',409);
}

function restoreArchive(storage:DirectoryStorage,archive:AuthorityArchive,celld:boolean){
 const joined=joinedTransactions(storage);return joined.transactionSync(()=>{
  const objects=joined.sql.exec<{name:string;type:string;tbl_name:string}>("SELECT name, type, tbl_name FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").toArray();
  const containsApplicationSchema=objects.some(({name,type,tbl_name})=>!celld||!(
   (type==='table'&&celldReservedTables.has(name))||
   (type==='trigger'&&tbl_name==='_cf_ALARM'&&celldAlarmTriggers.has(name))
  ));
  if(containsApplicationSchema)throw new ControlError('FINOPS_RESTORE_REQUIRES_EMPTY_DATABASE',409);
  for(const table of archive.snapshot.tables){joined.sql.exec(`CREATE TABLE ${table.name} (${schemas[table.name]})`);for(const row of table.rows)joined.sql.exec(`INSERT INTO ${table.name} (${columns(table.name).join(', ')}) VALUES (${row.map(()=>'?').join(', ')})`,...row);}
  validate(joined,archive.snapshot.kind,archive.snapshot.tenant,archive.snapshot.schemaVersion);if(rateHash(capture(joined,archive.snapshot.kind,archive.snapshot.schemaVersion))!==archive.manifest.sourceRevision)throw new ControlError('FINOPS_RESTORE_READBACK_MISMATCH',503);
  return {kind:archive.manifest.kind,tenantId:archive.manifest.tenantId,digest:archive.manifest.digest,sourceRevision:archive.manifest.sourceRevision,tableCounts:archive.manifest.tableCounts,operationalResume:'DISABLED' as const};
 });
}
