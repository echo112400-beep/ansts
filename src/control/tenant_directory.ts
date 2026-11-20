import { createHash } from 'node:crypto';
import { assertControlAccess, assertExpectedVersion, ControlActor, ControlError } from './contracts.js';

export interface DirectorySql {
  exec<T = Record<string, unknown>>(query: string, ...bindings: unknown[]): { toArray(): T[] };
}
export interface DirectoryStorage {
  sql: DirectorySql;
  /** Must be a real durable transaction, not the legacy JSON SQL emulator. */
  transactionSync<T>(callback: () => T): T;
}
export interface TenantRecord {
  id: string;
  region: string;
  name: string;
  costCenter: string;
  status: 'ACTIVE' | 'DISABLED';
  version: number;
  costCenterHistory?: {costCenter:string;version:number;timestamp:string|null}[];
}
interface AuditRecord {
  sequence: number;
  timestamp: string;
  actor: string;
  tenantId: string;
  reason: string;
  requestId: string;
  changes: {field:string;before:string|null;after:string}[];
  previousVersion: number;
  version: number;
}
export interface UnifiedAuditRecord {id:string;domain:"tenant"|"cost-center"|"rate-card"|"rate-activation";targetId:string;actor:string;reason:string;requestId:string;timestamp:string;previousVersion:number;version:number;changes:{field:string;before:string|null;after:string}[]}
export interface CostCenterRecord { id:string; name:string; status:"ACTIVE"|"DISABLED"; version:number; }
export interface CostCenterMutation extends Omit<CostCenterRecord,"version"> { expectedVersion:number; reason:string; requestId:string; idempotencyKey:string; }
interface Snapshot {
  schemaVersion: 1;
  costCenters?: CostCenterRecord[];
  costCenterAudit?: {actor:string;id:string;reason:string;requestId:string;previousVersion:number;version:number;timestamp:string;changes:{field:string;before:string|null;after:string}[]}[];
  costCenterReceipts?: Record<string,{digest:string;result:CostCenterRecord}>;
  tenants: TenantRecord[];
  audit: AuditRecord[];
  receipts: Record<string, { digest: string; result: TenantRecord }>;
}
export interface TenantMutation {
  id: string;
  region: string;
  name: string;
  costCenter: string;
  status: 'ACTIVE' | 'DISABLED';
  expectedVersion: number;
  reason: string;
  requestId: string;
  idempotencyKey: string;
}
function bounded(value: unknown, max: number): value is string {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= max && !/[\u0000-\u001f]/.test(value);
}
/** Small authoritative directory. Every mutation and receipt commits with its audit. */
export class TenantDirectory {
  constructor(private readonly storage: DirectoryStorage) {
    if (typeof storage.transactionSync !== 'function') throw new ControlError('DURABLE_TRANSACTION_REQUIRED', 503);
    storage.transactionSync(() => {
      storage.sql.exec('CREATE TABLE IF NOT EXISTS control_directory (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)');
      const existing = storage.sql.exec<{payload_json: string}>('SELECT payload_json FROM control_directory WHERE id = ?', 'directory').toArray();
      if (!existing.length) this.save({ schemaVersion: 1, tenants: [], audit: [], receipts: {} });
      this.load();
      storage.sql.exec("CREATE TABLE IF NOT EXISTS control_directory_archives (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, digest TEXT NOT NULL, manifest_json TEXT NOT NULL)");
      storage.sql.exec("CREATE TABLE IF NOT EXISTS control_directory_archive_requests (id TEXT PRIMARY KEY, digest TEXT NOT NULL, archive_id TEXT NOT NULL)");
    });
  }
  private load(): Snapshot {
    const rows = this.storage.sql.exec<{payload_json: string}>('SELECT payload_json FROM control_directory WHERE id = ?', 'directory').toArray();
    let state: Snapshot | null;
    try { state = rows.length ? JSON.parse(rows[0].payload_json) as Snapshot : null; } catch { throw new ControlError("DIRECTORY_STATE_CORRUPT",503); }
    if (!state || state.schemaVersion !== 1 || !Array.isArray(state.tenants) || !Array.isArray(state.audit) || !state.receipts) {
      throw new ControlError('DIRECTORY_SCHEMA_UNSUPPORTED', 503);
    }
    return state;
  }
  private save(state: Snapshot): void {
    this.storage.sql.exec('INSERT OR REPLACE INTO control_directory (id, payload_json) VALUES (?, ?)', 'directory', JSON.stringify(state));
  }
  tenant(actor:ControlActor,id:string):TenantRecord {
    const tenant=this.load().tenants.find(record=>record.id===id);
    if(!tenant)throw new ControlError('TENANT_NOT_FOUND',404);
    try{assertControlAccess(actor,'read',id,tenant.region);}catch{throw new ControlError('TENANT_NOT_FOUND',404);}
    return tenant;
  }
  list(actor: ControlActor): { tenants: TenantRecord[]; version: number } {
    const state = this.load();
    const tenants = state.tenants.filter(t => {
      try { assertControlAccess(actor, 'read', t.id, t.region); return true; } catch { return false; }
    });
    return { tenants, version: state.audit.length };
  }
  audit(actor: ControlActor): AuditRecord[] {
    if (actor.role !== 'SRE_ADMIN') throw new ControlError('CONTROL_FORBIDDEN', 403);
    const state = this.load();
    return state.audit.filter(a => {
      const tenant = state.tenants.find(t => t.id === a.tenantId);
      if (!tenant) return false;
      try { assertControlAccess(actor, 'read', tenant.id, tenant.region); return true; } catch { return false; }
    });
  }
  private page<T>(actor: ControlActor, rows: T[], version: number, options: {limit?:number;cursor?:string}, kind: string): {items:T[];version:number;nextCursor:string|null} {
    const limit = options.limit ?? 50;
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new ControlError('INVALID_PAGE_LIMIT',422);
    const scope = createHash('sha256').update(JSON.stringify([actor.sub,actor.role,actor.scopeTenant,actor.scopeRegion,kind])).digest('hex');
    let offset = 0;
    if (options.cursor) {
      try {
        if (options.cursor.length>1024) throw new Error();
        const cursor=JSON.parse(Buffer.from(options.cursor,'base64url').toString('utf8'));
        if(cursor.scope!==scope || cursor.version!==version) throw new ControlError('CURSOR_STALE',409);
        if(!Number.isSafeInteger(cursor.offset)||cursor.offset<0||cursor.offset>rows.length) throw new Error();
        offset=cursor.offset;
      } catch(e) {if(e instanceof ControlError) throw e;throw new ControlError('INVALID_CURSOR',422);}
    }
    const items=rows.slice(offset,offset+limit);
    const next=offset+items.length;
    return {items,version,nextCursor:next<rows.length?Buffer.from(JSON.stringify({scope,version,offset:next})).toString('base64url'):null};
  }
  listPage(actor: ControlActor, options: {limit?:number;cursor?:string} = {}) {
    const listed=this.list(actor);
    return this.page(actor,listed.tenants.sort((a,b)=>a.id<b.id?-1:a.id>b.id?1:0),listed.version,options,'tenants');
  }
  auditPage(actor: ControlActor, options: {limit?:number;cursor?:string} = {}) {
    const state=this.load();
    return this.page(actor,this.audit(actor),state.audit.length,options,'audit');
  }
  private globalAdministrator(actor:ControlActor):void {
    if(actor.role!=='SRE_ADMIN'||actor.scopeTenant!=='all'||actor.scopeRegion!=='all'||!actor.sub)throw new ControlError('CONTROL_FORBIDDEN',403);
  }
  archiveStatus(actor:ControlActor) {
    this.globalAdministrator(actor);const state=this.load();
    const rows=this.storage.sql.exec<{manifest_json:string}>('SELECT manifest_json FROM control_directory_archives').toArray();
    return {currentVersion:state.audit.length+(state.costCenterAudit?.length??0),items:rows.map(row=>JSON.parse(row.manifest_json)),receiptRetention:'ALL_RETAINED',safeReplayWatermark:null};
  }
  readArchive(actor:ControlActor,id:string) {
    this.globalAdministrator(actor);
    if(!/^[a-f0-9]{64}$/.test(id))throw new ControlError('ARCHIVE_NOT_FOUND',404);
    const row=this.storage.sql.exec<{payload_json:string;digest:string;manifest_json:string}>('SELECT payload_json, digest, manifest_json FROM control_directory_archives WHERE id = ?',id).toArray()[0];
    if(!row)throw new ControlError('ARCHIVE_NOT_FOUND',404);
    if(createHash('sha256').update(row.payload_json).digest('hex')!==row.digest)throw new ControlError('ARCHIVE_INTEGRITY_FAILED',503);
    let manifest,snapshot;
    try {manifest=JSON.parse(row.manifest_json);snapshot=JSON.parse(row.payload_json);}catch{throw new ControlError("ARCHIVE_INTEGRITY_FAILED",503);}
    if(manifest.id!==id||manifest.schemaVersion!==1||manifest.digest!==row.digest||manifest.bytes!==Buffer.byteLength(row.payload_json)||snapshot.schemaVersion!==1)throw new ControlError('ARCHIVE_INTEGRITY_FAILED',503);
    return {manifest,snapshot};
  }
  archive(actor:ControlActor,input:{expectedVersion:number;reason:string;idempotencyKey:string;requestId:string}) {
    this.globalAdministrator(actor);
    if(!input||!bounded(input.reason,1000)||!bounded(input.idempotencyKey,128)||!bounded(input.requestId,128))throw new ControlError('INVALID_ARCHIVE_REQUEST',422);
    const key=createHash('sha256').update(JSON.stringify([actor.sub,input.idempotencyKey])).digest('hex');
    const requestDigest=createHash('sha256').update(JSON.stringify([input.expectedVersion,input.reason])).digest('hex');
    return this.storage.transactionSync(()=>{
      const receipt=this.storage.sql.exec<{digest:string;archive_id:string}>('SELECT digest, archive_id FROM control_directory_archive_requests WHERE id = ?',key).toArray()[0];
      if(receipt){if(receipt.digest!==requestDigest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);return this.readArchive(actor,receipt.archive_id).manifest;}
      const state=this.load();const version=state.audit.length+(state.costCenterAudit?.length??0);assertExpectedVersion(input.expectedVersion,version);
      if(this.storage.sql.exec('SELECT id FROM control_directory_archives').toArray().length>=100)throw new ControlError('ARCHIVE_CAPACITY_REACHED',503);
      const payload=JSON.stringify(state),digest=createHash('sha256').update(payload).digest('hex');
      const id=createHash('sha256').update(JSON.stringify([key,requestDigest,digest])).digest('hex');
      const manifest={id,schemaVersion:1,sourceVersion:version,digest,actor:actor.sub,reason:input.reason,requestId:input.requestId,createdAt:new Date().toISOString(),bytes:Buffer.byteLength(payload),tenantAuditWatermark:state.audit.length,costCenterAuditWatermark:state.costCenterAudit?.length??0,tenantReceipts:Object.keys(state.receipts).length,costCenterReceipts:Object.keys(state.costCenterReceipts??{}).length,receiptRetention:'ALL_RETAINED',safeReplayWatermark:null};
      this.storage.sql.exec('INSERT INTO control_directory_archives (id, payload_json, digest, manifest_json) VALUES (?, ?, ?, ?)',id,payload,digest,JSON.stringify(manifest));
      this.storage.sql.exec('INSERT INTO control_directory_archive_requests (id, digest, archive_id) VALUES (?, ?, ?)',key,requestDigest,id);
      // Read-back must verify the committed candidate before publishing its receipt.
      this.readArchive(actor,id);return manifest;
    });
  }
  operationId(domain:'tenant'|'cost-center',id:string,idempotencyKey:string):string {
    return domain+':'+createHash('sha256').update(JSON.stringify([id,idempotencyKey])).digest('hex');
  }
  operation(actor:ControlActor,id:string) {
    if(actor.role!=='SRE_ADMIN')throw new ControlError('CONTROL_FORBIDDEN',403);
    if(!/^(tenant|cost-center):[a-f0-9]{64}$/.test(id))throw new ControlError('OPERATION_NOT_FOUND',404);
    const [domain,key]=id.split(':');const state=this.load();
    if(domain==='cost-center') {
      if(actor.scopeTenant!=='all'||actor.scopeRegion!=='all')throw new ControlError('OPERATION_NOT_FOUND',404);
      const receipt=state.costCenterReceipts?.[key];if(!receipt)throw new ControlError('OPERATION_NOT_FOUND',404);
      const audit=state.costCenterAudit?.find(entry=>entry.id===receipt.result.id&&entry.version===receipt.result.version);
      return {operationId:id,status:'SUCCEEDED' as const,domain,targetId:receipt.result.id,resultVersion:receipt.result.version,requestId:audit?.requestId??null,timestamp:audit?.timestamp??null};
    }
    const receipt=state.receipts[key];if(!receipt)throw new ControlError('OPERATION_NOT_FOUND',404);
    const current=state.tenants.find(t=>t.id===receipt.result.id);
    try {assertControlAccess(actor,'read',receipt.result.id,receipt.result.region);if(!current)throw new Error();assertControlAccess(actor,'read',current.id,current.region);}catch{throw new ControlError('OPERATION_NOT_FOUND',404);}
    const audit=state.audit.find(entry=>entry.tenantId===receipt.result.id&&entry.version===receipt.result.version);
    return {operationId:id,status:'SUCCEEDED' as const,domain,targetId:receipt.result.id,resultVersion:receipt.result.version,requestId:audit?.requestId??null,timestamp:audit?.timestamp??null};
  }
  unifiedAuditPage(actor:ControlActor,options:{limit?:number;cursor?:string;domain?:string}={}) {
    const domain=options.domain??'all';
    if(!['all','tenant','cost-center','rate-card'].includes(domain))throw new ControlError('INVALID_AUDIT_DOMAIN',422);
    const state=this.load();
    const records:UnifiedAuditRecord[]=this.audit(actor).map(entry=>({...entry,id:'tenant:'+entry.tenantId+':'+entry.sequence,domain:'tenant',targetId:entry.tenantId}));
    if(actor.scopeTenant==='all'&&actor.scopeRegion==='all') for(const [index,entry] of (state.costCenterAudit??[]).entries()) records.push({...entry,id:'cost-center:'+entry.id+':'+index,domain:'cost-center',targetId:entry.id});
    let rateCount=0;
    const rateTable=this.storage.sql.exec("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'directory_rate_audit'").toArray()[0];
    if(rateTable&&actor.scopeTenant==='all'&&actor.scopeRegion==='all')for(const row of this.storage.sql.exec<{version:number;payload_json:string}>('SELECT version, payload_json FROM directory_rate_audit ORDER BY version').toArray()){
      const entry=JSON.parse(row.payload_json);rateCount++;
      records.push({id:'rate-card:'+row.version,domain:'rate-card',targetId:'rate-card:'+row.version,actor:entry.actor,reason:entry.reason,requestId:entry.requestId,timestamp:new Date(entry.at).toISOString(),previousVersion:row.version-1,version:row.version,changes:[{field:'digest',before:null,after:entry.digest}]});
    }
    const visible=records.filter(entry=>domain==='all'||entry.domain===domain).sort((a,b)=>a.timestamp<b.timestamp?-1:a.timestamp>b.timestamp?1:a.id<b.id?-1:a.id>b.id?1:0);
    return {...this.page(actor,visible,state.audit.length+(state.costCenterAudit?.length??0)+rateCount,options,'unified-audit:'+domain),coverage:{includedDomains:['tenant','cost-center','rate-card'],tenantSelectionRequired:['rate-activation']}};
  }
  costCenters(actor:ControlActor) {
    const state=this.load();
    const visible=new Set(this.list(actor).tenants.map(t=>t.costCenter));
    return (state.costCenters??[]).filter(c=>(actor.scopeTenant==='all'&&actor.scopeRegion==='all')||visible.has(c.id));
  }
  costCentersPage(actor:ControlActor,options:{limit?:number;cursor?:string}={}) {
    const state=this.load();
    return this.page(actor,this.costCenters(actor).sort((a,b)=>a.id<b.id?-1:a.id>b.id?1:0),(state.costCenterAudit?.length??0)+state.audit.length,options,'cost-centers');
  }
  mutateCostCenter(actor:ControlActor,input:CostCenterMutation):CostCenterRecord {
    if(actor.role!=='SRE_ADMIN'||actor.scopeTenant!=='all'||actor.scopeRegion!=='all'||!actor.sub) throw new ControlError('CONTROL_FORBIDDEN',403);
    if(!input||!bounded(input.id,128)||!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]*$/.test(input.id)||!bounded(input.name,200)||!bounded(input.reason,1000)||!bounded(input.requestId,128)||!bounded(input.idempotencyKey,128)||!['ACTIVE','DISABLED'].includes(input.status)) throw new ControlError('INVALID_COST_CENTER',422);
    const digest=createHash('sha256').update(JSON.stringify([actor.sub,input.id,input.name,input.status,input.expectedVersion,input.reason])).digest('hex');
    const key=createHash('sha256').update(JSON.stringify([input.id,input.idempotencyKey])).digest('hex');
    return this.storage.transactionSync(()=>{
      const state=this.load();state.costCenters??=[];state.costCenterAudit??=[];state.costCenterReceipts??={};
      const receipt=state.costCenterReceipts[key];
      if(receipt){if(receipt.digest!==digest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);return receipt.result;}
      const current=state.costCenters.find(c=>c.id===input.id);assertExpectedVersion(input.expectedVersion,current?.version??0);
      if(state.costCenterAudit.length>=10000)throw new ControlError('DIRECTORY_CAPACITY_REACHED',503);
      const result:CostCenterRecord={id:input.id,name:input.name,status:input.status,version:input.expectedVersion+1};
      state.costCenters=state.costCenters.filter(c=>c.id!==input.id).concat(result);
      state.costCenterAudit.push({actor:actor.sub,id:input.id,reason:input.reason,requestId:input.requestId,previousVersion:input.expectedVersion,version:result.version,timestamp:new Date().toISOString(),changes:(['name','status'] as const).flatMap(field=>current?.[field]===result[field]?[]:[{field,before:current?.[field]??null,after:result[field]}])});
      state.costCenterReceipts[key]={digest,result};
      if(JSON.stringify(state).length>8*1024*1024)throw new ControlError('DIRECTORY_CAPACITY_REACHED',503);
      this.save(state);return result;
    });
  }
  mutate(actor: ControlActor, input: TenantMutation): TenantRecord {
    if (!input || !bounded(input.id, 128) || !/^[a-zA-Z0-9][a-zA-Z0-9_.:-]*$/.test(input.id) ||
        !bounded(input.region, 128) || !bounded(input.name, 200) || !bounded(input.costCenter, 128) ||
        !bounded(input.reason, 1000) || !bounded(input.requestId, 128) || !bounded(input.idempotencyKey, 128) ||
        !['ACTIVE', 'DISABLED'].includes(input.status)) throw new ControlError('INVALID_TENANT_MUTATION', 422);
    assertControlAccess(actor, 'administer', input.id, input.region);
    // Canonical field order: caller JSON ordering and request IDs do not affect replay.
    const { id, region, name, costCenter, status, expectedVersion, reason } = input;
    const digest = createHash('sha256').update(JSON.stringify({actor:actor.sub,id,region,name,costCenter,status,expectedVersion,reason})).digest('hex');
    const key = createHash('sha256').update(JSON.stringify([id, input.idempotencyKey])).digest('hex');
    return this.storage.transactionSync(() => {
      const state = this.load();
      const existing = state.tenants.find(t => t.id === id);
      // Check old scope too: a scoped administrator cannot move an out-of-scope tenant.
      if (existing) assertControlAccess(actor, 'administer', id, existing.region);
      const receipt = state.receipts[key];
      if (receipt) {
        if (receipt.digest !== digest) throw new ControlError('IDEMPOTENCY_CONFLICT', 409);
        return receipt.result;
      }
      assertExpectedVersion(expectedVersion, existing?.version ?? 0);
      if(!existing || existing.costCenter!==costCenter) {
        const center=state.costCenters?.find(c=>c.id===costCenter);
        if(!center||center.status!=='ACTIVE')throw new ControlError('COST_CENTER_NOT_ACTIVE',422);
      }
      // Bound snapshot growth until row-based storage/archival is implemented.
      if (state.audit.length >= 10000 || JSON.stringify(state).length > 8*1024*1024) throw new ControlError('DIRECTORY_CAPACITY_REACHED',503);
      const timestamp=new Date().toISOString();
      const history=existing?.costCenterHistory ?? (existing ? [{costCenter:existing.costCenter,version:existing.version,timestamp:null}] : []);
      const changedCenter=!existing || existing.costCenter!==costCenter;
      const result: TenantRecord = { id, region, name, costCenter, status, version: expectedVersion + 1,
        costCenterHistory: changedCenter ? [...history,{costCenter,version:expectedVersion+1,timestamp}] : history };
      const changes=['name','region','costCenter','status'].flatMap(field=> {
        const key=field as 'name'|'region'|'costCenter'|'status';
        return existing?.[key]===result[key]?[]:[{field,before:existing?.[key]??null,after:result[key]}];
      });
      state.tenants = state.tenants.filter(t => t.id !== id).concat(result);
      state.audit.push({sequence:state.audit.length+1,timestamp,changes,actor:actor.sub,tenantId:id,reason,requestId:input.requestId,previousVersion:expectedVersion,version:result.version});
      state.receipts[key] = {digest,result};
      if (JSON.stringify(state).length > 8*1024*1024) throw new ControlError("DIRECTORY_CAPACITY_REACHED",503);
      this.save(state);
      return result;
    });
  }
}
