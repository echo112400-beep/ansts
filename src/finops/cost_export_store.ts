import {createHash} from 'node:crypto';
import {assertControlAccess,ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage,TenantRecord} from '../control/tenant_directory.js';
import {encodeCostCsv} from './cost_csv.js';

interface CostExport {
  id:string;
  owner:string;
  tenantId:string;
  tenantVersion:number;
  scopeTenant:string;
  scopeRegion:string;
  status:'PENDING'|'READY';
  csv:string|null;
  expiresAt:number;
}

const EXPORT_TTL_MS=24*60*60*1000;
const MAX_EXPORTS_PER_TENANT=64;

export class CostExportStore {
  constructor(private readonly storage:DirectoryStorage){
    storage.sql.exec('CREATE TABLE IF NOT EXISTS cost_exports (id TEXT PRIMARY KEY, payload TEXT NOT NULL)');
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS cost_export_audit (
      ordinal INTEGER PRIMARY KEY, export_id TEXT NOT NULL, actor TEXT NOT NULL,
      tenant_id TEXT NOT NULL, tenant_version INTEGER NOT NULL, action TEXT NOT NULL, occurred_at INTEGER NOT NULL
    )`);
  }

  private read(actor:ControlActor,tenant:TenantRecord,id:string):CostExport{
    assertControlAccess(actor,'read',tenant.id,tenant.region);
    if(tenant.status!=='ACTIVE')throw new ControlError('CONTROL_FORBIDDEN',403);
    if(!/^[a-f0-9]{64}$/.test(id))throw new ControlError('COST_EXPORT_NOT_FOUND',404);
    const [row]=this.storage.sql.exec<{payload:string}>('SELECT payload FROM cost_exports WHERE id = ?',id).toArray();
    if(!row)throw new ControlError('COST_EXPORT_NOT_FOUND',404);
    const job=JSON.parse(row.payload) as CostExport;
    if(!Number.isSafeInteger(job.expiresAt)||job.expiresAt<=Date.now()||job.tenantId!==tenant.id||job.owner!==actor.sub||job.tenantVersion!==tenant.version||
       job.scopeTenant!==actor.scopeTenant||job.scopeRegion!==actor.scopeRegion)
      throw new ControlError('COST_EXPORT_NOT_FOUND',404);
    return job;
  }

  private audit(job:CostExport,action:'CREATE'|'GENERATE'|'DOWNLOAD'){
    this.storage.sql.exec(`INSERT INTO cost_export_audit
      (export_id,actor,tenant_id,tenant_version,action,occurred_at) VALUES (?,?,?,?,?,?)`,
      job.id,job.owner,job.tenantId,job.tenantVersion,action,Date.now());
  }

  private save(job:CostExport){
    this.storage.sql.exec('INSERT OR REPLACE INTO cost_exports VALUES (?, ?)',job.id,JSON.stringify(job));
  }

  create(actor:ControlActor,tenant:TenantRecord,key:string){
    assertControlAccess(actor,'read',tenant.id,tenant.region);
    if(tenant.status!=='ACTIVE')throw new ControlError('CONTROL_FORBIDDEN',403);
    if(typeof key!=='string'||!/^[a-zA-Z0-9_.:-]{1,128}$/.test(key))
      throw new ControlError('INVALID_IDEMPOTENCY_KEY',422);
    const id=createHash('sha256').update(JSON.stringify([tenant.id,tenant.version,actor.sub,actor.scopeTenant,actor.scopeRegion,key])).digest('hex');
    return this.storage.transactionSync(()=>{
      const now=Date.now();
      this.storage.sql.exec("DELETE FROM cost_exports WHERE COALESCE(json_extract(payload,'$.expiresAt'),0)<=?",now);
      const [existing]=this.storage.sql.exec<{payload:string}>('SELECT payload FROM cost_exports WHERE id = ?',id).toArray();
      if(existing)return this.status(actor,tenant,id);
      const [count]=this.storage.sql.exec<{total:number}>(
        "SELECT COUNT(*) AS total FROM cost_exports WHERE json_extract(payload,'$.tenantId')=?",tenant.id,
      ).toArray();
      if(count.total>=MAX_EXPORTS_PER_TENANT)throw new ControlError('COST_EXPORT_LIMIT_REACHED',429);
      const job:CostExport={id,tenantId:tenant.id,owner:actor.sub,tenantVersion:tenant.version,scopeTenant:actor.scopeTenant,scopeRegion:actor.scopeRegion,status:'PENDING',csv:null,expiresAt:now+EXPORT_TTL_MS};
      this.save(job);this.audit(job,'CREATE');
      return {id,status:'PENDING' as const};
    });
  }

  status(actor:ControlActor,tenant:TenantRecord,id:string){
    const {status}=this.read(actor,tenant,id);
    return {id,status};
  }

  complete(actor:ControlActor,tenant:TenantRecord,id:string,rows:Iterable<readonly string[]>){
    return this.storage.transactionSync(()=>{
      const job=this.read(actor,tenant,id);
      if(job.status==='READY')return {id,status:job.status};
      const csv=encodeCostCsv(rows);
      if(new TextEncoder().encode(csv).length>512*1024)throw new ControlError('COST_EXPORT_TOO_LARGE',413);
      this.save({...job,status:'READY',csv});
      this.audit(job,'GENERATE');
      return {id,status:'READY' as const};
    });
  }

  download(actor:ControlActor,tenant:TenantRecord,id:string){
    return this.storage.transactionSync(()=>{
      const job=this.read(actor,tenant,id);
      if(job.csv===null)throw new ControlError('COST_EXPORT_PENDING',409);
      this.audit(job,'DOWNLOAD');
      return job.csv;
    });
  }
}
