/** Per-scope approval history. Actor and eligible administrator roster are server-owned. */
import {createHash} from 'node:crypto';
import {ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';

export interface ConfigApprovalBasis {
  revision:number;
  contentDigest:string;
  targetIds:readonly string[];
}

export interface ConfigApproval {
  sequence:number;
  basis:ConfigApprovalBasis;
  basisDigest:string;
  requestedBy:string;
  status:'SUBMITTED'|'APPROVED'|'REJECTED';
  decidedBy:string|null;
}

const identityPattern=/^github:[1-9][0-9]{0,15}$/;

function basisValue(input:ConfigApprovalBasis):ConfigApprovalBasis {
  if(!input||!Number.isSafeInteger(input.revision)||input.revision<1||
    typeof input.contentDigest!=='string'||!/^[a-f0-9]{64}$/.test(input.contentDigest)||
    !Array.isArray(input.targetIds)||!input.targetIds.length||input.targetIds.length>24||
    input.targetIds.some(id=>typeof id!=='string'||!/^[a-z0-9-]{1,64}$/.test(id))||
    new Set(input.targetIds).size!==input.targetIds.length){
    throw new ControlError('INVALID_CONFIG_APPROVAL_BASIS',422);
  }
  return {revision:input.revision,contentDigest:input.contentDigest,targetIds:[...input.targetIds].sort()};
}

function digest(basis:ConfigApprovalBasis):string {
  return createHash('sha256').update(JSON.stringify(basis)).digest('hex');
}

export class ConfigApprovals {
  constructor(private readonly storage:DirectoryStorage){
    storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS config_approvals (sequence INTEGER PRIMARY KEY, payload_json TEXT NOT NULL)'));
  }

  private authorize(actor:ControlActor,roster:readonly string[]):string {
    if(!actor||actor.role!=='SRE_ADMIN'||actor.scopeTenant!=='all'||actor.scopeRegion!=='all'||
      !identityPattern.test(actor.sub)||!roster.includes(actor.sub)){
      throw new ControlError('CONTROL_FORBIDDEN',403);
    }
    if(new Set(roster.filter(subject=>identityPattern.test(subject))).size<2){
      throw new ControlError('INDEPENDENT_APPROVER_UNAVAILABLE',409);
    }
    return actor.sub;
  }

  latest():ConfigApproval|null {
    const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM config_approvals ORDER BY sequence DESC LIMIT 1').toArray()[0];
    return row?JSON.parse(row.payload_json) as ConfigApproval:null;
  }

  requireApproved(actor:ControlActor,roster:readonly string[],current:ConfigApprovalBasis):ConfigApproval {
    this.authorize(actor,roster);
    const record=this.latest();
    if(!record||record.status!=='APPROVED'||record.basisDigest!==digest(basisValue(current))||
      !record.decidedBy||!roster.includes(record.requestedBy)||!roster.includes(record.decidedBy)){
      throw new ControlError('CONFIG_APPROVAL_REQUIRED',409);
    }
    return record;
  }

  submit(actor:ControlActor,roster:readonly string[],current:ConfigApprovalBasis):ConfigApproval {
    const requestedBy=this.authorize(actor,roster);
    const basis=basisValue(current),basisDigest=digest(basis);
    return this.storage.transactionSync(()=>{
      const previous=this.latest();
      if(previous?.basisDigest===basisDigest&&previous.status!=='REJECTED'){
        if(previous.requestedBy!==requestedBy)throw new ControlError('CONFIG_APPROVAL_ALREADY_SUBMITTED',409);
        return previous;
      }
      const record:ConfigApproval={sequence:(previous?.sequence??0)+1,basis,basisDigest,requestedBy,status:'SUBMITTED',decidedBy:null};
      this.storage.sql.exec('INSERT INTO config_approvals (sequence, payload_json) VALUES (?, ?)',record.sequence,JSON.stringify(record));
      return record;
    });
  }

  decide(actor:ControlActor,roster:readonly string[],sequence:number,current:ConfigApprovalBasis,approved:boolean):ConfigApproval {
    const decidedBy=this.authorize(actor,roster);
    const basisDigest=digest(basisValue(current));
    if(typeof approved!=='boolean')throw new ControlError('INVALID_CONFIG_APPROVAL_DECISION',422);
    return this.storage.transactionSync(()=>{
      const record=this.latest();
      if(!record||record.sequence!==sequence||record.basisDigest!==basisDigest){
        throw new ControlError('CONFIG_APPROVAL_STALE',409);
      }
      if(record.requestedBy===decidedBy)throw new ControlError('INDEPENDENT_APPROVER_REQUIRED',403);
      if(!roster.includes(record.requestedBy))throw new ControlError('CONFIG_REQUESTER_NO_LONGER_ELIGIBLE',409);
      const status=approved?'APPROVED':'REJECTED';
      if(record.status!=='SUBMITTED'){
        if(record.status===status&&record.decidedBy===decidedBy)return record;
        throw new ControlError('CONFIG_APPROVAL_TERMINAL',409);
      }
      const result:ConfigApproval={...record,status,decidedBy};
      this.storage.sql.exec('UPDATE config_approvals SET payload_json = ? WHERE sequence = ?',JSON.stringify(result),sequence);
      return result;
    });
  }
}
