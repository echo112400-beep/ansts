/** Versioned allowlist for draft configuration, independent of runtime application. */
import {createHash} from 'node:crypto';
import {assertExpectedVersion,ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';

export interface ConfigContent {
  defaultTaskTimeoutSeconds?:number;
  unhealthyNodeThresholdBps?:number;
  modelCredentialRef?:'provider/default';
}

export interface ConfigVersion {
  schemaVersion:1;
  scope:string;
  revision:number;
  baseRevision:number;
  contentDigest:string;
  content:ConfigContent;
  actor:string;
  createdAt:number;
  rollback?:{sourceGeneration:number;replacesGeneration:number};
}

function invalid():never {throw new ControlError('INVALID_CONFIG_CONTENT',422);}
function numberInRange(value:unknown,min:number,max:number):number {
  if(typeof value!=='number'||!Number.isSafeInteger(value)||value<min||value>max)invalid();
  return value;
}

export function canonicalConfigContent(value:unknown):ConfigContent {
  if(!value||typeof value!=='object'||Array.isArray(value)||
    ![Object.prototype,null].includes(Object.getPrototypeOf(value)))invalid();
  const fields=value as Record<string,unknown>,keys=Object.keys(fields);
  if(!keys.length||keys.some(key=>!['defaultTaskTimeoutSeconds','unhealthyNodeThresholdBps','modelCredentialRef'].includes(key)))invalid();
  const content:ConfigContent={};
  if(Object.hasOwn(fields,'defaultTaskTimeoutSeconds'))content.defaultTaskTimeoutSeconds=numberInRange(fields.defaultTaskTimeoutSeconds,1,86400);
  if(Object.hasOwn(fields,'unhealthyNodeThresholdBps'))content.unhealthyNodeThresholdBps=numberInRange(fields.unhealthyNodeThresholdBps,0,10000);
  if(Object.hasOwn(fields,'modelCredentialRef')){
    if(fields.modelCredentialRef!=='provider/default')invalid();
    content.modelCredentialRef=fields.modelCredentialRef;
  }
  return content;
}

export function configContentDigest(content:ConfigContent):string {
  return createHash('sha256').update(JSON.stringify({schemaVersion:1,content})).digest('hex');
}

export class ConfigVersions {
  constructor(private readonly storage:DirectoryStorage){
    storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS config_versions (scope TEXT NOT NULL, revision INTEGER NOT NULL, payload_json TEXT NOT NULL, PRIMARY KEY (scope, revision))'));
  }

  private authorize(actor:ControlActor,scope:string,write=false):void {
    if(typeof scope!=='string'||!/^[a-z0-9][a-z0-9_-]{0,79}$/.test(scope))throw new ControlError('INVALID_CONFIG_SCOPE',422);
    if(!actor?.sub||actor.scopeTenant!=='all'||actor.scopeRegion!=='all'||
      !['VIEWER','OPERATOR','SRE_ADMIN'].includes(actor.role)||(write&&actor.role!=='SRE_ADMIN')){
      throw new ControlError('CONTROL_FORBIDDEN',403);
    }
  }

  private read(scope:string,revision?:number):ConfigVersion|null {
    const row=revision===undefined?
      this.storage.sql.exec<{revision:number;payload_json:string}>('SELECT revision, payload_json FROM config_versions WHERE scope = ? ORDER BY revision DESC LIMIT 1',scope).toArray()[0]:
      this.storage.sql.exec<{revision:number;payload_json:string}>('SELECT revision, payload_json FROM config_versions WHERE scope = ? AND revision = ?',scope,revision).toArray()[0];
    if(!row)return null;
    try{
      const record=JSON.parse(row.payload_json) as ConfigVersion;
      const content=canonicalConfigContent(record.content);
      if(record.schemaVersion===1&&record.scope===scope&&record.revision===row.revision&&
        record.baseRevision===row.revision-1&&record.contentDigest===configContentDigest(content)){
        return {schemaVersion:1,scope,revision:row.revision,baseRevision:record.baseRevision,contentDigest:record.contentDigest,
          content,actor:record.actor,createdAt:record.createdAt,...(record.rollback?{rollback:record.rollback}:{})};
      }
    }catch{/* Reject unreadable or non-canonical persisted values. */}
    throw new ControlError('CONFIG_VERSION_CORRUPT',503);
  }

  get(actor:ControlActor,scope:string,revision?:number):ConfigVersion|null {
    this.authorize(actor,scope);
    if(revision!==undefined&&(!Number.isSafeInteger(revision)||revision<1))throw new ControlError('INVALID_VERSION',422);
    return this.read(scope,revision);
  }

  create(actor:ControlActor,scope:string,schemaVersion:number,baseRevision:number,value:unknown,now=Date.now(),
    rollback?:ConfigVersion['rollback']):ConfigVersion {
    this.authorize(actor,scope,true);
    if(schemaVersion!==1)throw new ControlError('UNSUPPORTED_CONFIG_SCHEMA',422);
    const content=canonicalConfigContent(value);
    return this.storage.transactionSync(()=>{
      const previous=this.read(scope);
      assertExpectedVersion(baseRevision,previous?.revision??0);
      const record:ConfigVersion={schemaVersion:1,scope,revision:baseRevision+1,baseRevision,
        contentDigest:configContentDigest(content),content,actor:actor.sub,createdAt:now,...(rollback?{rollback}:{})};
      this.storage.sql.exec('INSERT INTO config_versions (scope, revision, payload_json) VALUES (?, ?, ?)',scope,record.revision,JSON.stringify(record));
      return record;
    });
  }
}
