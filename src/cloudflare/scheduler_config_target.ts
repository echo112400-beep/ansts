/** Scheduler-owned configuration consumed by task submission and readiness. */
import {ConfigTargetApplication,type TargetReleaseCommand} from '../config_release/config_target_application.js';
import {canonicalConfigContent,type ConfigContent} from '../config_release/config_versions.js';
import {ControlError} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';
import {equalBodyToken} from './body_auth.js';

export class SchedulerConfigTarget {
  private readonly application:ConfigTargetApplication;

  constructor(private readonly storage:DirectoryStorage){
    storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS scheduler_runtime_config (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)'));
    this.application=new ConfigTargetApplication(storage,{
      read:()=>this.read(),
      apply:content=>{storage.sql.exec('INSERT OR REPLACE INTO scheduler_runtime_config (id, payload_json) VALUES (?, ?)',
        'scheduler',JSON.stringify(content));},
    });
  }

  private read():ConfigContent|null {
    const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM scheduler_runtime_config WHERE id = ?', 'scheduler').toArray()[0];
    return row?canonicalConfigContent(JSON.parse(row.payload_json)):null;
  }

  get defaultTaskTimeoutSeconds():number|undefined {return this.read()?.defaultTaskTimeoutSeconds;}
  get unhealthyNodeThresholdBps():number|undefined {return this.read()?.unhealthyNodeThresholdBps;}

  async fetch(request:Request,path:string,token:string|undefined):Promise<Response>{
    if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token)){
      return Response.json({code:'SERVICE_AUTH_REQUIRED'},{status:401});
    }
    try{
      if(path==='/config-target/readback'&&request.method==='GET')return Response.json(this.application.readback());
      if(request.method!=='POST'||!['/config-target/dry-run','/config-target/apply'].includes(path)){
        return Response.json({code:'NOT_FOUND'},{status:404});
      }
      const raw=await request.text();
      if(raw.length>16384)return Response.json({code:'PAYLOAD_TOO_LARGE'},{status:413});
      const command=JSON.parse(raw) as TargetReleaseCommand;
      const content=canonicalConfigContent(command?.content);
      if(content.modelCredentialRef!==undefined||
        content.defaultTaskTimeoutSeconds!==undefined&&content.defaultTaskTimeoutSeconds<5){
        throw new ControlError('CONFIG_TARGET_FIELD_UNSUPPORTED',422);
      }
      return Response.json(path==='/config-target/apply'?this.application.apply(command):this.application.dryRun(command));
    }catch(error){
      if(error instanceof ControlError)return Response.json({code:error.code},{status:error.status});
      if(error instanceof SyntaxError)return Response.json({code:'INVALID_JSON'},{status:400});
      return Response.json({code:'CONFIG_TARGET_UNAVAILABLE'},{status:503});
    }
  }
}
