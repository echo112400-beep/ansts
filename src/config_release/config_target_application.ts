/** Apply and read back a configuration against one target's native transaction. */
import {canonicalConfigContent,configContentDigest,type ConfigContent} from './config_versions.js';
import {ControlError} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';

export interface TargetReleaseCommand {
  releaseId:string;
  generation:number;
  contentDigest:string;
  content:unknown;
  rollback?:true;
}

export interface TargetConfigRuntime {
  /** These methods must use the target storage passed to ConfigTargetApplication. */
  read():ConfigContent|null;
  apply(content:ConfigContent):void;
}

interface AppliedRelease {
  releaseId:string;
  generation:number;
  actualDigest:string;
}

export class ConfigTargetApplication {
  constructor(private readonly storage:DirectoryStorage,private readonly runtime:TargetConfigRuntime){
    storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS config_applied_release (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)'));
  }

  private last():AppliedRelease|null {
    const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM config_applied_release WHERE id = ?', 'target').toArray()[0];
    return row?JSON.parse(row.payload_json) as AppliedRelease:null;
  }

  private actualDigest():string|null {
    const actual=this.runtime.read();
    return actual===null?null:configContentDigest(canonicalConfigContent(actual));
  }

  private validate(command:TargetReleaseCommand,last:AppliedRelease|null):ConfigContent {
    if(!command||typeof command.releaseId!=='string'||!/^[a-zA-Z0-9_.:-]{1,128}$/.test(command.releaseId)||
      !Number.isSafeInteger(command.generation)||command.generation<1){
      throw new ControlError('INVALID_CONFIG_RELEASE_COMMAND',422);
    }
    const content=canonicalConfigContent(command.content);
    if(configContentDigest(content)!==command.contentDigest)throw new ControlError('CONFIG_DIGEST_CONFLICT',409);
    if(last&&command.generation<last.generation)throw new ControlError('STALE_CONFIG_GENERATION',409);
    if(last&&command.generation===last.generation&&
      (command.releaseId!==last.releaseId||command.contentDigest!==last.actualDigest)){
      throw new ControlError('CONFIG_GENERATION_CONFLICT',409);
    }
    if(content.modelCredentialRef!==undefined)throw new ControlError('CONFIG_RESTART_UNSUPPORTED',422);
    const actual=this.runtime.read();
    if(command.rollback!==true&&actual&&Object.keys(actual).some(key=>!Object.hasOwn(content,key))){
      throw new ControlError('CONFIG_FIELD_REMOVAL_UNSUPPORTED',422);
    }
    return content;
  }

  dryRun(command:TargetReleaseCommand){
    return this.storage.transactionSync(()=>{
      const previous=this.last();
      const content=this.validate(command,previous);
      return {generation:command.generation,desiredDigest:configContentDigest(content),actualDigest:this.actualDigest()};
    });
  }

  readback(){
    return this.storage.transactionSync(()=>{
      const last=this.last(),actualDigest=this.actualDigest();
      return {generation:last?.generation??0,actualDigest,verified:!!last&&last.actualDigest===actualDigest};
    });
  }

  apply(command:TargetReleaseCommand):AppliedRelease {
    return this.storage.transactionSync(()=>{
      const last=this.last(),content=this.validate(command,last);
      if(!last||last.generation!==command.generation)this.runtime.apply(content);
      const actualDigest=this.actualDigest();
      if(actualDigest!==command.contentDigest)throw new ControlError('CONFIG_READBACK_MISMATCH',409);
      if(last?.generation===command.generation)return last;
      const applied={releaseId:command.releaseId,generation:command.generation,actualDigest};
      this.storage.sql.exec('INSERT OR REPLACE INTO config_applied_release (id, payload_json) VALUES (?, ?)', 'target',JSON.stringify(applied));
      return applied;
    });
  }
}
