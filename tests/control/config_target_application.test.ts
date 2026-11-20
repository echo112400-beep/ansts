import assert from 'node:assert/strict';
import {test} from 'node:test';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {ConfigTargetApplication} from '../../src/config_release/config_target_application.js';
import {configContentDigest,type ConfigContent} from '../../src/config_release/config_versions.js';
import type {DirectoryStorage} from '../../src/control/tenant_directory.js';

const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');

test('target acknowledges only matching persisted readback and fences stale or conflicting commands',()=>{
  const db=new DatabaseSync(':memory:');
  const storage:DirectoryStorage={sql:{exec<T>(query:string,...args:unknown[]){
    const rows=db.prepare(query).all(...args) as T[];
    return {toArray:()=>rows};
  }},transactionSync<T>(fn:()=>T){
    db.exec('BEGIN IMMEDIATE');
    try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}
  }};
  db.exec('CREATE TABLE runtime_config (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)');
  let calls=0,misapply=false;
  const runtime={
    read(){
      const row=storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM runtime_config').toArray()[0];
      return row?JSON.parse(row.payload_json) as ConfigContent:null;
    },
    apply(content:ConfigContent){
      calls++;
      storage.sql.exec('INSERT OR REPLACE INTO runtime_config (id, payload_json) VALUES (?, ?)', 'actual',
        JSON.stringify(misapply?{defaultTaskTimeoutSeconds:123}:content));
    },
  };
  try{
    const adapter=new ConfigTargetApplication(storage,runtime);
    const content={defaultTaskTimeoutSeconds:60};
    const command={releaseId:'release-2',generation:2,content,contentDigest:configContentDigest(content)};
    assert.equal(adapter.dryRun(command).actualDigest,null);
    assert.equal(calls,0);
    misapply=true;
    assert.throws(()=>adapter.apply(command),{code:'CONFIG_READBACK_MISMATCH'});
    assert.equal(runtime.read(),null);
    assert.equal(adapter.readback().generation,0);
    misapply=false;
    const ack=adapter.apply(command);
    const restarted=new ConfigTargetApplication(storage,runtime);
    assert.deepEqual(restarted.apply(command),ack);
    assert.equal(calls,2);
    assert.deepEqual(restarted.readback(),{generation:2,actualDigest:command.contentDigest,verified:true});
    assert.throws(()=>restarted.apply({...command,generation:1}),{code:'STALE_CONFIG_GENERATION'});
    assert.throws(()=>restarted.apply({...command,releaseId:'different'}),{code:'CONFIG_GENERATION_CONFLICT'});
    runtime.apply({defaultTaskTimeoutSeconds:90});
    assert.equal(restarted.readback().verified,false);
    assert.throws(()=>restarted.apply(command),{code:'CONFIG_READBACK_MISMATCH'});
    const withThreshold={...content,unhealthyNodeThresholdBps:500};
    restarted.apply({...command,generation:3,content:withThreshold,contentDigest:configContentDigest(withThreshold)});
    const restore={...command,releaseId:'rollback-4',generation:4};
    assert.throws(()=>restarted.apply(restore),{code:'CONFIG_FIELD_REMOVAL_UNSUPPORTED'});
    assert.equal(restarted.apply({...restore,rollback:true}).generation,4);
    assert.deepEqual(runtime.read(),content);
  }finally{db.close();}
});
