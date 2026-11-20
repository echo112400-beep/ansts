import assert from 'node:assert/strict';
import {test} from 'node:test';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {ConfigVersions} from '../../src/config_release/config_versions.js';
import type {ControlActor} from '../../src/control/contracts.js';
import type {DirectoryStorage} from '../../src/control/tenant_directory.js';

const admin:ControlActor={sub:'config-admin',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'};
const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');

test('config drafts use per-scope CAS and reconstruct immutable allowlisted contents',()=>{
 const db=new DatabaseSync(':memory:');
 const storage:DirectoryStorage={sql:{exec<T>(query:string,...args:unknown[]){
  const rows=db.prepare(query).all(...args) as T[];
  return {toArray:()=>rows};
 }},transactionSync<T>(fn:()=>T){
  db.exec('BEGIN IMMEDIATE');
  try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}
 }};
 try{
  const store=new ConfigVersions(storage);
  const content={modelCredentialRef:'provider/default',defaultTaskTimeoutSeconds:60};
  const first=store.create(admin,'global',1,0,content);
  const other=store.create(admin,'dev',1,0,{defaultTaskTimeoutSeconds:60,modelCredentialRef:'provider/default'});
  assert.equal(other.contentDigest,first.contentDigest);
  assert.throws(()=>store.create(admin,'global',1,0,content),{code:'VERSION_CONFLICT'});
  for(const invalid of [{databaseUrl:'private'}, {modelCredentialRef:'raw-key'}, {defaultTaskTimeoutSeconds:86401}]){
   assert.throws(()=>store.create(admin,'global',1,1,invalid),{code:'INVALID_CONFIG_CONTENT'});
  }
  assert.throws(()=>store.create({...admin,role:'VIEWER'},'global',1,1,content),{code:'CONTROL_FORBIDDEN'});
  const second=store.create(admin,'global',1,1,{defaultTaskTimeoutSeconds:90});
  const reopened=new ConfigVersions(storage);
  assert.deepEqual(reopened.get(admin,'global',1),first);
  assert.deepEqual(reopened.get(admin,'global'),second);
  assert.deepEqual(reopened.get(admin,'dev'),other);
 }finally{db.close();}
});
