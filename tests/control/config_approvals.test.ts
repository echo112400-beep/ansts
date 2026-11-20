import assert from 'node:assert/strict';
import {test} from 'node:test';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {ConfigApprovals} from '../../src/config_release/config_approvals.js';
import type {ControlActor} from '../../src/control/contracts.js';
import type {DirectoryStorage} from '../../src/control/tenant_directory.js';

const requester:ControlActor={sub:'github:1',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'};
const approver:ControlActor={...requester,sub:'github:2'};
const admins=[requester.sub,approver.sub];
const basis={revision:1,contentDigest:'a'.repeat(64),targetIds:['fleet-b','fleet-a']};
const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');

test('configuration approval requires an independent admin and freezes the revision and target set',()=>{
  const db=new DatabaseSync(':memory:');
  const storage:DirectoryStorage={sql:{exec<T>(query:string,...args:unknown[]){
    const rows=db.prepare(query).all(...args) as T[];
    return {toArray:()=>rows};
  }},transactionSync<T>(fn:()=>T){
    db.exec('BEGIN IMMEDIATE');
    try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}
  }};
  try{
    const approvals=new ConfigApprovals(storage);
    assert.throws(()=>approvals.submit(requester,[requester.sub,requester.sub],basis),{code:'INDEPENDENT_APPROVER_UNAVAILABLE'});
    const submitted=approvals.submit(requester,admins,basis);
    assert.deepEqual(submitted.basis.targetIds,['fleet-a','fleet-b']);
    assert.throws(()=>approvals.decide(requester,admins,submitted.sequence,basis,true),{code:'INDEPENDENT_APPROVER_REQUIRED'});
    assert.throws(()=>approvals.decide(approver,admins,submitted.sequence,{...basis,targetIds:['fleet-a']},true),{code:'CONFIG_APPROVAL_STALE'});
    assert.throws(()=>approvals.decide(approver,admins,submitted.sequence,{...basis,contentDigest:'b'.repeat(64)},true),{code:'CONFIG_APPROVAL_STALE'});
    const approved=new ConfigApprovals(storage).decide(approver,admins,submitted.sequence,basis,true);
    assert.equal(approved.status,'APPROVED');assert.equal(approved.decidedBy,approver.sub);
    assert.deepEqual(approvals.decide(approver,admins,submitted.sequence,basis,true),approved);
    const changed={...basis,targetIds:['fleet-a']};
    const replacement=approvals.submit(requester,admins,changed);
    assert.equal(replacement.sequence,submitted.sequence+1);
    assert.equal(replacement.status,'SUBMITTED');
    assert.throws(()=>approvals.decide(approver,admins,submitted.sequence,basis,true),{code:'CONFIG_APPROVAL_STALE'});
  }finally{db.close();}
});
