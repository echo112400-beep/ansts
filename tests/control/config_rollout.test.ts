import assert from 'node:assert/strict';
import {test} from 'node:test';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {ConfigRollouts} from '../../src/config_release/config_rollout.js';
import type {DirectoryStorage} from '../../src/control/tenant_directory.js';

const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');
test('rollout resumes a frozen batch after missing evidence and rejects ACKs from an older release',()=>{
  const dir=mkdtempSync(join(tmpdir(),'ansts-rollout-')),path=join(dir,'rollout.sqlite');
  let db=new DatabaseSync(path);
  const storage:DirectoryStorage={sql:{exec<T>(query:string,...args:unknown[]){
    const rows=db.prepare(query).all(...args) as T[];
    return {toArray:()=>rows};
  }},transactionSync<T>(fn:()=>T){
    db.exec('BEGIN IMMEDIATE');
    try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}
  }};
  try{
    const plan={releaseId:'release-1',contentDigest:'a'.repeat(64),batches:[['first'],['second']],healthWindowMs:100};
    const rollouts=new ConfigRollouts(storage);
    const first=rollouts.start(plan);
    assert.deepEqual(rollouts.start(plan),first);
    assert.throws(()=>rollouts.start({...plan,releaseId:'release-2'}),{code:'CONFIG_ROLLOUT_ACTIVE'});
    const ack={releaseId:plan.releaseId,generation:first.generation,targetId:'first',actualDigest:plan.contentDigest};
    assert.throws(()=>rollouts.acknowledge({...ack,targetId:'second'},1000),{code:'CONFIG_ACK_MISMATCH'});
    assert.equal(rollouts.acknowledge(ack,1000).verifyAfter,1100);
    assert.equal(new ConfigRollouts(storage).verify(ack.releaseId,ack.generation,[],1100).status,'PAUSED');
    db.close();db=new DatabaseSync(path);
    const restarted=new ConfigRollouts(storage);
    assert.equal(restarted.resume(ack.releaseId,ack.generation,1200).verifyAfter,1300);
    assert.equal(restarted.verify(ack.releaseId,ack.generation,[{...ack,observedAt:1300,healthy:true}],1300).batchIndex,1);
    const second={...ack,targetId:'second'};
    assert.equal(restarted.acknowledge(second,1301).status,'VERIFYING');
    const finished=restarted.verify(ack.releaseId,ack.generation,[{...second,observedAt:1401,healthy:true}],1401);
    assert.equal(finished.status,'SUCCEEDED');
    assert.equal(finished.verifiedBatches.length,2);
    assert.equal(restarted.start({...plan,releaseId:'release-2'}).generation,2);
    db.close();db=new DatabaseSync(path);
    const current=new ConfigRollouts(storage);
    const before=current.get(2);
    assert.throws(()=>current.acknowledge(ack,1402),{code:'STALE_CONFIG_ROLLOUT'});
    assert.deepEqual(current.get(2),before);
    const paused=restarted.pause('release-2',2);
    const rollback=restarted.start({...plan,releaseId:'rollback-3',rollback:{sourceGeneration:1,replacesGeneration:2}});
    assert.equal(rollback.generation,3);
    assert.deepEqual(restarted.get(1),finished);
    assert.deepEqual(restarted.get(2),paused);
    assert.throws(()=>restarted.resume('release-2',2,1500),{code:'STALE_CONFIG_ROLLOUT'});
  }finally{db.close();rmSync(dir,{recursive:true,force:true});}
});
