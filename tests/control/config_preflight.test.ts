import assert from 'node:assert/strict';
import {test} from 'node:test';
import {assessConfigImpact,type ConfigTarget} from '../../src/config_release/config_preflight.js';

const targets:ConfigTarget[]=[
  {id:'scheduler-b',fields:['defaultTaskTimeoutSeconds']},
  {id:'scheduler-a',fields:['defaultTaskTimeoutSeconds','unhealthyNodeThresholdBps','modelCredentialRef']},
];
const observations=[{targetId:'scheduler-a',observedAt:950,healthy:true},{targetId:'scheduler-b',observedAt:960,healthy:true}];

test('preflight reports the actual affected targets in stable order and distinguishes application effects',()=>{
  const result=assessConfigImpact({defaultTaskTimeoutSeconds:60},{defaultTaskTimeoutSeconds:90,unhealthyNodeThresholdBps:500},targets,observations,1000,100);
  assert.equal(result.eligible,true);
  assert.deepEqual(result.changes.map(({field,effect,before,after,targetIds})=>({field,effect,before,after,targetIds})),[
    {field:'defaultTaskTimeoutSeconds',effect:'NEW_TASKS',before:60,after:90,targetIds:['scheduler-a','scheduler-b']},
    {field:'unhealthyNodeThresholdBps',effect:'HOT_RELOAD',before:null,after:500,targetIds:['scheduler-a']},
  ]);
});

test('preflight blocks unknown fields, restarts, removals and missing or stale observations',()=>{
  const invalid=assessConfigImpact(null,{databaseUrl:'private'},targets,observations,1000,100);
  assert.deepEqual(invalid.blockers,[{code:'INVALID_CONFIG_CONTENT'}]);
  const result=assessConfigImpact({defaultTaskTimeoutSeconds:60},{modelCredentialRef:'provider/default'},targets,
    [{targetId:'scheduler-a',observedAt:800,healthy:true}],1000,100);
  assert.equal(result.eligible,false);
  assert.deepEqual(result.blockers.map(item=>item.code),[
    'CONFIG_FIELD_REMOVAL_UNSUPPORTED','CONFIG_RESTART_UNSUPPORTED','CONFIG_OBSERVATION_STALE','CONFIG_OBSERVATION_MISSING',
  ]);
});
