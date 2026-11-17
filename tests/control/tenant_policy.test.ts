import {test} from 'node:test';
import assert from 'node:assert/strict';
import {validateTenantQuotaPolicy,internalBudgetPeriod,type TenantQuotaPolicy} from '../../src/finops/tenant_policy.js';
const policy:TenantQuotaPolicy={schemaVersion:1,tenantId:'a',version:1,resources:{concurrentAttempts:1,cpuMilliCores:500,memoryMiB:128,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'9007199254740993',softThresholdBps:8000},createdAt:'2026-11-15T00:00:00Z',actor:'github:1',reason:'internal allocation'};
test('quota budget preserves integer credit amounts and validates every resource dimension',()=>{
 validateTenantQuotaPolicy(policy);
 assert.throws(()=>validateTenantQuotaPolicy({...policy,resources:{...policy.resources,cpuMilliCores:.5}}),{code:'INVALID_RESOURCE_QUOTA'});
 assert.throws(()=>validateTenantQuotaPolicy({...policy,budget:{...policy.budget,limitCreditUnits:'1.2'}}),{code:'INVALID_CREDIT_UNITS'});
 assert.throws(()=>validateTenantQuotaPolicy({...policy,budget:{...policy.budget,softThresholdBps:10001}}),{code:'INVALID_INTERNAL_BUDGET'});
 assert.throws(()=>validateTenantQuotaPolicy({...policy,version:0}),{code:'INVALID_TENANT_QUOTA_POLICY'});
});
test('budget windows are explicitly UTC across day, month and year boundaries',()=>{
 const before=Date.parse('2026-12-31T23:59:59Z'),after=before+1000;
 assert.equal(internalBudgetPeriod(before,'UTC_DAY'),'UTC_DAY:2026-12-31');
 assert.equal(internalBudgetPeriod(after,'UTC_DAY'),'UTC_DAY:2027-01-01');
 assert.equal(internalBudgetPeriod(after,'UTC_MONTH'),'UTC_MONTH:2027-01');
 assert.throws(()=>internalBudgetPeriod(NaN,'UTC_DAY'),{code:'INVALID_BUDGET_PERIOD'});
});
