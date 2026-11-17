/** Versioned internal quota policy. Runtime enforcement is integrated separately by QF-02/QF-08. */
import {ControlError,parseCreditUnits,validateQuota,type ResourceQuota} from '../control/contracts.js';
export interface InternalBudget {
 period:'UTC_DAY'|'UTC_MONTH';
 limitCreditUnits:string;
 softThresholdBps:number;
}
export interface TenantQuotaPolicy {
 schemaVersion:1;
 tenantId:string;
 version:number;
 resources:ResourceQuota;
 budget:InternalBudget;
 createdAt:string;
 actor:string;
 reason:string;
}
export function validateInternalBudget(budget:InternalBudget):void {
 if(!budget||!['UTC_DAY','UTC_MONTH'].includes(budget.period)||!Number.isSafeInteger(budget.softThresholdBps)||budget.softThresholdBps<0||budget.softThresholdBps>10000)throw new ControlError('INVALID_INTERNAL_BUDGET',422);
 parseCreditUnits(budget.limitCreditUnits);
}
export function validateTenantQuotaPolicy(policy:TenantQuotaPolicy):void {
 if(!policy||policy.schemaVersion!==1||typeof policy.tenantId!=='string'||!policy.tenantId.trim()||policy.tenantId.length>128||!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]*$/.test(policy.tenantId)||!Number.isSafeInteger(policy.version)||policy.version<1||typeof policy.actor!=='string'||!policy.actor.trim()||typeof policy.reason!=='string'||!policy.reason.trim()||policy.reason.length>1000||typeof policy.createdAt!=='string'||!Number.isFinite(Date.parse(policy.createdAt))||!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(policy.createdAt)||new Date(policy.createdAt).toISOString().replace('.000Z','Z')!==policy.createdAt.replace('.000Z','Z'))throw new ControlError('INVALID_TENANT_QUOTA_POLICY',422);
 validateQuota(policy.resources);validateInternalBudget(policy.budget);
}
export function internalBudgetPeriod(atMs:number,period:InternalBudget['period']):string {
 if(!Number.isSafeInteger(atMs)||!Number.isFinite(new Date(atMs).getTime())||!['UTC_DAY','UTC_MONTH'].includes(period))throw new ControlError('INVALID_BUDGET_PERIOD',422);
 const date=new Date(atMs);if(date.getUTCFullYear()<0||date.getUTCFullYear()>9999)throw new ControlError('INVALID_BUDGET_PERIOD',422);
 const iso=date.toISOString();
 return period+':'+iso.slice(0,period==='UTC_DAY'?10:7);
}
