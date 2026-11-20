/** Read-only service acceptance, except rebuildable derived cost projections and a private local checkpoint. */
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,renameSync} from 'node:fs';
import {gatewayDevAuthorization} from './gateway_dev_identity.mjs';
const base='http://127.0.0.1:8088',mode=process.argv[2]??'capture',file='.dev-secrets/redeployment/finops-service-acceptance.json';
const {tenantId}=JSON.parse(readFileSync('.dev-secrets/redeployment/usage-acceptance.json','utf8'));assert.match(tenantId,/^m0-usage-[a-z0-9-]+$/);
const admin=gatewayDevAuthorization('all','SRE_ADMIN',undefined,'development-finops-verifier'),viewer=gatewayDevAuthorization(tenantId,'VIEWER');
async function call(path,auth=admin,origin=base){const response=await fetch(origin+path,{headers:{authorization:auth},signal:AbortSignal.timeout(15000),redirect:'error'});return {status:response.status,data:await response.json()};}
async function good(path,auth=admin,origin=base){const result=await call(path,auth,origin);assert.equal(result.status,200,path);return result.data;}
async function entries(origin=base){let cursor=null,items=[],summary;do{const page=await good('/v1/admin/tenants/'+tenantId+'/costs?limit=1'+(cursor?'&cursor='+encodeURIComponent(cursor):''),admin,origin);items.push(...page.items);summary=page.summary;cursor=page.nextCursor;}while(cursor);return {items,summary};}
const rates=await good('/v1/admin/finops/rate-cards'),audit=await good('/v1/admin/finops/rate-audit'),activation=await good('/v1/admin/tenants/'+tenantId+'/rate-card'),costs=await entries();
assert.equal(activation.current,null);assert.equal(costs.summary.totalCreditUnits,null);assert.equal(costs.summary.final,false);assert.ok(costs.summary.reasons.includes('PRICE_SNAPSHOT_MISSING'));assert.ok(costs.items.length>0);assert.ok(costs.items.every(row=>row.entry&&row.entry.context===null));
assert.equal((await call('/v1/admin/finops/rate-cards',viewer)).status,403);assert.equal((await call('/v1/admin/tenants/'+tenantId+'/costs',viewer)).status,200);
assert.ok([403,404].includes((await call('/v1/admin/tenants/'+tenantId+'/costs',gatewayDevAuthorization('other','VIEWER'))).status));
assert.equal((await fetch(base+'/v1/admin/finops/rate-cards')).status,401);
const current={rates,audit,activation,costs};
if(mode==='capture'){writeFileSync(file+'.tmp',JSON.stringify(current),{mode:0o600});renameSync(file+'.tmp',file);console.log('PASS: deployed rate history/audit/activation and signed cost paging enforce scope; retained real usage stays unpriced and pending. No rates, activations, tasks or provider calls created.');}
else if(mode==='verify-recovery'){assert.deepEqual(current,JSON.parse(readFileSync(file,'utf8')));for(const origin of ['http://127.0.0.1:18080','http://127.0.0.1:18081']){assert.deepEqual(await entries(origin),costs);assert.deepEqual(await good('/v1/admin/finops/rate-cards',admin,origin),rates);}console.log('PASS: actual restart retained exact derived entries, original received times, null historical prices and summary; both Gateways share rates and cost authority. No new execution or rate policy.');}
else throw new Error('Use capture or verify-recovery');
