/** Browser contract test: all /v1 requests are intercepted; no production mutations. */
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mkdirSync,readFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';
const {chromium}=await import(pathToFileURL(process.env.PLAYWRIGHT_MODULE).href);
const browser=await chromium.launch({executablePath:process.env.CHROME_PATH,headless:true,args:['--no-sandbox','--no-proxy-server']});
const origin=process.env.ANSTS_CONSOLE_URL??'http://127.0.0.1:8088';
try {
 const page=await browser.newPage({viewport:{width:1440,height:1000}});const errors=[];
 page.on('pageerror',error=>errors.push(error.message));
 const counts={total:0,active:0,queued:0,completed:0,failed:0,cancelled:0,uncertain:0};
 let quotaVersions=[];let quotaConflict=true;
 let expired=false;
 let archives=[];let archiveVersion=2;let archiveConflict=true;let corruptArchive=false;
 const archiveSnapshot={schemaVersion:1,audit:[],receipts:{a:"x"}};
 let unavailable=true;let writes=0;let centerWrites=0;let role="SRE_ADMIN";
 let centers=[{id:"eng",name:"Engineering",status:"ACTIVE",version:1},{id:"research",name:"Research",status:"ACTIVE",version:1}];
 let items=[{id:'team-a',name:'Team A',region:'dev',costCenter:'eng',status:'ACTIVE',version:1,costCenterHistory:[{costCenter:'eng',version:1,timestamp:null}]}];
 await page.route('**/v1/**',async route=>{
  const request=route.request();const path=new URL(request.url()).pathname;
  const respond=(status,body)=>route.fulfill({status,contentType:'application/json',body:JSON.stringify(body)});
  if(request.headers().authorization!=='Bearer ui-only-test-token')return respond(401,{error:'USER_AUTH_REQUIRED'});
  if(expired&&path.startsWith('/v1/admin/'))return respond(401,{code:'USER_AUTH_REQUIRED'});
  if(path==='/v1/auth/me')return respond(200,{user:{sub:'github:1',role,scopeTenant:'all',scopeRegion:'all',canWrite:role!=="VIEWER"},expiresAt:Date.now()+300000});
  if(path==='/v1/auth/profile')return respond(503,{error:'PROFILE_UNAVAILABLE'});
  if(path==='/v1/admin/cost-centers'&&request.method()==='GET')return respond(200,{items:centers,version:centers.length,nextCursor:null});
  if(path==='/v1/admin/cost-centers'&&request.method()==='POST'){centerWrites++;const body=request.postDataJSON();const created={...body,version:1};centers.push(created);return respond(200,created);}
  if(path==='/v1/admin/cost-centers/ops'&&request.method()==='PUT'){centerWrites++;const body=request.postDataJSON();const updated={...body,id:'ops',version:2};centers=centers.map(c=>c.id==='ops'?updated:c);return respond(200,updated);}
  if(path==='/v1/admin/tenants/team-a/quota') {
   const template={schemaVersion:1,tenantId:'team-a',tenantVersion:1,region:'dev',costCenter:'eng',createdAt:'2026-11-15T00:00:00Z',actor:'github:1',resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:128,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'5',softThresholdBps:8000}};
   if(request.method()==='PUT') {const body=request.postDataJSON();assert.equal(typeof body.budget.limitCreditUnits,'string');
    if(quotaConflict){quotaConflict=false;quotaVersions=[{...template,version:1,reason:'External policy'}];return respond(409,{code:'VERSION_CONFLICT'});}
    const saved={...template,...body,version:2,reason:'Quota UI verified'};quotaVersions.push(saved);return respond(200,{policy:saved,enforcement:'NOT_CONNECTED'});
   }
   const next=new URL(request.url()).searchParams.has('cursor');
   return respond(200,{current:quotaVersions.at(-1)??null,history:next?quotaVersions.slice(1):quotaVersions.slice(0,1),nextCursor:!next&&quotaVersions.length>1?'quota-page-2':null,historyTotal:quotaVersions.length,enforcement:'NOT_CONNECTED',tenantVersion:1,tenantStatus:'ACTIVE'});
  }
  if(path==='/v1/admin/archives'&&request.method()==='GET')return respond(200,{items:archives,currentVersion:archiveVersion,receiptRetention:'ALL_RETAINED',safeReplayWatermark:null});
  if(path==='/v1/admin/archives'&&request.method()==='POST'){
   if(archiveConflict){archiveConflict=false;archiveVersion=3;return respond(409,{code:'VERSION_CONFLICT'});}
   const body=request.postDataJSON();const payload=JSON.stringify(archiveSnapshot);
   const manifest={id:'a'.repeat(64),schemaVersion:1,sourceVersion:body.expectedVersion,digest:createHash('sha256').update(payload).digest('hex'),bytes:Buffer.byteLength(payload),actor:'github:1',reason:body.reason,requestId:'archive-test',createdAt:'2026-11-15T00:00:00Z',tenantAuditWatermark:0,costCenterAuditWatermark:0,receiptRetention:'ALL_RETAINED',safeReplayWatermark:null};archives=[manifest];return respond(200,manifest);
  }
  if(path.startsWith('/v1/admin/archives/'))return respond(200,{manifest:archives[0],snapshot:corruptArchive?{...archiveSnapshot,receipts:{a:'y'}}:archiveSnapshot});
  if(path.startsWith('/v1/admin/operations/')) return respond(200,{operationId:decodeURIComponent(path.split('/').pop()),status:'SUCCEEDED',domain:'tenant',targetId:'team-a',resultVersion:1,requestId:'lookup-request',timestamp:'2026-11-15T00:00:00Z'});
  if(path==='/v1/admin/audit') {
   const query=new URL(request.url()).searchParams;
   const tenant={id:'tenant:team-a:1',domain:'tenant',targetId:'team-a',actor:'github:1',reason:'Registered team',requestId:'req-tenant',timestamp:'2026-11-15T00:00:00Z',previousVersion:0,version:1,changes:[{field:'name',before:null,after:'Team A'}]};
   const center={...tenant,id:'cost-center:eng:0',domain:'cost-center',targetId:'eng',reason:'Registered engineering',requestId:'req-center'};
   return respond(200,{items:query.get('domain')==='tenant'?[tenant]:query.get('domain')==='cost-center'||query.has('cursor')?[center]:[tenant],version:2,nextCursor:query.get('domain')==='all'&&!query.has('cursor')?'page-2':null});
  }
  if(path==='/v1/console/management')return respond(200,{counts,tasks:[],tenants:[],configuration:{},refreshedAt:Date.now()});
  if(path==='/v1/console/tasks')return respond(200,{counts,tasks:[],attention:[],total:0,refreshedAt:Date.now(),capabilities:{agentEnabled:false,sandboxImage:null}});
  if(path==='/v1/admin/tenants'&&request.method()==='GET')return unavailable?respond(503,{code:'DIRECTORY_UNAVAILABLE'}):respond(200,{items,version:1,nextCursor:null});
  if(path==='/v1/admin/tenants/team-a'&&request.method()==='PUT'){
   writes++;assert.ok(request.headers()['idempotency-key']);return respond(409,{code:'VERSION_CONFLICT'});
  }
  if(path==='/v1/admin/tenants'&&request.method()==='POST'){
   writes++;const body=request.postDataJSON();items=[...items,{...body,version:1}];return respond(200,items.at(-1));
  }
  return respond(404,{error:'NOT_FOUND'});
 });
 await page.goto(origin+'/manage/tenants');
 await page.getByText('使用短期开发凭据').click();
 await page.locator('input[type=password]').fill('ui-only-test-token');
 await page.getByRole('button',{name:'连接工作区'}).click();
 const panel=page.locator('section').filter({has:page.getByRole('heading',{name:'租户目录',exact:true})});
 await panel.getByRole('alert').waitFor();
 assert.equal(await panel.getByRole('button',{name:'登记租户'}).isDisabled(),true);
 unavailable=false;await panel.getByRole('button',{name:'刷新首页'}).click();
 const focused=async locator=>page.waitForFunction(element=>document.activeElement===element,await locator.elementHandle());
 await page.waitForFunction(()=>[...document.querySelectorAll('button')].some(button=>button.textContent==='登记租户'&&!button.disabled));
 const register=panel.getByRole('button',{name:'登记租户',exact:true});await register.focus();await register.press('Enter');
 const firstInput=panel.getByLabel('租户 ID',{exact:true});await firstInput.waitFor();await focused(firstInput);
 await firstInput.press('Tab');await focused(panel.getByLabel('名称',{exact:true}));
 await panel.getByLabel('名称',{exact:true}).press('Shift+Tab');await focused(firstInput);
 const cancel=panel.getByRole('button',{name:'取消编辑',exact:true});await cancel.focus();await cancel.press('Enter');await firstInput.waitFor({state:'hidden'});await focused(register);

 const centerPanel=page.locator('section').filter({has:page.getByRole('heading',{name:'成本中心',exact:true})});
 await centerPanel.getByRole('button',{name:'登记成本中心'}).click();
 await centerPanel.getByLabel('成本中心 ID',{exact:true}).fill('ops');
 await centerPanel.getByLabel('成本中心名称').fill('Operations');
 await centerPanel.getByLabel('成本中心变更原因').fill('register operations');
 await centerPanel.getByRole('button',{name:'确认保存成本中心',exact:true}).click();
 await centerPanel.getByRole('row').filter({hasText:'Operations'}).waitFor();
 await centerPanel.getByRole('row').filter({hasText:'Operations'}).getByRole('button',{name:'编辑成本中心'}).click();
 await centerPanel.getByLabel('成本中心状态').selectOption('DISABLED');
 await centerPanel.getByLabel('成本中心变更原因').fill('retire operations');
 await centerPanel.getByRole('button',{name:'确认保存成本中心',exact:true}).click();
 await centerPanel.getByRole('row').filter({hasText:'Operations'}).getByText('已停用',{exact:true}).waitFor();
 assert.equal(centerWrites,2);

 await panel.getByRole('button',{name:'载入版本编辑'}).click();
 await panel.getByLabel('名称',{exact:true}).fill('Preserved draft');
 await panel.getByLabel('变更原因').fill('review conflict');
 await panel.getByRole('button',{name:'确认保存'}).click();
 await panel.getByText(/版本或幂等键冲突/).waitFor();
 assert.equal(await panel.getByLabel('名称',{exact:true}).inputValue(),'Preserved draft');assert.equal(writes,1);
 await panel.getByRole('button',{name:'取消编辑'}).click();
 await panel.getByRole('button',{name:'登记租户'}).click();
 await panel.getByLabel('租户 ID',{exact:true}).fill('team-b');
 await panel.getByLabel('名称',{exact:true}).fill('Team B');
 await panel.getByLabel('地域',{exact:true}).fill('dev');
 await panel.getByRole('combobox',{name:'成本中心',exact:true}).selectOption('research');
 assert.equal(await panel.getByRole('combobox',{name:'成本中心',exact:true}).locator('option[value=ops]').count(),0);
 await panel.getByLabel('变更原因').fill('register team');
 await panel.getByRole('button',{name:'确认保存'}).click();
 await panel.getByRole('row').filter({hasText:'Team B'}).waitFor();assert.equal(writes,2);
 await panel.getByRole('row').filter({hasText:'Team A'}).getByRole('button',{name:'查看配额策略'}).click();
 const quotaPanel=page.locator('[aria-labelledby=tenant-quota-title]');await quotaPanel.getByRole('button',{name:'配置额度策略',exact:true}).click();
 for(const [name,value] of [['并发执行数','2'],['CPU（milli-core）','1000'],['内存（MiB）','512'],['队列任务数','10']])await quotaPanel.getByLabel(name,{exact:true}).fill(value);
 await quotaPanel.getByLabel('内部预算（credit整数单位）',{exact:true}).fill('9007199254740993');await quotaPanel.getByLabel('配额变更原因').fill('allocate');
 await quotaPanel.getByRole('button',{name:'确认保存策略',exact:true}).click();await quotaPanel.getByText(/版本冲突，输入已保留/).waitFor();assert.equal(await quotaPanel.getByLabel('内部预算（credit整数单位）',{exact:true}).inputValue(),'9007199254740993');
 await quotaPanel.getByRole('button',{name:'取消策略编辑'}).click();await quotaPanel.getByRole('button',{name:'刷新策略首页'}).click();await quotaPanel.getByText(/当前 v1/).waitFor();
 await quotaPanel.getByRole('button',{name:'编辑额度策略',exact:true}).click();await quotaPanel.getByLabel('内部预算（credit整数单位）',{exact:true}).fill('9007199254740993');await quotaPanel.getByLabel('配额变更原因').fill('Quota UI verified');await quotaPanel.getByRole('button',{name:'确认保存策略',exact:true}).click();await quotaPanel.getByText(/策略版本 2 已保存，尚未连接执行准入/).waitFor();
 await quotaPanel.getByRole('button',{name:'下一页策略历史'}).click();await quotaPanel.getByText('Quota UI verified',{exact:true}).waitFor();
 mkdirSync('docs/validation/admin-platform',{recursive:true});
 await page.screenshot({path:'docs/validation/admin-platform/tenant-directory.png'});
 await page.setViewportSize({width:390,height:844});
 assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
 await centerPanel.getByRole('row').filter({hasText:'Research'}).getByRole('button',{name:'编辑成本中心'}).click();
 await centerPanel.getByLabel('成本中心名称').waitFor();await centerPanel.getByRole('button',{name:'取消成本中心编辑'}).click();
 for(const nextRole of ['VIEWER','OPERATOR']) {
  role=nextRole;await page.reload();await page.getByText('使用短期开发凭据').click();await page.locator('input[type=password]').fill('ui-only-test-token');await page.getByRole('button',{name:'连接工作区'}).click();
  await page.getByRole('heading',{name:'租户目录',exact:true}).waitFor();
  assert.equal(await page.getByRole('button',{name:'登记租户',exact:true}).count(),0);
  assert.equal(await page.getByRole('button',{name:'登记成本中心',exact:true}).count(),0);
  await panel.getByRole('row').filter({hasText:'Team A'}).getByRole('button',{name:'查看配额策略'}).click();await quotaPanel.getByText(/当前 v2/).waitFor();assert.equal(await quotaPanel.getByRole('button',{name:'编辑额度策略',exact:true}).count(),0);
 }
 role='SRE_ADMIN';await page.goto(origin+'/manage/approvals');await page.getByText('使用短期开发凭据').click();await page.locator('input[type=password]').fill('ui-only-test-token');await page.getByRole('button',{name:'连接工作区'}).click();
 const auditPanel=page.locator('section').filter({has:page.getByRole('heading',{name:'管理变更审计',exact:true})});
 await auditPanel.getByLabel('操作 ID',{exact:true}).fill('tenant:'+ '1'.repeat(64));
 await auditPanel.getByRole('button',{name:'查询操作',exact:true}).click();
 await auditPanel.getByText(/请求 lookup-request/).waitFor();
 await auditPanel.getByText('Registered team',{exact:true}).waitFor();
 await auditPanel.getByRole('button',{name:'下一页审计'}).click();
 await auditPanel.getByText('Registered engineering',{exact:true}).waitFor();
 await auditPanel.getByRole('combobox',{name:'审计对象'}).selectOption('tenant');
 await auditPanel.getByText('Registered team',{exact:true}).waitFor();
 await auditPanel.getByText('变更差异与请求',{exact:true}).click();
 await auditPanel.getByText('请求 req-tenant',{exact:true}).waitFor();
 const archivePanel=page.locator('[aria-labelledby=directory-archives-title]');
 await archivePanel.getByLabel('归档原因').fill('Recovery checkpoint');
 await archivePanel.getByRole('button',{name:'创建检查点',exact:true}).click();
 await archivePanel.getByText(/源目录已变化或幂等键冲突/).waitFor();
 assert.equal(await archivePanel.getByLabel('归档原因').inputValue(),'Recovery checkpoint');
 await archivePanel.getByRole('button',{name:'刷新归档',exact:true}).click();
 await archivePanel.getByText(/源版本 3/).waitFor();
 await archivePanel.getByRole('button',{name:'创建检查点',exact:true}).click();
 await archivePanel.getByRole('button',{name:'校验并下载',exact:true}).waitFor();
 let downloads=0;page.on('download',()=>downloads++);
 const downloaded=page.waitForEvent('download');await archivePanel.getByRole('button',{name:'校验并下载',exact:true}).click();
 const download=await downloaded;assert.equal(download.suggestedFilename(),'directory-archive-'+ 'a'.repeat(64)+'.json');
 assert.deepEqual(JSON.parse(readFileSync(await download.path(),'utf8')).snapshot,archiveSnapshot);
 await archivePanel.getByText('摘要校验通过，归档文件已下载。',{exact:true}).waitFor();
 corruptArchive=true;await archivePanel.getByRole('button',{name:'校验并下载',exact:true}).click();
 await archivePanel.getByText('下载或摘要校验失败，未导出文件。',{exact:true}).waitFor();assert.equal(downloads,1);
 await page.screenshot({path:'docs/validation/admin-platform/management-audit.png'});
 expired=true;await auditPanel.getByRole('button',{name:'刷新审计首页',exact:true}).click();await page.getByText('使用短期开发凭据',{exact:true}).waitFor();
 assert.deepEqual(errors,[]);
 console.log('PASS: cost center create/disable, active-only assignment, bearer forwarding, unavailable state, retained CAS draft, tenant create, three-role UI matrix, audit paging/filter/details and operation lookup, archive CAS/verified download/corruption refusal, keyboard focus/Tab/restore, 401 session reset, mobile width; all APIs mocked.');
} finally {await browser.close();}
