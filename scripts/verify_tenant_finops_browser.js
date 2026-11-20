async (page)=>{
 const verified=[];
 for(const [width,role,emptyCosts=false] of [[390,'VIEWER'],[390,'OPERATOR'],[1280,'SRE_ADMIN'],[390,'VIEWER',true]]){
  const context=await page.context().browser().newContext({viewport:{width,height:900}});
  const tab=await context.newPage();
  const job='a'.repeat(64),actions=[];
  await tab.route('**/v1/**',async route=>{
   const path=new URL(route.request().url()).pathname;
   let data={items:[],tasks:[],tenants:[],counts:{uncertain:0}};
   if(path==='/v1/auth/me')data={user:{sub:'fixture',role,scopeTenant:'tenant',scopeRegion:'all',canWrite:role!=='VIEWER'},expiresAt:Date.now()+600000};
   if(path==='/v1/admin/tenants')data={items:[{id:'tenant',name:'Tenant fixture',version:1,status:'ACTIVE',region:'dev',costCenter:'center'}]};
   if(path.endsWith('/quota')){
    if(route.request().method()==='PUT')return route.fulfill({status:409,json:{code:'VERSION_CONFLICT'}});
    data={history:[],current:role==='SRE_ADMIN'?{version:1,tenantVersion:1,costCenter:'center',
     resources:{concurrentAttempts:2,cpuMilliCores:1000,memoryMiB:512,queuedTasks:3},
     budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000}}:null,tenantStatus:'ACTIVE'};
   }
   if(path.endsWith('/usage')){
    if(emptyCosts)return route.fulfill({status:503,json:{code:'UNAVAILABLE'}});
    data={events:[],watermark:1};
   }
   if(path.endsWith('/reservation-summary')){
    if(role!=='SRE_ADMIN'&&!emptyCosts)return route.fulfill({status:503,json:{code:'UNAVAILABLE'}});
    data={policyVersion:1,used:{slots:1,cpuMilliCores:500,memoryMiB:256}};
   }
   if(path.endsWith('/finops/summary'))data={knownSubtotalCreditUnits:'3',totalCreditUnits:null,unit:'credit-subunit',tenantCount:1,directoryWatermark:2,missingTenantIds:[],pendingTenantIds:['tenant'],costCenters:[{costCenter:'fixture-center',costCenterVersion:1,rateCardVersion:2,activationVersion:3,knownSubtotalCreditUnits:'3'}]};
   if(path.endsWith('/costs'))data={summary:{knownSubtotalCreditUnits:'3',totalCreditUnits:null,unit:'credit-subunit',reasons:['PRICE_SNAPSHOT_MISSING']},
    watermark:1,items:[{eventId:'fixture-event',entry:{periodId:'2026-11-20',pending:[],context:null,lines:[]}}]};
   if(emptyCosts&&path.endsWith('/costs'))data={summary:{knownSubtotalCreditUnits:'0',
    totalCreditUnits:'0',unit:'credit-subunit',reasons:[]},watermark:0,items:[]};
   if(path.endsWith('/cost-exports')){actions.push('create');return route.fulfill({status:202,json:{id:job,status:'PENDING'}});}
   if(path.endsWith('/run')){actions.push('run');data={id:job,status:'READY'};}
   if(path.endsWith('/download')){actions.push('download');return route.fulfill({contentType:'text/csv',body:'event_id\r\nfixture-event\r\n'});}
   await route.fulfill({json:data});
  });
  try{
   await tab.goto('http://127.0.0.1:3197/manage/tenants');
   await tab.getByText('待核算租户：tenant').waitFor();
   await tab.getByText('成本中心小计与版本',{exact:true}).press('Enter');
   await tab.getByRole('rowheader',{name:'fixture-center'}).waitFor({timeout:10000});

   await tab.getByRole('button',{name:'查看配额策略'}).click();
   if(emptyCosts){
    await tab.getByRole('alert').filter({hasText:'无法读取用量。'}).waitFor();
    await tab.getByText('尚无费用事件。',{exact:true}).waitFor();
    await tab.getByText('策略 v1 · 1 执行槽 · 500 mCPU · 256 MiB',{exact:true}).waitFor();
    if(await tab.getByText('尚无用量事件。',{exact:true}).count())throw new Error('Usage failure presented as empty');
    if(await tab.getByRole('button',{name:'导出费用 CSV'}).count())throw new Error('Viewer sees export creation');
    verified.push('empty costs and independent usage failure:390');
    continue;
   }
   await tab.getByText('尚无用量事件。').waitFor();
   if(role!=='SRE_ADMIN'){
    await tab.getByText('暂无可读的预留占用，可能尚未配置有效策略。').waitFor();
    if(await tab.getByRole('button',{name:'编辑额度策略'}).count())throw new Error('Non-admin sees quota edit');
   }else{
    await tab.getByRole('button',{name:'编辑额度策略'}).click();
    await tab.getByLabel('并发执行数').fill('0');
    await tab.getByLabel('配额变更原因').fill('保留输入验证');
    await tab.getByText('查看拟议变更',{exact:true}).press('Enter');
    await tab.getByRole('button',{name:'核对预留占用'}).click();
    await tab.getByText('拟议额度低于部分当前占用。',{exact:false}).waitFor();
    await tab.getByRole('button',{name:'确认保存策略'}).click();
    await tab.getByText('版本冲突，输入已保留。',{exact:false}).waitFor();
    if(await tab.getByLabel('配额变更原因').inputValue()!=='保留输入验证')throw new Error('Draft lost on conflict');
   }
   await tab.getByText('2026-11-20 · fixture-event',{exact:true}).press('Enter');
   await tab.getByText('成本中心：未知',{exact:false}).waitFor();
   if(role==='VIEWER'){
    if(await tab.getByRole('button',{name:'导出费用 CSV'}).count())throw new Error('Viewer sees export creation');
   }else{
   const downloaded=tab.waitForEvent('download');
   await tab.getByRole('button',{name:'导出费用 CSV'}).press('Enter');
   if((await downloaded).suggestedFilename()!=='cost-export.csv'||actions.join(',')!=='create,run,download')throw new Error('Unexpected export flow');
   }
   if(!await tab.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth))throw new Error('Page overflow at '+width);
   await tab.screenshot({path:'/tmp/ansts-va02-finops-'+role.toLowerCase()+'-'+width+'.png',fullPage:true});
   verified.push(role+':'+width);
  }finally{await context.close();}
 }
 return 'PASS: tenant finops browser '+verified.join(',');
}
