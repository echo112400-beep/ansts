// Playwright CLI run-code fixture. Run against an isolated Next console on port 3197.
async (page) => {
  const assert = (value, message) => { if (!value) throw new Error(message); };
  const origin = 'http://127.0.0.1:3197';
  const gateway = null; // Native harness can substitute its isolated Gateway origin.
  const credentials = {};
  const resources = {cpuMilliCores:100,memoryMiB:128,slots:1};
  const verified = [];
  for (const role of ['VIEWER','OPERATOR','SRE_ADMIN']) {
    const context = await page.context().browser().newContext({viewport:{width:390,height:844}});
    const tab = await context.newPage();
    tab.setDefaultTimeout(5000);
    const errors = [];
    tab.on('pageerror', error => errors.push(error.message));
    let request = null;
    let enabled = true;
    await tab.route('**/v1/**', async route => {
      const url = new URL(route.request().url());
      const method = route.request().method();
      if(gateway && (url.pathname==='/v1/auth/me'||url.pathname.startsWith('/v1/admin/capacity/'))){
        const response=await tab.request.fetch(gateway+'/gateway-a'+url.pathname+url.search,{
          method,headers:{...route.request().headers(),authorization:credentials[role]},
          data:route.request().postData()??undefined,
        });
        await route.fulfill({response});
        return;
      }
      let data = {counts:{uncertain:0},tasks:[]};
      if (url.pathname === '/v1/auth/me') data = {user:{
        sub:'fixture',role,scopeTenant:role==='SRE_ADMIN'?'all':'team',scopeRegion:'all',canWrite:role!=='VIEWER',
      },expiresAt:Date.now()+600000};
      if (url.pathname.endsWith('/catalog')) data = {pools:[{
        id:'pool',name:'测试资源池',region:'dev',status:'ACTIVE',capacity:resources,available:resources,
        retainedReserved:resources,complete:false,allocationEnabled:enabled,borrowed:{cpuMilliCores:0,memoryMiB:0,slots:0},guaranteedShare:resources,activeGrants:0,hostCount:1,isolatedHostCount:0,
      }]};
      if (url.pathname.endsWith('/allocation-control')) {
        if(method==='PUT')enabled=route.request().postDataJSON().enabled;
        data={enabled,version:enabled?0:1};
      }
      if (url.pathname.endsWith('/requests')) {
        if(method==='POST')request={id:'fixture',tenantId:'team',resources,state:'QUEUED'};
        if(method==='DELETE')request={...request,state:'CANCELLED'};
        data=method==='GET'?{requests:request?[request]:[]}:request;
      }
      await route.fulfill({json:data});
    });
    await tab.goto(origin+'/manage/capacity');
    await tab.getByRole('heading',{name:'容量分配'}).waitFor();
    if(role==='SRE_ADMIN')await tab.getByLabel('租户',{exact:true}).fill('team');
    await tab.locator('select').filter({has:tab.locator('option[value=pool]')}).selectOption('pool');
    await tab.getByText('租户保障份额',{exact:true}).waitFor();
    for(const label of ['保留占用','容量报告','新授权状态']){
      await tab.getByText(label,{exact:true}).waitFor();
    }
    if(role!=='SRE_ADMIN')assert(await tab.getByRole('button',{name:'暂停新授权'}).count()===0,'Pool admin action visible to '+role);

    if(role==='OPERATOR'){
      await tab.getByLabel('CPU (mCPU)').fill('100');
      await tab.getByLabel('内存 (MiB)').fill('128');
      await tab.getByRole('button',{name:'提交容量申请'}).click();
      await tab.getByRole('button',{name:'取消',exact:true}).click();
      await tab.waitForFunction(()=>document.body.innerText.includes('已取消')).catch(async () => { throw new Error(errors.join(', ')+' '+await tab.locator('body').innerText()); });
    }else if(role==='SRE_ADMIN'){
      await tab.getByRole('button',{name:'暂停新授权'}).click();
      await tab.getByRole('button',{name:'恢复授权'}).click();
      await tab.getByRole('button',{name:'暂停新授权'}).waitFor();
    }else{
      assert(await tab.getByRole('button',{name:'提交容量申请'}).count()===0,'Viewer submit action is visible');
    }
    assert(await tab.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth),role+' page overflows');
    assert(errors.length===0,errors.join('\n'));
    await context.close();
    verified.push(role);
  }
  return verified.join(',');
}


