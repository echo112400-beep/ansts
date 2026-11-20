async (page) => {
  const context=await page.context().browser().newContext({viewport:{width:390,height:844}});
  const tab=await context.newPage();
  const errors=[];
  tab.on('pageerror',error=>errors.push(error.message));
  let rows=[];
  await tab.route('**/v1/**',async route=>{
    const path=new URL(route.request().url()).pathname;
    let data={counts:{uncertain:0},tasks:[]};
    if(path==='/v1/auth/me')data={user:{sub:'fixture',role:'VIEWER',scopeTenant:'all',scopeRegion:'all',canWrite:false},expiresAt:Date.now()+600000};
    if(path==='/v1/admin/regions')data={regions:rows};
    if(path==='/v1/admin/regions/simulate'){
      const {input}=route.request().postDataJSON();
      data={selectedRegion:input.jurisdictions.length?null:'fixture-a',
        candidates:[{regionId:'fixture-a',reasons:input.jurisdictions.length?['JURISDICTION_NOT_ALLOWED']:[]}]};
    }
    await route.fulfill({json:data});
  });
  try{
    await tab.goto('http://127.0.0.1:3197/manage/federation');
    await tab.getByText('尚未登记地域。',{exact:true}).waitFor();
    rows=[{id:'fixture-a',jurisdiction:'DE',failureDomain:'domain-a',models:['fixture-model'],
      dataTypes:['internal'],trust:'CURRENT',evidence:{expiresAt:Date.now()+60000}}];
    await tab.reload();
    await tab.getByText('证据有效期：',{exact:false}).waitFor();
    await tab.getByLabel('司法辖区').fill('US');
    await tab.getByRole('button',{name:'对比策略'}).click();
    await tab.getByText('fixture-a：基线「可放置」，拟议「不符合驻留辖区」',{exact:true}).waitFor();
    await tab.getByText('跨地域灾备：未验收。',{exact:false}).waitFor();
    for(const text of ['RPO 目标','RPO 实测','RTO 目标','RTO 实测'])await tab.getByText(text,{exact:true}).waitFor();
    if(!await tab.evaluate(()=>document.documentElement.scrollWidth<=window.innerWidth))throw new Error('Page overflow');
    if(errors.length)throw new Error(errors.join('\n'));
    return 'PASS: region governance empty state, comparison, evidence and recovery status';
  }finally{await context.close();}
}
