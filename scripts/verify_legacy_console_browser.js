async(page)=>{
 const context=await page.context().browser().newContext();
 const tab=await context.newPage();
 let authed=false;
 const task={task_id:'va02-trace-fixture',tenant_id:'tenant',category:'completed',state:'COMPLETED',
  execution_kind:'trace',assigned_node_id:null,created_at:Date.now()/1000};
 await tab.route('**/v1/**',async route=>{
  const path=new URL(route.request().url()).pathname;
  if(path==='/v1/auth/me'){
   if(route.request().headers().authorization==='Bearer fixture')authed=true;
   if(!authed)return route.fulfill({status:401,json:{error:'USER_AUTH_REQUIRED'}});
   return route.fulfill({json:{user:{sub:'fixture',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all',canWrite:true},expiresAt:Date.now()+600000}});
  }
  if(path==='/v1/auth/profile')return route.fulfill({json:{profile:null}});
  if(path.startsWith('/v1/tasks/'))return route.fulfill({json:{task}});
  if(path==='/v1/console/system')return route.fulfill({json:{schemaVersion:1,observedAt:Date.now(),startedAt:Date.now(),
   refreshIntervalMs:15000,status:'unverified',nodes:[],edges:[],summary:{healthy:0,degraded:0,unavailable:0,unverified:0,
    probed:0,total:0,slots:null,availableSlots:null,capacityReporting:0,executors:0,fleetReady:0,fleetTotal:0},logicalServices:[],boundaries:[]}});
  return route.fulfill({json:{tasks:[task],attention:[],counts:{completed:1},total:1,refreshedAt:Date.now(),capabilities:{sandboxImage:null,agentEnabled:false}}});
 });
 try{
  await tab.goto('http://127.0.0.1:3197');
  await tab.getByRole('link',{name:'使用 GitHub 登录'}).waitFor({timeout:10000});
  await tab.getByText('使用短期开发凭据').press('Enter');
  await tab.locator('input[type=password]').fill('fixture');
  await tab.getByRole('button',{name:'连接工作区'}).click();
  await tab.getByRole('button',{name:'打开个人资料'}).press('Enter');
  await tab.getByRole('dialog',{name:'个人资料'}).waitFor();
  await tab.keyboard.press('Escape');
  await tab.getByRole('button',{name:/^va02-trace-fixture tenant$/}).press('Enter');
  await tab.getByRole('dialog',{name:'任务详情'}).waitFor();
  await tab.keyboard.press('Escape');
  await tab.goto('http://127.0.0.1:3197/system');
  await tab.getByRole('heading',{name:/系统拓扑/,level:1}).waitFor();
  await tab.getByRole('button',{name:'全景拓扑'}).waitFor();
  return 'PASS: login, profile, task detail and system topology';
 }finally{await context.close();}
}
