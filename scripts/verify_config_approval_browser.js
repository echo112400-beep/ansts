async (page)=>{
 for(const sub of ['author','reviewer']){
  const context=await page.context().browser().newContext();
  const tab=await context.newPage();
  let writes=0;
  const approval={sequence:1,status:'SUBMITTED',requestedBy:'author',decidedBy:null,basis:{revision:1}};
  await tab.route('**/v1/**',async route=>{
   const path=new URL(route.request().url()).pathname;
   let data={items:[],tasks:[],tenants:[],counts:{uncertain:0}};
   if(path==='/v1/auth/me')data={user:{sub,role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all',canWrite:true},expiresAt:Date.now()+600000};
   if(path.endsWith('/draft'))data=null;
   if(path.endsWith('/approvals/status'))data=approval;
   if(path.endsWith('/releases/status'))data=null;
   if(path.endsWith('/approvals')){
    writes++;
    if(sub==='author')throw new Error('Author attempted approval');
    const input=route.request().postDataJSON();
    if(input.revision!==1||input.sequence!==1||input.approved!==true)throw new Error('Incorrect approval basis');
    data={...approval,status:'APPROVED',decidedBy:sub};
   }
   await route.fulfill({json:data});
  });
  try{
   await tab.goto('http://127.0.0.1:3197/manage/configs');
   const approve=tab.getByRole('button',{name:'批准',exact:true});
   await approve.waitFor({timeout:10000});
   if(sub==='author'){
    await tab.getByText('提交人不能审批自己的草稿。').waitFor();
    if(!await approve.isDisabled()||writes)throw new Error('Self approval not disabled');
   }else{
    await approve.press('Enter');
    await tab.getByText('审批人 reviewer',{exact:false}).waitFor();
    if(writes!==1)throw new Error('Approval was not sent exactly once');
   }
  }finally{await context.close();}
 }
 return 'PASS: requester cannot approve, independent reviewer can approve the pinned revision';
}
