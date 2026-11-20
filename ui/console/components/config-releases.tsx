"use client";
import { useEffect, useState, type FormEvent } from "react";
import type { User } from "../lib/console";

type Content={defaultTaskTimeoutSeconds?:number;unhealthyNodeThresholdBps?:number;modelCredentialRef?:string};
type Draft={revision:number;baseRevision:number;contentDigest:string;content:Content;rollback?:{sourceGeneration:number;replacesGeneration:number}};
type Approval={sequence:number;requestedBy:string;decidedBy:string|null;status:string;basis:{revision:number}};
type Evidence={targetId:string;actualDigest:string};
type Release={generation:number;batchIndex:number;status:string;pauseReason:string|null;pendingTargetIds?:string[];
  plan:{releaseId:string;revision:number;batches:string[][];healthWindowMs:number};
  acknowledgements:Evidence[];verifiedBatches:Evidence[][]};
type Impact={eligible:boolean;changes:{field:string;before:unknown;after:unknown;effect:string;risk:string;targetIds:string[]}[];
  blockers:{code:string;targetId?:string;field?:string}[]};

const labels:Record<string,string> = {
  defaultTaskTimeoutSeconds:"默认任务超时",unhealthyNodeThresholdBps:"不健康节点阈值",modelCredentialRef:"模型密钥引用",
  NEW_TASKS:"新任务",HOT_RELOAD:"热更新",RESTART_REQUIRED:"需要重启",SUBMITTED:"待独立审批",
  APPROVED:"已批准",REJECTED:"已拒绝",APPLYING:"正在应用",VERIFYING:"正在验证",PAUSED:"已暂停",SUCCEEDED:"已完成",
  CONFIG_OBSERVATION_MISSING:"缺少目标观测",CONFIG_OBSERVATION_STALE:"目标观测已过期",CONFIG_TARGET_UNHEALTHY:"目标不健康",
  CONFIG_RESTART_UNSUPPORTED:"当前不支持重启动作",CONFIG_FIELD_REMOVAL_UNSUPPORTED:"当前不支持直接移除字段",
  CONFIG_HEALTH_NOT_VERIFIED:"目标健康未通过验证",CONFIG_APPLY_INTERRUPTED:"目标失联或读回未确认，应用中断",
  INDEPENDENT_APPROVER_REQUIRED:"提交人不能审批自己的草稿",INDEPENDENT_APPROVER_UNAVAILABLE:"缺少第二位合格管理员",
};
const label=(key:string)=>labels[key]??key;
const value=(input:unknown)=>input==null?"未设置":String(input);

class ConfigError extends Error {
  constructor(readonly code:string,readonly status:number){super(code);}
}

export default function ConfigReleases({user,fetcher=fetch}:{user:User;fetcher?:typeof fetch}){
  const [scopeInput,setScopeInput]=useState("global"),[scope,setScope]=useState("global");
  const [draft,setDraft]=useState<Draft|null>(null),[approval,setApproval]=useState<Approval|null>(null);
  const [release,setRelease]=useState<Release|null>(null),[impact,setImpact]=useState<Impact|null>(null);
  const [timeout,setTimeoutValue]=useState(""),[threshold,setThreshold]=useState(""),[credential,setCredential]=useState("");
  const [base,setBase]=useState(0),[batch,setBatch]=useState("1"),[windowMs,setWindowMs]=useState("30000");
  const [sourceGeneration,setSourceGeneration]=useState(""),[busy,setBusy]=useState(false);
  const [error,setError]=useState(""),[notice,setNotice]=useState("");
  const canWrite=user.role==="SRE_ADMIN"&&user.canWrite&&user.scopeTenant==="all"&&user.scopeRegion==="all";
  const path="/v1/admin/config-bundles/"+encodeURIComponent(scope);
  async function request<T>(action:string,input?:object,allowMissing=false):Promise<T>{
    const result=await fetcher(path+"/"+action,{method:input?"POST":"GET",cache:"no-store",
      ...(input?{headers:{"content-type":"application/json"},body:JSON.stringify(input)}:{})});
    if(allowMissing&&result.status===404)return null as T;
    const data=await result.json();
    if(!result.ok)throw new ConfigError(data.code??data.error??"CONFIG_UNAVAILABLE",result.status);
    return data as T;
  }
  function useDraft(next:Draft|null){
    setDraft(next);setBase(next?.revision??0);setImpact(null);
    setTimeoutValue(String(next?.content.defaultTaskTimeoutSeconds??""));
    setThreshold(String(next?.content.unhealthyNodeThresholdBps??""));
    setCredential(next?.content.modelCredentialRef??"");
  }
  function displayError(cause:unknown){
    if(cause instanceof ConfigError){
      setError(cause.status===409?"版本或发布状态已变化，输入已保留。"+label(cause.code):label(cause.code));
    }else setError("配置服务暂时不可用。");
  }
  async function run(action:()=>Promise<void>){
    setBusy(true);setError("");setNotice("");
    try{await action();}catch(cause){displayError(cause);}finally{setBusy(false);}
  }
  /* The currently selected scope owns all loaded records. Scope changes discard old results. */
  useEffect(()=>{
    if(!canWrite)return;
    let active=true;
    setBusy(true);setDraft(null);setApproval(null);setRelease(null);setImpact(null);setError("");setNotice("");
    void Promise.all([request<Draft|null>("draft"),request<Approval|null>("approvals/status"),request<Release|null>("releases/status",undefined,true)])
      .then(([latest,decision,current])=>{if(active){useDraft(latest);setApproval(decision);setRelease(current);}})
      .catch(cause=>{if(active)displayError(cause);}).finally(()=>{if(active)setBusy(false);});
    return()=>{active=false;};
    // Request uses the selected scope and session fetcher.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  },[scope,canWrite,fetcher]);
  useEffect(()=>{
    if(busy||!release||!["APPLYING","VERIFYING"].includes(release.status))return;
    let active=true;
    const timer=setInterval(()=>{void request<Release>("releases/status").then(next=>{if(active)setRelease(next);})
      .catch(cause=>{if(active)displayError(cause);});},3000);
    return()=>{active=false;clearInterval(timer);};
    // eslint-disable-next-line react-hooks/exhaustive-deps
  },[scope,release?.generation,release?.status,fetcher,busy]);
  if(!canWrite)return <section className="admin-panel"><h2>配置发布</h2><p>此工作台需要全范围 SRE 管理员权限。</p></section>;
  function save(event:FormEvent){
    event.preventDefault();
    const content:Content={};
    if(timeout!=="")content.defaultTaskTimeoutSeconds=Number(timeout);
    if(threshold!=="")content.unhealthyNodeThresholdBps=Number(threshold);
    if(credential)content.modelCredentialRef=credential;
    void run(async()=>{useDraft(await request<Draft>("draft",{baseRevision:base,content}));setApproval(null);setNotice("草稿已保存。");});
  }
  const sameActor=approval?.requestedBy===user.sub;
  const maySubmit=impact&&(impact.eligible||draft?.rollback&&impact.blockers.every(item=>
    ["CONFIG_OBSERVATION_MISSING","CONFIG_OBSERVATION_STALE","CONFIG_TARGET_UNHEALTHY"].includes(item.code)));
  const verified=new Map(release?.verifiedBatches.flat().map(item=>[item.targetId,item.actualDigest]));
  const acknowledged=new Map(release?.acknowledgements.map(item=>[item.targetId,item.actualDigest]));
  return <section className="admin-panel management-directory-panel" aria-labelledby="config-release-title">
    <div className="admin-panel-heading"><h2 id="config-release-title">配置发布</h2><span>{scope}</span></div>
    <form className="directory-actions" onSubmit={event=>{event.preventDefault();setScope(scopeInput);}}>
      <label>作用域 <input required pattern="[a-z0-9][a-z0-9_-]{0,79}" value={scopeInput} onChange={event=>setScopeInput(event.target.value)}/></label>
      <button className="secondary" disabled={busy}>打开作用域</button>
      <button type="button" className="secondary" disabled={busy} onClick={()=>void run(async()=>{
        const [latest,decision,current]=await Promise.all([request<Draft|null>("draft"),request<Approval|null>("approvals/status"),request<Release|null>("releases/status",undefined,true)]);
        setDraft(latest);setApproval(decision);setRelease(current);setImpact(null);setNotice("状态已刷新，表单输入仍保留。");
      })}>刷新状态</button>
    </form>
    {error&&<div role="alert" className="banner error">{error}</div>}{notice&&<p role="status">{notice}</p>}
    <h3>配置草稿 {draft&&('v'+draft.revision)}</h3>
    <form className="create-form directory-editor" onSubmit={save}>
      <label>默认任务超时（秒）<input type="number" min="5" max="86400" value={timeout} onChange={event=>setTimeoutValue(event.target.value)}/></label>
      <label>不健康节点阈值（基点）<input type="number" min="0" max="10000" value={threshold} onChange={event=>setThreshold(event.target.value)}/></label>
      <label>模型密钥引用<select value={credential} onChange={event=>setCredential(event.target.value)}><option value="">未设置</option><option value="provider/default">provider/default</option></select></label>
      <div className="directory-actions">
        <button className="primary" disabled={busy||!(timeout||threshold||credential)}>保存草稿</button>
        {draft&&base!==draft.revision&&<button type="button" className="secondary" onClick={()=>setBase(draft.revision)}>以 v{draft.revision} 为基线保留输入</button>}
      </div>
    </form>
    {draft&&<div className="directory-actions">
      <button className="secondary" disabled={busy} onClick={()=>void run(async()=>setImpact(await request<Impact>("validate",{revision:draft.revision})))}>运行预检</button>
      <button className="primary" disabled={busy||!maySubmit||approval?.status==="SUBMITTED"||approval?.status==="APPROVED"} onClick={()=>void run(async()=>setApproval(await request<Approval>("submit",{revision:draft.revision})))}>提交审批</button>
    </div>}
    {impact&&<div>
      <p>{impact.eligible?"预检通过":maySubmit?"回退可提交审批，目标恢复仍需通过验证。":"预检存在阻断项"}</p>
      <div className="admin-table-scroll"><table><thead><tr><th>字段</th><th>原值</th><th>新值</th><th>生效范围</th><th>目标</th><th>风险</th></tr></thead>
      <tbody>{impact.changes.map(change=><tr key={change.field}><td>{label(change.field)}</td><td>{value(change.before)}</td><td>{value(change.after)}</td><td>{label(change.effect)}</td><td>{change.targetIds.join("、")}</td><td>{change.risk}</td></tr>)}</tbody></table></div>
      {!!impact.blockers.length&&<ul>{impact.blockers.map((item,index)=><li key={index}>{label(item.code)} {item.targetId??(item.field?label(item.field):"")}</li>)}</ul>}
    </div>}
    {approval&&<section aria-label="审批状态">
      <h3>审批 · {label(approval.status)}</h3><p>提交人 {approval.requestedBy}{approval.decidedBy&&(' · 审批人 '+approval.decidedBy)}</p>
      {approval.status==="SUBMITTED"&&<>
        {sameActor&&<p role="status">提交人不能审批自己的草稿。</p>}
        <div className="directory-actions">{[true,false].map(approved=><button key={String(approved)} className={approved?"primary":"secondary"} disabled={busy||sameActor}
          onClick={()=>void run(async()=>setApproval(await request<Approval>("approvals",{revision:approval.basis.revision,sequence:approval.sequence,approved})))}>{approved?"批准":"拒绝"}</button>)}</div>
      </>}
      {approval.status==="APPROVED"&&draft?.revision===approval.basis.revision&&<form className="directory-actions" onSubmit={event=>{
        event.preventDefault();void run(async()=>setRelease(await request<Release>("releases",{revision:draft.revision,releaseId:crypto.randomUUID(),batchSize:Number(batch),healthWindowMs:Number(windowMs)})));
      }}><label>批次大小 <input required type="number" min="1" max="24" value={batch} onChange={event=>setBatch(event.target.value)}/></label>
        <label>健康等待窗口（毫秒）<input required type="number" min="1" max="3600000" value={windowMs} onChange={event=>setWindowMs(event.target.value)}/></label>
        <button className="primary" disabled={busy}>开始分批发布</button></form>}
    </section>}
    {release&&<section aria-label="发布进度">
      <h3>发布进度 · {label(release.status)}</h3>
      {release.pauseReason&&<p role="status">{label(release.pauseReason)}</p>}
      {!!release.pendingTargetIds?.length&&<p>待验证目标：{release.pendingTargetIds.join("、")}</p>}
      <div className="admin-table-scroll"><table><thead><tr><th>批次</th><th>目标</th><th>状态</th><th>实际摘要</th></tr></thead>
      <tbody>{release.plan.batches.flatMap((targets,index)=>targets.map(id=><tr key={id}><td>{index+1}</td><td>{id}</td>
        <td>{verified.has(id)?"已验证":acknowledged.has(id)?"已读回，待验证":index===release.batchIndex&&release.status==="PAUSED"?"待恢复":"待应用"}</td>
        <td>{verified.get(id)??acknowledged.get(id)??"—"}</td></tr>))}</tbody></table></div>
      {release.status!=="SUCCEEDED"&&<button className="secondary" disabled={busy} onClick={()=>void run(async()=>{
        setRelease(await request<Release>(release.status==="PAUSED"?"releases/resume":"releases/pause",{releaseId:release.plan.releaseId,generation:release.generation}));
      })}>{release.status==="PAUSED"?"恢复发布":"暂停发布"}</button>}
      {draft&&release.generation>1&&["PAUSED","SUCCEEDED"].includes(release.status)&&<form className="directory-actions" onSubmit={event=>{
        event.preventDefault();void run(async()=>{useDraft(await request<Draft>("rollback",{sourceGeneration:Number(sourceGeneration),replacesGeneration:release.generation,baseRevision:draft.revision}));setApproval(null);setNotice("回退草稿已创建，需重新审批。");});
      }}><label>已验证的来源代际 <input required type="number" min="1" max={release.generation-1} value={sourceGeneration} onChange={event=>setSourceGeneration(event.target.value)}/></label>
        <button className="secondary" disabled={busy}>创建回退草稿</button></form>}
    </section>}
  </section>;
}
