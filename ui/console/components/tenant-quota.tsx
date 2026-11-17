"use client";
import {useCallback,useEffect,useRef,useState,type FormEvent} from 'react';
import {controlClient,ControlApiError,type DirectoryTenant,type TenantQuotaView,type TenantQuotaEdit} from '../lib/control';
import type {User} from '../lib/console';
const dimensions=[['concurrentAttempts','并发执行数'],['cpuMilliCores','CPU（milli-core）'],['memoryMiB','内存（MiB）'],['queuedTasks','队列任务数']] as const;
export default function TenantQuota({tenant,user,client}:{tenant:DirectoryTenant;user:User;client:ReturnType<typeof controlClient>}) {
 const [pageSize,setPageSize]=useState(10);
 const [data,setData]=useState<TenantQuotaView|null>(null),[busy,setBusy]=useState(false),[saving,setSaving]=useState(false),[error,setError]=useState(''),[notice,setNotice]=useState('');
 const [draft,setDraft]=useState<{expectedVersion:number;values:Record<string,string>;period:'UTC_DAY'|'UTC_MONTH';credits:string;bps:string;reason:string}|null>(null);
 const alive=useRef(true),sequence=useRef(0),receipt=useRef<{payload:string;key:string}|null>(null),form=useRef<HTMLFormElement>(null);
 const load=useCallback(async(cursor?:string)=>{
  const serial=++sequence.current;setBusy(true);setError('');
  try{const result=await client.quota(tenant.id,{cursor,limit:pageSize});if(alive.current&&serial===sequence.current)setData(result);}
  catch(e){if(alive.current&&serial===sequence.current)setError(e instanceof ControlApiError&&e.conflict?'策略已变化，请刷新首页后继续。':'配额策略读取失败，保留上次数据。');}
  finally{if(alive.current&&serial===sequence.current)setBusy(false);}
 },[client,tenant.id,tenant.version,pageSize]);
 useEffect(()=>{alive.current=true;setData(null);setDraft(null);setNotice('');void load();return()=>{alive.current=false;sequence.current++;};},[load]);
 function edit(){if(!data)return;const current=data.current;setDraft({expectedVersion:current?.version??0,values:Object.fromEntries(dimensions.map(([key])=>[key,current?String(current.resources[key]):''])),period:current?.budget.period??'UTC_DAY',credits:current?.budget.limitCreditUnits??'',bps:String(current?.budget.softThresholdBps??8000),reason:''});receipt.current=null;setNotice('');requestAnimationFrame(()=>form.current?.querySelector<HTMLInputElement>('input')?.focus());}
 async function save(event:FormEvent){
  event.preventDefault();if(!draft||saving)return;
  const resources=Object.fromEntries(dimensions.map(([key])=>[key,Number(draft.values[key])])) as TenantQuotaEdit['resources'];
  if(dimensions.some(([key])=>!draft.values[key]||!Number.isSafeInteger(resources[key])||resources[key]<0)||!Number.isSafeInteger(Number(draft.bps))||Number(draft.bps)<0||Number(draft.bps)>10000||!/^(0|[1-9][0-9]{0,77})$/.test(draft.credits)){setNotice('请检查整数额度、credit和阈值。');return;}
  const input:TenantQuotaEdit={expectedVersion:draft.expectedVersion,resources,budget:{period:draft.period,limitCreditUnits:draft.credits,softThresholdBps:Number(draft.bps)},reason:draft.reason};
  const payload=JSON.stringify(input);if(receipt.current?.payload!==payload)receipt.current={payload,key:crypto.randomUUID()};setSaving(true);setNotice('');
  try{const result=await client.saveQuota(tenant.id,input,receipt.current!.key);if(!alive.current)return;setNotice(`策略版本 ${result.policy.version} 已保存，尚未连接执行准入。`);await load();if(alive.current)setDraft(null);}
  catch(e){if(alive.current)setNotice(e instanceof ControlApiError&&e.conflict?'版本冲突，输入已保留。刷新并核对最新版本后重新编辑。':'保存失败，输入已保留，请检查权限或租户状态。');}
  finally{if(alive.current)setSaving(false);}
 }
 return <section className="admin-panel management-directory-panel" aria-labelledby="tenant-quota-title">
  <div className="admin-panel-heading"><h2 id="tenant-quota-title">配额策略 · {tenant.id}</h2><span>仅策略管理，执行准入未启用</span></div>
  <p className="audit-record-detail">credit为内部核算单位。这里展示额度，不代表已计量用量或已扣费。</p>
  <div className="directory-actions"><label>历史每页<select value={pageSize} disabled={busy||saving||!!draft} onChange={e=>setPageSize(Number(e.target.value))}>{[1,10,25,50].map(size=><option key={size} value={size}>{size} 版</option>)}</select></label><button className="secondary" disabled={busy||saving} onClick={()=>void load()}>刷新策略首页</button>{data?.nextCursor&&<button className="secondary" disabled={busy||saving} onClick={()=>void load(data.nextCursor!)}>下一页策略历史</button>}{user.role==='SRE_ADMIN'&&user.canWrite&&<button className="primary" disabled={!data||!!error||busy||saving||data.tenantStatus!=='ACTIVE'} onClick={edit}>{data?.current?'编辑额度策略':'配置额度策略'}</button>}</div>
  {error&&<div role="alert" className="banner error">{error}</div>}{notice&&<p role="status">{notice}</p>}
  {data&&!data.current&&<div className="admin-empty">尚未配置配额策略，未填充默认额度。</div>}
  {data?.current&&<p>当前 v{data.current.version} · 目录快照 v{data.current.tenantVersion} · 成本中心 {data.current.costCenter}</p>}
  {data&&data.history.length>0&&<div className="admin-table-scroll"><table><thead><tr><th>版本 / 时间</th><th>资源额度</th><th>预算</th><th>操作者 / 原因</th></tr></thead><tbody>{data.history.map(policy=><tr key={policy.version}><td>v{policy.version}<br/>{new Date(policy.createdAt).toLocaleString('zh-CN')}</td><td>{dimensions.map(([key,label])=><div key={key}>{label}: {policy.resources[key]}</div>)}</td><td>{policy.budget.limitCreditUnits} credit<br/>{policy.budget.period==='UTC_DAY'?'UTC日':'UTC月'} · 阈值 {policy.budget.softThresholdBps/100}%</td><td>{policy.actor}<p className="audit-record-detail">{policy.reason}</p></td></tr>)}</tbody></table></div>}
  {draft&&<form ref={form} className="create-form directory-editor" onSubmit={save}>
   <h3>编辑策略 · 基于版本 {draft.expectedVersion}</h3>
   {dimensions.map(([key,label])=><label key={key}>{label}<input type="number" min="0" step="1" required disabled={saving} value={draft.values[key]} onChange={e=>setDraft({...draft,values:{...draft.values,[key]:e.target.value}})}/></label>)}
   <label>内部预算（credit整数单位）<input required inputMode="numeric" pattern="0|[1-9][0-9]{0,77}" maxLength={78} disabled={saving} value={draft.credits} onChange={e=>setDraft({...draft,credits:e.target.value})}/></label>
   <label>预算窗口<select disabled={saving} value={draft.period} onChange={e=>setDraft({...draft,period:e.target.value as 'UTC_DAY'|'UTC_MONTH'})}><option value="UTC_DAY">UTC日</option><option value="UTC_MONTH">UTC月</option></select></label>
   <label>软阈值（基点，8000 = 80%）<input type="number" min="0" max="10000" step="1" required disabled={saving} value={draft.bps} onChange={e=>setDraft({...draft,bps:e.target.value})}/></label>
   <label>配额变更原因<textarea required maxLength={1000} disabled={saving} value={draft.reason} onChange={e=>setDraft({...draft,reason:e.target.value})}/></label>
   <div className="directory-actions"><button type="submit" className="primary" disabled={saving}>确认保存策略</button><button type="button" className="secondary" disabled={saving} onClick={()=>setDraft(null)}>取消策略编辑</button></div>
  </form>}
 </section>;
}
