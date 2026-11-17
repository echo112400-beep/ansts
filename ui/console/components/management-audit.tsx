"use client";
import {useCallback,useEffect,useMemo,useRef,useState} from 'react';
import {controlClient,ControlApiError,type ControlPage,type ManagementAudit,type ControlOperation} from '../lib/control';
import DirectoryArchives from "./directory-archives";
import type {User} from '../lib/console';
export default function ManagementAuditPanel({user,fetcher=fetch}:{user:User;fetcher?:typeof fetch}) {
 const client=useMemo(()=>controlClient(fetcher),[fetcher]);
 const [page,setPage]=useState<ControlPage<ManagementAudit>|null>(null);
 const [operationId,setOperationId]=useState(''),[operation,setOperation]=useState<ControlOperation|null>(null),[operationError,setOperationError]=useState(''),[operationBusy,setOperationBusy]=useState(false);
 const [domain,setDomain]=useState('all'),[busy,setBusy]=useState(false),[error,setError]=useState('');
 const sequence=useRef(0),alive=useRef(true);
 const load=useCallback(async(cursor?:string)=>{
  const serial=++sequence.current;setBusy(true);setError('');
  try{const next=await client.audit({domain,cursor,limit:25});if(alive.current&&serial===sequence.current)setPage(next);}
  catch(e){if(alive.current&&serial===sequence.current)setError(e instanceof ControlApiError&&e.conflict?'审计数据已变化，请刷新首页。':'审计读取失败，保留上次数据。');}
  finally{if(alive.current&&serial===sequence.current)setBusy(false);}
 },[client,domain]);
 useEffect(()=>{alive.current=true;if(user.role==='SRE_ADMIN')void load();return()=>{alive.current=false;};},[load,user.role]);
 if(user.role!=='SRE_ADMIN')return <section className="admin-panel management-directory-panel"><h2>管理变更审计</h2><p>当前角色没有管理审计查询权限。</p></section>;
 return <><DirectoryArchives client={client} user={user}/><section className="admin-panel management-directory-panel" aria-labelledby="management-audit-title">
  <div className="admin-panel-heading"><h2 id="management-audit-title">管理变更审计</h2><span>租户与成本中心 · 持久记录</span></div>
  <form className="create-form directory-editor" onSubmit={async event=>{
    event.preventDefault();if(operationBusy)return;setOperationBusy(true);setOperation(null);setOperationError('');
    try {const result=await client.operation(operationId.trim());if(alive.current)setOperation(result);}
    catch(e){if(alive.current)setOperationError(e instanceof ControlApiError&&e.status===404?'未找到操作，或当前范围不可见。':'操作查询失败，请稍后重试。');}
    finally {if(alive.current)setOperationBusy(false);}
  }}>
   <label>操作 ID<input required maxLength={80} pattern="(tenant|cost-center):[a-f0-9]{64}" value={operationId} onChange={e=>setOperationId(e.target.value)} disabled={operationBusy}/></label>
   <button className="secondary" type="submit" disabled={operationBusy}>查询操作</button>
   {operationError&&<p role="alert">{operationError}</p>}
   {operation&&<p role="status" className="audit-record-detail">已提交 · {operation.targetId} · 版本 {operation.resultVersion} · 请求 {operation.requestId??'未知'} · {operation.timestamp??'时间未知'}</p>}
  </form>
  <div className="directory-actions"><label>审计对象 <select value={domain} disabled={busy} onChange={e=>{setPage(null);setDomain(e.target.value);}}><option value="all">全部对象</option><option value="tenant">租户</option><option value="cost-center">成本中心</option></select></label>
  <button className="secondary" disabled={busy} onClick={()=>void load()}>刷新审计首页</button>{page?.nextCursor&&<button className="secondary" disabled={busy} onClick={()=>void load(page.nextCursor!)}>下一页审计</button>}{busy&&<span role="status">正在读取…</span>}</div>
  {error&&<div className="banner error" role="alert">{error}</div>}
  {page&&<div className="admin-table-scroll"><table><thead><tr><th>时间</th><th>对象</th><th>操作者</th><th>版本</th><th>原因与证据</th></tr></thead><tbody>{page.items.map(entry=><tr key={entry.id}>
   <td>{new Date(entry.timestamp).toLocaleString('zh-CN')}</td><td>{entry.domain==='tenant'?'租户':'成本中心'}<br/><span className="mono">{entry.targetId}</span></td><td className="mono">{entry.actor}</td><td className="mono">{entry.previousVersion} → {entry.version}</td>
   <td><p className="audit-record-detail">{entry.reason}</p><details><summary>变更差异与请求</summary><p className="mono audit-record-detail">请求 {entry.requestId}</p><p className="mono audit-record-detail">记录 {entry.id}</p>{entry.changes?.length?<ul>{entry.changes.map(change=><li className="audit-record-detail" key={change.field}>{change.field}: {change.before??'不存在'} → {change.after}</li>)}</ul>:<p>旧记录未保存字段差异。</p>}</details></td>
  </tr>)}</tbody></table>{!page.items.length&&<div className="admin-empty">当前授权范围暂无此类管理变更。</div>}</div>}
 </section></>;
}
